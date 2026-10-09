"""
Throwaway WebSocket probe for the Pixel Maker live race.

Not part of the real backend. It answers one question: does Flask-SocketIO
in threaded mode behind `gunicorn --worker-class gthread -w 1` hold up
WebSockets on Render's free tier at race-like traffic (4 players, 20 Hz)?
"""

import os
import resource
import threading
import time

from flask import Flask, jsonify
from flask_socketio import SocketIO, emit, join_room

app = Flask(__name__)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

ROOM = "probe"
STATS = {"started": time.time(), "pos_in": 0, "pos_out": 0, "connects": 0, "disconnects": 0}


@app.get("/health")
def health():
    return jsonify(ok=True)


@app.get("/stats")
def stats():
    uptime = time.time() - STATS["started"]
    return jsonify(uptime_s=round(uptime, 1), **STATS)


@socketio.on("connect")
def on_connect():
    STATS["connects"] += 1


@socketio.on("disconnect")
def on_disconnect(reason=None):
    STATS["disconnects"] += 1
    print(f"[disc] reason={reason} live={STATS['connects'] - STATS['disconnects']}", flush=True)


@socketio.on("join")
def on_join(data):
    join_room(ROOM)
    emit("joined", {"name": (data or {}).get("name", "?")})


@socketio.on("pos")
def on_pos(data):
    """A position update: relay to the other players, and ack the sender."""
    STATS["pos_in"] += 1
    emit("pos", data, to=ROOM, include_self=False)
    STATS["pos_out"] += 1
    return {"server_time": time.time()}  # the ack the sender times


def _monitor():
    """Every 5 s while there is traffic: CPU use, message rate, live sockets, memory."""
    last_wall, last_cpu, last_in = time.time(), time.process_time(), 0
    while True:
        time.sleep(5)
        wall, cpu, pos_in = time.time(), time.process_time(), STATS["pos_in"]
        span = wall - last_wall
        live = STATS["connects"] - STATS["disconnects"]
        if live > 0 or pos_in != last_in:
            peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            print(
                f"[mon] cpu={100 * (cpu - last_cpu) / span:.0f}% in={(pos_in - last_in) / span:.0f}/s "
                f"live={live} threads={threading.active_count()} peak_rss={peak_mb:.0f}MB",
                flush=True,
            )
        last_wall, last_cpu, last_in = wall, cpu, pos_in


threading.Thread(target=_monitor, daemon=True).start()


@app.get("/")
def phone_page():
    return PHONE_PAGE


PHONE_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Race probe</title>
<style>
 body{font:16px system-ui;margin:16px;background:#111;color:#eee}
 button{font:inherit;padding:10px 14px;margin:4px 4px 4px 0}
 pre{background:#000;padding:8px;height:42vh;overflow:auto;font-size:12px}
 b{color:#7fd}
</style></head><body>
<h3>Race probe</h3>
<div>transport: <b id="tr">-</b> &nbsp; state: <b id="st">-</b></div>
<div>RTT ms (last / p50 / p95 / max): <b id="rtt">-</b></div>
<div>relayed in: <b id="rx">0</b> &nbsp; disconnects: <b id="dc">0</b></div>
<div>
 <button id="go">Start sending 20 Hz</button>
 <button id="stop">Stop</button>
</div>
<p style="font-size:13px">Open this on 2-4 devices. To test backgrounding: tap
Start, switch apps for 15 s (then 60 s), come back, and read the log.</p>
<pre id="log"></pre>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
const $=id=>document.getElementById(id);
const log=m=>{const t=new Date().toISOString().slice(11,23);$('log').textContent=t+'  '+m+'\\n'+$('log').textContent;};
const name='p'+Math.floor(Math.random()*1000);
const s=io({transports:['websocket','polling']});
let rtts=[],rx=0,dc=0,seq=0,timer=null,hiddenAt=null;
const pct=(a,p)=>{if(!a.length)return 0;const b=[...a].sort((x,y)=>x-y);return b[Math.min(b.length-1,Math.floor(p*b.length))];};
s.on('connect',()=>{$('st').textContent='connected';$('tr').textContent=s.io.engine.transport.name;
 s.io.engine.on('upgrade',t=>{$('tr').textContent=t.name;log('transport upgraded to '+t.name);});
 s.emit('join',{name});log('connected as '+name+' via '+s.io.engine.transport.name);});
s.on('disconnect',r=>{dc++;$('dc').textContent=dc;$('st').textContent='disconnected';log('DISCONNECT: '+r);});
s.on('pos',()=>{rx++;$('rx').textContent=rx;});
$('go').onclick=()=>{if(timer)return;timer=setInterval(()=>{
  const t0=performance.now();
  s.timeout(5000).emit('pos',{n:name,i:seq++,x:Math.random()*800,y:Math.random()*600},(err)=>{
   if(err){log('ack timeout');return;}
   const r=performance.now()-t0;rtts.push(r);if(rtts.length>400)rtts.shift();
   $('rtt').textContent=[r,pct(rtts,.5),pct(rtts,.95),Math.max(...rtts)].map(v=>v.toFixed(0)).join(' / ');});
 },50);log('sending at 20 Hz');};
$('stop').onclick=()=>{clearInterval(timer);timer=null;log('stopped');};
document.addEventListener('visibilitychange',()=>{
 if(document.hidden){hiddenAt=Date.now();log('page hidden');}
 else{log('page visible after '+Math.round((Date.now()-hiddenAt)/1000)+' s; socket '+(s.connected?'still connected':'NOT connected'));}});
</script></body></html>"""


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5055)), allow_unsafe_werkzeug=True)