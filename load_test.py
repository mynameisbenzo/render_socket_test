"""
Simulates race traffic against the probe and prints numbers.

  python load_test.py https://your-probe.onrender.com            # 4 players, 20 Hz, 120 s
  python load_test.py http://127.0.0.1:5055 --seconds 20          # local smoke test
  python load_test.py https://your-probe.onrender.com --idle 600  # idle-hold test, 10 min
"""

import argparse
import statistics
import threading
import time

import requests
import socketio


def pct(values, p):
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def plain_rtt_ms(base, n=15):
    """Plain HTTP round trip to /health on a kept-alive connection."""
    session = requests.Session()
    t_wake = time.perf_counter()
    session.get(f"{base}/health", timeout=120)  # also wakes a sleeping free service
    print(f"first request took {(time.perf_counter() - t_wake) * 1000:.0f} ms (cold wake if the service was asleep)")
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        session.get(f"{base}/health", timeout=30)
        samples.append((time.perf_counter() - t0) * 1000)
        time.sleep(0.1)
    return statistics.median(samples)


class Player:
    def __init__(self, base, name):
        self.name = name
        self.base = base
        self.sio = socketio.Client(reconnection=False)
        self.rtts = []
        self.arrivals = []
        self.ack_timeouts = 0
        self.disconnects = 0
        self.drop_reason = None
        self.drop_at = None
        self.t0 = None
        self.transport = None
        self.sio.on("pos", self._on_pos)
        self.sio.on("disconnect", self._on_disconnect)

    def _on_pos(self, _data):
        self.arrivals.append(time.perf_counter())

    def _on_disconnect(self, reason=None):
        self.disconnects += 1
        self.drop_reason = reason
        if self.t0 is not None:
            self.drop_at = time.perf_counter() - self.t0

    def connect(self):
        self.sio.connect(self.base, transports=["websocket"], wait_timeout=90)
        self.transport = self.sio.transport()
        self.sio.emit("join", {"name": self.name})

    def run(self, seconds, hz=20):
        interval = 1.0 / hz
        next_at = time.perf_counter()
        self.t0 = next_at
        end = next_at + seconds
        i = 0
        while time.perf_counter() < end and self.sio.connected:
            t0 = time.perf_counter()
            try:
                self.sio.call("pos", {"n": self.name, "i": i, "x": 1.0, "y": 2.0}, timeout=5)
                self.rtts.append((time.perf_counter() - t0) * 1000)
            except socketio.exceptions.TimeoutError:
                self.ack_timeouts += 1
            i += 1
            next_at += interval
            time.sleep(max(0.0, next_at - time.perf_counter()))


def race(base, seconds, players=4, hz=20):
    base_rtt = plain_rtt_ms(base)
    print(f"plain HTTP round trip (median of 15): {base_rtt:.0f} ms")

    crowd = [Player(base, f"p{i}") for i in range(players)]
    for p in crowd:
        p.connect()
    print("transport:", {p.transport for p in crowd})
    time.sleep(1)

    started = time.perf_counter()
    threads = [threading.Thread(target=p.run, args=(seconds, hz)) for p in crowd]
    [t.start() for t in threads]
    [t.join() for t in threads]
    elapsed = time.perf_counter() - started
    time.sleep(1)

    all_rtts = [r for p in crowd for r in p.rtts]
    sent = sum(len(p.rtts) + p.ack_timeouts for p in crowd)
    expected_rx = sent * (players - 1)
    got_rx = sum(len(p.arrivals) for p in crowd)
    gaps = []
    for p in crowd:
        gaps += [(b - a) * 1000 for a, b in zip(p.arrivals, p.arrivals[1:])]

    p95 = pct(all_rtts, 0.95)
    over = p95 - base_rtt
    disconnects = sum(p.disconnects for p in crowd)
    print(f"\n{players} players, {seconds}s, {sent / elapsed:.0f} msgs/s in, {got_rx / elapsed:.0f} msgs/s relayed out")
    print(f"ack RTT ms   p50 {pct(all_rtts, .5):.0f}   p95 {p95:.0f}   p99 {pct(all_rtts, .99):.0f}   max {max(all_rtts):.0f}")
    print(f"relay gaps   p95 {pct(gaps, .95):.0f} ms   max {max(gaps):.0f} ms   (20 Hz = 50 ms ideal)")
    print(f"relayed      {got_rx}/{expected_rx} ({100 * got_rx / expected_rx:.1f}%)   ack timeouts {sum(p.ack_timeouts for p in crowd)}")
    print(f"disconnects  {disconnects}")
    for p in crowd:
        if p.drop_at is not None:
            print(f"  {p.name} dropped {p.drop_at:.1f}s into the run, client-side reason: {p.drop_reason}")
    ok_ws = all(p.transport == "websocket" for p in crowd)
    print("\nPASS/FAIL")
    print(f"  websocket transport .......... {'PASS' if ok_ws else 'FAIL'}")
    print(f"  p95 RTT within 100 ms of HTTP  {'PASS' if over <= 100 else 'FAIL'}  (p95 {p95:.0f} vs plain {base_rtt:.0f}, +{over:.0f})")
    print(f"  no disconnects ............... {'PASS' if disconnects == 0 else 'FAIL'}")
    for p in crowd:
        p.sio.disconnect()


def idle(base, seconds):
    """Connect 4 players, send nothing of our own, and see who is still there."""
    crowd = [Player(base, f"i{i}") for i in range(4)]
    for p in crowd:
        p.connect()
    print(f"4 idle clients connected via {crowd[0].transport}; holding {seconds}s ...")
    start = time.time()
    while time.time() - start < seconds:
        time.sleep(10)
        alive = sum(p.sio.connected for p in crowd)
        print(f"  t={int(time.time() - start):>4}s  connected {alive}/4", flush=True)
        if alive == 0:
            break
    lost = sum(p.disconnects for p in crowd)
    print(f"disconnects {lost}  -> idle hold {'PASS' if lost == 0 else 'FAIL'}")
    for p in crowd:
        if p.sio.connected:
            p.sio.disconnect()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("base")
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--idle", type=int, default=0, help="hold N idle seconds instead of racing")
    ap.add_argument("--players", type=int, default=4)
    ap.add_argument("--hz", type=int, default=20)
    args = ap.parse_args()
    base = args.base.rstrip("/")
    if args.idle:
        idle(base, args.idle)
    else:
        race(base, args.seconds, args.players, args.hz)