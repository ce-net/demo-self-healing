#!/usr/bin/env python3
"""End to end on one machine: three replicas and the front, writes in flight, kill the leader three times.
Passes when every kill elects a new leader in a higher epoch, the killed leader's replayed write is refused by both
peers, and the proof finds equal chain heads and no acknowledged write missing. python3 tests/test_failover.py"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = int(os.environ.get("TEST_PORT", "18971"))


def get(path):
    with urllib.request.urlopen("http://127.0.0.1:%d%s" % (BASE + 3, path), timeout=10) as r:
        return json.loads(r.read())


def post(path, obj=None):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (BASE + 3, path), data=json.dumps(obj or {}).encode(),
                                 method="POST", headers={"Content-Type": "application/json",
                                                         "X-Ce-Client-Ip": "t%f" % time.time()})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def until(pred, secs, what):
    end = time.time() + secs
    while time.time() < end:
        try:
            v = pred()
        except OSError:
            v = None
        if v:
            return v
        time.sleep(0.1)
    raise SystemExit("FAIL: %s" % what)


def main():
    d = tempfile.mkdtemp(prefix="gb-test-")
    with open(os.path.join(d, "secret"), "w") as f:
        f.write("test-secret")
    ids = {"a": BASE, "b": BASE + 1, "c": BASE + 2}
    procs, stop = {}, threading.Event()

    def start(rid):
        peers = ",".join("%s=127.0.0.1:%d" % (k, p) for k, p in ids.items() if k != rid)
        procs[rid] = subprocess.Popen([sys.executable, os.path.join(ROOT, "guestbook/replica.py"), "--id", rid,
                                       "--listen", "127.0.0.1:%d" % ids[rid], "--peers", peers, "--data",
                                       os.path.join(d, rid), "--secret-file", os.path.join(d, "secret"),
                                       "--allow-crash"])

    def supervise():
        while not stop.is_set():
            for rid in ids:
                if procs[rid].poll() is not None:
                    time.sleep(1)
                    start(rid)
            time.sleep(0.2)

    for rid in ids:
        start(rid)
    fr = subprocess.Popen([sys.executable, os.path.join(ROOT, "front/front.py"), "--listen", "127.0.0.1:%d" % (BASE + 3),
                           "--secret-file", os.path.join(d, "secret"), "--data", os.path.join(d, "front"),
                           "--auto-rate", "10"] + sum((["--replica", "%s=127.0.0.1:%d=%s" % (k, p, k.upper())]
                                                       for k, p in ids.items()), []))
    threading.Thread(target=supervise, daemon=True).start()
    try:
        until(lambda: get("/api/state")["leader"], 10, "no first leader")
        for i in range(5):
            assert post("/api/sign", {"name": "test", "msg": "hello %d" % i})["ok"]
        for k in range(3):
            s0 = get("/api/state")
            t0 = time.time()
            r = post("/api/kill")
            assert r["ok"], r
            s1 = until(lambda: (lambda s: s if s["leader"] and s["epoch"] > s0["epoch"] else None)(get("/api/state")),
                       10, "no new leader")
            print("kill %d: %s (epoch %d) -> %s (epoch %d) in %.2f s" % (k + 1, r["killed"], s0["epoch"], s1["leader"],
                                                                        s1["epoch"], time.time() - t0))
            p = until(lambda: (lambda s: s["proof"] if s["proof"] and s["proof"]["at"] > t0 * 1000 else None)(
                get("/api/state")), 30, "no proof after failover")
            st = p["stale"]
            refused = [a for a in st["answers"] if a.get("refused")]
            print("  proof pass=%s acked=%d heads_equal=%s at seq %d missing=%d; stale write epoch %d refused by %d/2"
                  % (p["pass"], p["acked"], p["heads_equal"], p["head_seq"], p["missing"], st["stale_epoch"],
                     len(refused)))
            assert p["pass"], json.dumps(p)[:2000]
            assert len(refused) == 2, st
            time.sleep(12)
        ev = get("/api/events?after=0")["events"]
        kinds = [e["kind"] for e in ev]
        for k in ("kill", "died", "elected", "caught-up", "stale-refused", "proof"):
            assert k in kinds, (k, kinds)
        print("PASS: 3 failovers, %d acked writes, every one present on all three replicas" % get("/api/state")["acked"])
    finally:
        stop.set()
        fr.terminate()
        for p in procs.values():
            p.terminate()
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
