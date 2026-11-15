#!/usr/bin/env python3
"""Kill the leader of a running deployment N times and print the proof. python3 tests/live_proof.py http://127.0.0.1:8974 3"""
import json
import sys
import time
import urllib.request

URL, N = sys.argv[1].rstrip("/"), int(sys.argv[2]) if len(sys.argv) > 2 else 1


def get(p):
    with urllib.request.urlopen(URL + p, timeout=10) as r:
        return json.loads(r.read())


def post(p, o=None):
    req = urllib.request.Request(URL + p, data=json.dumps(o or {}).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def until(f, s, what):
    end = time.time() + s
    while time.time() < end:
        try:
            v = f()
        except OSError:
            v = None
        if v:
            return v
        time.sleep(0.05)
    raise SystemExit("FAIL: " + what)


ok = True
for k in range(N):
    until(lambda: (lambda s: s["leader"] and not s["killing"] and all(r["up"] for r in s["replicas"]))(get("/api/state")),
          60, "cluster not healthy")
    time.sleep(13)
    w = post("/api/sign", {"name": "live-proof", "msg": "written before kill %d" % (k + 1)})
    s0 = get("/api/state")
    t0 = time.time()
    r = post("/api/kill")
    if not r.get("ok"):
        raise SystemExit("kill refused: %s" % r)
    amber = until(lambda: (lambda s: s if not s["leader"] or s["epoch"] > s0["epoch"] else None)(get("/api/state")), 10, "x")
    s1 = until(lambda: (lambda s: s if s["leader"] and s["epoch"] > s0["epoch"] else None)(get("/api/state")), 15,
               "no new leader")
    dt = time.time() - t0
    p = until(lambda: (lambda s: s["proof"] if s["proof"] and s["proof"]["at"] > t0 * 1000 else None)(get("/api/state")),
              60, "no proof")
    st = p["stale"] or {}
    ref = [a["peer"] for a in st.get("answers", []) if a.get("refused")]
    print("kill %d: leader %s epoch %d killed; %s elected epoch %d after %.2f s | signed #%d before kill" %
          (k + 1, r["place"], s0["epoch"], next(x["place"] for x in s1["replicas"] if x["id"] == s1["leader"]),
           s1["epoch"], dt, w["seq"]))
    print("  proof %s: %d acked checked on each replica, %d missing, heads at seq %d equal=%s %s, chains ok=%s;"
          " stale epoch-%s write refused by %s" % ("PASS" if p["pass"] else "FAIL", p["acked"], p["missing"],
                                                   p["head_seq"], p["heads_equal"],
                                                   sorted({h[:12] for h in p["heads"].values()}), p["chains_ok"],
                                                   st.get("stale_epoch"), ref))
    ok = ok and p["pass"] and len(ref) == 2
print("RESULT", "PASS" if ok else "FAIL")
