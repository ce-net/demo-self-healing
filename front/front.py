#!/usr/bin/env python3
"""self-healing demo front: the page's only server.

Serves the page, forwards viewers' writes to the guestbook@1 leader, keeps every acknowledged (seq, hash), watches
the replicas and streams died / elected / caught-up events (the orch@1 Watch event shape), sends the kill button's
Crash op to the current leader process, and runs the proof: every replica's chain checked from genesis, heads
compared, every acknowledged write looked up, and a stale-leader write replayed and refused.

Stdlib only. Listens on loopback; a reverse proxy (or the ce gate) puts it on the web.
"""
import argparse
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WEB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "web")
TYPES = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
         ".svg": "image/svg+xml", ".woff2": "font/woff2"}


def sign(secret, path, body):
    return hmac.new(secret, path.encode() + b"\n" + body, hashlib.sha256).hexdigest()


def now_ms():
    return int(time.time() * 1000)


class Front:
    def __init__(self, replicas, secret, data_dir, auto_rate):
        self.replicas = replicas  # [{id, addr, place}]
        self.secret, self.dir = secret, data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.status = {r["id"]: None for r in replicas}
        self.down_since = {}
        self.behind = set()
        self.events, self.ev_seq = [], 0
        self.leader, self.epoch = None, 0
        self.acked_path = os.path.join(data_dir, "acked.jsonl")
        self.acked = []
        if os.path.exists(self.acked_path):
            with open(self.acked_path) as f:
                for line in f:
                    try:
                        self.acked.append(tuple(json.loads(line)))
                    except ValueError:
                        pass
        self.acked_f = open(self.acked_path, "a")
        self.unacked = 0
        self.last_kill = 0.0
        self.killed = None  # {id, epoch, at}: the leader the button killed, until its stale write is refused
        self.proof = None
        self.stale = None
        self.last_view = 0.0
        self.auto_rate = auto_rate
        self.rate = {}

    def call(self, rid, path, obj, timeout=0.6):
        addr = next(r["addr"] for r in self.replicas if r["id"] == rid)
        body = json.dumps(obj).encode()
        req = urllib.request.Request("http://%s%s" % (addr, path), data=body, method="POST",
                                     headers={"Content-Type": "application/json",
                                              "X-GB-Sig": sign(self.secret, path, body)})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")

    def emit(self, kind, **kw):
        with self.lock:
            self.ev_seq += 1
            self.events.append(dict(kw, n=self.ev_seq, kind=kind, cell="guestbook@1", at=now_ms()))
            del self.events[:-300]
            self.cv.notify_all()

    def place(self, rid):
        return next(r["place"] for r in self.replicas if r["id"] == rid)

    # --- watch: poll every replica, turn changes into events ---------------------------------------------------
    def watch(self):
        while True:
            t0 = time.monotonic()
            got = {}

            def one(rid):
                try:
                    got[rid] = self.call(rid, "/status", {}, 0.4)
                except Exception:  # noqa: BLE001
                    got[rid] = None

            ts = [threading.Thread(target=one, args=(r["id"],)) for r in self.replicas]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            self._diff(got)
            time.sleep(max(0.05, 0.2 - (time.monotonic() - t0)))

    def _diff(self, got):
        events = []
        with self.lock:
            leaders = [s for s in got.values() if s and s["role"] == "leader"]
            top = max(leaders, key=lambda s: s["epoch"]) if leaders else None
            for rid, s in got.items():
                was = self.status[rid]
                if s is None and was is not None:
                    self.down_since[rid] = now_ms()
                    events.append(("died", {"holder": rid, "place": self.place(rid), "was": was["role"],
                                            "epoch": was["epoch"]}))
                    self.behind.add(rid)
                elif s is not None and was is None and rid in self.down_since:
                    events.append(("back", {"holder": rid, "place": self.place(rid),
                                            "down_ms": now_ms() - self.down_since.pop(rid)}))
                elif s is not None and was is not None and s["boot"] != was["boot"]:
                    events.append(("restarted", {"holder": rid, "place": self.place(rid)}))
                    self.behind.add(rid)
                self.status[rid] = s
            if top and (top["id"] != self.leader or top["epoch"] != self.epoch):
                events.append(("elected", {"holder": top["id"], "place": self.place(top["id"]), "epoch": top["epoch"],
                                           "prev_epoch": self.epoch, "prev": self.leader}))
                self.leader, self.epoch = top["id"], top["epoch"]
            elif not top and self.leader and got.get(self.leader) is None:
                self.leader = None
            if top:
                for rid in list(self.behind):
                    s = got.get(rid)
                    if s and s["epoch"] == top["epoch"] and s["commit"] >= top["commit"] - 2 and \
                            s["last_seq"] >= top["commit"]:
                        self.behind.discard(rid)
                        events.append(("caught-up", {"holder": rid, "place": self.place(rid), "seq": s["last_seq"],
                                                     "epoch": s["epoch"]}))
            killed = self.killed
        for k, kw in events:
            self.emit(k, **kw)
        if killed and any(k == "caught-up" and kw["holder"] == killed["id"] for k, kw in events):
            threading.Thread(target=self.after_failover, args=(killed,), daemon=True).start()

    def after_failover(self, killed):
        try:
            r = self.call(killed["id"], "/probe-stale", {}, 3)
        except Exception as ex:  # noqa: BLE001
            r = {"ok": False, "error": str(ex)[:80]}
        refused = [a for a in r.get("answers", []) if a.get("refused")]
        self.stale = dict(r, holder=killed["id"], at=now_ms())
        if r.get("ok"):
            self.emit("stale-refused", holder=killed["id"], place=self.place(killed["id"]), stale_epoch=r["stale_epoch"],
                      epoch=r["current_epoch"], refused_by=[a["peer"] for a in refused],
                      accepted_by=[a["peer"] for a in r["answers"] if a.get("ok")])
        with self.lock:
            self.killed = None
        self.run_proof()

    # --- writes --------------------------------------------------------------------------------------------------
    def write(self, kind, data):
        end = time.monotonic() + 4
        tried = None
        while time.monotonic() < end:
            with self.lock:
                rid = self.leader
            if not rid:
                time.sleep(0.05)
                continue
            try:
                r = self.call(rid, "/write", {"kind": kind, "data": data}, 3.5)
            except Exception:  # noqa: BLE001
                r = {"ok": False, "error": "unreachable"}
                if tried == rid:
                    with self.lock:
                        self.unacked += 1
            tried = rid
            if r.get("ok"):
                with self.lock:
                    self.acked.append((r["seq"], r["hash"]))
                    self.acked_f.write(json.dumps([r["seq"], r["hash"]]) + "\n")
                    self.acked_f.flush()
                return r
            if r.get("leader") and r["leader"] != rid:
                with self.lock:
                    self.leader = r["leader"]
            time.sleep(0.05)
        return {"ok": False, "error": "no leader within 4 s; not acknowledged"}

    def auto_traffic(self):
        """the counter keeps moving while someone watches, so a failover lands in the middle of writes"""
        while True:
            time.sleep(1.0 / self.auto_rate)
            if time.monotonic() - self.last_view < 60:
                self.write("count", {"by": "demo"})

    def kill(self):
        with self.lock:
            if time.monotonic() - self.last_kill < 12:
                return {"ok": False, "error": "a failover is still settling; try again in a few seconds",
                        "wait_s": round(12 - (time.monotonic() - self.last_kill), 1)}
            if self.killed:
                return {"ok": False, "error": "the last killed leader has not caught up yet"}
            rid, epoch = self.leader, self.epoch
            if not rid:
                return {"ok": False, "error": "no leader right now"}
            self.last_kill = time.monotonic()
        try:
            r = self.call(rid, "/crash", {}, 1.0)
        except Exception as ex:  # noqa: BLE001
            return {"ok": False, "error": "crash op failed: %s" % str(ex)[:60]}
        with self.lock:
            self.killed = {"id": rid, "epoch": epoch, "at": now_ms()}
        self.emit("kill", holder=rid, place=self.place(rid), epoch=epoch, pid=r.get("pid"))
        return {"ok": True, "killed": rid, "place": self.place(rid), "epoch": epoch}

    # --- proof ---------------------------------------------------------------------------------------------------
    def run_proof(self):
        with self.lock:
            st = [v for v in self.status.values() if v]
            floor = min((v["commit"] for v in st), default=0) if len(st) == len(self.replicas) else 0
            acked = [a for a in self.acked if a[0] <= floor]
            newer = len(self.acked) - len(acked)
        res = {}
        for r in self.replicas:
            try:
                res[r["id"]] = self.call(r["id"], "/verify", {"acked": acked}, 8)
            except Exception as ex:  # noqa: BLE001
                res[r["id"]] = {"id": r["id"], "error": str(ex)[:80]}
        ok = [v for v in res.values() if "error" not in v]
        # traffic keeps flowing, so replicas stand at slightly different seqs; a hash chain makes "same hash at one
        # seq" mean "same history up to that seq", so the heads are compared at the lowest commit of the three
        low = min((v["commit"] for v in ok), default=0)
        at_low = {v["id"]: self._hash_at(v["id"], low) for v in ok}
        p = {"at": now_ms(), "acked": len(acked), "acked_newer": newer, "replicas": res,
             "all_reachable": len(ok) == len(self.replicas),
             "chains_ok": bool(ok) and all(v["chain_ok"] for v in ok),
             "head_seq": low, "heads": at_low,
             "heads_equal": bool(ok) and None not in at_low.values() and len(set(at_low.values())) == 1,
             "missing": sum(v["missing_count"] for v in ok), "stale": self.stale}
        p["pass"] = p["all_reachable"] and p["chains_ok"] and p["heads_equal"] and p["missing"] == 0
        with self.lock:
            self.proof = p
        self.emit("proof", passed=p["pass"], acked=len(acked), head_seq=low, heads_equal=p["heads_equal"],
                  missing=p["missing"])
        return p

    def _hash_at(self, rid, seq):
        if seq == 0:
            return "0" * 64
        try:
            e = self.call(rid, "/entries", {"from": seq, "n": 1}, 2)["entries"]
            return e[0]["hash"] if e else None
        except Exception:  # noqa: BLE001
            return None

    def state(self):
        with self.lock:
            st = dict(self.status)
            lead = st.get(self.leader) if self.leader else None
            reps = []
            for r in self.replicas:
                s = st.get(r["id"])
                reps.append({"id": r["id"], "place": r["place"], "up": s is not None,
                             **({k: s[k] for k in ("role", "epoch", "last_seq", "commit", "head", "commit_hash",
                                                   "refused")} if s else {}),
                             "behind": r["id"] in self.behind})
            return {"leader": self.leader, "epoch": self.epoch, "replicas": reps,
                    "counter": lead["counter"] if lead else None, "guests": lead["guests"] if lead else [],
                    "acked": len(self.acked), "unacked": self.unacked, "last_event": self.ev_seq,
                    "proof": self.proof, "killing": bool(self.killed)}

    def events_after(self, n, wait_s):
        end = time.monotonic() + wait_s
        with self.lock:
            while self.ev_seq <= n and time.monotonic() < end:
                self.cv.wait(end - time.monotonic())
            return [e for e in self.events if e["n"] > n]

    def limited(self, ip, key, per_s, burst):
        t = time.monotonic()
        with self.lock:
            tokens, last = self.rate.get((ip, key), (burst, t))
            tokens = min(burst, tokens + (t - last) * per_s)
            if tokens < 1:
                self.rate[(ip, key)] = (tokens, t)
                return True
            self.rate[(ip, key)] = (tokens - 1, t)
            if len(self.rate) > 20000:
                self.rate.clear()
            return False


def clean(s, n):
    s = "".join(c for c in str(s or "") if c.isprintable()).strip()
    return s[:n]


def serve(front, listen, origins):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_a):
            pass

        def ip(self):
            return self.headers.get("X-Ce-Client-Ip") or self.headers.get("CF-Connecting-IP") or self.client_address[0]

        def _send(self, code, body, ctype="application/json", cache="no-store"):
            if not isinstance(body, bytes):
                body = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", cache)
            o = self.headers.get("Origin")
            if o and o in origins:
                self.send_header("Access-Control-Allow-Origin", o)
                self.send_header("Vary", "Origin")
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self):
            o = self.headers.get("Origin")
            self.send_response(204)
            if o in origins:
                self.send_header("Access-Control-Allow-Origin", o)
                self.send_header("Access-Control-Allow-Methods", "GET, POST")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Access-Control-Max-Age", "600")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            path, _, q = self.path.partition("?")
            if path == "/api/state":
                front.last_view = time.monotonic()
                return self._send(200, front.state())
            if path == "/api/events":
                front.last_view = time.monotonic()
                args = dict(p.split("=", 1) for p in q.split("&") if "=" in p)
                try:
                    n = int(args.get("after", "0"))
                except ValueError:
                    n = 0
                return self._send(200, {"events": front.events_after(n, 15)})
            name = "index.html" if path in ("/", "") else path.lstrip("/")
            if "/" in name or name.startswith(".") or os.path.splitext(name)[1] not in TYPES:
                return self._send(404, {"error": "not found"})
            fp = os.path.join(WEB, name)
            if not os.path.isfile(fp):
                return self._send(404, {"error": "not found"})
            with open(fp, "rb") as f:
                return self._send(200, f.read(), TYPES[os.path.splitext(name)[1]], "public, max-age=60")

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n > 4096:
                return self._send(413, {"error": "too big"})
            try:
                req = json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            ip = self.ip()
            if self.path == "/api/count":
                if front.limited(ip, "count", 5, 10):
                    return self._send(429, {"ok": False, "error": "slow down"})
                return self._send(200, front.write("count", {"by": "viewer"}))
            if self.path == "/api/sign":
                name, msg = clean(req.get("name"), 24) or "someone", clean(req.get("msg"), 120)
                if not msg:
                    return self._send(400, {"ok": False, "error": "write a message"})
                if front.limited(ip, "sign", 0.2, 3):
                    return self._send(429, {"ok": False, "error": "one message every few seconds"})
                return self._send(200, front.write("sign", {"name": name, "msg": msg}))
            if self.path == "/api/kill":
                if front.limited(ip, "kill", 0.05, 2):
                    return self._send(429, {"ok": False, "error": "you just did that"})
                return self._send(200, front.kill())
            if self.path == "/api/proof":
                if front.limited(ip, "proof", 0.2, 2):
                    return self._send(429, {"ok": False, "error": "slow down"})
                return self._send(200, front.run_proof())
            return self._send(404, {"error": "not found"})

    host, port = listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), H)
    srv.daemon_threads = True
    srv.serve_forever()


def main():
    ap = argparse.ArgumentParser(description="self-healing demo front")
    ap.add_argument("--listen", default="127.0.0.1:8974")
    ap.add_argument("--replica", action="append", required=True, help="id=host:port=place label (3 times)")
    ap.add_argument("--secret-file", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--auto-rate", type=float, default=2.0, help="demo writes per second while someone watches")
    ap.add_argument("--origin", action="append", default=[], help="page origin allowed to call the API (CORS)")
    a = ap.parse_args()
    with open(a.secret_file, "rb") as f:
        secret = f.read().strip()
    reps = []
    for spec in a.replica:
        rid, addr, place = spec.split("=", 2)
        reps.append({"id": rid, "addr": addr, "place": place})
    front = Front(reps, secret, a.data, a.auto_rate)
    for fn in (front.watch, front.auto_traffic):
        threading.Thread(target=fn, daemon=True).start()
    serve(front, a.listen, set(a.origin))


if __name__ == "__main__":
    main()
