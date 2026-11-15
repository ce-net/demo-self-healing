#!/usr/bin/env python3
"""guestbook@1: one replica of a replicated guestbook and write counter.

Three (or any odd number of) replicas keep one append-only log. A write is acknowledged only after a majority has
written it to disk, so losing any minority of replicas loses no acknowledged write. One leader per epoch, elected by
majority vote; a replica refuses every message from an epoch older than its own (fencing), so a leader that was
replaced cannot write. Every entry carries a sequence number and the hash of the entry before it (a hash chain).

Peers and the demo front talk HTTP+JSON, every request signed with HMAC-SHA256 over a shared secret.
Stdlib only, Python 3.9+.
"""
import argparse
import hashlib
import hmac
import json
import os
import random
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

GENESIS = "0" * 64
HEARTBEAT_S = 0.1
ELECTION_MIN_S, ELECTION_MAX_S = 0.5, 0.8
BATCH = 500
MAX_GUESTS = 50


def now_ms():
    return int(time.time() * 1000)


def entry_hash(prev, seq, epoch, kind, data, ts):
    body = json.dumps({"seq": seq, "epoch": epoch, "kind": kind, "data": data, "ts": ts},
                      sort_keys=True, separators=(",", ":"))
    return hashlib.sha256((prev + "|" + body).encode()).hexdigest()


def sign(secret, path, body):
    return hmac.new(secret, path.encode() + b"\n" + body, hashlib.sha256).hexdigest()


def call(secret, addr, path, obj, timeout):
    body = json.dumps(obj).encode()
    req = urllib.request.Request("http://%s%s" % (addr, path), data=body, method="POST",
                                 headers={"Content-Type": "application/json", "X-GB-Sig": sign(secret, path, body)})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


class Replica:
    def __init__(self, rid, peers, data_dir, secret):
        self.id, self.peers, self.dir, self.secret = rid, peers, data_dir, secret
        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.boot = now_ms()
        self.meta_path = os.path.join(data_dir, "meta.json")
        self.log_path = os.path.join(data_dir, "log.jsonl")
        meta = {}
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                meta = json.load(f)
        self.epoch = meta.get("epoch", 0)
        self.voted_for = meta.get("voted_for")
        # the newest epoch this replica led: after a restart it is the "stale leader" the demo replays
        self.last_led_epoch = meta.get("last_led_epoch", 0)
        self.log = []
        if os.path.exists(self.log_path):
            with open(self.log_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        self.log.append(json.loads(line))
                    except ValueError:
                        break  # a torn last line from a crash mid-write: never acknowledged, drop it
            self._rewrite_log()
        self.role, self.leader = "follower", None
        self.commit = 0
        self.applied, self.counter, self.guests = 0, 0, []
        self.next_idx, self.match = {}, {}
        self.deadline = time.monotonic() + self._timeout()
        self.refused = 0
        self.log_f = open(self.log_path, "a")

    # --- persistence -------------------------------------------------------------------------------------------
    def _save_meta(self):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"epoch": self.epoch, "voted_for": self.voted_for, "last_led_epoch": self.last_led_epoch}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.meta_path)

    def _rewrite_log(self):
        tmp = self.log_path + ".tmp"
        with open(tmp, "w") as f:
            for e in self.log:
                f.write(json.dumps(e, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.log_path)

    def _append(self, entries):
        for e in entries:
            self.log.append(e)
            self.log_f.write(json.dumps(e, separators=(",", ":")) + "\n")
        self.log_f.flush()
        os.fsync(self.log_f.fileno())

    def _truncate(self, seq):
        """drop entries from seq on (they were never committed: a leader of a newer epoch disagrees)"""
        self.log = self.log[:seq - 1]
        self.log_f.close()
        self._rewrite_log()
        self.log_f = open(self.log_path, "a")
        if self.applied >= seq:
            self._reapply()

    # --- log helpers --------------------------------------------------------------------------------------------
    def last_seq(self):
        return len(self.log)

    def hash_at(self, seq):
        return GENESIS if seq == 0 else self.log[seq - 1]["hash"]

    def last_epoch(self):
        return self.log[-1]["epoch"] if self.log else 0

    def _reapply(self):
        self.applied, self.counter, self.guests = 0, 0, []
        self._apply()

    def _apply(self):
        while self.applied < self.commit:
            e = self.log[self.applied]
            self.applied += 1
            if e["kind"] == "count":
                self.counter += 1
            elif e["kind"] == "sign":
                self.guests.append({"seq": e["seq"], "name": e["data"].get("name", ""),
                                    "msg": e["data"].get("msg", ""), "ts": e["ts"]})
                del self.guests[:-MAX_GUESTS]

    def _new_entry(self, kind, data):
        seq, ts, prev = self.last_seq() + 1, now_ms(), self.hash_at(self.last_seq())
        return {"seq": seq, "epoch": self.epoch, "kind": kind, "data": data, "ts": ts, "prev": prev,
                "hash": entry_hash(prev, seq, self.epoch, kind, data, ts)}

    def _timeout(self):
        return random.uniform(ELECTION_MIN_S, ELECTION_MAX_S)

    def _majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _step_down(self, epoch, leader=None):
        if epoch > self.epoch:
            self.epoch, self.voted_for = epoch, None
            self._save_meta()
        self.role, self.leader = "follower", leader
        self.changed.notify_all()

    # --- RPC handlers (called with no lock held) -----------------------------------------------------------------
    def on_vote(self, req):
        with self.lock:
            if req["epoch"] < self.epoch:
                return {"granted": False, "epoch": self.epoch}
            if req["epoch"] > self.epoch:
                self._step_down(req["epoch"])
            up_to_date = (req["last_epoch"], req["last_seq"]) >= (self.last_epoch(), self.last_seq())
            if up_to_date and self.voted_for in (None, req["candidate"]):
                self.voted_for = req["candidate"]
                self._save_meta()
                self.deadline = time.monotonic() + self._timeout()
                return {"granted": True, "epoch": self.epoch}
            if not up_to_date:
                # the candidate cannot win against this log; this replica can, so it stands at once (always safe:
                # it is only an earlier election) and the candidate waits a round
                self.deadline = min(self.deadline, time.monotonic())
                return {"granted": False, "epoch": self.epoch, "behind": True}
            return {"granted": False, "epoch": self.epoch}

    def on_append(self, req):
        with self.lock:
            if req["epoch"] < self.epoch:
                self.refused += 1
                return {"ok": False, "refused": "stale epoch", "epoch": self.epoch, "from_epoch": req["epoch"]}
            if req["epoch"] > self.epoch or self.role != "follower" or self.leader != req["leader"]:
                self._step_down(req["epoch"], req["leader"])
            self.deadline = time.monotonic() + self._timeout()
            p = req["prev_seq"]
            if p > self.last_seq() or self.hash_at(p) != req["prev_hash"]:
                return {"ok": False, "epoch": self.epoch, "last_seq": min(self.last_seq(), max(p - 1, 0))}
            fresh, prev, broke = [], None, False
            for e in req["entries"]:
                s = e["seq"]
                if not fresh and s <= self.last_seq():
                    if self.log[s - 1]["hash"] == e["hash"]:
                        continue
                    self._truncate(s)
                want = prev if fresh else self.hash_at(s - 1)
                if s != (fresh[-1]["seq"] + 1 if fresh else self.last_seq() + 1) or e["prev"] != want or \
                        e["hash"] != entry_hash(e["prev"], s, e["epoch"], e["kind"], e["data"], e["ts"]):
                    broke = True
                    break  # keep the good prefix; the leader resends the rest
                fresh.append(e)
                prev = e["hash"]
            if fresh:
                self._append(fresh)
            if broke:
                return {"ok": False, "epoch": self.epoch, "last_seq": self.last_seq(), "bad_hash": True}
            match = p + len(req["entries"])
            self.commit = max(self.commit, min(req["commit"], match))
            self._apply()
            return {"ok": True, "epoch": self.epoch, "match": match}

    def on_write(self, req):
        kind, data = req.get("kind"), req.get("data") or {}
        if kind not in ("count", "sign"):
            return {"ok": False, "error": "unknown kind"}
        with self.lock:
            if self.role != "leader":
                return {"ok": False, "error": "not leader", "leader": self.leader, "epoch": self.epoch}
            e = self._new_entry(kind, data)
            self._append([e])
            self.match[self.id] = e["seq"]
            self.changed.notify_all()
            end = time.monotonic() + 3
            while self.commit < e["seq"]:
                left = end - time.monotonic()
                if left <= 0 or self.role != "leader" or self.last_seq() < e["seq"] or \
                        self.log[e["seq"] - 1]["hash"] != e["hash"]:
                    return {"ok": False, "error": "not committed", "seq": e["seq"]}
                self.changed.wait(min(left, 0.05))
            return {"ok": True, "seq": e["seq"], "hash": e["hash"], "epoch": e["epoch"], "leader": self.id}

    def status(self):
        with self.lock:
            return {"id": self.id, "role": self.role, "epoch": self.epoch, "leader": self.leader,
                    "last_seq": self.last_seq(), "head": self.hash_at(self.last_seq()), "commit": self.commit,
                    "commit_hash": self.hash_at(self.commit), "counter": self.counter, "guests": self.guests[-20:],
                    "last_led_epoch": self.last_led_epoch, "boot": self.boot, "refused": self.refused}

    def verify(self, req):
        """recompute the whole chain from genesis, then check the caller's acknowledged (seq, hash) pairs"""
        with self.lock:
            log, commit = list(self.log), self.commit
        prev, bad = GENESIS, None
        for e in log:
            if e["prev"] != prev or e["hash"] != entry_hash(prev, e["seq"], e["epoch"], e["kind"], e["data"], e["ts"]):
                bad = e["seq"]
                break
            prev = e["hash"]
        missing = [s for s, h in req.get("acked", []) if s > len(log) or log[s - 1]["hash"] != h]
        epochs = sorted({e["epoch"] for e in log})
        return {"id": self.id, "chain_ok": bad is None, "first_bad": bad, "last_seq": len(log), "commit": commit,
                "head": log[-1]["hash"] if log else GENESIS, "commit_hash": log[commit - 1]["hash"] if commit else GENESIS,
                "missing": missing[:50], "missing_count": len(missing), "checked": len(req.get("acked", [])),
                "epochs": epochs[-20:]}

    def entries(self, req):
        a = max(1, int(req.get("from", 1)))
        with self.lock:
            return {"entries": self.log[a - 1:a - 1 + min(int(req.get("n", 100)), 1000)], "last_seq": self.last_seq()}

    def probe_stale(self):
        """Replay a write as the leader this replica was before it was killed: its old epoch, its old head. Every peer
        must refuse it. Nothing is changed on any replica."""
        with self.lock:
            old = self.last_led_epoch
            ok_epoch = old and old < self.epoch
            e = {"seq": self.last_seq() + 1, "epoch": old, "kind": "count", "data": {"by": "stale-leader-probe"},
                 "ts": now_ms(), "prev": self.hash_at(self.last_seq())}
            e["hash"] = entry_hash(e["prev"], e["seq"], old, e["kind"], e["data"], e["ts"])
            req = {"epoch": old, "leader": self.id, "prev_seq": self.last_seq(), "prev_hash": self.hash_at(self.last_seq()),
                   "entries": [e], "commit": 0}
            cur = self.epoch
        if not ok_epoch:
            return {"ok": False, "error": "this replica has not led an older epoch", "epoch": cur, "last_led": old}
        out = []
        for pid, addr in self.peers.items():
            try:
                out.append(dict(call(self.secret, addr, "/append", req, 1.0), peer=pid))
            except Exception as ex:  # noqa: BLE001
                out.append({"peer": pid, "error": str(ex)[:80]})
        return {"ok": True, "stale_epoch": old, "current_epoch": cur, "answers": out}

    # --- background ----------------------------------------------------------------------------------------------
    def run(self):
        for pid in self.peers:
            threading.Thread(target=self._replicate, args=(pid,), daemon=True).start()
        while True:
            time.sleep(0.02)
            with self.lock:
                if self.role != "leader" and time.monotonic() > self.deadline:
                    self._start_election()

    def _start_election(self):
        self.epoch += 1
        self.role, self.voted_for, self.leader = "candidate", self.id, None
        self._save_meta()
        self.deadline = time.monotonic() + self._timeout()
        epoch = self.epoch
        req = {"epoch": epoch, "candidate": self.id, "last_seq": self.last_seq(), "last_epoch": self.last_epoch()}
        votes = [1]

        def ask(addr):
            try:
                r = call(self.secret, addr, "/vote", req, 0.5)
            except Exception:  # noqa: BLE001
                return
            with self.lock:
                if r["epoch"] > self.epoch:
                    self._step_down(r["epoch"])
                elif r.get("behind") and self.role == "candidate" and self.epoch == epoch:
                    self.deadline = time.monotonic() + ELECTION_MAX_S + self._timeout()
                elif r.get("granted") and self.role == "candidate" and self.epoch == epoch:
                    votes[0] += 1
                    if votes[0] >= self._majority():
                        self._become_leader()

        for addr in self.peers.values():
            threading.Thread(target=ask, args=(addr,), daemon=True).start()

    def _become_leader(self):
        self.role, self.leader = "leader", self.id
        self.last_led_epoch = self.epoch
        self._save_meta()
        for pid in self.peers:
            self.next_idx[pid], self.match[pid] = self.last_seq() + 1, 0
        # a no-op of the new epoch: entries of older epochs commit only with it (Raft 5.4.2)
        e = self._new_entry("elected", {"leader": self.id})
        self._append([e])
        self.match[self.id] = e["seq"]
        self.changed.notify_all()

    def _replicate(self, pid):
        addr = self.peers[pid]
        while True:
            with self.lock:
                if self.role != "leader":
                    self.changed.wait(0.2)
                    continue
                epoch, ni = self.epoch, self.next_idx.get(pid, self.last_seq() + 1)
                ni = max(1, min(ni, self.last_seq() + 1))
                req = {"epoch": epoch, "leader": self.id, "prev_seq": ni - 1, "prev_hash": self.hash_at(ni - 1),
                       "entries": self.log[ni - 1:ni - 1 + BATCH], "commit": self.commit}
            try:
                r = call(self.secret, addr, "/append", req, 1.0)
            except Exception:  # noqa: BLE001
                time.sleep(HEARTBEAT_S)
                continue
            with self.lock:
                if r.get("epoch", 0) > self.epoch:
                    self._step_down(r["epoch"])
                    continue
                if self.role != "leader" or self.epoch != epoch:
                    continue
                if r.get("ok"):
                    self.match[pid] = max(self.match.get(pid, 0), r["match"])
                    self.next_idx[pid] = r["match"] + 1
                    self._advance_commit()
                else:
                    self.next_idx[pid] = max(1, min(ni - 1, r.get("last_seq", ni - 1) + 1))
                more = self.next_idx[pid] <= self.last_seq()
                if not more:
                    self.changed.wait(HEARTBEAT_S)

    def _advance_commit(self):
        self.match[self.id] = self.last_seq()
        ms = sorted(self.match.get(p, 0) for p in [self.id, *self.peers])
        n = ms[len(ms) - self._majority()]
        if n > self.commit and self.log[n - 1]["epoch"] == self.epoch:
            self.commit = n
            self._apply()
            self.changed.notify_all()


def serve(rep, listen, crash_ok):
    secret = rep.secret
    routes = {"/vote": rep.on_vote, "/append": rep.on_append, "/write": rep.on_write,
              "/status": lambda _r: rep.status(), "/verify": rep.verify, "/entries": rep.entries,
              "/probe-stale": lambda _r: rep.probe_stale()}

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_a):
            pass

        def _send(self, code, obj):
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n > 8 << 20:
                return self._send(413, {"error": "too big"})
            body = self.rfile.read(n)
            if not hmac.compare_digest(self.headers.get("X-GB-Sig", ""), sign(secret, self.path, body)):
                return self._send(403, {"error": "bad signature"})
            try:
                req = json.loads(body or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            if self.path == "/crash":
                if not crash_ok:
                    return self._send(403, {"error": "crash is off"})
                self._send(200, {"ok": True, "id": rep.id, "pid": os.getpid()})
                self.wfile.flush()
                # the demo's kill button: this process and nothing else ends, mid-flight, as a real crash would
                threading.Timer(0.02, lambda: os._exit(70)).start()
                return
            fn = routes.get(self.path)
            if not fn:
                return self._send(404, {"error": "no such op"})
            try:
                return self._send(200, fn(req))
            except (KeyError, TypeError, ValueError) as ex:
                return self._send(400, {"error": "bad request: %s" % ex})

    host, port = listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), H)
    srv.daemon_threads = True
    srv.serve_forever()


def main():
    ap = argparse.ArgumentParser(description="guestbook@1 replica")
    ap.add_argument("--id", required=True)
    ap.add_argument("--listen", required=True, help="host:port")
    ap.add_argument("--peers", required=True, help="id=host:port,id=host:port")
    ap.add_argument("--data", required=True)
    ap.add_argument("--secret-file", required=True)
    ap.add_argument("--allow-crash", action="store_true", help="answer the demo's Crash op (ends this process)")
    a = ap.parse_args()
    with open(a.secret_file, "rb") as f:
        secret = f.read().strip()
    peers = dict(p.split("=", 1) for p in a.peers.split(",") if p)
    rep = Replica(a.id, peers, a.data, secret)
    threading.Thread(target=serve, args=(rep, a.listen, a.allow_crash), daemon=True).start()
    rep.run()


if __name__ == "__main__":
    main()
