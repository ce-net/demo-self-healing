# Self-healing: kill a node, keep the data

A live counter and a guestbook take writes from viewers. Three replicas hold every write. A big red button kills the
current leader process. The page goes amber for about a second, a new leader is elected, and every write that was
acknowledged is still there. The page then proves it from the raw data on all three replicas.

The public page goes live at ce-net.com/demos/self-healing after the next site deploy. Until then, the demo runs locally as described below.

## Run it in under 5 minutes

Needs Python 3.9+ and nothing else (stdlib only, no build).

```sh
git clone https://github.com/ce-net/demo-self-healing && cd demo-self-healing
./run-local.sh                 # three replicas + the front; each replica restarts 2 s after it dies
open http://127.0.0.1:8974/    # sign the guestbook, then press "Kill the leader"
```

Automated check (three failovers with writes in flight, about 60 s):

```sh
python3 tests/test_failover.py
```

```
kill 1: b (epoch 1) -> a (epoch 2) in 0.72 s
  proof pass=True acked=13 heads_equal=True at seq 15 missing=0; stale write epoch 1 refused by 2/2
kill 2: a (epoch 2) -> b (epoch 3) in 0.74 s
  proof pass=True acked=129 heads_equal=True at seq 132 missing=0; stale write epoch 2 refused by 2/2
kill 3: b (epoch 3) -> a (epoch 4) in 0.62 s
  proof pass=True acked=245 heads_equal=True at seq 250 missing=0; stale write epoch 3 refused by 2/2
PASS: 3 failovers, 355 acked writes, every one present on all three replicas
```

## What is proven, and how

| Claim | How the page checks it |
|---|---|
| No acknowledged write is lost | A write is acknowledged only after two of three replicas wrote it to disk (fsync). The front keeps every acknowledged `(seq, hash)`; after a failover each replica looks every one of them up in its own log. `missing` must be 0. |
| All replicas hold the same history | Each entry stores the hash of the entry before it. Every replica recomputes its chain from genesis (`chain_ok`), and the hash at one sequence number (the lowest commit of the three, since traffic keeps flowing) must be equal on all three. Equal hash at seq N means equal history up to N. |
| A replaced leader cannot write | One leader per epoch, elected by majority vote. The killed leader comes back, and the front asks it to replay a write at the epoch it used to lead. Both peers answer `refused: stale epoch` and change nothing. |
| The epoch goes up | Every election raises it; the page shows it and every event carries it. |

## Pieces (each one replaceable)

| Cell | Path | Job |
|---|---|---|
| `guestbook@1` | `guestbook/replica.py` | One replica: replicated log (leader election, majority commit, epoch fencing, hash chain), the counter and the guestbook, and a demo-only `Crash` op that ends this process and nothing else. |
| `demos.self-healing` | `front/front.py` | The page's server: forwards writes to the leader, watches the replicas and streams `died`, `elected`, `caught-up` events, sends `Crash` to the leader, runs the proof. |
| page | `web/` | Static HTML/JS/CSS; reads only the front's API. |

Replicas talk HTTP+JSON, every request HMAC-SHA256 signed with a shared secret. Timings: heartbeat 100 ms, election
timeout 500-800 ms; failover measured 0.6-0.9 s between two machines 37 ms apart.

## Deploying on your own machines

```sh
./deploy/install.sh   # on every machine: code to ~/.local/lib/demo-self-healing, systemd --user units
```

Put the same random secret in `~/.config/demo-self-healing/secret` (mode 600) on each machine, and per replica
`~/.config/demo-self-healing/replica-<id>.env`:

```
LISTEN=<this replica's address:port>
PEERS=<id>=<addr:port>,<id>=<addr:port>
```

Start `systemctl --user enable --now guestbook-replica@<id>`. On the front's machine write `front.env`
(`LISTEN`, and `R1`..`R3` as `id=addr:port=label`) and start `self-healing-front`. Then:

```sh
python3 tests/live_proof.py http://127.0.0.1:8974 3   # kills the leader 3 times and prints the proof
node tests/shot.mjs http://127.0.0.1:8974/ shots      # headless Chromium: before, electing (amber), after
```

## Limits

- The log is never compacted: about 250 bytes per write. Fine for a demo; a real cell would snapshot.
- Three replicas tolerate one failure. Two down means no majority: writes wait (never acknowledged), nothing is lost.
- Reads come from the leader without a lease check, so a just-deposed leader can serve a slightly old view for a
  moment. Writes are always fenced.

## License

Apache-2.0
