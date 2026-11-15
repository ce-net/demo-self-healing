#!/bin/sh
# Three guestbook@1 replicas and the front on this machine, each replica restarted 2 s after it dies.
# Open http://127.0.0.1:8974/ and press the red button. Ctrl-C stops everything.
set -eu
cd "$(dirname "$0")"
D=${DATA:-./.run}
mkdir -p "$D"
[ -s "$D/secret" ] || (umask 077; python3 -c 'import secrets; print(secrets.token_hex(32))' > "$D/secret")
P="a=127.0.0.1:8971,b=127.0.0.1:8972,c=127.0.0.1:8973"
peers() { echo "$P" | tr , '\n' | grep -v "^$1=" | paste -sd, -; }
replica() {  # id port
  while :; do
    python3 guestbook/replica.py --id "$1" --listen "127.0.0.1:$2" --peers "$(peers "$1")" \
      --data "$D/$1" --secret-file "$D/secret" --allow-crash || true
    sleep 2
  done
}
trap 'kill 0' INT TERM EXIT
replica a 8971 & replica b 8972 & replica c 8973 &
python3 front/front.py --listen 127.0.0.1:8974 --secret-file "$D/secret" --data "$D/front" \
  --replica "a=127.0.0.1:8971=Replica A" --replica "b=127.0.0.1:8972=Replica B" --replica "c=127.0.0.1:8973=Replica C"
