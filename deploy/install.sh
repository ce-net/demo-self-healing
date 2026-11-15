#!/bin/sh
# Install the code snapshot and the units on this machine. Then write the env files (see README) and start them.
set -eu
cd "$(dirname "$0")/.."
L=$HOME/.local/lib/demo-self-healing
mkdir -p "$L" "$HOME/.config/systemd/user" "$HOME/.config/demo-self-healing" "$HOME/.local/state/demo-self-healing"
cp -r guestbook front web "$L/"
cp deploy/guestbook-replica@.service deploy/self-healing-front.service "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
echo "installed to $L"
