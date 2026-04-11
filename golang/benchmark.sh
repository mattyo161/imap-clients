#!/usr/bin/env bash
set -euo pipefail
HOST="" USER="" PASS="" MAILBOX="INBOX"
while [[ $# -gt 0 ]]; do
  case $1 in
    --host) HOST="$2"; shift 2 ;;
    --user) USER="$2"; shift 2 ;;
    --password) PASS="$2"; shift 2 ;;
    --mailbox) MAILBOX="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -z $HOST || -z $USER || -z $PASS ]] && { echo "Usage: $0 --host H --user U --password P"; exit 1; }
CLI="./imap-cli --host $HOST --user $USER --password $PASS --no-cache"
for N in 1000 10000 100000; do
  echo -n "messages --limit $N: "
  START=$(date +%s%N)
  $CLI messages --mailbox "$MAILBOX" --limit "$N" | wc -l
  END=$(date +%s%N)
  echo "  $(( (END-START)/1000000 )) ms"
done
echo ""
echo "Full pipeline (mailboxes -> messages --limit 100 -> fetch):"
START=$(date +%s%N)
$CLI mailboxes | $CLI messages --limit 100 | $CLI fetch --no-attachments | wc -l
END=$(date +%s%N)
echo "  $(( (END-START)/1000000 )) ms"
