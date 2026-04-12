# imap-cli — Python

## Requirements
- Python 3.9+
- `pip install imapclient`

## Install
```bash
pip install -r requirements.txt
chmod +x imap_cli.py
```

## Usage
```bash
# List mailboxes
python imap_cli.py --host imap.example.com --user me@example.com --password secret mailboxes

# Pipeline: mailboxes → messages → fetch
python imap_cli.py ... mailboxes \
  | python imap_cli.py ... messages \
  | python imap_cli.py ... fetch

# Single mailbox, limit 1000
python imap_cli.py ... messages --mailbox INBOX --limit 1000

# Fetch one message
python imap_cli.py ... fetch --mailbox INBOX --uid 42
```

## Benchmark
```bash
bash benchmark.sh --host imap.example.com --user me@example.com --password secret
```
