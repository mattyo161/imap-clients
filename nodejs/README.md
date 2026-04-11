# imap-cli — Node.js

## Requirements
- Node.js 18+
- npm

## Install
```bash
npm install
chmod +x src/index.js
```

## Usage
```bash
node src/index.js --host imap.example.com --user me@example.com --password secret mailboxes

# Pipeline
node src/index.js ... mailboxes \
  | node src/index.js ... messages \
  | node src/index.js ... fetch

# Single mailbox
node src/index.js ... messages --mailbox INBOX --limit 1000
```

## Benchmark
```bash
bash benchmark.sh --host imap.example.com --user me@example.com --password secret
```
