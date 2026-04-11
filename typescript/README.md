# imap-cli — TypeScript

## Requirements
- Node.js 18+
- npm

## Install & build
```bash
npm install
npm run build
chmod +x dist/index.js
```

## Usage
```bash
node dist/index.js --host imap.example.com --user me@example.com --password secret mailboxes

# Pipeline
node dist/index.js ... mailboxes \
  | node dist/index.js ... messages \
  | node dist/index.js ... fetch

# Single mailbox
node dist/index.js ... messages --mailbox INBOX --limit 1000
```

## Benchmark
```bash
bash benchmark.sh --host imap.example.com --user me@example.com --password secret
```
