# imap-cli — Go

## Requirements
- Go 1.21+
- CGo toolchain (for go-sqlite3)

## Install
```bash
go mod download
go mod tidy
go build -o imap-cli .
```

## Usage
```bash
./imap-cli --host imap.example.com --user me@example.com --password secret mailboxes

# Pipeline
./imap-cli ... mailboxes | ./imap-cli ... messages | ./imap-cli ... fetch

# Single mailbox, limit 1000
./imap-cli ... messages --mailbox INBOX --limit 1000
```

## Benchmark
```bash
bash benchmark.sh --host imap.example.com --user me@example.com --password secret
```
