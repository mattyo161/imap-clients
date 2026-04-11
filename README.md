# imap-clients

Universal IMAP CLI tools implemented in multiple languages. Each implementation exposes
an identical CLI interface and produces identical **JSON Lines** output so results can be
pipelined and compared across languages.

## Implementations

| Language | Directory | Key library |
|---|---|---|
| Python | `python/` | [imapclient](https://imapclient.readthedocs.io/) |
| Go | `golang/` | [go-imap/v2](https://github.com/emersion/go-imap) |
| TypeScript | `typescript/` | [imapflow](https://imapflow.com/) |
| Node.js | `nodejs/` | [imapflow](https://imapflow.com/) |
| Rust | `rust/` | [async-imap](https://crates.io/crates/async-imap) |

## Quick start

```bash
# List all mailboxes
imap-cli --host imap.example.com --user me@example.com --password secret mailboxes

# List messages in each mailbox (pipeline)
imap-cli ... mailboxes | imap-cli ... messages

# Fetch full message content for every message
imap-cli ... mailboxes | imap-cli ... messages | imap-cli ... fetch

# Fetch messages from a single mailbox, most recent 1000
imap-cli ... messages --mailbox INBOX --limit 1000
```

## Common CLI interface

Every implementation accepts the same flags and commands.

### Global flags

```
--host HOST            IMAP server hostname (required)
--port PORT            Port (default: 993 with SSL, 143 without)
--user USER            Username / email (required)
--password PASS        Password (required)
--no-ssl               Disable TLS
--pool-size N          Connection pool size (default: 5)
--throttle-delay SEC   Seconds to sleep between requests (default: 0)
--max-retries N        Retries on transient failures (default: 3)
--cache-dir DIR        SQLite cache directory (default: ~/.imap-cache)
--no-cache             Bypass cache
--clear-cache          Wipe cache then run
--verbose / -v         Log debug info to stderr
```

### Commands

#### `mailboxes`
List every folder/mailbox on the server.

#### `messages` [--mailbox MAILBOX] [--limit N] [--since DATE] [--before DATE]
Enumerate messages. Reads **mailbox** JSON Lines from stdin (pipeline), or use
`--mailbox` for a single folder.

| flag | description |
|---|---|
| `--mailbox` / `-m` | Folder name |
| `--limit` / `-n` | Return at most N messages (most-recent) |
| `--since` | Messages since IMAP date string, e.g. `01-Jan-2024` |
| `--before` | Messages before IMAP date string |

#### `fetch` [--mailbox MAILBOX] [--uid UID] [--no-attachments]
Fetch complete message content. Reads **message** JSON Lines from stdin (pipeline),
or use `--mailbox` + `--uid`.

| flag | description |
|---|---|
| `--mailbox` / `-m` | Folder name |
| `--uid` / `-u` | Message UID |
| `--no-attachments` | Omit attachment bodies from output |

---

## JSON Lines output format

Every line on stdout is a self-contained JSON object. Field names and types are
identical across all implementations.

### Mailbox object
```json
{
  "type":      "mailbox",
  "name":      "INBOX",
  "delimiter": "/",
  "flags":     ["\\HasNoChildren"],
  "exists":    1234,
  "unseen":    56
}
```

### Message object (output of `messages`)
```json
{
  "type":       "message",
  "mailbox":    "INBOX",
  "uid":        12345,
  "message_id": "<abc123@example.com>",
  "subject":    "Hello World",
  "from":       ["Sender Name <sender@example.com>"],
  "to":         ["Recipient <recipient@example.com>"],
  "cc":         [],
  "reply_to":   [],
  "date":       "2024-01-01T12:00:00+00:00",
  "size":       4096,
  "flags":      ["\\Seen"]
}
```

### Message content object (output of `fetch`)
```json
{
  "type":        "message_content",
  "mailbox":     "INBOX",
  "uid":         12345,
  "message_id":  "<abc123@example.com>",
  "subject":     "Hello World",
  "from":        ["Sender Name <sender@example.com>"],
  "to":          ["Recipient <recipient@example.com>"],
  "cc":          [],
  "reply_to":    [],
  "date":        "2024-01-01T12:00:00+00:00",
  "size":        4096,
  "flags":       ["\\Seen"],
  "headers":     {"Content-Type": "multipart/mixed; boundary=\"abc\""},
  "body_text":   "Hello World\n",
  "body_html":   "<html>…</html>",
  "attachments": [
    {
      "filename":     "document.pdf",
      "content_type": "application/pdf",
      "size":         102400,
      "content_id":   ""
    }
  ]
}
```

### Error object (written to stderr)
```json
{"type": "error", "message": "connection refused"}
```

---

## Caching

Each implementation maintains a local **SQLite** database in `--cache-dir`
(default `~/.imap-cache`). The database is keyed by `host + user` so separate
servers never collide.

Three tables are maintained:

| table | key | description |
|---|---|---|
| `mailboxes` | `name` | Folder list |
| `messages` | `(mailbox, uid)` | Message headers |
| `message_content` | `(mailbox, uid)` | Full message content |

On subsequent runs the cached data is returned immediately without touching
the server. Pass `--no-cache` to force live fetches, or `--clear-cache` to
wipe and re-seed the cache.

---

## Throttle handling

Use `--throttle-delay SEC` to insert a delay between IMAP requests. When the
server returns a transient error (e.g. capacity / rate-limit), every
implementation retries with **exponential back-off** (base 2) up to
`--max-retries` attempts.

---

## Test IMAP server (local)

A Dovecot container makes an ideal test target:

```bash
docker run -d --name dovecot-test \
  -p 143:143 -p 993:993 \
  -e DOVECOT_PASS=testpass \
  dovecot/dovecot

# Seed with messages using imaptest or similar tool
```

Or use any free IMAP account (Gmail app-password, Fastmail, etc.).

---

## Performance benchmarks

Each language directory contains a `benchmark.sh` (or `.ps1`) that runs
three scenarios and reports wall-clock time:

| scenario | description |
|---|---|
| `small` | First 1 000 messages |
| `medium` | First 10 000 messages |
| `large` | First 100 000 messages |

Run with:
```bash
cd python
./benchmark.sh --host imap.example.com --user me@example.com --password s
```
