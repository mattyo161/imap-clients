# SQLite Cache — Schema and Query Reference

The cache database lives at `~/.imap-cache/<host>_<user>.db` (WAL mode).
Open it with any SQLite client:

```bash
sqlite3 ~/.imap-cache/imap.example.com_you@example.com.db
```

---

## Schema

```sql
-- One row per folder/mailbox
CREATE TABLE mailboxes (
    name       TEXT PRIMARY KEY,
    data       TEXT NOT NULL,   -- JSON blob (mailbox object)
    fetched_at TEXT NOT NULL    -- ISO 8601 UTC timestamp
);

-- One row per message (envelope + metadata, no body)
CREATE TABLE messages (
    mailbox    TEXT NOT NULL,
    uid        INTEGER NOT NULL,
    data       TEXT NOT NULL,   -- JSON blob (message object)
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (mailbox, uid)
);

-- One row per fully fetched message (includes body + attachments)
CREATE TABLE message_content (
    mailbox    TEXT NOT NULL,
    uid        INTEGER NOT NULL,
    data       TEXT NOT NULL,   -- JSON blob (message_content object)
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (mailbox, uid)
);
```

### JSON field reference

**`mailboxes.data`**
```json
{
  "type": "mailbox",
  "name": "INBOX",
  "delimiter": "/",
  "flags": ["\\HasNoChildren"],
  "exists": 1042,
  "unseen": 3
}
```

**`messages.data`**
```json
{
  "type": "message",
  "mailbox": "INBOX",
  "uid": 12345,
  "message_id": "<abc@example.com>",
  "subject": "Hello",
  "from": ["Alice <alice@example.com>"],
  "to": ["Bob <bob@example.com>"],
  "cc": [],
  "reply_to": ["Alice <alice@example.com>"],
  "date": "2024-01-15T10:30:00+00:00",
  "internal_date": "2024-01-15T10:30:05+00:00",
  "size": 4096,
  "flags": ["\\Seen"]
}
```

**`message_content.data`** — all fields from `messages.data` plus:
```json
{
  "headers": { "Received": ["..."], "X-Mailer": "Outlook" },
  "body_text": "Plain text body...",
  "body_html": "<html>...</html>",
  "attachments": [
    { "filename": "report.pdf", "content_type": "application/pdf", "size": 98304, "content_id": "" }
  ]
}
```

---

## Mailbox Queries

### List all mailboxes with message counts

```sql
SELECT
    name,
    json_extract(data, '$.exists') AS total,
    json_extract(data, '$.unseen') AS unseen,
    fetched_at
FROM mailboxes
ORDER BY name;
```

### Top mailboxes by message count

```sql
SELECT
    name,
    json_extract(data, '$.exists') AS total
FROM mailboxes
ORDER BY total DESC
LIMIT 20;
```

### Mailboxes with unread messages

```sql
SELECT
    name,
    json_extract(data, '$.unseen') AS unseen
FROM mailboxes
WHERE json_extract(data, '$.unseen') > 0
ORDER BY unseen DESC;
```

---

## Message Queries

### Count cached messages per mailbox

```sql
SELECT
    mailbox,
    COUNT(*) AS cached_messages
FROM messages
GROUP BY mailbox
ORDER BY cached_messages DESC;
```

### Total cached messages across all mailboxes

```sql
SELECT COUNT(*) AS total_messages FROM messages;
```

### Most recent messages (across all mailboxes)

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject')       AS subject,
    json_extract(data, '$.from[0]')       AS from_addr,
    json_extract(data, '$.internal_date') AS received
FROM messages
ORDER BY received DESC
LIMIT 50;
```

### Search messages by subject

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject')       AS subject,
    json_extract(data, '$.from[0]')       AS from_addr,
    json_extract(data, '$.internal_date') AS received
FROM messages
WHERE json_extract(data, '$.subject') LIKE '%invoice%'
ORDER BY received DESC;
```

### Messages by date range

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject')       AS subject,
    json_extract(data, '$.internal_date') AS received
FROM messages
WHERE json_extract(data, '$.internal_date') BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY received DESC;
```

### Largest messages

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject') AS subject,
    json_extract(data, '$.size')    AS size_bytes,
    ROUND(json_extract(data, '$.size') / 1048576.0, 2) AS size_mb
FROM messages
ORDER BY size_bytes DESC
LIMIT 20;
```

### Unread messages

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject')       AS subject,
    json_extract(data, '$.from[0]')       AS from_addr,
    json_extract(data, '$.internal_date') AS received
FROM messages
WHERE NOT (json_extract(data, '$.flags') LIKE '%\\Seen%')
ORDER BY received DESC;
```

---

## Address Queries

> **Note:** `from`, `to`, `cc`, and `reply_to` are JSON arrays.
> SQLite's `json_each()` table-valued function expands them into rows.

### Distinct `From` addresses (all mailboxes)

```sql
SELECT DISTINCT addr.value AS address
FROM messages,
     json_each(json_extract(messages.data, '$.from')) AS addr
WHERE addr.value IS NOT NULL AND addr.value != ''
ORDER BY address;
```

### Distinct `Reply-To` addresses

```sql
SELECT DISTINCT addr.value AS address
FROM messages,
     json_each(json_extract(messages.data, '$.reply_to')) AS addr
WHERE addr.value IS NOT NULL AND addr.value != ''
ORDER BY address;
```

### Most frequent senders (top 25)

```sql
SELECT
    addr.value AS address,
    COUNT(*)   AS message_count
FROM messages,
     json_each(json_extract(messages.data, '$.from')) AS addr
WHERE addr.value IS NOT NULL AND addr.value != ''
GROUP BY addr.value
ORDER BY message_count DESC
LIMIT 25;
```

### All unique addresses ever seen (from + to + cc + reply_to combined)

```sql
SELECT DISTINCT addr.value AS address
FROM messages,
     json_each(
         json_array(
             json_extract(messages.data, '$.from'),
             json_extract(messages.data, '$.to'),
             json_extract(messages.data, '$.cc'),
             json_extract(messages.data, '$.reply_to')
         )
     ) AS field,
     json_each(field.value) AS addr
WHERE addr.value IS NOT NULL AND addr.value != ''
ORDER BY address;
```

### Senders you have never replied to

```sql
SELECT DISTINCT from_addr.value AS sender
FROM messages,
     json_each(json_extract(messages.data, '$.from')) AS from_addr
WHERE from_addr.value IS NOT NULL
  AND from_addr.value NOT IN (
      SELECT DISTINCT to_addr.value
      FROM messages,
           json_each(json_extract(messages.data, '$.to')) AS to_addr
      WHERE to_addr.value IS NOT NULL
  )
ORDER BY sender;
```

---

## Attachment Queries

> These require the `message_content` table (populated by the `fetch` command).

### Messages with attachments

```sql
SELECT
    mailbox,
    uid,
    json_extract(data, '$.subject')              AS subject,
    json_array_length(json_extract(data, '$.attachments')) AS attachment_count
FROM message_content
WHERE json_array_length(json_extract(data, '$.attachments')) > 0
ORDER BY attachment_count DESC;
```

### All attachment filenames and sizes

```sql
SELECT
    mc.mailbox,
    mc.uid,
    json_extract(mc.data, '$.subject')     AS subject,
    att.value ->> '$.filename'             AS filename,
    att.value ->> '$.content_type'         AS content_type,
    ROUND((att.value ->> '$.size') / 1024.0, 1) AS size_kb
FROM message_content mc,
     json_each(json_extract(mc.data, '$.attachments')) AS att
ORDER BY size_kb DESC;
```

### PDF attachments only

```sql
SELECT
    mc.mailbox,
    mc.uid,
    json_extract(mc.data, '$.subject') AS subject,
    att.value ->> '$.filename'         AS filename
FROM message_content mc,
     json_each(json_extract(mc.data, '$.attachments')) AS att
WHERE att.value ->> '$.content_type' = 'application/pdf'
ORDER BY mc.mailbox, mc.uid;
```

---

## Cache Health

### Rows per table

```sql
SELECT 'mailboxes'      AS tbl, COUNT(*) AS rows FROM mailboxes
UNION ALL
SELECT 'messages'       AS tbl, COUNT(*) AS rows FROM messages
UNION ALL
SELECT 'message_content'AS tbl, COUNT(*) AS rows FROM message_content;
```

### When was each table last refreshed?

```sql
SELECT 'mailboxes' AS tbl, MAX(fetched_at) AS last_fetch FROM mailboxes
UNION ALL
SELECT 'messages',  MAX(fetched_at) FROM messages
UNION ALL
SELECT 'message_content', MAX(fetched_at) FROM message_content;
```

### Coverage — messages fetched vs envelope-only

```sql
SELECT
    m.mailbox,
    COUNT(DISTINCT m.uid)  AS envelope_count,
    COUNT(DISTINCT mc.uid) AS content_fetched,
    COUNT(DISTINCT m.uid) - COUNT(DISTINCT mc.uid) AS not_yet_fetched
FROM messages m
LEFT JOIN message_content mc USING (mailbox, uid)
GROUP BY m.mailbox
ORDER BY not_yet_fetched DESC;
```
