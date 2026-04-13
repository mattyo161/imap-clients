# Gmail IMAP — Design Decisions

This document records the design decisions made when adding Gmail IMAP support
to `imap_cli.py`, including the problems encountered and the reasoning behind
each choice.

---

## 1. Authentication

**Decision:** App Password over OAuth2.

Gmail no longer accepts plain account passwords for IMAP — attempts fail with
an authentication error. Two alternatives exist:

| Approach | Complexity | Suitable for |
|----------|-----------|--------------|
| App Password | Low — 16-char password, works with `c.login()` | Personal accounts with 2-Step Verification enabled |
| OAuth2 / XOAUTH2 | High — GCP project, consent screen, token refresh loop | Multi-user apps or Workspace accounts where App Passwords are disabled |

App Passwords are generated at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords) and
used exactly like a regular password — no code changes required. The
existing `c.login(user, password)` call in `ConnectionPool._new_conn` works
unchanged.

---

## 2. Gmail's IMAP folder model

Gmail exposes labels as IMAP folders. The same physical message appears in
multiple "folders" simultaneously (e.g. a message in your Inbox is also in
`[Gmail]/All Mail`). This has two practical consequences:

- Fetching all user-visible folders and deduplicating by Message-ID produces
  fewer messages than actually exist, because messages that only exist in
  `[Gmail]/All Mail` (archived, never explicitly labelled) are missed.
- `[Gmail]/All Mail` (advertised with the `\All` special-use flag) is the
  canonical complete set — every message appears exactly once.

**Decision:** When fetching all Gmail messages, target `[Gmail]/All Mail`
exclusively rather than iterating all folders.

---

## 3. Gmail IMAP extensions (`X-GM-EXT-1`)

Google advertises three proprietary IMAP fetch attributes via the `X-GM-EXT-1`
capability:

| Attribute | Type | Description |
|-----------|------|-------------|
| `X-GM-MSGID` | `int` | Globally unique message ID (64-bit unsigned); same as the hex ID in Gmail web URLs |
| `X-GM-THRID` | `int` | Thread ID — links messages in the same conversation |
| `X-GM-LABELS` | `tuple[str]` | All Gmail labels on the message (e.g. `\\Inbox`, `\\Sent`, `"My Label"`) |

These are especially valuable when fetching `[Gmail]/All Mail` because
`X-GM-LABELS` reveals which labels each message carries, giving full label
coverage without fetching every folder separately.

### Capability detection

After login, `ConnectionPool._new_conn` calls `c.capabilities()` and stores
the result on the connection object. If `b"X-GM-EXT-1"` is present, the
additional fetch properties are registered:

```python
c._capabilities = c.capabilities()
c._additional_fetch_properties = []
if b"X-GM-EXT-1" in c._capabilities:
    c._additional_fetch_properties = [b"X-GM-LABELS", b"X-GM-MSGID", b"X-GM-THRID"]
```

**Why store on the connection object?** The pool can hold connections to any
server. Storing the flag per-connection means the decision is made once at
login time and is available cheaply inside every fetch call without re-querying
capabilities.

**Pitfall avoided:** The capability string must use byte literals (`b"X-GM-EXT-1"`).
IMAPClient returns capabilities as a set of bytes. Using a plain string
`"X-GM-EXT-1"` never matches and silently disables the extensions — a bug
that is very hard to spot because the condition never raises an error.

### Type handling

Each attribute returns a different Python type from IMAPClient:

- `X-GM-LABELS` → `tuple` of `str` (label names, possibly already decoded)
- `X-GM-MSGID` → `int`
- `X-GM-THRID` → `int`

The generic decoding loop must handle all three:

```python
for prop in [b"X-GM-LABELS", b"X-GM-MSGID", b"X-GM-THRID"]:
    if prop in item:
        value = item.get(prop)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            value = [l if isinstance(l, str) else l.decode("utf-8") for l in value]
        elif isinstance(value, bytes):
            value = value.decode("utf-8")
        # ints pass through as-is
        obj[prop.decode("utf-8")] = value
```

### Sparse field output

The Gmail attributes are only added to the output object when they are present
in the IMAP response. Non-Gmail servers simply never return these keys, so the
JSONL schema is unchanged for non-Gmail accounts — no empty/null fields appear.
Consumers use `msg.get("X-GM-LABELS")` and handle `None`.

---

## 4. SEARCH response size limit (`imaplib._MAXLINE`)

`imaplib` (the standard-library module underlying IMAPClient) enforces a
default maximum line length of 1,000,000 bytes. A `SEARCH ALL` response for a
mailbox with ~200,000 messages returns roughly 1.4 MB (200,000 UIDs × ~7 bytes
each), exceeding the limit and raising:

```
command: SEARCH => got more than 1000000 bytes
```

### Options considered

| Option | Verdict |
|--------|---------|
| Raise `imaplib._MAXLINE` to 10 MB | Simple one-liner; works for up to ~1.4 M messages, but is a magic number and holds the entire UID list in memory |
| UID-range pagination via `UIDNEXT` | No magic number, bounded memory and response size, correct IMAP idiom |

**Decision:** UID-range pagination.

### How it works

When a folder is `SELECT`ed, the server returns `UIDNEXT` — the next UID that
will be assigned. UIDs are non-sequential (gaps exist from deleted messages)
but are always less than `UIDNEXT`. Paging in fixed-width UID ranges:

```
UID SEARCH UID 1:50000
UID SEARCH UID 50001:100000
...
```

returns only the UIDs that actually exist within each range. Gaps are
automatically absent. `SEARCH_PAGE = 50_000` caps each response at
~350 KB — well within `imaplib`'s limit and easily tunable.

**Why not sequence-number pagination?** Sequence numbers (1, 2, 3…) are
contiguous but are reassigned whenever messages are deleted, making them
unreliable across reconnections. UIDs are stable for the lifetime of a
`UIDVALIDITY` value.

**Why not `imaplib._MAXLINE`?** It is a private, undocumented attribute, and
raising it to accommodate one large mailbox affects all connections globally.
It also does not help memory: the entire UID list is still returned in one
shot and held in RAM.

---

## 5. Streaming output from `op_messages`

### Original behaviour

`op_messages` accumulated all results in `results: List[Dict]` and yielded
only after the entire mailbox was processed. For 200,000 messages this meant:

- No output for potentially many minutes
- The downstream `fetch` stage was blocked waiting for input
- The SQLite cache was written only at the very end (lost on crash)

### New behaviour

`op_messages` is split into two phases:

**Phase 1 — SEARCH** (one retry loop around the full UID collection):

```python
status = conn.select_folder(mailbox, readonly=True)
uid_next = int(status.get(b"UIDNEXT", 1))
for start in range(1, uid_next, SEARCH_PAGE):
    end = start + SEARCH_PAGE - 1
    page_uids = conn.search(["UID", f"{start}:{end}", *date_criteria])
    all_uids.extend(page_uids)
```

**Phase 2 — FETCH** (one retry loop per `FETCH_BATCH`):

```python
for i in range(0, len(all_uids), FETCH_BATCH):
    batch_uids = all_uids[i: i + FETCH_BATCH]
    # ... fetch, parse, decode Gmail props ...
    cache.set_messages(mailbox, batch_results)   # incremental cache write
    yield from batch_results                      # stream immediately
```

**Why separate retry scopes?** Phase 1 (SEARCH) must complete fully before
Phase 2 (FETCH) can begin, because `limit` is applied to the complete sorted
UID list. Wrapping Phase 1 in its own retry loop means a transient network
error during UID collection retries the entire SEARCH phase cleanly. Phase 2
retries are per-batch: a failed FETCH retries only that 200-UID batch, not the
entire mailbox.

**Cache consistency:** `cache.set_messages` uses `INSERT OR REPLACE`, so
incremental writes are idempotent. A crash mid-run leaves partial results in
the cache that are valid and can be resumed (the cache is checked at the start
of the next run).

---

## 6. Constants

```python
FETCH_BATCH  = 200     # UIDs per FETCH command
SEARCH_PAGE  = 50_000  # UIDs per SEARCH UID range (~350 KB max response)
```

`FETCH_BATCH` was pre-existing. `SEARCH_PAGE = 50_000` was chosen so that:

- Each SEARCH response is ≤ 350 KB (50,000 × 7 bytes), comfortably below the
  1 MB imaplib limit
- The number of round-trips for a 200,000-message mailbox is only 4–5
- The value is visible and easy to tune without hunting through the code
