#!/usr/bin/env python3
"""
imap-cli — Universal IMAP client CLI (Python implementation)

Dependencies:  pip install imapclient
Pipeline:      imap-cli ... mailboxes | imap-cli ... messages | imap-cli ... fetch
"""

__version__ = "0.1.0"

import argparse
import contextlib
import fcntl
import email as email_lib
import email.header
import email.policy
import json
import logging
import os
import queue
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

try:
    from imapclient import IMAPClient
except ImportError:
    print("Error: imapclient not installed.  Run: pip install imapclient", file=sys.stderr)
    sys.exit(1)

log = logging.getLogger(__name__)
_output_lock = threading.Lock()
_PID = os.getpid()
_call_cached = threading.local()    # per-thread flag: cache hit signal from op_*
_current_conn_id = threading.local()  # per-thread: ID of the connection currently in use
_shutdown = threading.Event()         # set on Ctrl-C; workers check this to exit early
FETCH_BATCH = 200    # UIDs per FETCH command
SEARCH_PAGE = 50_000  # UIDs per SEARCH UID range (~350KB max response)
_PROGRESS_DIR = "/tmp/.imap_cli_progress"  # coordination files for multi-process progress


class _JsonLogFormatter(logging.Formatter):
    """Emit each log record as a single JSON Line."""

    def format(self, record: logging.LogRecord) -> str:
        obj: Dict[str, Any] = {
            "type": "log",
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "pid": _PID,
            "thread": record.threadName,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def decode_header(value: Any) -> str:
    """Decode an RFC 2047-encoded header value to a plain string."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    try:
        parts = email_lib.header.decode_header(str(value))
        out: List[str] = []
        for part, charset in parts:
            if isinstance(part, bytes):
                out.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                out.append(str(part))
        return "".join(out).strip()
    except Exception:
        return str(value)


def addrs_to_strings(addr_list: Any) -> List[str]:
    """Convert imapclient envelope address list → ['Name <email>', ...] strings."""
    if not addr_list:
        return []
    result: List[str] = []
    for addr in addr_list:
        if addr is None:
            continue
        name = decode_header(addr.name) if addr.name else ""
        mb = addr.mailbox.decode("utf-8", errors="replace") if isinstance(addr.mailbox, bytes) else (addr.mailbox or "")
        host = addr.host.decode("utf-8", errors="replace") if isinstance(addr.host, bytes) else (addr.host or "")
        if mb and host:
            email_str = f"{mb}@{host}"
            result.append(f"{name} <{email_str}>" if name else email_str)
        elif name:
            result.append(name)
    return result


def parse_date(val: Any) -> str:
    """Return ISO 8601 string for any date-like value."""
    if not val:
        return ""
    try:
        if isinstance(val, datetime):
            if val.tzinfo is None:
                val = val.replace(tzinfo=timezone.utc)
            return val.isoformat()
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(str(val)).isoformat()
    except Exception:
        return str(val)


def flags_to_list(flags: Any) -> List[str]:
    if not flags:
        return []
    return [f.decode("utf-8") if isinstance(f, bytes) else str(f) for f in flags]


def parse_envelope(env) -> Dict:
    """Convert an imapclient Envelope namedtuple into a plain dict."""
    mid = env.message_id or b""
    if isinstance(mid, bytes):
        mid = mid.decode("utf-8", errors="replace")
    subject = decode_header(env.subject) if env.subject else ""
    return {
        "message_id": mid.strip(),
        "subject": subject,
        "from": addrs_to_strings(env.from_),
        "to": addrs_to_strings(env.to),
        "cc": addrs_to_strings(env.cc),
        "reply_to": addrs_to_strings(env.reply_to),
        "date": parse_date(env.date),
    }


def parse_body(raw: bytes) -> Tuple[str, str, List[Dict]]:
    """Parse raw RFC 822 bytes → (body_text, body_html, attachments)."""
    body_text = body_html = ""
    attachments: List[Dict] = []
    try:
        msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.compat32)
    except Exception:
        return "", "", []

    def walk(part):
        nonlocal body_text, body_html
        ct = part.get_content_type()
        disp = (part.get_content_disposition() or "").lower()
        fname = part.get_filename()
        if fname:
            fname = decode_header(fname)

        if "attachment" in disp or (fname and disp != "inline"):
            payload = part.get_payload(decode=True) or b""
            attachments.append({
                "filename": fname or "",
                "content_type": ct,
                "size": len(payload),
                "content_id": (part.get("Content-ID") or "").strip(),
            })
        elif ct == "text/plain" and not body_text:
            payload = part.get_payload(decode=True)
            if payload:
                cs = part.get_content_charset() or "utf-8"
                try:
                    body_text = payload.decode(cs, errors="replace")
                except (LookupError, Exception):
                    body_text = payload.decode("utf-8", errors="replace")
        elif ct == "text/html" and not body_html:
            payload = part.get_payload(decode=True)
            if payload:
                cs = part.get_content_charset() or "utf-8"
                try:
                    body_html = payload.decode(cs, errors="replace")
                except (LookupError, Exception):
                    body_html = payload.decode("utf-8", errors="replace")

    if msg.is_multipart():
        for part in msg.walk():
            if not part.is_multipart():
                walk(part)
    else:
        walk(msg)

    return body_text, body_html, attachments


def parse_headers(raw: bytes) -> Dict[str, Any]:
    """
    Extract top-level headers as {name: value_or_list}.
    Headers that appear once → string value.
    Headers that appear multiple times (e.g. Received:) → list of strings.
    Values are RFC 2047-decoded.
    """
    try:
        msg = email_lib.message_from_bytes(raw)
        seen: Dict[str, List[str]] = {}
        for key in msg.keys():
            vals = [decode_header(v) for v in (msg.get_all(key) or [])]
            if key in seen:
                seen[key].extend(vals)
            else:
                seen[key] = vals
        return {k: v[0] if len(v) == 1 else v for k, v in seen.items()}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class Cache:
    """Thread-safe SQLite cache for IMAP results."""

    def __init__(self, cache_dir: str, host: str, user: str):
        safe = re.sub(r"[^\w.-]", "_", f"{host}_{user}")
        path = os.path.join(cache_dir, f"{safe}.db")
        os.makedirs(cache_dir, exist_ok=True)
        self._path = path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS mailboxes (
                    name       TEXT PRIMARY KEY,
                    data       TEXT NOT NULL,
                    fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    mailbox    TEXT NOT NULL,
                    uid        INTEGER NOT NULL,
                    data       TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (mailbox, uid)
                );
                CREATE TABLE IF NOT EXISTS message_content (
                    mailbox    TEXT NOT NULL,
                    uid        INTEGER NOT NULL,
                    data       TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (mailbox, uid)
                );
            """)
            conn.commit()
            conn.close()

    def get_mailboxes(self) -> Optional[List[Dict]]:
        with self._lock:
            conn = self._connect()
            rows = conn.execute("SELECT data FROM mailboxes ORDER BY name").fetchall()
            conn.close()
        return [json.loads(r["data"]) for r in rows] if rows else None

    def set_mailboxes(self, items: List[Dict]):
        with self._lock:
            conn = self._connect()
            conn.execute("DELETE FROM mailboxes")
            conn.executemany(
                "INSERT OR REPLACE INTO mailboxes (name, data, fetched_at) VALUES (?,?,?)",
                [(m["name"], json.dumps(m, ensure_ascii=False), now_iso()) for m in items],
            )
            conn.commit()
            conn.close()

    def get_messages(self, mailbox: str) -> Optional[List[Dict]]:
        with self._lock:
            conn = self._connect()
            rows = conn.execute(
                "SELECT data FROM messages WHERE mailbox=? ORDER BY uid", (mailbox,)
            ).fetchall()
            conn.close()
        return [json.loads(r["data"]) for r in rows] if rows else None

    def set_messages(self, mailbox: str, items: List[Dict]):
        with self._lock:
            conn = self._connect()
            conn.executemany(
                "INSERT OR REPLACE INTO messages (mailbox, uid, data, fetched_at) VALUES (?,?,?,?)",
                [(mailbox, m["uid"], json.dumps(m, ensure_ascii=False), now_iso()) for m in items],
            )
            conn.commit()
            conn.close()

    def get_message_content(self, mailbox: str, uid: int) -> Optional[Dict]:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT data FROM message_content WHERE mailbox=? AND uid=?", (mailbox, uid)
            ).fetchone()
            conn.close()
        return json.loads(row["data"]) if row else None

    def set_message_content(self, mailbox: str, uid: int, data: Dict):
        with self._lock:
            conn = self._connect()
            conn.execute(
                "INSERT OR REPLACE INTO message_content (mailbox, uid, data, fetched_at) VALUES (?,?,?,?)",
                (mailbox, uid, json.dumps(data, ensure_ascii=False), now_iso()),
            )
            conn.commit()
            conn.close()

    def clear(self):
        with self._lock:
            conn = self._connect()
            conn.executescript(
                "DELETE FROM mailboxes; DELETE FROM messages; DELETE FROM message_content;"
            )
            conn.commit()
            conn.close()


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

class _ResizableSemaphore:
    """
    A semaphore whose upper limit can be changed at runtime.
    Used to dynamically reduce active IMAP connections when the server
    signals throttling, then restore once requests succeed again.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._count = 0
        self._cond = threading.Condition(threading.Lock())

    def acquire(self) -> None:
        with self._cond:
            while self._count >= self._limit:
                if _shutdown.is_set():
                    raise KeyboardInterrupt
                self._cond.wait(timeout=0.5)
            self._count += 1

    def release(self) -> None:
        with self._cond:
            self._count -= 1
            self._cond.notify_all()

    @property
    def count(self) -> int:
        with self._cond:
            return self._count

    def set_limit(self, new_limit: int) -> None:
        with self._cond:
            self._limit = max(1, new_limit)
            self._cond.notify_all()

    @property
    def limit(self) -> int:
        with self._cond:
            return self._limit


class ConnectionPool:
    """Thread-safe pool of authenticated IMAPClient connections."""

    def __init__(self, host: str, port: int, user: str, password: str,
                 ssl: bool, pool_size: int = 5,
                 throttle_delay: float = 0.0, max_retries: int = 5):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssl = ssl
        self.pool_size = pool_size
        self.throttle_delay = throttle_delay
        self.max_retries = max_retries
        self._additional_fetch_properties = []
        self._capabilities = []
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._sem = _ResizableSemaphore(pool_size)
        self._throttle_lock = threading.Lock()
        self._throttle_events: int = 0  # count of in-flight throttle recoveries
        self._backoff_until: float = 0.0  # monotonic timestamp: don't acquire before this
        self._conn_id_counter: int = 0    # incremented per new connection

    # -- throttle management -------------------------------------------------

    def _flush_idle(self) -> None:
        """Drain and close all idle connections sitting in the pool queue."""
        while True:
            try:
                conn = self._pool.get_nowait()
                try:
                    conn.logout()
                except Exception:
                    pass
            except queue.Empty:
                break

    def on_throttle(self, backoff_seconds: float = 0.0) -> None:
        """
        Called when any IMAP operation receives a server error.
        Reduces active concurrency to 1, extends the global backoff deadline,
        and flushes idle connections so retries start with fresh ones.
        """
        with self._throttle_lock:
            self._throttle_events += 1
            deadline = time.monotonic() + backoff_seconds
            if deadline > self._backoff_until:
                self._backoff_until = deadline
            self._sem.set_limit(1)
            log.debug(
                "throttle detected — reducing pool concurrency to 1 "
                "(throttle_events=%d, backoff=%.1fs)", self._throttle_events, backoff_seconds
            )
        self._flush_idle()

    def on_success(self) -> None:
        """
        Called after a successful IMAP operation.
        Decrements the throttle event counter and restores full concurrency
        once all in-flight recoveries have completed.
        """
        with self._throttle_lock:
            if self._throttle_events > 0:
                self._throttle_events -= 1
            if self._throttle_events == 0 and self._sem.limit < self.pool_size:
                self._sem.set_limit(self.pool_size)
                log.debug("throttle cleared — restoring pool concurrency to %d",
                          self.pool_size)

    # -- connection lifecycle ------------------------------------------------

    def _new_conn(self) -> IMAPClient:
        for attempt in range(self.max_retries):
            try:
                c = IMAPClient(
                    host=self.host, port=self.port,
                    ssl=self.ssl, use_uid=True, timeout=30,
                )
                c.login(self.user, self.password)
                with self._throttle_lock:
                    self._conn_id_counter += 1
                    conn_id = self._conn_id_counter
                c._imap_cli_conn_id = conn_id  # type: ignore[attr-defined]
                c._capabilities = c.capabilities()
                if b"X-GM-EXT-1" in c._capabilities:
                    log.debug(f"Adding additional fetch properties: X-GM-LABELS, X-GM-MSGID, X-GM-THRID")
                    c._additional_fetch_properties = [b"X-GM-LABELS", b"X-GM-MSGID", b"X-GM-THRID"]
                return c
            except Exception as exc:
                if attempt >= self.max_retries - 1:
                    raise
                delay = 5 * (2 ** attempt)
                emit_pause(delay, api="connect", attempt=attempt + 1,
                           error=str(exc))
        raise RuntimeError("unreachable")

    @contextlib.contextmanager
    def acquire(self):
        """Yield a live IMAPClient; return it to the pool afterwards."""
        # Honour the global backoff deadline before competing for a slot.
        while not _shutdown.is_set():
            remaining = self._backoff_until - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 0.5))
        if _shutdown.is_set():
            raise KeyboardInterrupt
        self._sem.acquire()
        conn = None
        _current_conn_id.value = 0  # reset before we know which conn we'll use
        try:
            try:
                conn = self._pool.get_nowait()
                conn.noop()  # liveness check
            except (queue.Empty, Exception):
                conn = None
            if conn is None:
                conn = self._new_conn()
            _current_conn_id.value = getattr(conn, "_imap_cli_conn_id", 0)
            if self.throttle_delay > 0:
                time.sleep(self.throttle_delay)
            yield conn
        except Exception:
            conn = None
            raise
        finally:
            self._sem.release()
            if conn is not None:
                try:
                    self._pool.put_nowait(conn)
                except queue.Full:
                    try:
                        conn.logout()
                    except Exception:
                        pass

    def active_count(self) -> int:
        """Number of connections currently held by worker threads."""
        return self._sem.count

    def idle_count(self) -> int:
        """Number of connections sitting idle in the pool queue."""
        return self._pool.qsize()

    def close(self):
        while not self._pool.empty():
            try:
                self._pool.get_nowait().logout()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

_IMAP_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _to_imap_date(value: str) -> str:
    """
    Accept ISO date (YYYY-MM-DD) or IMAP date (DD-Mon-YYYY) and always
    return IMAP format.  Unknown formats are passed through unchanged.
    """
    if not value:
        return value
    try:
        dt = datetime.strptime(value.strip(), "%Y-%m-%d")
        return f"{dt.day:02d}-{_IMAP_MONTHS[dt.month - 1]}-{dt.year}"
    except ValueError:
        return value  # already IMAP format or unrecognised — leave as-is


# ---------------------------------------------------------------------------
# IMAP operations
# ---------------------------------------------------------------------------

def op_mailboxes(pool: ConnectionPool, cache: Optional[Cache],
                 no_cache: bool) -> Iterator[Dict]:
    if not no_cache and cache:
        cached = cache.get_mailboxes()
        if cached is not None:
            log.debug("mailboxes: cache hit (%d)", len(cached))
            _call_cached.value = True
            yield from cached
            return
    _call_cached.value = False

    results: List[Dict] = []
    with pool.acquire() as conn:
        for flags, delimiter, name in conn.list_folders():
            if isinstance(name, bytes):
                name = name.decode("utf-8", errors="replace")
            if isinstance(delimiter, bytes):
                delimiter = delimiter.decode("utf-8", errors="replace")
            obj: Dict = {
                "type": "mailbox",
                "name": name,
                "delimiter": delimiter or "/",
                "flags": flags_to_list(flags),
                "exists": None,
                "unseen": None,
            }
            try:
                st = conn.folder_status(name, ["MESSAGES", "UNSEEN"])
                obj["exists"] = st.get(b"MESSAGES", 0)
                obj["unseen"] = st.get(b"UNSEEN", 0)
            except Exception as exc:
                emit_err(str(exc), api="mailboxes", mailbox=name,
                         error_code=_imap_error_code(exc))
            results.append(obj)

    if cache:
        cache.set_mailboxes(results)
    yield from results


def op_messages(pool: ConnectionPool, mailbox: str,
                cache: Optional[Cache], no_cache: bool,
                limit: Optional[int], since: Optional[str],
                before: Optional[str]) -> Iterator[Dict]:
    if not no_cache and cache:
        cached = cache.get_messages(mailbox)
        if cached is not None:
            log.debug("messages: cache hit for %s (%d)", mailbox, len(cached))
            _call_cached.value = True
            items = cached if not limit else cached[-limit:]
            yield from items
            return
    _call_cached.value = False

    date_criteria: List[Any] = []
    if since:
        date_criteria += ["SINCE", _to_imap_date(since)]
    if before:
        date_criteria += ["BEFORE", _to_imap_date(before)]

    # --- Phase 1: collect all matching UIDs via paginated SEARCH UID ranges ---
    # Each page issues a bounded SEARCH so the response never exceeds ~350KB,
    # avoiding imaplib's line-length limit on large mailboxes.

    uid_next: int = 1  # will be set from SELECT response
    all_uids: List[int] = []
    last_exc: Optional[Exception] = None

    for attempt in range(pool.max_retries):
        try:
            all_uids = []
            with pool.acquire() as conn:
                status = conn.select_folder(mailbox, readonly=True)
                uid_next = int(status.get(b"UIDNEXT", 1))
                for start in range(1, uid_next, SEARCH_PAGE):
                    end = start + SEARCH_PAGE - 1
                    # Combine UID range with any date filters; no "ALL" needed
                    # because the UID range already restricts the result set.
                    page_criteria: List[Any] = ["UID", f"{start}:{end}"]
                    if date_criteria:
                        page_criteria += date_criteria
                    page_uids = conn.search(page_criteria)
                    log.debug("search page uid %d:%d → %d hits in %s",
                              start, end, len(page_uids), mailbox)
                    all_uids.extend(page_uids)
            pool.on_success()
            break
        except Exception as exc:
            last_exc = exc
            all_uids = []
            delay = 5 * (2 ** attempt)
            pool.on_throttle(delay)
            if attempt < pool.max_retries - 1:
                if _progress:
                    _progress.start_pause()
                try:
                    emit_pause(delay, api="messages",
                               mailbox=mailbox, attempt=attempt + 1,
                               error=str(exc))
                finally:
                    if _progress:
                        _progress.end_pause()
    else:
        emit_err(str(last_exc), api="messages", mailbox=mailbox,
                 error_code=_imap_error_code(last_exc), failed=True)
        return

    if limit:
        all_uids = all_uids[-limit:]

    # --- Phase 2: FETCH metadata in batches, streaming each batch immediately ---

    for i in range(0, len(all_uids), FETCH_BATCH):
        batch_uids = all_uids[i: i + FETCH_BATCH]
        batch_results: List[Dict] = []
        last_exc = None

        for attempt in range(pool.max_retries):
            try:
                batch_results = []
                with pool.acquire() as conn:
                    conn.select_folder(mailbox, readonly=True)
                    fetch_properties = [
                        "ENVELOPE", "FLAGS", "RFC822.SIZE", "INTERNALDATE",
                        *conn._additional_fetch_properties,
                    ]
                    log.debug("fetching %d uids (batch %d/%d) from %s",
                              len(batch_uids), i // FETCH_BATCH + 1,
                              -(-len(all_uids) // FETCH_BATCH), mailbox)
                    data = conn.fetch(batch_uids, fetch_properties)
                    log.debug("fetched %d items from %s", len(data), mailbox)
                    for uid, item in data.items():
                        env_raw = item.get(b"ENVELOPE")
                        if env_raw is None:
                            continue
                        env = parse_envelope(env_raw)
                        internal_date_raw = item.get(b"INTERNALDATE")
                        obj: Dict = {
                            "type": "message",
                            "mailbox": mailbox,
                            "uid": uid,
                            **env,
                            "internal_date": parse_date(internal_date_raw) if internal_date_raw else "",
                            "size": item.get(b"RFC822.SIZE", 0),
                            "flags": flags_to_list(item.get(b"FLAGS", [])),
                        }
                        for prop in [b"X-GM-LABELS", b"X-GM-MSGID", b"X-GM-THRID"]:
                            if prop in item:
                                value = item.get(prop)
                                if value is None:
                                    continue
                                if isinstance(value, (list, tuple)):
                                    value = [l if isinstance(l, str) else l.decode("utf-8") for l in value]
                                elif isinstance(value, bytes):
                                    value = value.decode("utf-8")
                                obj[prop.decode("utf-8")] = value
                        batch_results.append(obj)
                pool.on_success()
                break
            except Exception as exc:
                last_exc = exc
                batch_results = []
                delay = 5 * (2 ** attempt)
                pool.on_throttle(delay)
                if attempt < pool.max_retries - 1:
                    if _progress:
                        _progress.start_pause()
                    try:
                        emit_pause(delay, api="messages",
                                   mailbox=mailbox, attempt=attempt + 1,
                                   error=str(exc))
                    finally:
                        if _progress:
                            _progress.end_pause()
        else:
            emit_err(str(last_exc), api="messages", mailbox=mailbox,
                     error_code=_imap_error_code(last_exc), failed=True)
            return

        if cache and batch_results:
            cache.set_messages(mailbox, batch_results)
        yield from batch_results


def op_fetch(pool: ConnectionPool, mailbox: str, uid: int,
             cache: Optional[Cache], no_cache: bool,
             no_attachments: bool) -> Optional[Dict]:
    if not no_cache and cache:
        cached = cache.get_message_content(mailbox, uid)
        if cached is not None:
            log.debug("fetch: cache hit %s/%d", mailbox, uid)
            _call_cached.value = True
            return cached
    _call_cached.value = False

    data: Optional[Dict] = None
    last_exc: Optional[Exception] = None

    for attempt in range(pool.max_retries):
        try:
            with pool.acquire() as conn:
                # Both select_folder and fetch are inside a single pool.acquire()
                # so that any IMAP error causes the connection to be discarded
                # and a fresh one opened on the next retry attempt.
                conn.select_folder(mailbox, readonly=True)
                fetch_properties = ["ENVELOPE", "FLAGS", "RFC822.SIZE", "INTERNALDATE", "RFC822", *conn._additional_fetch_properties]
                data = conn.fetch(
                    [uid],
                    fetch_properties,
                )
            pool.on_success()
            break  # success — exit retry loop
        except Exception as exc:
            last_exc = exc
            data = None
            delay = 5 * (2 ** attempt)
            pool.on_throttle(delay)
            if attempt < pool.max_retries - 1:
                if _progress:
                    _progress.start_pause()
                try:
                    emit_pause(delay, api="fetch", mailbox=mailbox,
                               uid=uid, attempt=attempt + 1, error=str(exc))
                finally:
                    if _progress:
                        _progress.end_pause()
    else:
        # All attempts exhausted
        emit_err(str(last_exc), api="fetch", mailbox=mailbox, uid=uid,
                 error_code=_imap_error_code(last_exc), failed=True)
        return None

    if data is None or uid not in data:
        return None
    item = data[uid]
    env_raw = item.get(b"ENVELOPE")
    if env_raw is None:
        return None

    env = parse_envelope(env_raw)
    internal_date_raw = item.get(b"INTERNALDATE")
    raw = item.get(b"RFC822", b"")
    body_text, body_html, atts = parse_body(raw)
    hdrs = parse_headers(raw)

    obj: Dict = {
        "type": "message_content",
        "mailbox": mailbox,
        "uid": uid,
        **env,
        "internal_date": parse_date(internal_date_raw) if internal_date_raw else "",
        "size": item.get(b"RFC822.SIZE", 0),
        "flags": flags_to_list(item.get(b"FLAGS", [])),
        "headers": hdrs,
        "body_text": body_text,
        "body_html": body_html,
        "attachments": [] if no_attachments else atts,
    }

    # check for custom properties
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

    if cache:
        cache.set_message_content(mailbox, uid, obj)
    return obj


def op_fetch_batch(
    pool: ConnectionPool,
    mailbox: str,
    uids: List[int],
    cache: Optional[Cache],
    no_cache: bool,
    no_attachments: bool,
) -> Tuple[List[Dict], int]:
    """
    Fetch multiple full messages from one mailbox in a single IMAP FETCH call.

    Cache hits are resolved locally first; only the uncached UIDs hit the
    network.  All results are cached individually after the batch fetch.

    Returns (results, cache_hit_count).
    """
    results: List[Dict] = []
    uncached: List[int] = []

    for uid in uids:
        if not no_cache and cache:
            cached_obj = cache.get_message_content(mailbox, uid)
            if cached_obj is not None:
                log.debug("fetch_batch: cache hit %s/%d", mailbox, uid)
                results.append(cached_obj)
                continue
        uncached.append(uid)

    cache_hits = len(uids) - len(uncached)

    if not uncached:
        return results, cache_hits  # every UID was a cache hit — no network needed

    data: Optional[Dict] = None
    last_exc: Optional[Exception] = None

    for attempt in range(pool.max_retries):
        try:
            with pool.acquire() as conn:
                conn.select_folder(mailbox, readonly=True)
                fetch_props = [
                    "ENVELOPE", "FLAGS", "RFC822.SIZE", "INTERNALDATE", "RFC822",
                    *conn._additional_fetch_properties,
                ]
                log.debug("fetch_batch: fetching %d uids from %s", len(uncached), mailbox)
                data = conn.fetch(uncached, fetch_props)
            pool.on_success()
            break
        except Exception as exc:
            last_exc = exc
            data = None
            delay = 5 * (2 ** attempt)
            pool.on_throttle(delay)
            if attempt < pool.max_retries - 1:
                if _progress:
                    _progress.start_pause()
                try:
                    emit_pause(delay, api="fetch", mailbox=mailbox,
                               attempt=attempt + 1, error=str(exc))
                finally:
                    if _progress:
                        _progress.end_pause()
    else:
        emit_err(str(last_exc), api="fetch", mailbox=mailbox,
                 error_code=_imap_error_code(last_exc), failed=True)
        return results, cache_hits

    if data is None:
        return results, cache_hits

    for uid in uncached:
        if uid not in data:
            continue
        item = data[uid]
        env_raw = item.get(b"ENVELOPE")
        if env_raw is None:
            continue
        env = parse_envelope(env_raw)
        internal_date_raw = item.get(b"INTERNALDATE")
        raw = item.get(b"RFC822", b"")
        body_text, body_html, atts = parse_body(raw)
        hdrs = parse_headers(raw)

        obj: Dict = {
            "type": "message_content",
            "mailbox": mailbox,
            "uid": uid,
            **env,
            "internal_date": parse_date(internal_date_raw) if internal_date_raw else "",
            "size": item.get(b"RFC822.SIZE", 0),
            "flags": flags_to_list(item.get(b"FLAGS", [])),
            "headers": hdrs,
            "body_text": body_text,
            "body_html": body_html,
            "attachments": [] if no_attachments else atts,
        }

        for prop in [b"X-GM-LABELS", b"X-GM-MSGID", b"X-GM-THRID"]:
            if prop in item:
                value = item.get(prop)
                if value is None:
                    continue
                if isinstance(value, (list, tuple)):
                    value = [l if isinstance(l, str) else l.decode("utf-8") for l in value]
                elif isinstance(value, bytes):
                    value = value.decode("utf-8")
                obj[prop.decode("utf-8")] = value

        if cache:
            cache.set_message_content(mailbox, uid, obj)
        results.append(obj)

    return results, cache_hits


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def emit(obj: Dict):
    with _output_lock:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def _imap_error_code(exc: Exception) -> str:
    """
    Extract the bracketed IMAP response code from an exception message.
    e.g. 'select failed: [SERVERBUG] ...' → 'SERVERBUG'
    Falls back to the exception class name if no code is found.
    """
    m = re.search(r'\[([A-Z][A-Z0-9\-]+)\]', str(exc))
    return m.group(1) if m else type(exc).__name__


def _fmt_elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    if h:
        return f"{h}h{m:02d}m{s:04.1f}s"
    if m:
        return f"{m}m{s:04.1f}s"
    return f"{s:.1f}s"


class Progress:
    """
    Live, multi-process-aware progress display rendered to stderr.

    Stats from all processes in the pipeline are aggregated by command type
    and rendered as a single updating line:

        mailboxes(c: 10, s: 20)  messages(s: 68000, q: 120000)  fetch(s: 68000, q: 120000, f: 100)

    c=cached  s=success  q=queued/in-flight  f=failed
    c, q, f are omitted when zero.

    Thread-safety: internal stats are protected by self._lock.
    Cross-process writes to stderr are serialised by _output_lock (same
    lock used by emit_err / emit_pause so progress never interleaves with
    error JSON lines).
    Cross-process coord-file access is serialised by an exclusive flock.
    """

    def __init__(self, cmd_name: str) -> None:
        self._cmd_name = cmd_name
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._requests_done: int = 0   # items fetched from network
        self._cache_hits: int = 0      # items served from cache
        self._active: int = 0          # items currently being processed
        self._queue_size: int = 0      # items submitted but not yet started
        self._total_time: float = 0.0  # wall time for network requests only
        self._errors: int = 0          # total error events emitted
        self._failed: int = 0          # requests that exhausted all retries
        self._paused: int = 0          # threads currently sleeping in backoff
        self._running: bool = False
        self._tty: bool = sys.stderr.isatty()
        self._thread: Optional[threading.Thread] = None
        # 0 or 1 — kept consistent under _output_lock so clear_line() is correct
        self._screen_lines: int = 0
        # Paths to coordination files (populated in _setup_coord)
        self._coord_path: str = ""
        self._lock_path: str = ""
        self._slot: int = 0            # index in the slots list (stable identifier)

        if self._tty:
            self._setup_coord()

    # -- lifecycle -----------------------------------------------------------

    def _setup_coord(self) -> None:
        """Register this process in the shared coord file and claim a slot."""
        pgid = os.getpgrp()
        os.makedirs(_PROGRESS_DIR, exist_ok=True, mode=0o700)
        self._coord_path = os.path.join(_PROGRESS_DIR, f"{pgid}.json")
        self._lock_path  = os.path.join(_PROGRESS_DIR, f"{pgid}.lock")
        with self._flock():
            data = self._read_coord()
            self._slot = len(data["slots"])
            data["slots"].append({
                "pid":     self._pid,
                "slot":    self._slot,
                "cmd":     self._cmd_name,
                "done":    False,
                "cached":  0,
                "success": 0,
                "queued":  0,
                "failed":  0,
            })
            self._write_coord(data)

    def start(self) -> None:
        if not self._tty:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="progress"
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop rendering and write the final consolidated progress line."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=0.5)
        if not self._tty:
            return

        with self._lock:
            cached  = self._cache_hits
            success = self._requests_done
            failed  = self._failed

        with self._flock():
            data = self._read_coord()
            for s in data["slots"]:
                if s["slot"] == self._slot:
                    s["done"]    = True
                    s["cached"]  = cached
                    s["success"] = success
                    s["queued"]  = 0   # done — nothing queued or in-flight
                    s["failed"]  = failed
                    break
            old_screen = data["screen_lines"]
            render_str = self._build_render_str(data["slots"], old_screen)
            all_done = all(s["done"] for s in data["slots"])
            data["screen_lines"] = 1
            self._write_coord(data)

            with _output_lock:
                sys.stderr.write(render_str)
                if all_done:
                    sys.stderr.write("\n")
                sys.stderr.flush()
                self._screen_lines = 0 if all_done else 1

        if all_done:
            with contextlib.suppress(OSError):
                os.unlink(self._coord_path)
            with contextlib.suppress(OSError):
                os.unlink(self._lock_path)

    # -- stats API (called from worker threads) ------------------------------

    def add_queue(self, n: int = 1) -> None:
        """Called when items are submitted to the thread pool."""
        with self._lock:
            self._queue_size += n

    def start_call(self, n: int = 1) -> None:
        """Called at the start of each worker: moves n items queue→active."""
        with self._lock:
            self._queue_size = max(0, self._queue_size - n)
            self._active += n

    def finish_call(
        self, elapsed: float, *, cached_count: int = 0, success_count: int = 1
    ) -> None:
        """
        Called at the end of each worker.
        cached_count:  items served from cache (elapsed excluded from avg).
        success_count: items fetched from the network (elapsed included in avg).
        Both may be non-zero for batch fetches with mixed cache/network results.
        """
        total = cached_count + success_count
        with self._lock:
            self._active = max(0, self._active - total)
            self._cache_hits += cached_count
            self._requests_done += success_count
            if success_count:
                self._total_time += elapsed

    def record_error(self, *, failed: bool = False) -> None:
        """
        Called by emit_err.
        failed=True  → the request exhausted all retries (counted in _failed).
        Every call increments _errors regardless.
        """
        with self._lock:
            self._errors += 1
            if failed:
                self._failed += 1

    def start_pause(self) -> None:
        """Called just before a backoff sleep: moves one item active→paused."""
        with self._lock:
            self._active = max(0, self._active - 1)
            self._paused += 1

    def end_pause(self) -> None:
        """Called just after a backoff sleep: moves one item paused→active."""
        with self._lock:
            self._paused = max(0, self._paused - 1)
            self._active += 1

    # -- line management (called inside _output_lock) -----------------------

    def clear_line(self) -> None:
        """
        Erase the single progress line.  MUST be called while _output_lock is held.
        """
        if not self._tty or not self._running or self._screen_lines == 0:
            return
        sys.stderr.write("\r\033[2K")
        self._screen_lines = 0

    # -- internal ------------------------------------------------------------

    @contextlib.contextmanager
    def _flock(self):
        """Exclusive cross-process file lock on _lock_path."""
        lf = open(self._lock_path, "w")
        try:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)
        finally:
            lf.close()

    def _read_coord(self) -> dict:
        try:
            with open(self._coord_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return {"screen_lines": 0, "slots": []}

    def _write_coord(self, data: dict) -> None:
        tmp = self._coord_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self._coord_path)

    def _loop(self) -> None:
        while self._running:
            self._render()
            time.sleep(0.1)

    @staticmethod
    def _build_consolidated_line(slots: list) -> str:
        """
        Aggregate stats across all slots by command type and return a single
        progress line in the format:

            mailboxes(c: 10, s: 20)  messages(s: 68000, q: 120000)  fetch(s: 68000, q: 120000, f: 100)

        c=cached  s=success  q=queued/in-flight  f=failed
        c, q, f are omitted when zero.
        """
        by_cmd: Dict[str, Dict[str, int]] = {}
        for s in slots:
            cmd = s.get("cmd", "")
            if cmd not in by_cmd:
                by_cmd[cmd] = {"cached": 0, "success": 0, "queued": 0, "failed": 0}
            by_cmd[cmd]["cached"]  += s.get("cached",  0)
            by_cmd[cmd]["success"] += s.get("success", 0)
            by_cmd[cmd]["queued"]  += s.get("queued",  0)
            by_cmd[cmd]["failed"]  += s.get("failed",  0)

        parts: List[str] = []
        for cmd in ("mailboxes", "messages", "fetch"):
            if cmd not in by_cmd:
                continue
            st = by_cmd[cmd]
            inner: List[str] = []
            if st["cached"]:
                inner.append(f"c: {st['cached']}")
            inner.append(f"s: {st['success']}")
            if st["queued"]:
                inner.append(f"q: {st['queued']}")
            if st["failed"]:
                inner.append(f"f: {st['failed']}")
            parts.append(f"{cmd}({', '.join(inner)})")
        return "  ".join(parts)

    @staticmethod
    def _build_render_str(slots: list, screen_lines: int) -> str:
        """Build the ANSI escape sequence that rewrites the single progress line."""
        return "\r\033[2K" + Progress._build_consolidated_line(slots)

    def _render(self) -> None:
        """Update the coord file and redraw the consolidated progress line."""
        with self._lock:
            cached  = self._cache_hits
            success = self._requests_done
            queued  = self._queue_size + self._active + self._paused
            failed  = self._failed

        with self._flock():
            data = self._read_coord()
            old_screen = data["screen_lines"]
            for s in data["slots"]:
                if s["slot"] == self._slot:
                    s["cached"]  = cached
                    s["success"] = success
                    s["queued"]  = queued
                    s["failed"]  = failed
                    break
            render_str = self._build_render_str(data["slots"], old_screen)
            data["screen_lines"] = 1
            self._write_coord(data)

        with _output_lock:
            sys.stderr.write(render_str)
            sys.stderr.flush()
            self._screen_lines = 1


# Module-level progress instance; set by main() when --progress is active.
_progress: Optional[Progress] = None


def emit_err(
    message: str,
    *,
    api: str = "",
    mailbox: str = "",
    uid: Optional[int] = None,
    message_id: str = "",
    error_code: str = "",
    failed: bool = False,
) -> None:
    """
    Write a structured error record to stderr as JSON Lines.
    failed=True signals that all retries were exhausted (counted separately
    in the progress display from transient/single errors).
    """
    conn_id = getattr(_current_conn_id, "value", 0)
    record: Dict[str, Any] = {
        "type": "error",
        "timestamp": now_iso(),
        "pid": _PID,
        "thread": threading.current_thread().name,
        "api": api,
        "message": message,
        "error_code": error_code,
    }
    if conn_id:
        record["conn_id"] = conn_id
    if mailbox:
        record["mailbox"] = mailbox
    if uid is not None:
        record["uid"] = uid
    if message_id:
        record["message_id"] = message_id
    if failed:
        record["failed"] = True
    with _output_lock:
        if _progress:
            _progress.clear_line()
            _progress.record_error(failed=failed)
        sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stderr.flush()


def emit_pause(
    delay: float,
    *,
    api: str = "",
    mailbox: str = "",
    uid: Optional[int] = None,
    attempt: int = 0,
    error: str = "",
) -> None:
    """Write a backoff-pause record to stderr as JSON Lines, then sleep."""
    conn_id = getattr(_current_conn_id, "value", 0)
    record: Dict[str, Any] = {
        "type": "pause",
        "timestamp": now_iso(),
        "pid": _PID,
        "thread": threading.current_thread().name,
        "api": api,
        "delay_seconds": round(delay, 3),
        "attempt": attempt,
    }
    if conn_id:
        record["conn_id"] = conn_id
    if error:
        record["error"] = error
    if mailbox:
        record["mailbox"] = mailbox
    if uid is not None:
        record["uid"] = uid
    with _output_lock:
        if _progress:
            _progress.clear_line()
        sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stderr.flush()
    # Interruptible sleep: wake every 0.2 s to check for shutdown signal.
    deadline = time.monotonic() + delay
    while not _shutdown.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 0.2))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_mailboxes(args, pool: ConnectionPool, cache: Optional[Cache]):
    if _progress:
        _progress.add_queue(1)
        _progress.start_call()
    t0 = time.monotonic()
    try:
        for mb in op_mailboxes(pool, cache, args.no_cache):
            emit(mb)
    finally:
        if _progress:
            _was_cached = getattr(_call_cached, "value", False)
            _progress.finish_call(
                time.monotonic() - t0,
                cached_count=1 if _was_cached else 0,
                success_count=0 if _was_cached else 1,
            )


def cmd_messages(args, pool: ConnectionPool, cache: Optional[Cache]):
    limit = getattr(args, "limit", None)
    since = getattr(args, "since", None)
    before = getattr(args, "before", None)

    def process(mailbox: str):
        if _progress:
            _progress.start_call()
        t0 = time.monotonic()
        try:
            for msg in op_messages(pool, mailbox, cache,
                                   no_cache=args.no_cache,
                                   limit=limit, since=since, before=before):
                emit(msg)
        finally:
            if _progress:
                _was_cached = getattr(_call_cached, "value", False)
                _progress.finish_call(
                    time.monotonic() - t0,
                    cached_count=1 if _was_cached else 0,
                    success_count=0 if _was_cached else 1,
                )

    if not sys.stdin.isatty():
        ex = ThreadPoolExecutor(max_workers=args.pool_size)
        try:
            futs = []
            for line in sys.stdin:
                if _shutdown.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("type") == "mailbox" and obj.get("name"):
                    if _progress:
                        _progress.add_queue(1)
                    futs.append(ex.submit(process, obj["name"]))
            for f in as_completed(futs):
                if _shutdown.is_set():
                    break
                try:
                    f.result()
                except Exception as exc:
                    emit_err(str(exc))
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
    elif getattr(args, "mailbox", None):
        if _progress:
            _progress.add_queue(1)
        process(args.mailbox)
    else:
        emit_err("Provide --mailbox or pipe mailbox JSON Lines from stdin.")


def cmd_fetch(args, pool: ConnectionPool, cache: Optional[Cache]):
    no_att = getattr(args, "no_attachments", False)
    fetch_size = getattr(args, "fetch_size", 1)

    # -- per-UID wrapper used by the single-message and streaming paths -------

    def process_one(mailbox: str, uid: int):
        if _progress:
            _progress.start_call()
        t0 = time.monotonic()
        try:
            obj = op_fetch(pool, mailbox, uid, cache,
                           no_cache=args.no_cache, no_attachments=no_att)
            if obj:
                emit(obj)
        finally:
            if _progress:
                _was_cached = getattr(_call_cached, "value", False)
                _progress.finish_call(
                    time.monotonic() - t0,
                    cached_count=1 if _was_cached else 0,
                    success_count=0 if _was_cached else 1,
                )

    # -- batch wrapper used when --fetch-size > 1 ----------------------------

    def process_batch(mailbox: str, uids: List[int]):
        n = len(uids)
        if _progress:
            _progress.start_call(n)
        t0 = time.monotonic()
        results: List[Dict] = []
        cache_hits = 0
        try:
            results, cache_hits = op_fetch_batch(
                pool, mailbox, uids, cache,
                no_cache=args.no_cache, no_attachments=no_att,
            )
            for obj in results:
                emit(obj)
        finally:
            if _progress:
                _progress.finish_call(
                    time.monotonic() - t0,
                    cached_count=cache_hits,
                    success_count=max(0, len(results) - cache_hits),
                )

    if not sys.stdin.isatty():
        if fetch_size > 1:
            # Batched mode: buffer stdin grouped by mailbox, then submit
            # fixed-size chunks so each thread issues one multi-UID FETCH.
            by_mailbox: Dict[str, List[int]] = {}
            for line in sys.stdin:
                if _shutdown.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if msg.get("type") == "message" and msg.get("mailbox") and msg.get("uid"):
                    mb = msg["mailbox"]
                    by_mailbox.setdefault(mb, []).append(int(msg["uid"]))

            ex = ThreadPoolExecutor(max_workers=args.pool_size)
            try:
                futs = []
                for mb, uids in by_mailbox.items():
                    for i in range(0, len(uids), fetch_size):
                        batch = uids[i : i + fetch_size]
                        if _progress:
                            _progress.add_queue(len(batch))
                        futs.append(ex.submit(process_batch, mb, batch))
                for f in as_completed(futs):
                    if _shutdown.is_set():
                        break
                    try:
                        f.result()
                    except Exception as exc:
                        emit_err(str(exc))
            finally:
                ex.shutdown(wait=False, cancel_futures=True)
        else:
            # Streaming mode (default): submit one task per UID as it arrives
            ex = ThreadPoolExecutor(max_workers=args.pool_size)
            try:
                futs = []
                for line in sys.stdin:
                    if _shutdown.is_set():
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("type") == "message" and obj.get("mailbox") and obj.get("uid"):
                        if _progress:
                            _progress.add_queue(1)
                        futs.append(ex.submit(process_one, obj["mailbox"], int(obj["uid"])))
                for f in as_completed(futs):
                    if _shutdown.is_set():
                        break
                    try:
                        f.result()
                    except Exception as exc:
                        emit_err(str(exc))
            finally:
                ex.shutdown(wait=False, cancel_futures=True)
    elif getattr(args, "mailbox", None) and getattr(args, "uid", None) is not None:
        if _progress:
            _progress.add_queue(1)
        process_one(args.mailbox, int(args.uid))
    else:
        emit_err("Provide --mailbox + --uid or pipe message JSON Lines from stdin.")


# ---------------------------------------------------------------------------
# Shutdown helpers
# ---------------------------------------------------------------------------

def _emit_shutdown_progress(pool: ConnectionPool) -> None:
    """
    Poll until all active connections have been released, emitting a JSON Lines
    status record each time the count changes.  Ends with a shutdown_complete
    record regardless of how long it takes.
    """
    t0 = time.monotonic()
    last_active = -1
    while True:
        active = pool.active_count()
        idle = pool.idle_count()
        if active == 0:
            break
        if active != last_active:
            record: Dict[str, Any] = {
                "type": "shutdown",
                "timestamp": now_iso(),
                "pid": _PID,
                "waiting_for": active,
                "idle_connections": idle,
                "message": f"waiting for {active} active connection(s) to close",
            }
            with _output_lock:
                if _progress:
                    _progress.clear_line()
                sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
                sys.stderr.flush()
            last_active = active
        time.sleep(0.25)

    elapsed = round(time.monotonic() - t0, 3)
    record = {
        "type": "shutdown_complete",
        "timestamp": now_iso(),
        "pid": _PID,
        "elapsed_seconds": elapsed,
        "message": "clean shutdown",
    }
    with _output_lock:
        if _progress:
            _progress.clear_line()
        sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="imap-cli",
        description="Universal IMAP client — outputs JSON Lines",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    g = p.add_argument_group("connection")
    g.add_argument("--host", required=True, help="IMAP hostname")
    g.add_argument("--port", type=int, default=0,
                   help="IMAP port (0 = auto: 993 SSL / 143 plain)")
    g.add_argument("--user", required=True, help="Username / email")
    g.add_argument("--password", default=None,
                   help="Password (plain text; prefer --password-env)")
    g.add_argument("--password-env", default=None, metavar="VAR",
                   help="Name of env var holding the password")
    g.add_argument("--no-ssl", action="store_true", help="Disable TLS")
    g.add_argument("--pool-size", type=int, default=5, metavar="N")
    g.add_argument("--throttle-delay", type=float, default=0.0, metavar="SEC")
    g.add_argument("--max-retries", type=int, default=5)

    c = p.add_argument_group("cache")
    c.add_argument("--cache-dir", default=os.path.expanduser("~/.imap-cache"))
    c.add_argument("--no-cache", action="store_true", help="Bypass cache")
    c.add_argument("--clear-cache", action="store_true",
                   help="Wipe cache before running")

    p.add_argument("--verbose", "-v", action="count", default=0,
                   help="-v: DEBUG for imap-cli, INFO for imapclient; "
                        "-vv: DEBUG for both")
    p.add_argument("--log-file", default=os.environ.get("IMAP_CLI_LOG_FILE", ""),
                   metavar="PATH",
                   help="Write log output to PATH instead of stderr "
                        "(env: IMAP_CLI_LOG_FILE)")
    p.add_argument("--progress", "-p", action="store_true",
                   help="Show live progress (spinner, elapsed, requests, queue, avg/req) on stderr")

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("mailboxes", help="List all mailboxes/folders")

    mp = sub.add_parser("messages",
                        help="List messages (reads mailbox jsonl from stdin)")
    mp.add_argument("--mailbox", "-m", help="Single mailbox (alternative to stdin)")
    mp.add_argument("--limit", "-n", type=int, help="Max messages to return")
    mp.add_argument("--since", help="Only messages since date (e.g. 01-Jan-2024)")
    mp.add_argument("--before", help="Only messages before date")

    fp = sub.add_parser("fetch",
                        help="Fetch full message (reads message jsonl from stdin)")
    fp.add_argument("--mailbox", "-m")
    fp.add_argument("--uid", "-u", type=int)
    fp.add_argument("--no-attachments", action="store_true")
    fp.add_argument("--fetch-size", type=int, default=1, metavar="N",
                    help="Batch N messages per IMAP FETCH call when reading from stdin "
                         "(default 1 = one message per request). "
                         "Higher values reduce round-trips at the cost of larger "
                         "individual responses. Ignored with --mailbox/--uid.")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Configure logging: JSON Lines format, verbosity-controlled levels.
    #   -v   → imap-cli=DEBUG,  imapclient=INFO
    #   -vv  → imap-cli=DEBUG,  imapclient=DEBUG
    #   none → WARNING for all
    our_level = logging.DEBUG if args.verbose >= 1 else logging.WARNING
    imap_level = logging.DEBUG if args.verbose >= 2 else (
        logging.INFO if args.verbose == 1 else logging.WARNING
    )
    log_stream: Any = open(args.log_file, "a", encoding="utf-8") if args.log_file else sys.stderr
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(_JsonLogFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)   # root wide-open; levels set per logger
    logging.getLogger(__name__).setLevel(our_level)
    logging.getLogger("imapclient").setLevel(imap_level)

    # Resolve password — env var takes precedence over --password
    password: str = ""
    if args.password_env:
        password = os.environ.get(args.password_env, "")
        if not password:
            emit_err(f"Environment variable {args.password_env!r} is not set or empty")
            sys.exit(1)
    elif args.password:
        password = args.password
    else:
        emit_err("No password provided. Use --password PASS or --password-env VAR")
        sys.exit(1)

    port = args.port or (143 if args.no_ssl else 993)

    cache: Optional[Cache] = None
    if not args.no_cache:
        cache = Cache(args.cache_dir, args.host, args.user)
        if args.clear_cache:
            cache.clear()

    pool = ConnectionPool(
        host=args.host, port=port,
        user=args.user, password=password,
        ssl=not args.no_ssl,
        pool_size=args.pool_size,
        throttle_delay=args.throttle_delay,
        max_retries=args.max_retries,
    )

    global _progress
    if getattr(args, "progress", False):
        _progress = Progress(args.command)
        _progress.start()

    try:
        dispatch = {
            "mailboxes": cmd_mailboxes,
            "messages": cmd_messages,
            "fetch": cmd_fetch,
        }
        dispatch[args.command](args, pool, cache)
    except KeyboardInterrupt:
        _shutdown.set()
        _emit_shutdown_progress(pool)
    finally:
        _shutdown.set()  # ensure workers see it even on normal exit
        if _progress:
            _progress.stop()
        pool.close()


if __name__ == "__main__":
    main()
