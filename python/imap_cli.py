#!/usr/bin/env python3
"""
imap-cli — Universal IMAP client CLI (Python implementation)

Dependencies:  pip install imapclient
Pipeline:      imap-cli ... mailboxes | imap-cli ... messages | imap-cli ... fetch
"""

import argparse
import contextlib
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
FETCH_BATCH = 200  # UIDs per FETCH command


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

class ConnectionPool:
    """Thread-safe pool of authenticated IMAPClient connections."""

    def __init__(self, host: str, port: int, user: str, password: str,
                 ssl: bool, pool_size: int = 5,
                 throttle_delay: float = 0.0, max_retries: int = 3):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssl = ssl
        self.pool_size = pool_size
        self.throttle_delay = throttle_delay
        self.max_retries = max_retries
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._sem = threading.Semaphore(pool_size)

    def _new_conn(self) -> IMAPClient:
        for attempt in range(self.max_retries):
            try:
                c = IMAPClient(
                    host=self.host, port=self.port,
                    ssl=self.ssl, use_uid=True, timeout=30,
                )
                c.login(self.user, self.password)
                return c
            except Exception as exc:
                if attempt >= self.max_retries - 1:
                    raise
                delay = (2 ** attempt) * max(0.5, self.throttle_delay)
                emit_err(str(exc), api="connect",
                         error_code=_imap_error_code(exc))
                emit_pause(delay, api="connect", attempt=attempt + 1)
        raise RuntimeError("unreachable")

    @contextlib.contextmanager
    def acquire(self):
        """Yield a live IMAPClient; return it to the pool afterwards."""
        self._sem.acquire()
        conn = None
        try:
            try:
                conn = self._pool.get_nowait()
                conn.noop()  # liveness check
            except (queue.Empty, Exception):
                conn = None
            if conn is None:
                conn = self._new_conn()
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
            yield from cached
            return

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
            items = cached if not limit else cached[-limit:]
            yield from items
            return

    results: List[Dict] = []
    with pool.acquire() as conn:
        try:
            conn.select_folder(mailbox, readonly=True)
        except Exception as exc:
            emit_err(str(exc), api="messages", mailbox=mailbox,
                     error_code=_imap_error_code(exc))
            return

        criteria: List[Any] = []
        if since:
            criteria += ["SINCE", _to_imap_date(since)]
        if before:
            criteria += ["BEFORE", _to_imap_date(before)]
        if not criteria:
            criteria = ["ALL"]

        try:
            uids: List[int] = conn.search(criteria)
        except Exception as exc:
            emit_err(str(exc), api="messages", mailbox=mailbox,
                     error_code=_imap_error_code(exc))
            return

        if not uids:
            return
        if limit:
            uids = uids[-limit:]

        for i in range(0, len(uids), FETCH_BATCH):
            batch = uids[i: i + FETCH_BATCH]
            for attempt in range(pool.max_retries):
                try:
                    data = conn.fetch(batch, ["ENVELOPE", "FLAGS", "RFC822.SIZE", "INTERNALDATE"])
                    break
                except Exception as exc:
                    if attempt >= pool.max_retries - 1:
                        emit_err(str(exc), api="messages", mailbox=mailbox,
                                 error_code=_imap_error_code(exc))
                        data = {}
                    else:
                        emit_pause(2 ** attempt, api="messages",
                                   mailbox=mailbox, attempt=attempt + 1)

            for uid, item in data.items():
                env_raw = item.get(b"ENVELOPE")
                if env_raw is None:
                    continue
                env = parse_envelope(env_raw)
                internal_date_raw = item.get(b"INTERNALDATE")
                results.append({
                    "type": "message",
                    "mailbox": mailbox,
                    "uid": uid,
                    **env,
                    "internal_date": parse_date(internal_date_raw) if internal_date_raw else "",
                    "size": item.get(b"RFC822.SIZE", 0),
                    "flags": flags_to_list(item.get(b"FLAGS", [])),
                })

    if cache and results:
        cache.set_messages(mailbox, results)
    yield from results


def op_fetch(pool: ConnectionPool, mailbox: str, uid: int,
             cache: Optional[Cache], no_cache: bool,
             no_attachments: bool) -> Optional[Dict]:
    if not no_cache and cache:
        cached = cache.get_message_content(mailbox, uid)
        if cached is not None:
            log.debug("fetch: cache hit %s/%d", mailbox, uid)
            return cached

    with pool.acquire() as conn:
        try:
            conn.select_folder(mailbox, readonly=True)
        except Exception as exc:
            emit_err(str(exc), api="fetch", mailbox=mailbox, uid=uid,
                     error_code=_imap_error_code(exc))
            return None

        for attempt in range(pool.max_retries):
            try:
                data = conn.fetch([uid], ["ENVELOPE", "FLAGS", "RFC822.SIZE", "INTERNALDATE", "RFC822"])
                break
            except Exception as exc:
                if attempt >= pool.max_retries - 1:
                    emit_err(str(exc), api="fetch", mailbox=mailbox, uid=uid,
                             error_code=_imap_error_code(exc))
                    return None
                emit_pause(2 ** attempt, api="fetch", mailbox=mailbox,
                           uid=uid, attempt=attempt + 1)

        if uid not in data:
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

    if cache:
        cache.set_message_content(mailbox, uid, obj)
    return obj


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


class Progress:
    """
    Live progress display rendered to stderr (only when stderr is a TTY).

    Thread-safe: all mutable state is protected by an internal lock.
    Coordinates with _output_lock so error/pause lines never interleave
    with the spinner line.

    Displayed fields:
      spinner  elapsed  requests=N  queue=N  avg=N.NNNs/req
    """

    _SPINNERS = r'|/-\\'
    _WIDTH = 80  # assumed max line width for blanking

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._start = time.monotonic()
        self._requests_done: int = 0
        self._queue_size: int = 0
        self._total_time: float = 0.0
        self._spinner_idx: int = 0
        self._running: bool = False
        self._tty: bool = sys.stderr.isatty()
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if not self._tty:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="progress"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=0.5)
        if self._tty:
            with _output_lock:
                sys.stderr.write("\r" + " " * self._WIDTH + "\r")
                sys.stderr.flush()

    # -- stats API (called from worker threads) ------------------------------

    def add_queue(self, n: int = 1) -> None:
        with self._lock:
            self._queue_size += n

    def record_call(self, elapsed: float) -> None:
        """Mark one item complete; decrements queue and accumulates timing."""
        with self._lock:
            self._requests_done += 1
            self._queue_size = max(0, self._queue_size - 1)
            self._total_time += elapsed

    # -- line management (called inside _output_lock) -----------------------

    def clear_line(self) -> None:
        """Blank the progress line. MUST be called while _output_lock is held."""
        if self._tty and self._running:
            sys.stderr.write("\r" + " " * self._WIDTH + "\r")

    # -- internal ------------------------------------------------------------

    def _loop(self) -> None:
        while self._running:
            self._render()
            time.sleep(0.1)

    def _render(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._start
            done = self._requests_done
            queue = self._queue_size
            total = self._total_time
            ch = self._SPINNERS[self._spinner_idx % len(self._SPINNERS)]
            self._spinner_idx += 1

        avg = total / done if done else 0.0

        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)
        s = elapsed % 60
        if h:
            elapsed_str = f"{h}h{m:02d}m{s:04.1f}s"
        elif m:
            elapsed_str = f"{m}m{s:04.1f}s"
        else:
            elapsed_str = f"{s:.1f}s"

        line = (
            f"\r{ch} {elapsed_str}"
            f"  requests={done}"
            f"  queue={queue}"
            f"  avg={avg:.3f}s/req"
        )
        # Pad to overwrite any longer previous line, keep cursor on same line
        pad = max(0, self._WIDTH - len(line) - 1)
        with _output_lock:
            sys.stderr.write(line + " " * pad)
            sys.stderr.flush()


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
) -> None:
    """Write a structured error record to stderr as JSON Lines."""
    record: Dict[str, Any] = {
        "type": "error",
        "timestamp": now_iso(),
        "pid": _PID,
        "api": api,
        "message": message,
        "error_code": error_code,
    }
    if mailbox:
        record["mailbox"] = mailbox
    if uid is not None:
        record["uid"] = uid
    if message_id:
        record["message_id"] = message_id
    with _output_lock:
        if _progress:
            _progress.clear_line()
        sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stderr.flush()


def emit_pause(
    delay: float,
    *,
    api: str = "",
    mailbox: str = "",
    uid: Optional[int] = None,
    attempt: int = 0,
) -> None:
    """Write a backoff-pause record to stderr as JSON Lines, then sleep."""
    record: Dict[str, Any] = {
        "type": "pause",
        "timestamp": now_iso(),
        "pid": _PID,
        "api": api,
        "delay_seconds": round(delay, 3),
        "attempt": attempt,
    }
    if mailbox:
        record["mailbox"] = mailbox
    if uid is not None:
        record["uid"] = uid
    with _output_lock:
        if _progress:
            _progress.clear_line()
        sys.stderr.write(json.dumps(record, ensure_ascii=False) + "\n")
        sys.stderr.flush()
    time.sleep(delay)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_mailboxes(args, pool: ConnectionPool, cache: Optional[Cache]):
    if _progress:
        _progress.add_queue(1)
    t0 = time.monotonic()
    try:
        for mb in op_mailboxes(pool, cache, args.no_cache):
            emit(mb)
    finally:
        if _progress:
            _progress.record_call(time.monotonic() - t0)


def cmd_messages(args, pool: ConnectionPool, cache: Optional[Cache]):
    limit = getattr(args, "limit", None)
    since = getattr(args, "since", None)
    before = getattr(args, "before", None)

    def process(mailbox: str):
        t0 = time.monotonic()
        try:
            for msg in op_messages(pool, mailbox, cache,
                                   no_cache=args.no_cache,
                                   limit=limit, since=since, before=before):
                emit(msg)
        finally:
            if _progress:
                _progress.record_call(time.monotonic() - t0)

    if not sys.stdin.isatty():
        with ThreadPoolExecutor(max_workers=args.pool_size) as ex:
            futs = []
            for line in sys.stdin:
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
                try:
                    f.result()
                except Exception as exc:
                    emit_err(str(exc))
    elif getattr(args, "mailbox", None):
        if _progress:
            _progress.add_queue(1)
        process(args.mailbox)
    else:
        emit_err("Provide --mailbox or pipe mailbox JSON Lines from stdin.")


def cmd_fetch(args, pool: ConnectionPool, cache: Optional[Cache]):
    no_att = getattr(args, "no_attachments", False)

    def process(mailbox: str, uid: int):
        t0 = time.monotonic()
        try:
            obj = op_fetch(pool, mailbox, uid, cache,
                           no_cache=args.no_cache, no_attachments=no_att)
            if obj:
                emit(obj)
        finally:
            if _progress:
                _progress.record_call(time.monotonic() - t0)

    if not sys.stdin.isatty():
        with ThreadPoolExecutor(max_workers=args.pool_size) as ex:
            futs = []
            for line in sys.stdin:
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
                    futs.append(ex.submit(process, obj["mailbox"], int(obj["uid"])))
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as exc:
                    emit_err(str(exc))
    elif getattr(args, "mailbox", None) and getattr(args, "uid", None) is not None:
        if _progress:
            _progress.add_queue(1)
        process(args.mailbox, int(args.uid))
    else:
        emit_err("Provide --mailbox + --uid or pipe message JSON Lines from stdin.")


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
    g.add_argument("--max-retries", type=int, default=3)

    c = p.add_argument_group("cache")
    c.add_argument("--cache-dir", default=os.path.expanduser("~/.imap-cache"))
    c.add_argument("--no-cache", action="store_true", help="Bypass cache")
    c.add_argument("--clear-cache", action="store_true",
                   help="Wipe cache before running")

    p.add_argument("--verbose", "-v", action="store_true")
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

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
    )

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
        _progress = Progress()
        _progress.start()

    try:
        dispatch = {
            "mailboxes": cmd_mailboxes,
            "messages": cmd_messages,
            "fetch": cmd_fetch,
        }
        dispatch[args.command](args, pool, cache)
    except KeyboardInterrupt:
        pass
    finally:
        if _progress:
            _progress.stop()
        pool.close()


if __name__ == "__main__":
    main()
