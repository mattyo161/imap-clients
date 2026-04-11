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


def parse_headers(raw: bytes) -> Dict[str, str]:
    try:
        msg = email_lib.message_from_bytes(raw)
        return {k: decode_header(v) for k, v in msg.items()}
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
                log.warning("connect attempt %d failed: %s — retry in %.1fs", attempt + 1, exc, delay)
                time.sleep(delay)
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
                log.debug("folder_status(%s): %s", name, exc)
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
            log.error("select_folder(%s): %s", mailbox, exc)
            return

        criteria: List[Any] = []
        if since:
            criteria += ["SINCE", since]
        if before:
            criteria += ["BEFORE", before]
        if not criteria:
            criteria = ["ALL"]

        try:
            uids: List[int] = conn.search(criteria)
        except Exception as exc:
            log.error("search(%s): %s", mailbox, exc)
            return

        if not uids:
            return
        if limit:
            uids = uids[-limit:]

        for i in range(0, len(uids), FETCH_BATCH):
            batch = uids[i: i + FETCH_BATCH]
            for attempt in range(pool.max_retries):
                try:
                    data = conn.fetch(batch, ["ENVELOPE", "FLAGS", "RFC822.SIZE"])
                    break
                except Exception as exc:
                    if attempt >= pool.max_retries - 1:
                        log.error("fetch batch: %s", exc)
                        data = {}
                    else:
                        time.sleep(2 ** attempt)

            for uid, item in data.items():
                env_raw = item.get(b"ENVELOPE")
                if env_raw is None:
                    continue
                env = parse_envelope(env_raw)
                results.append({
                    "type": "message",
                    "mailbox": mailbox,
                    "uid": uid,
                    **env,
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
            log.error("select_folder(%s): %s", mailbox, exc)
            return None

        for attempt in range(pool.max_retries):
            try:
                data = conn.fetch([uid], ["ENVELOPE", "FLAGS", "RFC822.SIZE", "RFC822"])
                break
            except Exception as exc:
                if attempt >= pool.max_retries - 1:
                    log.error("fetch(%s/%d): %s", mailbox, uid, exc)
                    return None
                time.sleep(2 ** attempt)

        if uid not in data:
            return None
        item = data[uid]
        env_raw = item.get(b"ENVELOPE")
        if env_raw is None:
            return None

        env = parse_envelope(env_raw)
        raw = item.get(b"RFC822", b"")
        body_text, body_html, atts = parse_body(raw)
        hdrs = parse_headers(raw)

        obj: Dict = {
            "type": "message_content",
            "mailbox": mailbox,
            "uid": uid,
            **env,
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


def emit_err(msg: str, **kw):
    with _output_lock:
        sys.stderr.write(
            json.dumps({"type": "error", "message": msg, **kw}, ensure_ascii=False) + "\n"
        )
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_mailboxes(args, pool: ConnectionPool, cache: Optional[Cache]):
    for mb in op_mailboxes(pool, cache, args.no_cache):
        emit(mb)


def cmd_messages(args, pool: ConnectionPool, cache: Optional[Cache]):
    limit = getattr(args, "limit", None)
    since = getattr(args, "since", None)
    before = getattr(args, "before", None)

    def process(mailbox: str):
        for msg in op_messages(pool, mailbox, cache,
                               no_cache=args.no_cache,
                               limit=limit, since=since, before=before):
            emit(msg)

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
                    futs.append(ex.submit(process, obj["name"]))
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as exc:
                    emit_err(str(exc))
    elif getattr(args, "mailbox", None):
        process(args.mailbox)
    else:
        emit_err("Provide --mailbox or pipe mailbox JSON Lines from stdin.")


def cmd_fetch(args, pool: ConnectionPool, cache: Optional[Cache]):
    no_att = getattr(args, "no_attachments", False)

    def process(mailbox: str, uid: int):
        obj = op_fetch(pool, mailbox, uid, cache,
                       no_cache=args.no_cache, no_attachments=no_att)
        if obj:
            emit(obj)

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
                    futs.append(ex.submit(process, obj["mailbox"], int(obj["uid"])))
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as exc:
                    emit_err(str(exc))
    elif getattr(args, "mailbox", None) and getattr(args, "uid", None) is not None:
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
    g.add_argument("--password", required=True, help="Password")
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

    port = args.port or (143 if args.no_ssl else 993)

    cache: Optional[Cache] = None
    if not args.no_cache:
        cache = Cache(args.cache_dir, args.host, args.user)
        if args.clear_cache:
            cache.clear()

    pool = ConnectionPool(
        host=args.host, port=port,
        user=args.user, password=args.password,
        ssl=not args.no_ssl,
        pool_size=args.pool_size,
        throttle_delay=args.throttle_delay,
        max_retries=args.max_retries,
    )

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
        pool.close()


if __name__ == "__main__":
    main()
