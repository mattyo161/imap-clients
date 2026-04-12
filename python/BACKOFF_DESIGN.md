# Design Decision Document: Backoff and Throttle Handling

**Component:** `python/imap_cli.py` — `ConnectionPool` + op retry loops  
**Status:** Implemented  
**Last updated:** 2026-04-12

---

## Problem Statement

IMAP servers — particularly hosted ones (e.g. Exchange Online, Gmail) — will
throttle clients that open too many parallel connections or issue too many
requests in a short window.  The symptom is a `[SERVERBUG]` or similar
bracketed IMAP response code on `SELECT`, `SEARCH`, or `FETCH`.

The naive fix is to catch the error and retry.  That alone is insufficient for
three reasons that were discovered in production:

1. **`select_folder` was not retried.** The original code only caught errors
   inside `conn.fetch()`.  A throttle error on `SELECT` silently returned
   empty results.
2. **Broken connections were recycled.** When an error occurred *inside* a
   `with pool.acquire()` block, the connection was returned to the pool in a
   broken state.  The next request to dequeue it would call `noop()`, which
   also failed, and the thread would create a new connection — but the broken
   one was never explicitly discarded, creating a slow leak.
3. **Retries at full concurrency made things worse.** Five threads hitting the
   server simultaneously, each retrying on the same schedule, meant the server
   saw the same burst that triggered throttling, just five seconds later.

---

## Goals

- Recover automatically from transient IMAP throttle errors.
- Reduce server pressure immediately when throttling is detected.
- Ensure every retry uses a fresh, uncompromised TCP connection.
- Give the server enough wall-clock time to clear its throttle window.
- Make the backoff visible in structured output so operators can diagnose it.

---

## Key Decisions

### Decision 1: Single `acquire()` scope covering the full operation

**What:** `select_folder`, `search`/`fetch` are all performed inside a
single `with pool.acquire() as conn:` block.

**Why:** Any exception propagating out of the `with` block triggers the
`except Exception: conn = None; raise` path in `acquire()`, which discards the
connection rather than returning it to the pool.  This guarantees that a
connection that received a throttle error is never reused.

**Rejected alternative:** Catching the error inside the `with` block and
returning a sentinel value.  That keeps the connection alive in the pool,
which risks replaying the same throttled session state on the next request.

---

### Decision 2: `for...else` retry loop

**What:** The retry loop is a Python `for attempt in range(max_retries): ...
else: <exhausted>`.  The `else` block only executes if the loop body never
called `break` (i.e., all attempts failed).

**Why:** Cleanly separates the "retry exhausted" path from the normal error
path without needing a sentinel flag variable.  `break` on success short-
circuits both the loop and the `else`.

---

### Decision 3: Exponential backoff at `5 * (2 ** attempt)` seconds

**What:** Delays are 5 s, 10 s, 20 s, 40 s for attempts 1–4 (attempt 0 is
the first try, no delay before it).

**Why:**
- 1 s base was tried first and still produced throttle cascades — the server's
  throttle window outlasted the delay.
- 5 s base gave enough wall time for the server to release the throttle.
- Doubling each time ensures later attempts are aggressive about backing off
  without making the first recovery too slow.

**Trade-off:** With `max_retries=5`, worst-case a single item waits
5+10+20+40 = 75 s before failing.  Accepted as a better outcome than silent
data loss.

---

### Decision 4: `_ResizableSemaphore` — runtime-adjustable concurrency limit

**What:** A custom semaphore backed by `threading.Condition` whose upper bound
can be changed while threads are blocked on it.

**Why:** The standard `threading.Semaphore` has a fixed count.  We need to:
- Start at `pool_size` (e.g. 5) for normal operation.
- Drop to 1 the moment throttling is detected (serialise retries).
- Restore to `pool_size` once the throttle event counter reaches zero.

`threading.BoundedSemaphore` cannot be resized.  A `Condition` with a simple
integer counter can.

**Recovery:** `on_success()` decrements `_throttle_events`.  When the counter
reaches zero (all in-flight throttled operations have either succeeded or
exhausted retries), the limit is restored to `pool_size`.

---

### Decision 5: Global backoff deadline (`_backoff_until`)

**What:** A monotonic timestamp stored on the pool.  `acquire()` busy-waits
(in 0.5 s increments) until `time.monotonic() >= _backoff_until` before
competing for the semaphore.

**Why:** Even with the semaphore reduced to 1, threads that were already
sleeping in their per-attempt delay would wake up and immediately try to
acquire the (now single) slot.  They would proceed sequentially but with zero
additional gap between them.  The global deadline ensures that no thread
*enters* a new IMAP operation until the server's backoff window has elapsed,
regardless of how many threads were queued.

**Interaction with `on_throttle`:** `on_throttle(backoff_seconds)` sets
`_backoff_until = max(_backoff_until, now + backoff_seconds)`.  Using `max`
means that if multiple threads call `on_throttle` concurrently with different
delay values, the deadline is always extended to the furthest point, never
shortened.

---

### Decision 6: `_flush_idle()` — close idle connections on throttle detection

**What:** When `on_throttle()` is called, all connections currently sitting
idle in the pool queue are dequeued and logged out before the next acquire.

**Why:** A throttled server is throttling the client's IP (or authenticated
session).  Idle connections that were opened before the throttle event are
likely to receive the same error on their next use.  Discarding them forces
`acquire()` to call `_new_conn()`, which creates a fresh TCP connection after
the backoff delay has elapsed.

**Order of operations:**
1. Lock: increment `_throttle_events`, extend `_backoff_until`, `set_limit(1)`.
2. Unlock (release `_throttle_lock`).
3. `_flush_idle()` — outside the lock to avoid blocking `acquire()` longer
   than necessary.

**Limitation:** Only *idle* connections are flushed.  Connections that are
currently in-use by other threads will be discarded organically: any exception
inside their `with pool.acquire()` block sets `conn = None`, so they are never
returned to the pool.

---

### Decision 7: Progress counter `_paused` — active count drops during sleep

**What:** Before calling `emit_pause()` (which calls `time.sleep(delay)`), the
worker calls `_progress.start_pause()`, which moves the thread's slot from
`active` to `paused`.  After the sleep, `end_pause()` moves it back.

**Why:** Without this, `active=5` stayed constant throughout a throttle event
even though all five threads were sleeping and no real IMAP work was happening.
The operator had no visibility into whether the tool was working or stuck.

**Displayed:** The progress spinner renders `paused=N` only when `N > 0`, so
it does not clutter clean runs.

---

### Decision 8: `emit_pause()` sleeps inside the function

**What:** `emit_pause(delay, ...)` writes the JSON Lines record to stderr and
then calls `time.sleep(delay)` before returning.

**Why:** Keeps the caller simple — no need for a separate `time.sleep` after
each `emit_pause` call.  The record is always written atomically with the lock
held, so the timestamp in the record accurately reflects when the sleep *starts*
rather than when it was scheduled.

**Consequence for `start_pause`/`end_pause`:** The caller must bracket the
`emit_pause` call:
```python
if _progress:
    _progress.start_pause()
try:
    emit_pause(delay, ...)
finally:
    if _progress:
        _progress.end_pause()
```
The `finally` ensures `end_pause` is always called even if the thread is
interrupted mid-sleep.

---

### Decision 9: Thread name in error/pause records

**What:** Both `emit_err()` and `emit_pause()` include
`"thread": threading.current_thread().name` in the output JSON.

**Why:** When five threads are retrying simultaneously, the error log would
show multiple interleaved records with no way to associate them.  Thread names
from `ThreadPoolExecutor` follow the pattern `ThreadPoolExecutor-0_N`, which
is stable within a process invocation and sufficient to group related records.

**Rejected alternative:** Include a sequential connection ID.  Connection IDs
are assigned per `_new_conn()` call, but a thread may attempt multiple
connections across retries, and the ID is not easily accessible at the
`emit_err` call site without threading.local.  Thread name achieves the same
correlation goal with less plumbing.  Connection IDs are stamped on the
`IMAPClient` object (`_imap_cli_conn_id`) for future use.

---

## Full Backoff Flow (Sequence)

```
Thread T1 calls op_fetch (attempt 0)
  └─ acquire(): _backoff_until=0, so no wait
     └─ _sem.acquire() → OK (count < limit)
     └─ _pool empty → _new_conn() → fresh connection C1
     └─ select_folder() → [SERVERBUG] ← throttled!
     └─ exception propagates out of `with` → conn=None (C1 discarded)

T1 except block (attempt 0):
  delay = 5s
  on_throttle(5):
    _throttle_events = 1
    _backoff_until = now + 5s
    _sem.set_limit(1)          ← other threads now blocked
    _flush_idle()              ← any cached connections closed

  _progress.start_pause()     ← active drops by 1
  emit_pause(5s, ...)         ← writes JSON record, sleeps 5s
  _progress.end_pause()       ← active restores

Thread T2 calls acquire() during T1's sleep:
  acquire(): remaining = 4.8s → sleep(0.5s) × ~10
  (T2 spends the 5s spinning on _backoff_until, not inside the server)

T1 wakes (attempt 1):
  acquire(): _backoff_until elapsed → OK
  _sem.acquire() → OK (T2 is still in the backoff wait loop, not competing yet)
  _new_conn() → fresh connection C2
  select_folder() → OK
  fetch() → OK
  on_success():
    _throttle_events = 0
    _sem.set_limit(pool_size)  ← concurrency restored

T2 eventually exits backoff wait, acquires semaphore, proceeds normally.
```

---

## Parameters and Defaults

| Parameter | Default | CLI flag | Effect |
|---|---|---|---|
| `pool_size` | 5 | `--pool-size` | Normal concurrency; semaphore upper limit |
| `max_retries` | 5 | `--max-retries` | Max attempts per operation |
| `throttle_delay` | 0.0 s | `--throttle-delay` | Fixed per-request delay (manual rate limiting) |
| Backoff base | 5 s | (hardcoded) | First retry delay |
| Backoff factor | 2× | (hardcoded) | Multiplier per attempt |

Delays by attempt: attempt 1 → 5 s, attempt 2 → 10 s, attempt 3 → 20 s,
attempt 4 → 40 s.  Maximum wait before final failure: 75 s.

---

## Known Limitations and Future Work

- **`_backoff_until` is not per-operation-type.** If `fetch` throttles, the
  `messages` workers in other threads also wait.  This is intentional (the
  throttle is IP-level), but could be refined if evidence shows the server
  throttles per-command.
- **No jitter.** All threads wake at roughly the same time after backoff, which
  could produce a small burst.  Adding `random.uniform(0, 1)` to the delay
  would spread them out.
- **`on_success` counter can undercount.** If a thread calls `on_throttle` and
  then exhausts all retries (never calls `on_success`), `_throttle_events`
  stays elevated and the semaphore limit is never restored.  In practice this
  self-corrects when a *subsequent* operation succeeds, but it means the pool
  can stay at concurrency=1 longer than necessary after a hard failure.
- **`throttle_delay` and `_backoff_until` are additive.** A non-zero
  `--throttle-delay` adds on top of the backoff wait.  This is correct (manual
  rate limiting is independent of error recovery) but may surprise users who
  set both.
