#!/usr/bin/env node
/**
 * imap-cli — Universal IMAP client CLI (Node.js / ESM)
 * Install: npm install
 * Pipeline: node src/index.js ... mailboxes | node src/index.js ... messages | node src/index.js ... fetch
 */

import { Command } from 'commander';
import { ImapFlow } from 'imapflow';
import Database from 'better-sqlite3';
import { simpleParser } from 'mailparser';
import * as readline from 'readline';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function emitErr(message) {
  process.stderr.write(JSON.stringify({ type: 'error', message }) + '\n');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Address / date helpers
// ---------------------------------------------------------------------------

function formatAddr(a) {
  if (!a) return null;
  if (a.address) return a.name ? `${a.name} <${a.address}>` : a.address;
  return a.name ?? null;
}

function formatAddrs(addrs) {
  if (!addrs) return [];
  return addrs.map(a => formatAddr(a)).filter(s => s !== null);
}

function dateToISO(d) {
  return d ? d.toISOString() : '';
}

function flagsToArray(flags) {
  if (!flags) return [];
  return Array.from(flags);
}

function parseImapDate(s) {
  if (!s) return null;
  const months = { Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11 };
  const m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
  if (!m) return null;
  const month = months[m[2]];
  if (month === undefined) return null;
  return new Date(Date.UTC(parseInt(m[1]), month, parseInt(m[3])));
}

// ---------------------------------------------------------------------------
// SQLite cache
// ---------------------------------------------------------------------------

class Cache {
  constructor(cacheDir, host, user) {
    fs.mkdirSync(cacheDir, { recursive: true });
    const safe = `${host}_${user}`.replace(/[^\w.-]/g, '_');
    this.db = new Database(path.join(cacheDir, `${safe}.db`));
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS mailboxes (
        name TEXT PRIMARY KEY, data TEXT NOT NULL, fetched_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS messages (
        mailbox TEXT NOT NULL, uid INTEGER NOT NULL,
        data TEXT NOT NULL, fetched_at TEXT NOT NULL,
        PRIMARY KEY (mailbox, uid)
      );
      CREATE TABLE IF NOT EXISTS message_content (
        mailbox TEXT NOT NULL, uid INTEGER NOT NULL,
        data TEXT NOT NULL, fetched_at TEXT NOT NULL,
        PRIMARY KEY (mailbox, uid)
      );
    `);
  }

  clear() {
    this.db.exec('DELETE FROM mailboxes; DELETE FROM messages; DELETE FROM message_content;');
  }

  getMailboxes() {
    const rows = this.db.prepare('SELECT data FROM mailboxes ORDER BY name').all();
    return rows.length === 0 ? null : rows.map(r => JSON.parse(r.data));
  }

  setMailboxes(items) {
    const now = new Date().toISOString();
    const del = this.db.prepare('DELETE FROM mailboxes');
    const ins = this.db.prepare('INSERT OR REPLACE INTO mailboxes(name,data,fetched_at) VALUES(?,?,?)');
    this.db.transaction(() => {
      del.run();
      for (const m of items) ins.run(m.name, JSON.stringify(m), now);
    })();
  }

  getMessages(mailbox) {
    const rows = this.db.prepare('SELECT data FROM messages WHERE mailbox=? ORDER BY uid').all(mailbox);
    return rows.length === 0 ? null : rows.map(r => JSON.parse(r.data));
  }

  setMessages(mailbox, items) {
    const now = new Date().toISOString();
    const ins = this.db.prepare('INSERT OR REPLACE INTO messages(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)');
    this.db.transaction(() => {
      for (const m of items) ins.run(mailbox, m.uid, JSON.stringify(m), now);
    })();
  }

  getMessageContent(mailbox, uid) {
    const row = this.db.prepare('SELECT data FROM message_content WHERE mailbox=? AND uid=?').get(mailbox, uid);
    return row ? JSON.parse(row.data) : null;
  }

  setMessageContent(mailbox, uid, obj) {
    this.db.prepare('INSERT OR REPLACE INTO message_content(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)')
      .run(mailbox, uid, JSON.stringify(obj), new Date().toISOString());
  }

  close() { this.db.close(); }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

class ImapPool {
  constructor(opts) {
    this.opts = opts;
    this.idle = [];
    this.waiting = [];
    this.created = 0;
  }

  async acquire() {
    while (this.idle.length > 0) {
      const c = this.idle.pop();
      if (c.usable) return [c, () => this._release(c)];
      this.created--;
    }
    if (this.created < this.opts.poolSize) {
      this.created++;
      try {
        const c = await this._newConn();
        return [c, () => this._release(c)];
      } catch (err) {
        this.created--;
        throw err;
      }
    }
    const c = await new Promise(res => this.waiting.push(res));
    return [c, () => this._release(c)];
  }

  _release(c) {
    if (this.waiting.length > 0) {
      this.waiting.shift()(c);
    } else if (c.usable) {
      this.idle.push(c);
    } else {
      this.created--;
    }
  }

  async _newConn() {
    const { host, port, noSsl, user, password, maxRetries, throttleDelay } = this.opts;
    const resolvedPort = port || (noSsl ? 143 : 993);
    let last;
    for (let i = 0; i < maxRetries; i++) {
      try {
        const c = new ImapFlow({
          host, port: resolvedPort,
          secure: !noSsl,
          auth: { user, pass: password },
          logger: false,
          tls: { rejectUnauthorized: false },
        });
        await c.connect();
        return c;
      } catch (err) {
        last = err;
        if (i < maxRetries - 1) {
          await sleep(Math.pow(2, i) * Math.max(500, throttleDelay * 1000));
        }
      }
    }
    throw last;
  }

  async close() {
    for (const c of this.idle) { try { await c.logout(); } catch {} }
    this.idle = [];
  }
}

// ---------------------------------------------------------------------------
// IMAP operations
// ---------------------------------------------------------------------------

const FETCH_BATCH = 200;

async function opMailboxes(pool, cache, noCache) {
  if (!noCache && cache) {
    const cached = cache.getMailboxes();
    if (cached) { cached.forEach(emit); return; }
  }

  const [client, release] = await pool.acquire();
  try {
    const list = await client.list();
    const results = [];
    for (const mb of list) {
      const obj = {
        type: 'mailbox',
        name: mb.path,
        delimiter: mb.delimiter ?? '/',
        flags: Array.from(mb.flags),
        exists: null,
        unseen: null,
      };
      try {
        const st = await client.status(mb.path, { messages: true, unseen: true });
        obj.exists = st.messages ?? null;
        obj.unseen = st.unseen ?? null;
      } catch { /* some folders don't support STATUS */ }
      results.push(obj);
    }
    if (cache) cache.setMailboxes(results);
    results.forEach(emit);
  } finally {
    release();
  }
}

async function opMessages(pool, mailbox, cache, noCache, limit, since, before) {
  if (!noCache && cache) {
    let cached = cache.getMessages(mailbox);
    if (cached) {
      if (limit > 0 && cached.length > limit) cached = cached.slice(-limit);
      cached.forEach(emit);
      return;
    }
  }

  const [client, release] = await pool.acquire();
  try {
    await client.mailboxOpen(mailbox, { readOnly: true });

    const criteria = {};
    const sinceDate = since ? parseImapDate(since) : null;
    const beforeDate = before ? parseImapDate(before) : null;
    if (sinceDate) criteria.since = sinceDate;
    if (beforeDate) criteria.before = beforeDate;
    if (!sinceDate && !beforeDate) criteria.all = true;

    let uids = await client.search(criteria, { uid: true });
    if (!uids || uids.length === 0) return;
    if (limit > 0 && uids.length > limit) uids = uids.slice(-limit);

    const results = [];
    for (let i = 0; i < uids.length; i += FETCH_BATCH) {
      const batch = uids.slice(i, i + FETCH_BATCH);
      let ok = false;
      for (let attempt = 0; attempt < 3 && !ok; attempt++) {
        try {
          for await (const msg of client.fetch(batch, { envelope: true, flags: true, size: true }, { uid: true })) {
            const env = msg.envelope;
            results.push({
              type: 'message',
              mailbox,
              uid: msg.uid,
              message_id: env?.messageId ?? '',
              subject: env?.subject ?? '',
              from: formatAddrs(env?.from),
              to: formatAddrs(env?.to),
              cc: formatAddrs(env?.cc),
              reply_to: formatAddrs(env?.replyTo),
              date: dateToISO(env?.date),
              size: msg.size ?? 0,
              flags: flagsToArray(msg.flags),
            });
          }
          ok = true;
        } catch (err) {
          if (attempt === 2) emitErr(`fetch batch in ${mailbox}: ${err.message}`);
          else await sleep(Math.pow(2, attempt) * 1000);
        }
      }
    }

    if (cache && results.length > 0) cache.setMessages(mailbox, results);
    results.forEach(emit);
  } finally {
    release();
  }
}

async function opFetch(pool, mailbox, uid, cache, noCache, noAttachments) {
  if (!noCache && cache) {
    const cached = cache.getMessageContent(mailbox, uid);
    if (cached) { emit(cached); return; }
  }

  const [client, release] = await pool.acquire();
  try {
    await client.mailboxOpen(mailbox, { readOnly: true });
    for await (const msg of client.fetch(uid.toString(), { envelope: true, flags: true, size: true, source: true }, { uid: true })) {
      const env = msg.envelope;
      const source = msg.source ?? Buffer.alloc(0);
      const parsed = await simpleParser(source, { skipHtmlToText: true });

      const hdrs = {};
      if (parsed.headers) {
        parsed.headers.forEach((val, key) => {
          if (typeof val === 'string') hdrs[key] = val;
          else if (Array.isArray(val)) hdrs[key] = val.join(', ');
          else if (val instanceof Date) hdrs[key] = val.toISOString();
          else if (val && typeof val === 'object' && 'text' in val) hdrs[key] = val.text;
          else hdrs[key] = String(val);
        });
      }

      const atts = noAttachments ? [] : (parsed.attachments ?? []).map(a => ({
        filename: a.filename ?? '',
        content_type: a.contentType,
        size: a.size ?? (a.content?.length ?? 0),
        content_id: a.contentId ?? '',
      }));

      const obj = {
        type: 'message_content',
        mailbox,
        uid: msg.uid,
        message_id: env?.messageId ?? '',
        subject: env?.subject ?? '',
        from: formatAddrs(env?.from),
        to: formatAddrs(env?.to),
        cc: formatAddrs(env?.cc),
        reply_to: formatAddrs(env?.replyTo),
        date: dateToISO(env?.date),
        size: msg.size ?? 0,
        flags: flagsToArray(msg.flags),
        headers: hdrs,
        body_text: parsed.text ?? '',
        body_html: typeof parsed.html === 'string' ? parsed.html : '',
        attachments: atts,
      };

      if (cache) cache.setMessageContent(mailbox, uid, obj);
      emit(obj);
    }
  } finally {
    release();
  }
}

// ---------------------------------------------------------------------------
// Stdin / parallel helpers
// ---------------------------------------------------------------------------

async function readJsonLines() {
  if (process.stdin.isTTY) return [];
  return new Promise(resolve => {
    const rl = readline.createInterface({ input: process.stdin, terminal: false });
    const items = [];
    rl.on('line', line => {
      line = line.trim();
      if (!line) return;
      try { items.push(JSON.parse(line)); } catch { /* skip */ }
    });
    rl.on('close', () => resolve(items));
  });
}

async function parallelLimit(items, concurrency, fn) {
  const queue = [...items];
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (queue.length > 0) {
        const item = queue.shift();
        try { await fn(item); } catch (err) { emitErr(err.message); }
      }
    },
  );
  await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function addGlobal(cmd) {
  return cmd
    .requiredOption('--host <host>', 'IMAP hostname')
    .option('--port <port>', 'IMAP port (0=auto: 993 TLS / 143 plain)', '0')
    .requiredOption('--user <user>', 'Username')
    .requiredOption('--password <pass>', 'Password')
    .option('--no-ssl', 'Disable TLS')
    .option('--pool-size <n>', 'Connection pool size', '5')
    .option('--throttle-delay <sec>', 'Seconds between requests', '0')
    .option('--max-retries <n>', 'Max retries', '3')
    .option('--cache-dir <dir>', 'Cache directory', path.join(os.homedir(), '.imap-cache'))
    .option('--no-cache', 'Bypass cache')
    .option('--clear-cache', 'Clear cache before running')
    .option('-v, --verbose', 'Verbose logging');
}

function parseGlobal(opts) {
  return {
    host: opts.host,
    port: parseInt(opts.port, 10) || 0,
    user: opts.user,
    password: opts.password,
    noSsl: !opts.ssl,
    poolSize: parseInt(opts.poolSize, 10) || 5,
    throttleDelay: parseFloat(opts.throttleDelay) || 0,
    maxRetries: parseInt(opts.maxRetries, 10) || 3,
    cacheDir: opts.cacheDir,
    noCache: !opts.cache,
    clearCache: !!opts.clearCache,
    verbose: !!opts.verbose,
  };
}

function setupCache(g) {
  if (g.noCache) return null;
  try {
    const c = new Cache(g.cacheDir, g.host, g.user);
    if (g.clearCache) c.clear();
    return c;
  } catch (err) {
    process.stderr.write(`cache unavailable: ${err.message}\n`);
    return null;
  }
}

const prog = new Command();
prog.name('imap-cli').description('Universal IMAP client \u2014 outputs JSON Lines').version('1.0.0');

// mailboxes
const mbsCmd = prog.command('mailboxes').description('List all mailboxes/folders');
addGlobal(mbsCmd);
mbsCmd.action(async (opts) => {
  const g = parseGlobal(opts);
  const pool = new ImapPool(g);
  const cache = setupCache(g);
  try {
    await opMailboxes(pool, cache, g.noCache);
  } catch (err) {
    emitErr(err.message);
  } finally {
    await pool.close();
    cache?.close();
  }
});

// messages
const msgsCmd = prog.command('messages').description('List messages (reads mailbox jsonl from stdin or --mailbox)');
addGlobal(msgsCmd);
msgsCmd
  .option('-m, --mailbox <mailbox>', 'Mailbox name')
  .option('-n, --limit <n>', 'Max messages (0=all)', '0')
  .option('--since <date>', 'Messages since date (e.g. 01-Jan-2024)')
  .option('--before <date>', 'Messages before date')
  .action(async (opts) => {
    const g = parseGlobal(opts);
    const pool = new ImapPool(g);
    const cache = setupCache(g);
    const limit = parseInt(opts.limit, 10) || 0;
    try {
      const stdinItems = await readJsonLines();
      const mailboxes = stdinItems
        .filter(o => o?.type === 'mailbox' && o?.name)
        .map(o => o.name);

      if (mailboxes.length > 0) {
        await parallelLimit(mailboxes, g.poolSize,
          mb => opMessages(pool, mb, cache, g.noCache, limit, opts.since ?? '', opts.before ?? ''));
      } else if (opts.mailbox) {
        await opMessages(pool, opts.mailbox, cache, g.noCache, limit, opts.since ?? '', opts.before ?? '');
      } else {
        emitErr('Provide --mailbox or pipe mailbox JSON Lines from stdin.');
      }
    } catch (err) {
      emitErr(err.message);
    } finally {
      await pool.close();
      cache?.close();
    }
  });

// fetch
const ftchCmd = prog.command('fetch').description('Fetch full message content (reads message jsonl from stdin)');
addGlobal(ftchCmd);
ftchCmd
  .option('-m, --mailbox <mailbox>', 'Mailbox name (with --uid)')
  .option('-u, --uid <uid>', 'Message UID (with --mailbox)')
  .option('--no-attachments', 'Omit attachment bodies')
  .action(async (opts) => {
    const g = parseGlobal(opts);
    const pool = new ImapPool(g);
    const cache = setupCache(g);
    const noAtt = !opts.attachments;
    try {
      const stdinItems = await readJsonLines();
      const messages = stdinItems.filter(o => o?.type === 'message' && o?.mailbox && o?.uid);

      if (messages.length > 0) {
        await parallelLimit(messages, g.poolSize,
          msg => opFetch(pool, msg.mailbox, Number(msg.uid), cache, g.noCache, noAtt));
      } else if (opts.mailbox && opts.uid) {
        await opFetch(pool, opts.mailbox, parseInt(opts.uid, 10), cache, g.noCache, noAtt);
      } else {
        emitErr('Provide --mailbox + --uid or pipe message JSON Lines from stdin.');
      }
    } catch (err) {
      emitErr(err.message);
    } finally {
      await pool.close();
      cache?.close();
    }
  });

prog.parse(process.argv);
