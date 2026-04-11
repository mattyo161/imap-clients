#!/usr/bin/env node
/**
 * imap-cli — Universal IMAP client CLI (TypeScript)
 * Install: npm install && npm run build
 * Pipeline: node dist/index.js ... mailboxes | node dist/index.js ... messages | node dist/index.js ... fetch
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
// Types
// ---------------------------------------------------------------------------

interface GlobalOptions {
  host: string;
  port: number;
  user: string;
  password: string;
  noSsl: boolean;
  poolSize: number;
  throttleDelay: number;
  maxRetries: number;
  cacheDir: string;
  noCache: boolean;
  clearCache: boolean;
  verbose: boolean;
}

interface MailboxObj {
  type: 'mailbox';
  name: string;
  delimiter: string;
  flags: string[];
  exists: number | null;
  unseen: number | null;
}

interface MessageObj {
  type: 'message';
  mailbox: string;
  uid: number;
  message_id: string;
  subject: string;
  from: string[];
  to: string[];
  cc: string[];
  reply_to: string[];
  date: string;
  size: number;
  flags: string[];
}

interface AttachmentObj {
  filename: string;
  content_type: string;
  size: number;
  content_id: string;
}

interface MessageContentObj {
  type: 'message_content';
  mailbox: string;
  uid: number;
  message_id: string;
  subject: string;
  from: string[];
  to: string[];
  cc: string[];
  reply_to: string[];
  date: string;
  size: number;
  flags: string[];
  headers: Record<string, string>;
  body_text: string;
  body_html: string;
  attachments: AttachmentObj[];
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

function emit(obj: unknown): void {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function emitErr(message: string): void {
  process.stderr.write(JSON.stringify({ type: 'error', message }) + '\n');
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Address / date helpers
// ---------------------------------------------------------------------------

function formatAddr(a: { name?: string; address?: string } | null | undefined): string | null {
  if (!a) return null;
  if (a.address) return a.name ? `${a.name} <${a.address}>` : a.address;
  return a.name ?? null;
}

function formatAddrs(addrs: Array<{ name?: string; address?: string }> | undefined): string[] {
  if (!addrs) return [];
  return addrs.map(a => formatAddr(a)).filter((s): s is string => s !== null);
}

function dateToISO(d: Date | undefined): string {
  return d ? d.toISOString() : '';
}

function flagsToArray(flags: Set<string> | string[] | undefined): string[] {
  if (!flags) return [];
  return Array.from(flags);
}

function parseImapDate(s: string): Date | null {
  if (!s) return null;
  const months: Record<string, number> = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11,
  };
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
  private db: Database.Database;

  constructor(cacheDir: string, host: string, user: string) {
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

  clear(): void {
    this.db.exec('DELETE FROM mailboxes; DELETE FROM messages; DELETE FROM message_content;');
  }

  getMailboxes(): MailboxObj[] | null {
    const rows = this.db.prepare('SELECT data FROM mailboxes ORDER BY name').all() as { data: string }[];
    if (rows.length === 0) return null;
    return rows.map(r => JSON.parse(r.data) as MailboxObj);
  }

  setMailboxes(items: MailboxObj[]): void {
    const now = new Date().toISOString();
    const del = this.db.prepare('DELETE FROM mailboxes');
    const ins = this.db.prepare('INSERT OR REPLACE INTO mailboxes(name,data,fetched_at) VALUES(?,?,?)');
    this.db.transaction(() => {
      del.run();
      for (const m of items) ins.run(m.name, JSON.stringify(m), now);
    })();
  }

  getMessages(mailbox: string): MessageObj[] | null {
    const rows = this.db.prepare('SELECT data FROM messages WHERE mailbox=? ORDER BY uid').all(mailbox) as { data: string }[];
    if (rows.length === 0) return null;
    return rows.map(r => JSON.parse(r.data) as MessageObj);
  }

  setMessages(mailbox: string, items: MessageObj[]): void {
    const now = new Date().toISOString();
    const ins = this.db.prepare('INSERT OR REPLACE INTO messages(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)');
    this.db.transaction(() => {
      for (const m of items) ins.run(mailbox, m.uid, JSON.stringify(m), now);
    })();
  }

  getMessageContent(mailbox: string, uid: number): MessageContentObj | null {
    const row = this.db.prepare('SELECT data FROM message_content WHERE mailbox=? AND uid=?').get(mailbox, uid) as { data: string } | undefined;
    return row ? JSON.parse(row.data) as MessageContentObj : null;
  }

  setMessageContent(mailbox: string, uid: number, obj: MessageContentObj): void {
    this.db.prepare('INSERT OR REPLACE INTO message_content(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)')
      .run(mailbox, uid, JSON.stringify(obj), new Date().toISOString());
  }

  close(): void { this.db.close(); }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

class ImapPool {
  private idle: ImapFlow[] = [];
  private waiting: Array<(c: ImapFlow) => void> = [];
  private created = 0;

  constructor(private opts: GlobalOptions) {}

  async acquire(): Promise<[ImapFlow, () => void]> {
    while (this.idle.length > 0) {
      const c = this.idle.pop()!;
      if (c.usable) return [c, () => this.release(c)];
      this.created--;
    }
    if (this.created < this.opts.poolSize) {
      this.created++;
      try {
        const c = await this.newConn();
        return [c, () => this.release(c)];
      } catch (err) {
        this.created--;
        throw err;
      }
    }
    const c = await new Promise<ImapFlow>(res => this.waiting.push(res));
    return [c, () => this.release(c)];
  }

  private release(c: ImapFlow): void {
    if (this.waiting.length > 0) {
      this.waiting.shift()!(c);
    } else if (c.usable) {
      this.idle.push(c);
    } else {
      this.created--;
    }
  }

  private async newConn(): Promise<ImapFlow> {
    const port = this.opts.port || (this.opts.noSsl ? 143 : 993);
    let last: Error | undefined;
    for (let i = 0; i < this.opts.maxRetries; i++) {
      try {
        const c = new ImapFlow({
          host: this.opts.host,
          port,
          secure: !this.opts.noSsl,
          auth: { user: this.opts.user, pass: this.opts.password },
          logger: false,
          tls: { rejectUnauthorized: false },
        });
        await c.connect();
        return c;
      } catch (err: unknown) {
        last = err as Error;
        if (i < this.opts.maxRetries - 1) {
          await sleep(Math.pow(2, i) * Math.max(500, this.opts.throttleDelay * 1000));
        }
      }
    }
    throw last ?? new Error('connection failed');
  }

  async close(): Promise<void> {
    for (const c of this.idle) { try { await c.logout(); } catch {} }
    this.idle = [];
  }
}

// ---------------------------------------------------------------------------
// IMAP operations
// ---------------------------------------------------------------------------

const FETCH_BATCH = 200;

async function opMailboxes(pool: ImapPool, cache: Cache | null, noCache: boolean): Promise<void> {
  if (!noCache && cache) {
    const cached = cache.getMailboxes();
    if (cached) { cached.forEach(emit); return; }
  }

  const [client, release] = await pool.acquire();
  try {
    const list = await client.list();
    const results: MailboxObj[] = [];

    for (const mb of list) {
      const obj: MailboxObj = {
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

async function opMessages(
  pool: ImapPool, mailbox: string, cache: Cache | null, noCache: boolean,
  limit: number, since: string, before: string,
): Promise<void> {
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

    const criteria: Record<string, unknown> = {};
    const sinceDate = since ? parseImapDate(since) : null;
    const beforeDate = before ? parseImapDate(before) : null;
    if (sinceDate) criteria['since'] = sinceDate;
    if (beforeDate) criteria['before'] = beforeDate;
    if (!sinceDate && !beforeDate) criteria['all'] = true;

    let uids = await client.search(criteria as Parameters<typeof client.search>[0], { uid: true });
    if (!uids || uids.length === 0) return;
    if (limit > 0 && uids.length > limit) uids = uids.slice(-limit);

    const results: MessageObj[] = [];

    for (let i = 0; i < uids.length; i += FETCH_BATCH) {
      const batch = uids.slice(i, i + FETCH_BATCH);
      let ok = false;
      for (let attempt = 0; attempt < 3 && !ok; attempt++) {
        try {
          for await (const msg of client.fetch(
            batch,
            { envelope: true, flags: true, size: true },
            { uid: true },
          )) {
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
        } catch (err: unknown) {
          if (attempt === 2) emitErr(`fetch batch in ${mailbox}: ${(err as Error).message}`);
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

async function opFetch(
  pool: ImapPool, mailbox: string, uid: number,
  cache: Cache | null, noCache: boolean, noAttachments: boolean,
): Promise<void> {
  if (!noCache && cache) {
    const cached = cache.getMessageContent(mailbox, uid);
    if (cached) { emit(cached); return; }
  }

  const [client, release] = await pool.acquire();
  try {
    await client.mailboxOpen(mailbox, { readOnly: true });

    for await (const msg of client.fetch(
      uid.toString(),
      { envelope: true, flags: true, size: true, source: true },
      { uid: true },
    )) {
      const env = msg.envelope;
      const source: Buffer = (msg as unknown as { source?: Buffer }).source ?? Buffer.alloc(0);
      const parsed = await simpleParser(source, { skipHtmlToText: true });

      const hdrs: Record<string, string> = {};
      if (parsed.headers) {
        parsed.headers.forEach((val: unknown, key: string) => {
          if (typeof val === 'string') hdrs[key] = val;
          else if (Array.isArray(val)) hdrs[key] = (val as string[]).join(', ');
          else if (val instanceof Date) hdrs[key] = val.toISOString();
          else if (val && typeof val === 'object' && 'text' in val) hdrs[key] = (val as { text: string }).text;
          else hdrs[key] = String(val);
        });
      }

      const atts: AttachmentObj[] = noAttachments ? [] : (parsed.attachments ?? []).map((a) => ({
        filename: a.filename ?? '',
        content_type: a.contentType,
        size: a.size ?? (a.content?.length ?? 0),
        content_id: (a as unknown as { contentId?: string }).contentId ?? '',
      }));

      const obj: MessageContentObj = {
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

async function readJsonLines(): Promise<unknown[]> {
  if (process.stdin.isTTY) return [];
  return new Promise(resolve => {
    const rl = readline.createInterface({ input: process.stdin, terminal: false });
    const items: unknown[] = [];
    rl.on('line', line => {
      line = line.trim();
      if (!line) return;
      try { items.push(JSON.parse(line)); } catch { /* skip malformed */ }
    });
    rl.on('close', () => resolve(items));
  });
}

async function parallelLimit<T>(
  items: T[], concurrency: number, fn: (item: T) => Promise<void>,
): Promise<void> {
  const queue = [...items];
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (queue.length > 0) {
        const item = queue.shift()!;
        try { await fn(item); } catch (err: unknown) { emitErr((err as Error).message); }
      }
    },
  );
  await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// CLI option helpers
// ---------------------------------------------------------------------------

function addGlobal(cmd: Command): Command {
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

function parseGlobal(opts: Record<string, unknown>): GlobalOptions {
  return {
    host: opts['host'] as string,
    port: parseInt(opts['port'] as string, 10) || 0,
    user: opts['user'] as string,
    password: opts['password'] as string,
    noSsl: !(opts['ssl'] as boolean),         // --no-ssl sets ssl=false
    poolSize: parseInt(opts['poolSize'] as string, 10) || 5,
    throttleDelay: parseFloat(opts['throttleDelay'] as string) || 0,
    maxRetries: parseInt(opts['maxRetries'] as string, 10) || 3,
    cacheDir: opts['cacheDir'] as string,
    noCache: !(opts['cache'] as boolean),      // --no-cache sets cache=false
    clearCache: !!(opts['clearCache'] as boolean),
    verbose: !!(opts['verbose'] as boolean),
  };
}

function setupCache(g: GlobalOptions): Cache | null {
  if (g.noCache) return null;
  try {
    const c = new Cache(g.cacheDir, g.host, g.user);
    if (g.clearCache) c.clear();
    return c;
  } catch (err: unknown) {
    process.stderr.write(`cache unavailable: ${(err as Error).message}\n`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

const prog = new Command();
prog.name('imap-cli').description('Universal IMAP client \u2014 outputs JSON Lines').version('1.0.0');

// mailboxes
const mbsCmd = prog.command('mailboxes').description('List all mailboxes/folders');
addGlobal(mbsCmd);
mbsCmd.action(async (opts: Record<string, unknown>) => {
  const g = parseGlobal(opts);
  const pool = new ImapPool(g);
  const cache = setupCache(g);
  try {
    await opMailboxes(pool, cache, g.noCache);
  } catch (err: unknown) {
    emitErr((err as Error).message);
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
  .action(async (opts: Record<string, unknown>) => {
    const g = parseGlobal(opts);
    const pool = new ImapPool(g);
    const cache = setupCache(g);
    const limit = parseInt(opts['limit'] as string, 10) || 0;
    const since = (opts['since'] as string) ?? '';
    const before = (opts['before'] as string) ?? '';
    try {
      const stdinItems = await readJsonLines();
      const mailboxes = (stdinItems as Array<Record<string, unknown>>)
        .filter(o => o?.['type'] === 'mailbox' && o?.['name'])
        .map(o => o['name'] as string);

      if (mailboxes.length > 0) {
        await parallelLimit(mailboxes, g.poolSize,
          mb => opMessages(pool, mb, cache, g.noCache, limit, since, before));
      } else if (opts['mailbox']) {
        await opMessages(pool, opts['mailbox'] as string, cache, g.noCache, limit, since, before);
      } else {
        emitErr('Provide --mailbox or pipe mailbox JSON Lines from stdin.');
      }
    } catch (err: unknown) {
      emitErr((err as Error).message);
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
  .action(async (opts: Record<string, unknown>) => {
    const g = parseGlobal(opts);
    const pool = new ImapPool(g);
    const cache = setupCache(g);
    const noAtt = !(opts['attachments'] as boolean);
    try {
      const stdinItems = await readJsonLines();
      type MsgInput = { type: string; mailbox: string; uid: number };
      const messages = (stdinItems as Array<Record<string, unknown>>)
        .filter(o => o?.['type'] === 'message' && o?.['mailbox'] && o?.['uid']) as MsgInput[];

      if (messages.length > 0) {
        await parallelLimit(messages, g.poolSize,
          msg => opFetch(pool, msg.mailbox, Number(msg.uid), cache, g.noCache, noAtt));
      } else if (opts['mailbox'] && opts['uid']) {
        await opFetch(pool, opts['mailbox'] as string, parseInt(opts['uid'] as string, 10),
          cache, g.noCache, noAtt);
      } else {
        emitErr('Provide --mailbox + --uid or pipe message JSON Lines from stdin.');
      }
    } catch (err: unknown) {
      emitErr((err as Error).message);
    } finally {
      await pool.close();
      cache?.close();
    }
  });

prog.parse(process.argv);
