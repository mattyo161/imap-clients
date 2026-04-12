#!/usr/bin/env node
/**
 * IMAP CLI Tool
 * Production-quality TypeScript IMAP client with connection pooling,
 * SQLite caching, and JSON Lines output.
 */

import { program } from 'commander';
import ImapFlow, { ListResponse, FetchMessageObject } from 'imapflow';
import Database from 'better-sqlite3';
import { simpleParser, ParsedMail, AddressObject, Attachment } from 'mailparser';
import * as readline from 'readline';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GlobalOptions {
  host: string;
  port: number;
  user: string;
  password: string;
  ssl: boolean;
  poolSize: number;
  throttleDelay: number;
  maxRetries: number;
  cacheDir: string;
  cache: boolean;
  clearCache: boolean;
  verbose: boolean;
}

interface MailboxRecord {
  type: 'mailbox';
  name: string;
  delimiter: string;
  flags: string[];
  exists: number | null;
  unseen: number | null;
}

interface MessageRecord {
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

interface AttachmentRecord {
  filename: string;
  content_type: string;
  size: number;
  content_id: string;
}

interface MessageContentRecord extends MessageRecord {
  headers: Record<string, string>;
  body_text: string;
  body_html: string;
  attachments: AttachmentRecord[];
}

type OutputRecord = MailboxRecord | MessageRecord | MessageContentRecord | ErrorRecord;

interface ErrorRecord {
  type: 'error';
  message: string;
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

function emit(obj: OutputRecord): void {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function emitErr(msg: string): void {
  process.stderr.write(JSON.stringify({ type: 'error', message: msg }) + '\n');
}

function log(msg: string, opts: GlobalOptions): void {
  if (opts.verbose) {
    process.stderr.write(`[verbose] ${msg}\n`);
  }
}

// ---------------------------------------------------------------------------
// Address formatting
// ---------------------------------------------------------------------------

interface EnvelopeAddress {
  name?: string | null;
  address?: string | null;
}

function formatAddress(addr: EnvelopeAddress): string {
  const name = addr.name?.trim() ?? '';
  const email = addr.address?.trim() ?? '';
  if (name && email) {
    return `${name} <${email}>`;
  }
  return email || name;
}

function formatAddressList(addrs: EnvelopeAddress[] | null | undefined): string[] {
  if (!addrs || !Array.isArray(addrs)) return [];
  return addrs.map(formatAddress).filter((a) => a.length > 0);
}

// ---------------------------------------------------------------------------
// Cache (SQLite)
// ---------------------------------------------------------------------------

let dbInstance: Database.Database | null = null;

function openDb(cacheDir: string): Database.Database {
  if (dbInstance) return dbInstance;
  fs.mkdirSync(cacheDir, { recursive: true });
  const dbPath = path.join(cacheDir, 'imap-cache.db');
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS mailboxes (
      name TEXT PRIMARY KEY,
      data TEXT NOT NULL,
      fetched_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
      mailbox TEXT NOT NULL,
      uid INTEGER NOT NULL,
      data TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      PRIMARY KEY (mailbox, uid)
    );
    CREATE TABLE IF NOT EXISTS message_content (
      mailbox TEXT NOT NULL,
      uid INTEGER NOT NULL,
      data TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      PRIMARY KEY (mailbox, uid)
    );
  `);
  dbInstance = db;
  return db;
}

function clearCache(cacheDir: string): void {
  const db = openDb(cacheDir);
  db.exec(`DELETE FROM mailboxes; DELETE FROM messages; DELETE FROM message_content;`);
}

function getCachedMailboxes(db: Database.Database): MailboxRecord[] | null {
  const row = db.prepare(`SELECT data FROM mailboxes LIMIT 1`).get() as
    | { data: string }
    | undefined;
  if (!row) return null;
  // Return all rows
  const rows = db.prepare(`SELECT data FROM mailboxes`).all() as { data: string }[];
  return rows.map((r) => JSON.parse(r.data) as MailboxRecord);
}

function cacheMailboxes(db: Database.Database, boxes: MailboxRecord[]): void {
  const now = new Date().toISOString();
  const upsert = db.prepare(
    `INSERT OR REPLACE INTO mailboxes (name, data, fetched_at) VALUES (?, ?, ?)`
  );
  const transaction = db.transaction((items: MailboxRecord[]) => {
    for (const box of items) {
      upsert.run(box.name, JSON.stringify(box), now);
    }
  });
  transaction(boxes);
}

function getCachedMessages(db: Database.Database, mailbox: string): MessageRecord[] | null {
  const rows = db
    .prepare(`SELECT data FROM messages WHERE mailbox = ?`)
    .all(mailbox) as { data: string }[];
  if (rows.length === 0) return null;
  return rows.map((r) => JSON.parse(r.data) as MessageRecord);
}

function cacheMessages(db: Database.Database, mailbox: string, msgs: MessageRecord[]): void {
  const now = new Date().toISOString();
  const upsert = db.prepare(
    `INSERT OR REPLACE INTO messages (mailbox, uid, data, fetched_at) VALUES (?, ?, ?, ?)`
  );
  const transaction = db.transaction((items: MessageRecord[]) => {
    for (const msg of items) {
      upsert.run(mailbox, msg.uid, JSON.stringify(msg), now);
    }
  });
  transaction(msgs);
}

function getCachedMessageContent(
  db: Database.Database,
  mailbox: string,
  uid: number
): MessageContentRecord | null {
  const row = db
    .prepare(`SELECT data FROM message_content WHERE mailbox = ? AND uid = ?`)
    .get(mailbox, uid) as { data: string } | undefined;
  if (!row) return null;
  return JSON.parse(row.data) as MessageContentRecord;
}

function cacheMessageContent(
  db: Database.Database,
  mailbox: string,
  uid: number,
  content: MessageContentRecord
): void {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT OR REPLACE INTO message_content (mailbox, uid, data, fetched_at) VALUES (?, ?, ?, ?)`
  ).run(mailbox, uid, JSON.stringify(content), now);
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

interface PoolConfig {
  host: string;
  port: number;
  secure: boolean;
  user: string;
  password: string;
  poolSize: number;
  throttleDelay: number;
  maxRetries: number;
  verbose: boolean;
}

interface Waiter {
  resolve: (client: ImapFlow) => void;
  reject: (err: Error) => void;
}

class ConnectionPool {
  private idle: Set<ImapFlow> = new Set();
  private waiters: Waiter[] = [];
  private all: ImapFlow[] = [];
  private config: PoolConfig;

  constructor(config: PoolConfig) {
    this.config = config;
  }

  private createClient(): ImapFlow {
    return new ImapFlow({
      host: this.config.host,
      port: this.config.port,
      secure: this.config.secure,
      auth: {
        user: this.config.user,
        pass: this.config.password,
      },
      logger: false,
      tls: {
        rejectUnauthorized: false,
      },
    });
  }

  async initialize(): Promise<void> {
    const promises: Promise<void>[] = [];
    for (let i = 0; i < this.config.poolSize; i++) {
      promises.push(this.addConnection());
    }
    await Promise.all(promises);
  }

  private async addConnection(): Promise<void> {
    let lastErr: Error | null = null;
    for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
      try {
        const client = this.createClient();
        await client.connect();
        this.all.push(client);
        this.idle.add(client);
        return;
      } catch (err) {
        lastErr = err instanceof Error ? err : new Error(String(err));
        if (this.config.verbose) {
          process.stderr.write(
            `[verbose] Connection attempt ${attempt + 1} failed: ${lastErr.message}\n`
          );
        }
        if (attempt < this.config.maxRetries - 1) {
          await delay(Math.pow(2, attempt) * 500);
        }
      }
    }
    throw lastErr ?? new Error('Failed to connect');
  }

  async acquireConnection(): Promise<ImapFlow> {
    if (this.config.throttleDelay > 0) {
      await delay(this.config.throttleDelay * 1000);
    }

    if (this.idle.size > 0) {
      const it = this.idle.values().next();
      if (!it.done) {
        const client = it.value;
        this.idle.delete(client);
        return client;
      }
    }

    // Wait for a connection to become available
    return new Promise<ImapFlow>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  releaseConnection(client: ImapFlow): void {
    if (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      // Apply throttle delay before resolving
      if (this.config.throttleDelay > 0) {
        delay(this.config.throttleDelay * 1000).then(() => waiter.resolve(client));
      } else {
        waiter.resolve(client);
      }
    } else {
      this.idle.add(client);
    }
  }

  async withConnection<T>(fn: (client: ImapFlow) => Promise<T>): Promise<T> {
    const client = await this.acquireConnection();
    try {
      return await fn(client);
    } finally {
      this.releaseConnection(client);
    }
  }

  async destroy(): Promise<void> {
    // Reject all pending waiters
    for (const waiter of this.waiters) {
      waiter.reject(new Error('Pool destroyed'));
    }
    this.waiters = [];
    // Logout all connections
    const logouts = this.all.map((c) => c.logout().catch(() => {}));
    await Promise.all(logouts);
    this.idle.clear();
    this.all = [];
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildUidRange(uids: number[]): string {
  // Build a compact UID range string for IMAP fetch
  if (uids.length === 0) return '';
  const sorted = [...uids].sort((a, b) => a - b);
  const ranges: string[] = [];
  let start = sorted[0];
  let end = sorted[0];
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i] === end + 1) {
      end = sorted[i];
    } else {
      ranges.push(start === end ? `${start}` : `${start}:${end}`);
      start = sorted[i];
      end = sorted[i];
    }
  }
  ranges.push(start === end ? `${start}` : `${start}:${end}`);
  return ranges.join(',');
}

function chunk<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function resolveCacheDir(cacheDir: string): string {
  if (cacheDir.startsWith('~')) {
    return path.join(os.homedir(), cacheDir.slice(1));
  }
  return cacheDir;
}

// ---------------------------------------------------------------------------
// IMAP operations
// ---------------------------------------------------------------------------

async function listMailboxes(pool: ConnectionPool, opts: GlobalOptions): Promise<MailboxRecord[]> {
  const db = opts.cache ? openDb(resolveCacheDir(opts.cacheDir)) : null;

  if (db) {
    const cached = getCachedMailboxes(db);
    if (cached) {
      log(`Using cached mailboxes (${cached.length} items)`, opts);
      return cached;
    }
  }

  return pool.withConnection(async (client) => {
    log('Fetching mailbox list from server', opts);
    const list: ListResponse[] = await client.list();

    const results: MailboxRecord[] = [];
    for (const item of list) {
      let exists: number | null = null;
      let unseen: number | null = null;
      try {
        const status = await client.status(item.path, { messages: true, unseen: true });
        exists = status.messages ?? null;
        unseen = status.unseen ?? null;
      } catch (err) {
        log(
          `Could not get status for ${item.path}: ${err instanceof Error ? err.message : String(err)}`,
          opts
        );
      }

      results.push({
        type: 'mailbox',
        name: item.path,
        delimiter: item.delimiter ?? '',
        flags: Array.from(item.flags ?? []),
        exists,
        unseen,
      });
    }

    if (db) {
      cacheMailboxes(db, results);
    }

    return results;
  });
}

async function listMessages(
  pool: ConnectionPool,
  mailbox: string,
  opts: GlobalOptions,
  limit?: number,
  since?: Date,
  before?: Date
): Promise<MessageRecord[]> {
  const db = opts.cache ? openDb(resolveCacheDir(opts.cacheDir)) : null;

  // Only use cache when no date filters and no limit (or we have enough cached)
  const useCache = db && !since && !before;
  if (useCache) {
    const cached = getCachedMessages(db, mailbox);
    if (cached) {
      const results = limit ? cached.slice(0, limit) : cached;
      log(`Using cached messages for ${mailbox} (${results.length} items)`, opts);
      return results;
    }
  }

  return pool.withConnection(async (client) => {
    log(`Opening mailbox: ${mailbox}`, opts);
    await client.mailboxOpen(mailbox, { readOnly: true });

    // Build search criteria
    type SearchCriteria = { all?: true; since?: Date; before?: Date };
    const searchCriteria: SearchCriteria = {};
    if (since || before) {
      if (since) searchCriteria.since = since;
      if (before) searchCriteria.before = before;
    } else {
      searchCriteria.all = true;
    }

    log(`Searching UIDs in ${mailbox}`, opts);
    const uids = await client.search(searchCriteria, { uid: true });
    log(`Found ${uids.length} UIDs`, opts);

    // Apply limit (take most recent = highest UIDs)
    const selectedUids = limit ? uids.slice(-limit) : uids;

    const allMessages: MessageRecord[] = [];
    const batches = chunk(selectedUids, 200);

    for (const batchUids of batches) {
      const uidRange = buildUidRange(batchUids);
      if (!uidRange) continue;

      for await (const msg of client.fetch(
        uidRange,
        { envelope: true, flags: true, size: true },
        { uid: true }
      )) {
        const record = buildMessageRecord(mailbox, msg);
        allMessages.push(record);
      }
    }

    // Cache only when fetching all (no date filter, no limit)
    if (useCache && !since && !before && !limit) {
      cacheMessages(db, mailbox, allMessages);
    }

    return allMessages;
  });
}

function buildMessageRecord(mailbox: string, msg: FetchMessageObject): MessageRecord {
  const env = msg.envelope ?? {};
  const dateVal = env.date;
  const dateStr = dateVal instanceof Date ? dateVal.toISOString() : (dateVal ?? '');

  return {
    type: 'message',
    mailbox,
    uid: msg.uid,
    message_id: env.messageId ?? '',
    subject: env.subject ?? '',
    from: formatAddressList(env.from as EnvelopeAddress[] | null | undefined),
    to: formatAddressList(env.to as EnvelopeAddress[] | null | undefined),
    cc: formatAddressList(env.cc as EnvelopeAddress[] | null | undefined),
    reply_to: formatAddressList(env.replyTo as EnvelopeAddress[] | null | undefined),
    date: dateStr,
    size: msg.size ?? 0,
    flags: Array.from(msg.flags ?? []),
  };
}

async function fetchMessage(
  pool: ConnectionPool,
  mailbox: string,
  uid: number,
  includeAttachments: boolean,
  opts: GlobalOptions
): Promise<MessageContentRecord | null> {
  const db = opts.cache ? openDb(resolveCacheDir(opts.cacheDir)) : null;

  if (db) {
    const cached = getCachedMessageContent(db, mailbox, uid);
    if (cached) {
      log(`Using cached content for uid ${uid} in ${mailbox}`, opts);
      return cached;
    }
  }

  return pool.withConnection(async (client) => {
    log(`Opening mailbox: ${mailbox}`, opts);
    await client.mailboxOpen(mailbox, { readOnly: true });

    let result: MessageContentRecord | null = null;

    for await (const msg of client.fetch(
      uid.toString(),
      { envelope: true, flags: true, size: true, source: true },
      { uid: true }
    )) {
      const baseRecord = buildMessageRecord(mailbox, msg);

      let parsed: ParsedMail | null = null;
      if (msg.source) {
        try {
          parsed = await simpleParser(msg.source);
        } catch (err) {
          log(
            `mailparser error for uid ${uid}: ${err instanceof Error ? err.message : String(err)}`,
            opts
          );
        }
      }

      // Extract headers
      const headers: Record<string, string> = {};
      if (parsed?.headers) {
        for (const [key, value] of parsed.headers) {
          if (typeof value === 'string') {
            headers[key] = value;
          } else if (Array.isArray(value)) {
            headers[key] = value.join(', ');
          } else if (value && typeof value === 'object' && 'text' in value) {
            headers[key] = String((value as { text: string }).text);
          } else {
            headers[key] = String(value);
          }
        }
      }

      // Extract attachments
      const attachments: AttachmentRecord[] = [];
      if (includeAttachments && parsed?.attachments) {
        for (const att of parsed.attachments as Attachment[]) {
          attachments.push({
            filename: att.filename ?? '',
            content_type: att.contentType ?? '',
            size: att.size ?? 0,
            content_id: att.contentId ?? '',
          });
        }
      }

      result = {
        ...baseRecord,
        headers,
        body_text: parsed?.text ?? '',
        body_html: parsed?.html !== false ? (parsed?.html ?? '') : '',
        attachments,
      };

      break; // We only expect one message per UID
    }

    if (result && db) {
      cacheMessageContent(db, mailbox, uid, result);
    }

    return result;
  });
}

// ---------------------------------------------------------------------------
// Pipeline (stdin) processing
// ---------------------------------------------------------------------------

async function readStdinLines(): Promise<string[]> {
  const lines: string[] = [];
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed) lines.push(trimmed);
  }
  return lines;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async function cmdMailboxes(opts: GlobalOptions): Promise<void> {
  const pool = new ConnectionPool({
    host: opts.host,
    port: opts.port,
    secure: opts.ssl,
    user: opts.user,
    password: opts.password,
    poolSize: opts.poolSize,
    throttleDelay: opts.throttleDelay,
    maxRetries: opts.maxRetries,
    verbose: opts.verbose,
  });

  try {
    await pool.initialize();
    const boxes = await listMailboxes(pool, opts);
    for (const box of boxes) {
      emit(box);
    }
  } finally {
    await pool.destroy();
  }
}

async function cmdMessages(
  opts: GlobalOptions,
  mailbox: string,
  limit?: number,
  since?: Date,
  before?: Date
): Promise<void> {
  const pool = new ConnectionPool({
    host: opts.host,
    port: opts.port,
    secure: opts.ssl,
    user: opts.user,
    password: opts.password,
    poolSize: opts.poolSize,
    throttleDelay: opts.throttleDelay,
    maxRetries: opts.maxRetries,
    verbose: opts.verbose,
  });

  try {
    await pool.initialize();

    // If stdin is not a TTY, read mailbox objects from stdin and process in parallel
    if (!process.stdin.isTTY) {
      const lines = await readStdinLines();
      const mailboxNames: string[] = [];

      for (const line of lines) {
        try {
          const obj = JSON.parse(line) as { type?: string; name?: string };
          if (obj.type === 'mailbox' && obj.name) {
            mailboxNames.push(obj.name);
          }
        } catch {
          // Skip invalid JSON lines
        }
      }

      if (mailboxNames.length > 0) {
        // Process mailboxes in parallel (up to poolSize)
        const semaphore = new Array(opts.poolSize).fill(null);
        let idx = 0;

        await Promise.all(
          semaphore.map(async () => {
            while (idx < mailboxNames.length) {
              const mboxName = mailboxNames[idx++];
              try {
                const msgs = await listMessages(pool, mboxName, opts, limit, since, before);
                for (const msg of msgs) {
                  emit(msg);
                }
              } catch (err) {
                emitErr(
                  `Failed to list messages for ${mboxName}: ${err instanceof Error ? err.message : String(err)}`
                );
              }
            }
          })
        );
        return;
      }
    }

    // Single mailbox from flag
    const msgs = await listMessages(pool, mailbox, opts, limit, since, before);
    for (const msg of msgs) {
      emit(msg);
    }
  } finally {
    await pool.destroy();
  }
}

async function cmdFetch(
  opts: GlobalOptions,
  mailbox: string,
  uid: number,
  includeAttachments: boolean
): Promise<void> {
  const pool = new ConnectionPool({
    host: opts.host,
    port: opts.port,
    secure: opts.ssl,
    user: opts.user,
    password: opts.password,
    poolSize: opts.poolSize,
    throttleDelay: opts.throttleDelay,
    maxRetries: opts.maxRetries,
    verbose: opts.verbose,
  });

  try {
    await pool.initialize();

    // If stdin is not a TTY, read message objects from stdin and process in parallel
    if (!process.stdin.isTTY) {
      const lines = await readStdinLines();
      const messages: Array<{ mailbox: string; uid: number }> = [];

      for (const line of lines) {
        try {
          const obj = JSON.parse(line) as { type?: string; mailbox?: string; uid?: number };
          if (obj.type === 'message' && obj.mailbox && typeof obj.uid === 'number') {
            messages.push({ mailbox: obj.mailbox, uid: obj.uid });
          }
        } catch {
          // Skip invalid JSON lines
        }
      }

      if (messages.length > 0) {
        let idx = 0;
        const workers = Math.min(opts.poolSize, messages.length);

        await Promise.all(
          new Array(workers).fill(null).map(async () => {
            while (idx < messages.length) {
              const item = messages[idx++];
              try {
                const content = await fetchMessage(
                  pool,
                  item.mailbox,
                  item.uid,
                  includeAttachments,
                  opts
                );
                if (content) {
                  emit(content);
                }
              } catch (err) {
                emitErr(
                  `Failed to fetch uid ${item.uid} from ${item.mailbox}: ${err instanceof Error ? err.message : String(err)}`
                );
              }
            }
          })
        );
        return;
      }
    }

    // Single message from flags
    const content = await fetchMessage(pool, mailbox, uid, includeAttachments, opts);
    if (content) {
      emit(content);
    } else {
      emitErr(`Message uid ${uid} not found in ${mailbox}`);
    }
  } finally {
    await pool.destroy();
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function parseDate(value: string): Date {
  const d = new Date(value);
  if (isNaN(d.getTime())) {
    throw new Error(`Invalid date: ${value}`);
  }
  return d;
}

function parseFloat_(value: string): number {
  const n = parseFloat(value);
  if (isNaN(n)) throw new Error(`Invalid float: ${value}`);
  return n;
}

function parseInt_(value: string): number {
  const n = parseInt(value, 10);
  if (isNaN(n)) throw new Error(`Invalid integer: ${value}`);
  return n;
}

program
  .name('imap-cli')
  .description('IMAP CLI tool with connection pooling and SQLite caching')
  .requiredOption('--host <host>', 'IMAP server hostname')
  .option('--port <port>', 'IMAP server port (default: 993 for SSL, 143 otherwise)', '0')
  .requiredOption('--user <user>', 'IMAP username')
  .requiredOption('--password <password>', 'IMAP password')
  .option('--no-ssl', 'Disable SSL/TLS (default: SSL enabled)')
  .option('--pool-size <n>', 'Connection pool size', '5')
  .option('--throttle-delay <seconds>', 'Delay between acquiring connections (seconds)', '0')
  .option('--max-retries <n>', 'Maximum connection retry attempts', '3')
  .option('--cache-dir <dir>', 'Cache directory', '~/.imap-cache')
  .option('--no-cache', 'Disable caching')
  .option('--clear-cache', 'Clear cache before running')
  .option('--verbose', 'Enable verbose logging to stderr');

program
  .command('mailboxes')
  .description('List all mailboxes')
  .action(async () => {
    const parentOpts = program.opts();
    const opts = resolveOptions(parentOpts);

    if (opts.clearCache) {
      clearCache(resolveCacheDir(opts.cacheDir));
    }

    try {
      await cmdMailboxes(opts);
    } catch (err) {
      emitErr(err instanceof Error ? err.message : String(err));
      process.exit(1);
    }
  });

program
  .command('messages')
  .description('List messages in a mailbox')
  .option('-m, --mailbox <mailbox>', 'Mailbox name', 'INBOX')
  .option('-n, --limit <n>', 'Maximum number of messages to return')
  .option('--since <date>', 'Only show messages since this date (ISO 8601)')
  .option('--before <date>', 'Only show messages before this date (ISO 8601)')
  .action(async (cmdOpts: { mailbox: string; limit?: string; since?: string; before?: string }) => {
    const parentOpts = program.opts();
    const opts = resolveOptions(parentOpts);

    if (opts.clearCache) {
      clearCache(resolveCacheDir(opts.cacheDir));
    }

    const limit = cmdOpts.limit ? parseInt_(cmdOpts.limit) : undefined;
    const since = cmdOpts.since ? parseDate(cmdOpts.since) : undefined;
    const before = cmdOpts.before ? parseDate(cmdOpts.before) : undefined;

    try {
      await cmdMessages(opts, cmdOpts.mailbox, limit, since, before);
    } catch (err) {
      emitErr(err instanceof Error ? err.message : String(err));
      process.exit(1);
    }
  });

program
  .command('fetch')
  .description('Fetch full message content')
  .option('-m, --mailbox <mailbox>', 'Mailbox name', 'INBOX')
  .option('-u, --uid <uid>', 'Message UID')
  .option('--no-attachments', 'Exclude attachment metadata')
  .action(async (cmdOpts: { mailbox: string; uid?: string; attachments: boolean }) => {
    const parentOpts = program.opts();
    const opts = resolveOptions(parentOpts);

    if (opts.clearCache) {
      clearCache(resolveCacheDir(opts.cacheDir));
    }

    const uid = cmdOpts.uid ? parseInt_(cmdOpts.uid) : 0;

    try {
      await cmdFetch(opts, cmdOpts.mailbox, uid, cmdOpts.attachments);
    } catch (err) {
      emitErr(err instanceof Error ? err.message : String(err));
      process.exit(1);
    }
  });

function resolveOptions(raw: Record<string, unknown>): GlobalOptions {
  const port = parseInt_(String(raw.port ?? '0'));
  const ssl = raw.ssl !== false; // --no-ssl sets ssl=false
  const cache = raw.cache !== false; // --no-cache sets cache=false

  // Default port based on SSL
  const resolvedPort = port === 0 ? (ssl ? 993 : 143) : port;

  return {
    host: String(raw.host),
    port: resolvedPort,
    user: String(raw.user),
    password: String(raw.password),
    ssl,
    poolSize: parseInt_(String(raw.poolSize ?? '5')),
    throttleDelay: parseFloat_(String(raw.throttleDelay ?? '0')),
    maxRetries: parseInt_(String(raw.maxRetries ?? '3')),
    cacheDir: String(raw.cacheDir ?? '~/.imap-cache'),
    cache,
    clearCache: Boolean(raw.clearCache),
    verbose: Boolean(raw.verbose),
  };
}

program.parseAsync(process.argv).catch((err: Error) => {
  emitErr(err.message);
  process.exit(1);
});
