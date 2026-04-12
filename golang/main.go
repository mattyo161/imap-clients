package main

import (
	"bufio"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	_ "github.com/emersion/go-message/charset"
	"github.com/spf13/cobra"
	_ "github.com/mattn/go-sqlite3"
)

// ---------------------------------------------------------------------------
// Config & global state
// ---------------------------------------------------------------------------

type Config struct {
	Host          string
	Port          int
	User          string
	Password      string
	NoSSL         bool
	PoolSize      int
	ThrottleDelay float64
	MaxRetries    int
	CacheDir      string
	NoCache       bool
	ClearCache    bool
	Verbose       bool
}

var cfg Config

// outputMu protects stdout JSON Lines output.
var outputMu sync.Mutex

// ---------------------------------------------------------------------------
// JSON output types
// ---------------------------------------------------------------------------

type MailboxRecord struct {
	Type      string   `json:"type"`
	Name      string   `json:"name"`
	Delimiter string   `json:"delimiter"`
	Flags     []string `json:"flags"`
	Exists    *int     `json:"exists"`
	Unseen    *int     `json:"unseen"`
}

type MessageRecord struct {
	Type      string   `json:"type"`
	Mailbox   string   `json:"mailbox"`
	UID       uint32   `json:"uid"`
	MessageID string   `json:"message_id"`
	Subject   string   `json:"subject"`
	From      []string `json:"from"`
	To        []string `json:"to"`
	CC        []string `json:"cc"`
	ReplyTo   []string `json:"reply_to"`
	Date      string   `json:"date"`
	Size      uint32   `json:"size"`
	Flags     []string `json:"flags"`
}

type Attachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int    `json:"size"`
	ContentID   string `json:"content_id"`
}

type MessageContentRecord struct {
	Type        string            `json:"type"`
	Mailbox     string            `json:"mailbox"`
	UID         uint32            `json:"uid"`
	MessageID   string            `json:"message_id"`
	Subject     string            `json:"subject"`
	From        []string          `json:"from"`
	To          []string          `json:"to"`
	CC          []string          `json:"cc"`
	ReplyTo     []string          `json:"reply_to"`
	Date        string            `json:"date"`
	Size        uint32            `json:"size"`
	Flags       []string          `json:"flags"`
	Headers     map[string]string `json:"headers"`
	BodyText    string            `json:"body_text"`
	BodyHTML    string            `json:"body_html"`
	Attachments []Attachment      `json:"attachments"`
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

func writeJSON(v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		writeError(fmt.Sprintf("json marshal: %v", err))
		return
	}
	outputMu.Lock()
	defer outputMu.Unlock()
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

func writeError(msg string) {
	b, _ := json.Marshal(map[string]string{"type": "error", "message": msg})
	outputMu.Lock()
	defer outputMu.Unlock()
	os.Stderr.Write(b)
	os.Stderr.Write([]byte("\n"))
}

func logVerbose(format string, args ...interface{}) {
	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "[verbose] "+format+"\n", args...)
	}
}

// ---------------------------------------------------------------------------
// Address formatting
// ---------------------------------------------------------------------------

func formatAddr(a *imap.Address) string {
	if a == nil {
		return ""
	}
	email := a.Mailbox + "@" + a.Host
	if a.Name != "" {
		return a.Name + " <" + email + ">"
	}
	return email
}

func formatAddrs(addrs []imap.Address) []string {
	out := make([]string, 0, len(addrs))
	for i := range addrs {
		s := formatAddr(&addrs[i])
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

func resolvePort() int {
	if cfg.Port != 0 {
		return cfg.Port
	}
	if cfg.NoSSL {
		return 143
	}
	return 993
}

func newConn() (*imapclient.Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, resolvePort())
	logVerbose("connecting to %s (ssl=%v)", addr, !cfg.NoSSL)

	var c *imapclient.Client
	var err error

	if cfg.NoSSL {
		c, err = imapclient.Dial(addr, nil)
	} else {
		tlsCfg := &tls.Config{ServerName: cfg.Host}
		c, err = imapclient.DialTLS(addr, &imapclient.Options{TLSConfig: tlsCfg})
	}
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	if err := c.Login(cfg.User, cfg.Password).Wait(); err != nil {
		c.Close()
		return nil, fmt.Errorf("login: %w", err)
	}
	logVerbose("logged in as %s", cfg.User)
	return c, nil
}

func newConnWithRetry() (*imapclient.Client, error) {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(math.Pow(2, float64(attempt-1))*500) * time.Millisecond
			logVerbose("retry %d after %v", attempt, delay)
			time.Sleep(delay)
		}
		c, err := newConn()
		if err == nil {
			return c, nil
		}
		lastErr = err
		logVerbose("connection attempt %d failed: %v", attempt+1, err)
	}
	return nil, fmt.Errorf("all %d connection attempts failed: %w", cfg.MaxRetries, lastErr)
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

type Pool struct {
	ch     chan *imapclient.Client
	config Config
	mu     sync.Mutex
}

func NewPool(config Config, size int) *Pool {
	return &Pool{
		ch:     make(chan *imapclient.Client, size),
		config: config,
	}
}

func (p *Pool) Acquire() (*imapclient.Client, func(), error) {
	// Non-blocking check for an available connection.
	select {
	case c := <-p.ch:
		// Check liveness with Noop.
		if err := c.Noop().Wait(); err != nil {
			logVerbose("pooled connection dead, discarding: %v", err)
			c.Close()
		} else {
			release := p.makeRelease(c)
			return c, release, nil
		}
	default:
	}

	// Create a new connection with retries.
	c, err := newConnWithRetry()
	if err != nil {
		return nil, nil, err
	}
	release := p.makeRelease(c)
	return c, release, nil
}

func (p *Pool) makeRelease(c *imapclient.Client) func() {
	return func() {
		select {
		case p.ch <- c:
			// returned to pool
		default:
			// pool full, close connection
			logVerbose("pool full, closing connection")
			c.Logout().Wait()
			c.Close()
		}
	}
}

func (p *Pool) Close() {
	for {
		select {
		case c := <-p.ch:
			c.Logout().Wait()
			c.Close()
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Cache (SQLite)
// ---------------------------------------------------------------------------

type Cache struct {
	db  *sql.DB
	mu  sync.Mutex
	dir string
}

func openCache(dir string) (*Cache, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	dbPath := filepath.Join(dir, "imap-cache.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma: %w", err)
		}
	}

	schema := `
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
);`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &Cache{db: db, dir: dir}, nil
}

func (c *Cache) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

func (c *Cache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tables := []string{"mailboxes", "messages", "message_content"}
	for _, t := range tables {
		if _, err := c.db.Exec("DELETE FROM " + t); err != nil {
			return fmt.Errorf("clear %s: %w", t, err)
		}
	}
	return nil
}

func (c *Cache) GetMailbox(name string) (*MailboxRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var data string
	err := c.db.QueryRow("SELECT data FROM mailboxes WHERE name=?", name).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec MailboxRecord
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (c *Cache) PutMailbox(rec *MailboxRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = c.db.Exec(
		"INSERT OR REPLACE INTO mailboxes(name,data,fetched_at) VALUES(?,?,?)",
		rec.Name, string(b), time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (c *Cache) GetMessage(mailbox string, uid uint32) (*MessageRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var data string
	err := c.db.QueryRow("SELECT data FROM messages WHERE mailbox=? AND uid=?", mailbox, uid).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec MessageRecord
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (c *Cache) PutMessage(rec *MessageRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = c.db.Exec(
		"INSERT OR REPLACE INTO messages(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)",
		rec.Mailbox, rec.UID, string(b), time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (c *Cache) GetMessageContent(mailbox string, uid uint32) (*MessageContentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var data string
	err := c.db.QueryRow("SELECT data FROM message_content WHERE mailbox=? AND uid=?", mailbox, uid).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec MessageContentRecord
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (c *Cache) PutMessageContent(rec *MessageContentRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = c.db.Exec(
		"INSERT OR REPLACE INTO message_content(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)",
		rec.Mailbox, rec.UID, string(b), time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

// ---------------------------------------------------------------------------
// Throttle helper
// ---------------------------------------------------------------------------

func throttle() {
	if cfg.ThrottleDelay > 0 {
		time.Sleep(time.Duration(cfg.ThrottleDelay * float64(time.Second)))
	}
}

// ---------------------------------------------------------------------------
// Mailboxes command
// ---------------------------------------------------------------------------

func runMailboxes(pool *Pool, cache *Cache) error {
	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	logVerbose("listing mailboxes")
	listCmd := c.List("", "*", nil)
	mailboxes, err := listCmd.Collect()
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}

	for _, mb := range mailboxes {
		name := mb.Mailbox
		delim := ""
		if mb.Delim != 0 {
			delim = string(mb.Delim)
		}

		flags := make([]string, 0, len(mb.Attrs))
		for _, f := range mb.Attrs {
			flags = append(flags, string(f))
		}

		rec := &MailboxRecord{
			Type:      "mailbox",
			Name:      name,
			Delimiter: delim,
			Flags:     flags,
		}

		// Try cache first (unless --no-cache).
		if !cfg.NoCache && cache != nil {
			if cached, err := cache.GetMailbox(name); err == nil && cached != nil {
				writeJSON(cached)
				continue
			}
		}

		// Fetch status (exists + unseen).
		throttle()
		statusData, err := c.Status(name, &imap.StatusOptions{
			Messages: true,
			Unseen:   true,
		}).Wait()
		if err != nil {
			logVerbose("status error for %s: %v", name, err)
		} else {
			if statusData.Messages != nil {
				v := int(*statusData.Messages)
				rec.Exists = &v
			}
			if statusData.Unseen != nil {
				v := int(*statusData.Unseen)
				rec.Unseen = &v
			}
		}

		if !cfg.NoCache && cache != nil {
			if err := cache.PutMailbox(rec); err != nil {
				logVerbose("cache put mailbox error: %v", err)
			}
		}

		writeJSON(rec)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Messages command
// ---------------------------------------------------------------------------

func runMessages(pool *Pool, cache *Cache, mailbox string, limit int, since, before string) error {
	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	logVerbose("selecting mailbox %s (read-only)", mailbox)
	_, err = c.Select(mailbox, &imap.SelectOptions{ReadOnly: true}).Wait()
	if err != nil {
		return fmt.Errorf("select %s: %w", mailbox, err)
	}

	criteria := &imap.SearchCriteria{}

	if since != "" {
		t, err := time.Parse("2006-01-02", since)
		if err != nil {
			return fmt.Errorf("invalid --since date: %w", err)
		}
		criteria.Since = t
	}
	if before != "" {
		t, err := time.Parse("2006-01-02", before)
		if err != nil {
			return fmt.Errorf("invalid --before date: %w", err)
		}
		criteria.Before = t
	}

	logVerbose("searching messages in %s", mailbox)
	throttle()
	searchData, err := c.UIDSearch(criteria, nil).Wait()
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}

	uids := searchData.AllUIDs()
	logVerbose("found %d UIDs in %s", len(uids), mailbox)

	// Apply limit (take last N = most recent).
	if limit > 0 && len(uids) > limit {
		uids = uids[len(uids)-limit:]
	}

	// Process in batches of 200.
	const batchSize = 200
	for i := 0; i < len(uids); i += batchSize {
		end := i + batchSize
		if end > len(uids) {
			end = len(uids)
		}
		batch := uids[i:end]

		// Check cache for all UIDs in batch.
		toFetch := make([]imap.UID, 0, len(batch))
		for _, uid := range batch {
			if !cfg.NoCache && cache != nil {
				cached, err := cache.GetMessage(mailbox, uint32(uid))
				if err == nil && cached != nil {
					writeJSON(cached)
					continue
				}
			}
			toFetch = append(toFetch, uid)
		}

		if len(toFetch) == 0 {
			continue
		}

		seqSet := imap.UIDSetNum(toFetch...)
		throttle()
		fetchCmd := c.Fetch(seqSet, &imap.FetchOptions{
			UID:       true,
			Envelope:  true,
			Flags:     true,
			RFC822Size: true,
		})

		for {
			msg := fetchCmd.Next()
			if msg == nil {
				break
			}

			rec := &MessageRecord{
				Type:    "message",
				Mailbox: mailbox,
				Flags:   []string{},
				From:    []string{},
				To:      []string{},
				CC:      []string{},
				ReplyTo: []string{},
			}

			for {
				item := msg.Next()
				if item == nil {
					break
				}
				switch v := item.(type) {
				case imap.FetchItemDataUID:
					rec.UID = uint32(v.UID)
				case imap.FetchItemDataEnvelope:
					env := v.Envelope
					rec.Subject = env.Subject
					rec.Date = formatDate(env.Date)
					if env.MessageID != "" {
						rec.MessageID = env.MessageID
					}
					rec.From = formatAddrs(env.From)
					rec.To = formatAddrs(env.To)
					rec.CC = formatAddrs(env.Cc)
					rec.ReplyTo = formatAddrs(env.ReplyTo)
				case imap.FetchItemDataFlags:
					for _, f := range v.Flags {
						rec.Flags = append(rec.Flags, string(f))
					}
				case imap.FetchItemDataRFC822Size:
					rec.Size = uint32(v.Size)
				}
			}

			if rec.UID == 0 {
				continue
			}

			if !cfg.NoCache && cache != nil {
				if err := cache.PutMessage(rec); err != nil {
					logVerbose("cache put message error: %v", err)
				}
			}

			writeJSON(rec)
		}

		if err := fetchCmd.Close(); err != nil {
			logVerbose("fetch close error: %v", err)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Body parsing
// ---------------------------------------------------------------------------

// decodeHeader decodes a MIME-encoded header value.
func decodeHeader(s string) string {
	dec := new(mime.WordDecoder)
	decoded, err := dec.DecodeHeader(s)
	if err != nil {
		return s
	}
	return decoded
}

// readBody reads all bytes from an io.Reader, returning empty on error.
func readBody(r io.Reader) []byte {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil
	}
	return b
}

// parseBody parses a raw RFC822 message and extracts text/html body parts
// and attachments.
func parseBody(raw []byte) (bodyText, bodyHTML string, attachments []Attachment, headers map[string]string) {
	headers = make(map[string]string)
	attachments = []Attachment{}

	msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		return
	}

	// Collect headers.
	for k, vs := range msg.Header {
		headers[k] = strings.Join(vs, ", ")
	}

	ct := msg.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(ct)
	if err != nil {
		// Treat as plain text.
		b := readBody(msg.Body)
		bodyText = string(b)
		return
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		bodyText, bodyHTML, attachments = parseMultipart(msg.Body, boundary, mediaType)
	} else if mediaType == "text/html" {
		b := readBody(msg.Body)
		bodyHTML = string(b)
	} else {
		b := readBody(msg.Body)
		bodyText = string(b)
	}
	return
}

func parseMultipart(r io.Reader, boundary, parentMediaType string) (bodyText, bodyHTML string, attachments []Attachment) {
	attachments = []Attachment{}
	mr := multipart.NewReader(r, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		partCT := part.Header.Get("Content-Type")
		disposition := part.Header.Get("Content-Disposition")
		contentID := part.Header.Get("Content-Id")
		contentID = strings.Trim(contentID, "<>")

		partMediaType, partParams, err := mime.ParseMediaType(partCT)
		if err != nil {
			part.Close()
			continue
		}

		dispType, dispParams, _ := mime.ParseMediaType(disposition)

		isAttachment := dispType == "attachment" || dispType == "inline" && contentID != ""

		if strings.HasPrefix(partMediaType, "multipart/") {
			subBoundary := partParams["boundary"]
			subText, subHTML, subAttach := parseMultipart(part, subBoundary, partMediaType)
			if bodyText == "" {
				bodyText = subText
			}
			if bodyHTML == "" {
				bodyHTML = subHTML
			}
			attachments = append(attachments, subAttach...)
		} else if partMediaType == "text/plain" && !isAttachment {
			b := readBody(part)
			if bodyText == "" {
				bodyText = string(b)
			}
		} else if partMediaType == "text/html" && !isAttachment {
			b := readBody(part)
			if bodyHTML == "" {
				bodyHTML = string(b)
			}
		} else {
			// Treat as attachment.
			filename := dispParams["filename"]
			if filename == "" {
				filename = partParams["name"]
			}
			if filename == "" {
				filename = contentID
			}
			b := readBody(part)
			attachments = append(attachments, Attachment{
				Filename:    decodeHeader(filename),
				ContentType: partMediaType,
				Size:        len(b),
				ContentID:   contentID,
			})
		}
		part.Close()
	}
	return
}

// ---------------------------------------------------------------------------
// Fetch command
// ---------------------------------------------------------------------------

func runFetch(pool *Pool, cache *Cache, mailbox string, uid uint32, noAttachments bool) error {
	// Try cache first.
	if !cfg.NoCache && cache != nil {
		cached, err := cache.GetMessageContent(mailbox, uid)
		if err == nil && cached != nil {
			if noAttachments {
				cached.Attachments = []Attachment{}
			}
			writeJSON(cached)
			return nil
		}
	}

	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	logVerbose("selecting mailbox %s (read-only) for fetch uid=%d", mailbox, uid)
	_, err = c.Select(mailbox, &imap.SelectOptions{ReadOnly: true}).Wait()
	if err != nil {
		return fmt.Errorf("select %s: %w", mailbox, err)
	}

	seqSet := imap.UIDSetNum(imap.UID(uid))
	throttle()

	fetchCmd := c.Fetch(seqSet, &imap.FetchOptions{
		UID:      true,
		Envelope: true,
		Flags:    true,
		RFC822Size: true,
		BodySection: []*imap.FetchItemBodySection{
			{Specifier: imap.PartSpecifierNone},
		},
	})

	rec := &MessageContentRecord{
		Type:        "message_content",
		Mailbox:     mailbox,
		UID:         uid,
		Flags:       []string{},
		From:        []string{},
		To:          []string{},
		CC:          []string{},
		ReplyTo:     []string{},
		Headers:     map[string]string{},
		Attachments: []Attachment{},
	}

	for {
		msg := fetchCmd.Next()
		if msg == nil {
			break
		}

		var rawBody []byte

		for {
			item := msg.Next()
			if item == nil {
				break
			}
			switch v := item.(type) {
			case imap.FetchItemDataUID:
				rec.UID = uint32(v.UID)
			case imap.FetchItemDataEnvelope:
				env := v.Envelope
				rec.Subject = env.Subject
				rec.Date = formatDate(env.Date)
				if env.MessageID != "" {
					rec.MessageID = env.MessageID
				}
				rec.From = formatAddrs(env.From)
				rec.To = formatAddrs(env.To)
				rec.CC = formatAddrs(env.Cc)
				rec.ReplyTo = formatAddrs(env.ReplyTo)
			case imap.FetchItemDataFlags:
				for _, f := range v.Flags {
					rec.Flags = append(rec.Flags, string(f))
				}
			case imap.FetchItemDataRFC822Size:
				rec.Size = uint32(v.Size)
			case imap.FetchItemDataBodySection:
				rawBody = readBody(v.Literal)
			}
		}

		if len(rawBody) > 0 {
			bodyText, bodyHTML, attachments, headers := parseBody(rawBody)
			rec.BodyText = bodyText
			rec.BodyHTML = bodyHTML
			rec.Attachments = attachments
			rec.Headers = headers
		}
	}

	if err := fetchCmd.Close(); err != nil {
		logVerbose("fetch close error: %v", err)
	}

	if noAttachments {
		rec.Attachments = []Attachment{}
	}

	if !cfg.NoCache && cache != nil {
		if err := cache.PutMessageContent(rec); err != nil {
			logVerbose("cache put message_content error: %v", err)
		}
	}

	writeJSON(rec)
	return nil
}

// ---------------------------------------------------------------------------
// stdin pipeline dispatcher
// ---------------------------------------------------------------------------

// isStdinPipe returns true when stdin is a pipe (not a terminal).
func isStdinPipe() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) == 0
}

type stdinMailboxLine struct {
	Mailbox string `json:"mailbox"`
	UID     *uint32 `json:"uid"`
}

// dispatchFromStdin reads JSON Lines from stdin and dispatches work using a
// goroutine worker pool of pool.PoolSize workers.
func dispatchFromStdin(pool *Pool, cache *Cache, workerFn func(line stdinMailboxLine) error) {
	jobs := make(chan stdinMailboxLine, cfg.PoolSize*2)
	var wg sync.WaitGroup

	for i := 0; i < cfg.PoolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := workerFn(job); err != nil {
					writeError(fmt.Sprintf("worker error: %v", err))
				}
			}
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var parsed stdinMailboxLine
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			writeError(fmt.Sprintf("invalid stdin JSON: %v", err))
			continue
		}
		jobs <- parsed
	}
	close(jobs)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Default cache dir
// ---------------------------------------------------------------------------

func defaultCacheDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".imap-cache"
	}
	return filepath.Join(home, ".imap-cache")
}

// ---------------------------------------------------------------------------
// main / cobra setup
// ---------------------------------------------------------------------------

func main() {
	rootCmd := &cobra.Command{
		Use:   "imap-cli",
		Short: "IMAP command-line client",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags.
			if cfg.Host == "" {
				return fmt.Errorf("--host is required")
			}
			if cfg.User == "" {
				return fmt.Errorf("--user is required")
			}
			if cfg.Password == "" {
				return fmt.Errorf("--password is required")
			}
			if cfg.PoolSize < 1 {
				cfg.PoolSize = 1
			}
			if cfg.MaxRetries < 1 {
				cfg.MaxRetries = 1
			}
			return nil
		},
	}

	// Global persistent flags.
	pf := rootCmd.PersistentFlags()
	pf.StringVar(&cfg.Host, "host", "", "IMAP server hostname (required)")
	pf.IntVar(&cfg.Port, "port", 0, "IMAP server port (0 = auto: 993 TLS / 143 plain)")
	pf.StringVar(&cfg.User, "user", "", "IMAP username (required)")
	pf.StringVar(&cfg.Password, "password", "", "IMAP password (required)")
	pf.BoolVar(&cfg.NoSSL, "no-ssl", false, "Disable TLS/SSL (use plain IMAP)")
	pf.IntVar(&cfg.PoolSize, "pool-size", 5, "Connection pool size")
	pf.Float64Var(&cfg.ThrottleDelay, "throttle-delay", 0, "Delay in seconds between IMAP operations")
	pf.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum connection retry attempts")
	pf.StringVar(&cfg.CacheDir, "cache-dir", defaultCacheDir(), "Directory for SQLite cache")
	pf.BoolVar(&cfg.NoCache, "no-cache", false, "Disable caching")
	pf.BoolVar(&cfg.ClearCache, "clear-cache", false, "Clear cache before running")
	pf.BoolVar(&cfg.Verbose, "verbose", false, "Enable verbose logging to stderr")

	// ---- mailboxes command ----
	mailboxesCmd := &cobra.Command{
		Use:   "mailboxes",
		Short: "List all mailboxes",
		RunE: func(cmd *cobra.Command, args []string) error {
			pool := NewPool(cfg, cfg.PoolSize)
			defer pool.Close()

			var cache *Cache
			if !cfg.NoCache {
				var err error
				cache, err = openCache(cfg.CacheDir)
				if err != nil {
					logVerbose("failed to open cache: %v", err)
				} else {
					defer cache.Close()
					if cfg.ClearCache {
						if err := cache.Clear(); err != nil {
							logVerbose("clear cache error: %v", err)
						}
					}
				}
			}

			return runMailboxes(pool, cache)
		},
	}

	// ---- messages command ----
	var (
		msgMailbox string
		msgLimit   int
		msgSince   string
		msgBefore  string
	)
	messagesCmd := &cobra.Command{
		Use:   "messages",
		Short: "List messages in a mailbox",
		RunE: func(cmd *cobra.Command, args []string) error {
			pool := NewPool(cfg, cfg.PoolSize)
			defer pool.Close()

			var cache *Cache
			if !cfg.NoCache {
				var err error
				cache, err = openCache(cfg.CacheDir)
				if err != nil {
					logVerbose("failed to open cache: %v", err)
				} else {
					defer cache.Close()
					if cfg.ClearCache {
						if err := cache.Clear(); err != nil {
							logVerbose("clear cache error: %v", err)
						}
					}
				}
			}

			// Pipeline mode: read mailboxes from stdin.
			if isStdinPipe() && msgMailbox == "" {
				dispatchFromStdin(pool, cache, func(line stdinMailboxLine) error {
					if line.Mailbox == "" {
						return fmt.Errorf("stdin line missing 'mailbox' field")
					}
					return runMessages(pool, cache, line.Mailbox, msgLimit, msgSince, msgBefore)
				})
				return nil
			}

			if msgMailbox == "" {
				return fmt.Errorf("--mailbox is required (or pipe mailbox names via stdin)")
			}
			return runMessages(pool, cache, msgMailbox, msgLimit, msgSince, msgBefore)
		},
	}
	messagesCmd.Flags().StringVarP(&msgMailbox, "mailbox", "m", "", "Mailbox name")
	messagesCmd.Flags().IntVarP(&msgLimit, "limit", "n", 0, "Maximum number of messages to return (0 = all)")
	messagesCmd.Flags().StringVar(&msgSince, "since", "", "Return messages since this date (YYYY-MM-DD)")
	messagesCmd.Flags().StringVar(&msgBefore, "before", "", "Return messages before this date (YYYY-MM-DD)")

	// ---- fetch command ----
	var (
		fetchMailbox      string
		fetchUID          uint32
		fetchNoAttachments bool
	)
	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch full message content",
		RunE: func(cmd *cobra.Command, args []string) error {
			pool := NewPool(cfg, cfg.PoolSize)
			defer pool.Close()

			var cache *Cache
			if !cfg.NoCache {
				var err error
				cache, err = openCache(cfg.CacheDir)
				if err != nil {
					logVerbose("failed to open cache: %v", err)
				} else {
					defer cache.Close()
					if cfg.ClearCache {
						if err := cache.Clear(); err != nil {
							logVerbose("clear cache error: %v", err)
						}
					}
				}
			}

			// Pipeline mode: read mailbox+uid pairs from stdin.
			if isStdinPipe() && (fetchMailbox == "" || fetchUID == 0) {
				dispatchFromStdin(pool, cache, func(line stdinMailboxLine) error {
					if line.Mailbox == "" || line.UID == nil {
						return fmt.Errorf("stdin line missing 'mailbox' or 'uid' field")
					}
					return runFetch(pool, cache, line.Mailbox, *line.UID, fetchNoAttachments)
				})
				return nil
			}

			if fetchMailbox == "" {
				return fmt.Errorf("--mailbox is required")
			}
			if fetchUID == 0 {
				return fmt.Errorf("--uid is required")
			}
			return runFetch(pool, cache, fetchMailbox, fetchUID, fetchNoAttachments)
		},
	}
	fetchCmd.Flags().StringVarP(&fetchMailbox, "mailbox", "m", "", "Mailbox name")
	fetchCmd.Flags().Uint32VarP(&fetchUID, "uid", "u", 0, "Message UID")
	fetchCmd.Flags().BoolVar(&fetchNoAttachments, "no-attachments", false, "Exclude attachment content")

	rootCmd.AddCommand(mailboxesCmd, messagesCmd, fetchCmd)

	if err := rootCmd.Execute(); err != nil {
		writeError(err.Error())
		os.Exit(1)
	}
}
