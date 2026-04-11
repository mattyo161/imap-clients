package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/textproto"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/spf13/cobra"
	_ "github.com/mattn/go-sqlite3"
)

// ---------------------------------------------------------------------------
// JSON output types
// ---------------------------------------------------------------------------

type MailboxObj struct {
	Type      string   `json:"type"`
	Name      string   `json:"name"`
	Delimiter string   `json:"delimiter"`
	Flags     []string `json:"flags"`
	Exists    *uint32  `json:"exists"`
	Unseen    *uint32  `json:"unseen"`
}

type MessageObj struct {
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

type MessageContentObj struct {
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
// Global config & output helpers
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
var stdoutMu sync.Mutex

func emit(v interface{}) {
	b, _ := json.Marshal(v)
	stdoutMu.Lock()
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
	stdoutMu.Unlock()
}

func emitErr(msg string) {
	b, _ := json.Marshal(map[string]string{"type": "error", "message": msg})
	os.Stderr.Write(b)
	os.Stderr.Write([]byte("\n"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var nonAlnum = regexp.MustCompile(`[^\w.\-]`)

func safeFilename(s string) string {
	return nonAlnum.ReplaceAllString(s, "_")
}

func formatAddr(a *imap.Address) string {
	if a == nil {
		return ""
	}
	if a.MailboxName != "" && a.HostName != "" {
		email := a.MailboxName + "@" + a.HostName
		if a.PersonalName != "" {
			return a.PersonalName + " <" + email + ">"
		}
		return email
	}
	return a.PersonalName
}

func formatAddrs(addrs []*imap.Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if s := formatAddr(a); s != "" {
			out = append(out, s)
		}
	}
	if out == nil {
		return []string{}
	}
	return out
}

func parseIMAPDate(s string) (time.Time, error) {
	return time.Parse("02-Jan-2006", s)
}

// ---------------------------------------------------------------------------
// Body parsing (stdlib only)
// ---------------------------------------------------------------------------

func decodeBody(r io.Reader, transferEnc string) []byte {
	raw, _ := io.ReadAll(r)
	switch strings.ToLower(strings.TrimSpace(transferEnc)) {
	case "base64":
		var filtered []byte
		for _, b := range raw {
			if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
				filtered = append(filtered, b)
			}
		}
		dec := make([]byte, base64.StdEncoding.DecodedLen(len(filtered)))
		n, err := base64.StdEncoding.Decode(dec, filtered)
		if err != nil {
			// try raw (no padding)
			dec2 := make([]byte, base64.RawStdEncoding.DecodedLen(len(filtered)))
			n2, _ := base64.RawStdEncoding.Decode(dec2, filtered)
			return dec2[:n2]
		}
		return dec[:n]
	case "quoted-printable":
		dec, _ := io.ReadAll(quotedprintable.NewReader(bytes.NewReader(raw)))
		return dec
	default:
		return raw
	}
}

func processPartRec(h textproto.MIMEHeader, r io.Reader,
	bodyText, bodyHTML *string, atts *[]Attachment) {

	ct := h.Get("Content-Type")
	if ct == "" {
		ct = "text/plain"
	}
	mediaType, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r, params["boundary"])
		for {
			part, err := mr.NextRawPart()
			if err != nil {
				break
			}
			processPartRec(textproto.MIMEHeader(part.Header), part, bodyText, bodyHTML, atts)
		}
		return
	}

	body := decodeBody(r, h.Get("Content-Transfer-Encoding"))

	disp := h.Get("Content-Disposition")
	dispType, dispParams, _ := mime.ParseMediaType(disp)
	filename := dispParams["filename"]
	if filename == "" {
		_, ctParams, _ := mime.ParseMediaType(ct)
		filename = ctParams["name"]
	}
	if filename != "" {
		if dec, err := new(mime.WordDecoder).DecodeHeader(filename); err == nil {
			filename = dec
		}
	}
	cid := strings.Trim(h.Get("Content-Id"), " <>")

	isAttachment := dispType == "attachment" || (filename != "" && dispType != "inline")
	if isAttachment {
		*atts = append(*atts, Attachment{
			Filename:    filename,
			ContentType: mediaType,
			Size:        len(body),
			ContentID:   cid,
		})
		return
	}

	switch mediaType {
	case "text/plain":
		if *bodyText == "" {
			*bodyText = string(body)
		}
	case "text/html":
		if *bodyHTML == "" {
			*bodyHTML = string(body)
		}
	}
}

func parseBodyBytes(rawMsg []byte) (bodyText, bodyHTML string, atts []Attachment, headers map[string]string) {
	headers = make(map[string]string)
	atts = []Attachment{}

	msg, err := mail.ReadMessage(bytes.NewReader(rawMsg))
	if err != nil {
		return
	}
	for k, vs := range msg.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	h := make(textproto.MIMEHeader)
	for k, vs := range msg.Header {
		h[k] = vs
	}
	processPartRec(h, msg.Body, &bodyText, &bodyHTML, &atts)
	return
}

// ---------------------------------------------------------------------------
// SQLite cache
// ---------------------------------------------------------------------------

type Cache struct {
	db *sql.DB
	mu sync.Mutex
}

func NewCache(cacheDir, host, user string) (*Cache, error) {
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(cacheDir, safeFilename(host+"_"+user)+".db")
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}
	c := &Cache{db: db}
	return c, c.init()
}

func (c *Cache) init() error {
	_, err := c.db.Exec(`
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
		);`)
	return err
}

func (c *Cache) Close() { c.db.Close() }

func (c *Cache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.db.Exec(`DELETE FROM mailboxes; DELETE FROM messages; DELETE FROM message_content;`)
	return err
}

func (c *Cache) GetMailboxes() ([]MailboxObj, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows, err := c.db.Query("SELECT data FROM mailboxes ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MailboxObj
	for rows.Next() {
		var s string
		if rows.Scan(&s) == nil {
			var o MailboxObj
			if json.Unmarshal([]byte(s), &o) == nil {
				out = append(out, o)
			}
		}
	}
	return out, nil
}

func (c *Cache) SetMailboxes(items []MailboxObj) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tx, _ := c.db.Begin()
	tx.Exec("DELETE FROM mailboxes")
	now := time.Now().UTC().Format(time.RFC3339)
	for _, m := range items {
		b, _ := json.Marshal(m)
		tx.Exec("INSERT OR REPLACE INTO mailboxes(name,data,fetched_at) VALUES(?,?,?)", m.Name, string(b), now)
	}
	return tx.Commit()
}

func (c *Cache) GetMessages(mailbox string) ([]MessageObj, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows, err := c.db.Query("SELECT data FROM messages WHERE mailbox=? ORDER BY uid", mailbox)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MessageObj
	for rows.Next() {
		var s string
		if rows.Scan(&s) == nil {
			var o MessageObj
			if json.Unmarshal([]byte(s), &o) == nil {
				out = append(out, o)
			}
		}
	}
	return out, nil
}

func (c *Cache) SetMessages(mailbox string, items []MessageObj) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	tx, _ := c.db.Begin()
	for _, m := range items {
		b, _ := json.Marshal(m)
		tx.Exec("INSERT OR REPLACE INTO messages(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)",
			mailbox, m.UID, string(b), now)
	}
	return tx.Commit()
}

func (c *Cache) GetMessageContent(mailbox string, uid uint32) (*MessageContentObj, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var s string
	err := c.db.QueryRow("SELECT data FROM message_content WHERE mailbox=? AND uid=?", mailbox, uid).Scan(&s)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var o MessageContentObj
	return &o, json.Unmarshal([]byte(s), &o)
}

func (c *Cache) SetMessageContent(mailbox string, uid uint32, obj *MessageContentObj) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, _ := json.Marshal(obj)
	_, err := c.db.Exec(
		"INSERT OR REPLACE INTO message_content(mailbox,uid,data,fetched_at) VALUES(?,?,?,?)",
		mailbox, uid, string(b), time.Now().UTC().Format(time.RFC3339))
	return err
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

type Pool struct {
	idle chan *client.Client
	sem  chan struct{}
	cfg  *Config
}

func NewPool(cfg *Config) *Pool {
	return &Pool{
		idle: make(chan *client.Client, cfg.PoolSize),
		sem:  make(chan struct{}, cfg.PoolSize),
		cfg:  cfg,
	}
}

func (p *Pool) newConn() (*client.Client, error) {
	port := p.cfg.Port
	if port == 0 {
		if p.cfg.NoSSL {
			port = 143
		} else {
			port = 993
		}
	}
	addr := net.JoinHostPort(p.cfg.Host, strconv.Itoa(port))

	var c *client.Client
	var err error
	for attempt := 0; attempt < p.cfg.MaxRetries; attempt++ {
		if p.cfg.NoSSL {
			c, err = client.Dial(addr)
		} else {
			c, err = client.DialTLS(addr, &tls.Config{ServerName: p.cfg.Host, InsecureSkipVerify: true})
		}
		if err == nil {
			err = c.Login(p.cfg.User, p.cfg.Password)
		}
		if err == nil {
			return c, nil
		}
		if attempt < p.cfg.MaxRetries-1 {
			delay := time.Duration(1<<uint(attempt)) * time.Second
			if p.cfg.ThrottleDelay > 0 {
				if d := time.Duration(p.cfg.ThrottleDelay * float64(time.Second)); d > delay {
					delay = d
				}
			}
			log.Printf("connect attempt %d failed: %v – retry in %v", attempt+1, err, delay)
			time.Sleep(delay)
		}
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", p.cfg.MaxRetries, err)
}

// Acquire returns a live connection and a release func.
func (p *Pool) Acquire() (*client.Client, func(), error) {
	p.sem <- struct{}{} // blocks until a slot is free

	// Try an idle connection
	select {
	case c := <-p.idle:
		if c.Noop() == nil {
			return c, func() { p.put(c) }, nil
		}
		// Dead — discard and fall through to create a new one
	default:
	}

	c, err := p.newConn()
	if err != nil {
		<-p.sem
		return nil, nil, err
	}
	if p.cfg.ThrottleDelay > 0 {
		time.Sleep(time.Duration(p.cfg.ThrottleDelay * float64(time.Second)))
	}
	return c, func() { p.put(c) }, nil
}

func (p *Pool) put(c *client.Client) {
	select {
	case p.idle <- c:
	default:
		c.Logout()
	}
	<-p.sem
}

func (p *Pool) Close() {
	for {
		select {
		case c := <-p.idle:
			c.Logout()
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// IMAP operations
// ---------------------------------------------------------------------------

const fetchBatch = 200

func setupPool(noCache bool) (*Pool, *Cache) {
	pool := NewPool(&cfg)
	var cache *Cache
	if !noCache {
		var err error
		cache, err = NewCache(cfg.CacheDir, cfg.Host, cfg.User)
		if err != nil {
			log.Printf("cache unavailable: %v", err)
		} else if cfg.ClearCache {
			cache.Clear()
		}
	}
	return pool, cache
}

func opMailboxes(pool *Pool, cache *Cache) error {
	if cache != nil {
		items, err := cache.GetMailboxes()
		if err == nil && len(items) > 0 {
			for _, m := range items {
				emit(m)
			}
			return nil
		}
	}

	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	// List all mailboxes
	mbCh := make(chan *imap.MailboxInfo, 20)
	done := make(chan error, 1)
	go func() { done <- c.List("", "*", mbCh) }()

	var infos []*imap.MailboxInfo
	for mb := range mbCh {
		infos = append(infos, mb)
	}
	if err := <-done; err != nil {
		return err
	}

	var results []MailboxObj
	for _, mb := range infos {
		obj := MailboxObj{
			Type:      "mailbox",
			Name:      mb.Name,
			Delimiter: mb.Delimiter,
			Flags:     mb.Attributes,
		}
		if obj.Flags == nil {
			obj.Flags = []string{}
		}

		st, err := c.Status(mb.Name, []imap.StatusItem{imap.StatusMessages, imap.StatusUnseen})
		if err == nil {
			obj.Exists = &st.Messages
			obj.Unseen = &st.Unseen
		}
		results = append(results, obj)
	}

	if cache != nil {
		cache.SetMailboxes(results)
	}
	for _, m := range results {
		emit(m)
	}
	return nil
}

func opMessages(pool *Pool, mailbox string, cache *Cache,
	limit int, since, before string) error {

	if cache != nil {
		items, err := cache.GetMessages(mailbox)
		if err == nil && len(items) > 0 {
			if limit > 0 && len(items) > limit {
				items = items[len(items)-limit:]
			}
			for _, m := range items {
				emit(m)
			}
			return nil
		}
	}

	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	if _, err := c.Select(mailbox, true); err != nil {
		return fmt.Errorf("select %q: %w", mailbox, err)
	}

	criteria := imap.NewSearchCriteria()
	if since != "" {
		if t, err := parseIMAPDate(since); err == nil {
			criteria.Since = t
		}
	}
	if before != "" {
		if t, err := parseIMAPDate(before); err == nil {
			criteria.Before = t
		}
	}

	uids, err := c.UidSearch(criteria)
	if err != nil {
		return fmt.Errorf("search %q: %w", mailbox, err)
	}
	if len(uids) == 0 {
		return nil
	}
	if limit > 0 && len(uids) > limit {
		uids = uids[len(uids)-limit:]
	}

	fetchItems := []imap.FetchItem{imap.FetchEnvelope, imap.FetchFlags, imap.FetchRFC822Size, imap.FetchUid}

	var results []MessageObj
	for i := 0; i < len(uids); i += fetchBatch {
		end := i + fetchBatch
		if end > len(uids) {
			end = len(uids)
		}
		batch := uids[i:end]

		seqset := new(imap.SeqSet)
		for _, u := range batch {
			seqset.AddNum(u)
		}

		var fetchErr error
		for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
			msgCh := make(chan *imap.Message, len(batch))
			doneCh := make(chan error, 1)
			go func() { doneCh <- c.UidFetch(seqset, fetchItems, msgCh) }()

			for msg := range msgCh {
				if msg.Envelope == nil {
					continue
				}
				env := msg.Envelope
				results = append(results, MessageObj{
					Type:      "message",
					Mailbox:   mailbox,
					UID:       msg.Uid,
					MessageID: env.MessageId,
					Subject:   env.Subject,
					From:      formatAddrs(env.From),
					To:        formatAddrs(env.To),
					CC:        formatAddrs(env.Cc),
					ReplyTo:   formatAddrs(env.ReplyTo),
					Date:      env.Date.Format(time.RFC3339),
					Size:      msg.Size,
					Flags:     msg.Flags,
				})
			}
			fetchErr = <-doneCh
			if fetchErr == nil {
				break
			}
			if attempt < cfg.MaxRetries-1 {
				time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
			}
		}
		if fetchErr != nil {
			log.Printf("fetch batch error: %v", fetchErr)
		}
	}

	for i := range results {
		if results[i].Flags == nil {
			results[i].Flags = []string{}
		}
	}

	if cache != nil && len(results) > 0 {
		cache.SetMessages(mailbox, results)
	}
	for _, m := range results {
		emit(m)
	}
	return nil
}

func opFetch(pool *Pool, mailbox string, uid uint32,
	cache *Cache, noAttachments bool) error {

	if cache != nil {
		obj, err := cache.GetMessageContent(mailbox, uid)
		if err == nil && obj != nil {
			emit(obj)
			return nil
		}
	}

	c, release, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer release()

	if _, err := c.Select(mailbox, true); err != nil {
		return fmt.Errorf("select %q: %w", mailbox, err)
	}

	section := &imap.BodySectionName{}
	fetchItems := []imap.FetchItem{
		imap.FetchEnvelope, imap.FetchFlags, imap.FetchRFC822Size, imap.FetchUid,
		section.FetchItem(),
	}

	seqset := new(imap.SeqSet)
	seqset.AddNum(uid)

	var fetchErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		msgCh := make(chan *imap.Message, 1)
		doneCh := make(chan error, 1)
		go func() { doneCh <- c.UidFetch(seqset, fetchItems, msgCh) }()

		for msg := range msgCh {
			if msg.Envelope == nil {
				continue
			}
			env := msg.Envelope

			var rawBody []byte
			if r := msg.GetBody(section); r != nil {
				rawBody, _ = io.ReadAll(r)
			}

			bodyText, bodyHTML, atts, hdrs := parseBodyBytes(rawBody)
			if noAttachments {
				atts = []Attachment{}
			}
			flags := msg.Flags
			if flags == nil {
				flags = []string{}
			}

			obj := &MessageContentObj{
				Type:        "message_content",
				Mailbox:     mailbox,
				UID:         msg.Uid,
				MessageID:   env.MessageId,
				Subject:     env.Subject,
				From:        formatAddrs(env.From),
				To:          formatAddrs(env.To),
				CC:          formatAddrs(env.Cc),
				ReplyTo:     formatAddrs(env.ReplyTo),
				Date:        env.Date.Format(time.RFC3339),
				Size:        msg.Size,
				Flags:       flags,
				Headers:     hdrs,
				BodyText:    bodyText,
				BodyHTML:    bodyHTML,
				Attachments: atts,
			}
			if cache != nil {
				cache.SetMessageContent(mailbox, uid, obj)
			}
			emit(obj)
		}
		fetchErr = <-doneCh
		if fetchErr == nil {
			break
		}
		if attempt < cfg.MaxRetries-1 {
			time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
		}
	}
	return fetchErr
}

// ---------------------------------------------------------------------------
// Command runners
// ---------------------------------------------------------------------------

func runMailboxes(cmd *cobra.Command, args []string) error {
	pool, cache := setupPool(cfg.NoCache)
	defer pool.Close()
	if cache != nil {
		defer cache.Close()
	}
	return opMailboxes(pool, cache)
}

func runMessages(cmd *cobra.Command, args []string) error {
	mailbox, _ := cmd.Flags().GetString("mailbox")
	limit, _ := cmd.Flags().GetInt("limit")
	since, _ := cmd.Flags().GetString("since")
	before, _ := cmd.Flags().GetString("before")

	pool, cache := setupPool(cfg.NoCache)
	defer pool.Close()
	if cache != nil {
		defer cache.Close()
	}

	process := func(mb string) {
		if err := opMessages(pool, mb, cache, limit, since, before); err != nil {
			emitErr(err.Error())
		}
	}

	if isPipe() {
		taskCh := make(chan string, cfg.PoolSize*2)
		var wg sync.WaitGroup
		for i := 0; i < cfg.PoolSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for mb := range taskCh {
					process(mb)
				}
			}()
		}
		scanJSONLines(func(obj map[string]interface{}) {
			if t, _ := obj["type"].(string); t == "mailbox" {
				if name, _ := obj["name"].(string); name != "" {
					taskCh <- name
				}
			}
		})
		close(taskCh)
		wg.Wait()
	} else if mailbox != "" {
		process(mailbox)
	} else {
		emitErr("provide --mailbox or pipe mailbox JSON Lines from stdin")
	}
	return nil
}

func runFetch(cmd *cobra.Command, args []string) error {
	mailbox, _ := cmd.Flags().GetString("mailbox")
	uid, _ := cmd.Flags().GetUint32("uid")
	noAtt, _ := cmd.Flags().GetBool("no-attachments")

	pool, cache := setupPool(cfg.NoCache)
	defer pool.Close()
	if cache != nil {
		defer cache.Close()
	}

	type task struct {
		mailbox string
		uid     uint32
	}
	process := func(t task) {
		if err := opFetch(pool, t.mailbox, t.uid, cache, noAtt); err != nil {
			emitErr(err.Error())
		}
	}

	if isPipe() {
		taskCh := make(chan task, cfg.PoolSize*2)
		var wg sync.WaitGroup
		for i := 0; i < cfg.PoolSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for t := range taskCh {
					process(t)
				}
			}()
		}
		scanJSONLines(func(obj map[string]interface{}) {
			if tp, _ := obj["type"].(string); tp == "message" {
				mb, _ := obj["mailbox"].(string)
				u, _ := obj["uid"].(float64)
				if mb != "" && u > 0 {
					taskCh <- task{mb, uint32(u)}
				}
			}
		})
		close(taskCh)
		wg.Wait()
	} else if mailbox != "" && uid > 0 {
		process(task{mailbox, uid})
	} else {
		emitErr("provide --mailbox + --uid or pipe message JSON Lines from stdin")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stdin helpers
// ---------------------------------------------------------------------------

func isPipe() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

func scanJSONLines(fn func(map[string]interface{})) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 10*1024*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if json.Unmarshal([]byte(line), &obj) == nil {
			fn(obj)
		}
	}
}

// ---------------------------------------------------------------------------
// CLI setup
// ---------------------------------------------------------------------------

func main() {
	root := &cobra.Command{
		Use:          "imap-cli",
		Short:        "Universal IMAP client \u2014 outputs JSON Lines",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if !cfg.Verbose {
				log.SetOutput(io.Discard)
			}
			return nil
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&cfg.Host, "host", "", "IMAP hostname (required)")
	pf.IntVar(&cfg.Port, "port", 0, "IMAP port (0=auto: 993 TLS / 143 plain)")
	pf.StringVar(&cfg.User, "user", "", "Username (required)")
	pf.StringVar(&cfg.Password, "password", "", "Password (required)")
	pf.BoolVar(&cfg.NoSSL, "no-ssl", false, "Disable TLS")
	pf.IntVar(&cfg.PoolSize, "pool-size", 5, "Connection pool size")
	pf.Float64Var(&cfg.ThrottleDelay, "throttle-delay", 0, "Seconds between requests")
	pf.IntVar(&cfg.MaxRetries, "max-retries", 3, "Max retries on failure")

	home, _ := os.UserHomeDir()
	pf.StringVar(&cfg.CacheDir, "cache-dir", filepath.Join(home, ".imap-cache"), "Cache directory")
	pf.BoolVar(&cfg.NoCache, "no-cache", false, "Bypass cache")
	pf.BoolVar(&cfg.ClearCache, "clear-cache", false, "Clear cache before running")
	pf.BoolVarP(&cfg.Verbose, "verbose", "v", false, "Verbose logging")

	root.MarkPersistentFlagRequired("host")
	root.MarkPersistentFlagRequired("user")
	root.MarkPersistentFlagRequired("password")

	// mailboxes
	root.AddCommand(&cobra.Command{
		Use:   "mailboxes",
		Short: "List all mailboxes/folders",
		RunE:  runMailboxes,
	})

	// messages
	msgsCmd := &cobra.Command{
		Use:   "messages",
		Short: "List messages (reads mailbox jsonl from stdin or --mailbox)",
		RunE:  runMessages,
	}
	msgsCmd.Flags().StringP("mailbox", "m", "", "Mailbox name")
	msgsCmd.Flags().IntP("limit", "n", 0, "Max messages (0=all)")
	msgsCmd.Flags().String("since", "", "Messages since date (e.g. 01-Jan-2024)")
	msgsCmd.Flags().String("before", "", "Messages before date")
	root.AddCommand(msgsCmd)

	// fetch
	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch full message content (reads message jsonl from stdin or --mailbox+--uid)",
		RunE:  runFetch,
	}
	fetchCmd.Flags().StringP("mailbox", "m", "", "Mailbox name")
	fetchCmd.Flags().Uint32P("uid", "u", 0, "Message UID")
	fetchCmd.Flags().Bool("no-attachments", false, "Omit attachment bodies")
	root.AddCommand(fetchCmd)

	if err := root.Execute(); err != nil {
		emitErr(err.Error())
		os.Exit(1)
	}
}
