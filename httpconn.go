package rqlitestorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// defaultLeaderRetry bounds how long post() keeps retrying an rqlite 503. Every
// peer reboot costs the cluster a Raft election (observed: 5-7 s on this fleet),
// and a running caddy that happens to renew, lock or read during one would
// otherwise see a hard error: a failed renewal, or a cert obtain aborted by
// CertMagic's storage preflight. Bounded rather than open-ended because caddy
// also loads every managed certificate through this path at startup, and an
// unbounded wait there would trade a visible failure for a hung process.
const defaultLeaderRetry = 10 * time.Second

// httpConn is the production conn: it speaks rqlite's HTTP data API
// (POST /db/execute, POST /db/query). Writes go through Raft (consistent);
// reads use the configured consistency level. The default is "weak"
// (leader-routed): with "none", a non-leader node can miss its own
// just-forwarded write (follower FSM lag), which fails CertMagic's
// write-then-read storage preflight and silently aborts cert obtains.
// "none" remains available as an explicit opt-in for single-node setups.
type httpConn struct {
	client      *http.Client
	baseURL     string
	username    string
	password    string
	readLevel   string
	leaderRetry time.Duration
}

var _ conn = (*httpConn)(nil)

// newHTTPConn builds a conn for the rqlite node at baseURL (e.g. http://127.0.0.1:4001).
// username/password may be empty if rqlite has no basic auth.
func newHTTPConn(baseURL, username, password string) *httpConn {
	return &httpConn{
		client:      &http.Client{Timeout: 10 * time.Second},
		baseURL:     strings.TrimRight(baseURL, "/"),
		username:    username,
		password:    password,
		readLevel:   "weak",
		leaderRetry: defaultLeaderRetry,
	}
}

// validReadLevels are the rqlite read-consistency levels accepted by setReadLevel.
var validReadLevels = map[string]bool{
	"none": true, "weak": true, "linearizable": true, "strong": true,
}

// setLeaderRetry bounds the retry budget for rqlite 503s. Zero disables retrying;
// negative is rejected so a typo cannot silently turn the retry off.
func (h *httpConn) setLeaderRetry(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("invalid rqlite leader retry %s (want a non-negative duration)", d)
	}
	h.leaderRetry = d
	return nil
}

func (h *httpConn) setReadLevel(level string) error {
	if !validReadLevels[level] {
		return fmt.Errorf("invalid rqlite read level %q (want none, weak, linearizable, or strong)", level)
	}
	h.readLevel = level
	return nil
}

type rqliteResponse struct {
	Results []rqliteResult `json:"results"`
}

type rqliteResult struct {
	Columns      []string `json:"columns"`
	Values       [][]any  `json:"values"`
	RowsAffected int64    `json:"rows_affected"`
	Error        string   `json:"error"`
}

// stmtToArray renders a Statement as rqlite's `["SQL", arg1, arg2, ...]` form.
func stmtToArray(s Statement) []any {
	out := make([]any, 0, len(s.Args)+1)
	out = append(out, s.SQL)
	out = append(out, s.Args...)
	return out
}

func (h *httpConn) Exec(ctx context.Context, stmts ...Statement) ([]int64, error) {
	body := make([]any, len(stmts))
	for i, s := range stmts {
		body[i] = stmtToArray(s)
	}
	resp, err := h.post(ctx, "/db/execute?transaction", body)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(resp.Results))
	for i, r := range resp.Results {
		if r.Error != "" {
			return nil, fmt.Errorf("rqlite execute: %s", r.Error)
		}
		out[i] = r.RowsAffected
	}
	return out, nil
}

func (h *httpConn) Query(ctx context.Context, stmt Statement) ([]string, [][]any, error) {
	resp, err := h.post(ctx, "/db/query?level="+h.readLevel, []any{stmtToArray(stmt)})
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Results) == 0 {
		return nil, nil, nil
	}
	r := resp.Results[0]
	if r.Error != "" {
		return nil, nil, fmt.Errorf("rqlite query: %s", r.Error)
	}
	return r.Columns, r.Values, nil
}

// post sends body to path, retrying while rqlite answers 503.
//
// 503 is rqlite's "leader not found": the node has no leader to apply or forward
// to, so the statement never entered the Raft log and a retry cannot double-apply
// it. That is why only this status is retried — a transport error (connection
// reset, client timeout) may have left a write applied with the response lost,
// and replaying it is not safe. Non-2xx statuses other than 503 are caller
// errors and would fail identically on a retry.
func (h *httpConn) post(ctx context.Context, path string, body any) (*rqliteResponse, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(h.leaderRetry)
	const maxBackoff = time.Second
	backoff := 100 * time.Millisecond
	for {
		resp, status, err := h.postOnce(ctx, path, buf)
		if err == nil || status != http.StatusServiceUnavailable || !time.Now().Before(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff = min(2*backoff, maxBackoff)
		}
	}
}

// postOnce performs a single request. It returns the HTTP status alongside the
// error so post can tell a retryable 503 from everything else; status is 0 when
// the request never produced a response.
func (h *httpConn) postOnce(ctx context.Context, path string, buf []byte) (*rqliteResponse, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.username != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, resp.StatusCode, fmt.Errorf("rqlite HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	// UseNumber so integer columns decode as json.Number (asInt64 handles it) rather
	// than lossy float64.
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var out rqliteResponse
	if err := dec.Decode(&out); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("rqlite decode: %w", err)
	}
	return &out, resp.StatusCode, nil
}
