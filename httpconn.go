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

// httpConn is the production conn: it speaks rqlite's HTTP data API
// (POST /db/execute, POST /db/query). Writes go through Raft (consistent);
// reads use the configured consistency level (default "none" — fast local reads,
// fine for cert/lock-state lookups since lock *acquisition* is a write).
type httpConn struct {
	client    *http.Client
	baseURL   string
	username  string
	password  string
	readLevel string
}

var _ conn = (*httpConn)(nil)

// newHTTPConn builds a conn for the rqlite node at baseURL (e.g. http://127.0.0.1:4001).
// username/password may be empty if rqlite has no basic auth.
func newHTTPConn(baseURL, username, password string) *httpConn {
	return &httpConn{
		client:    &http.Client{Timeout: 10 * time.Second},
		baseURL:   strings.TrimRight(baseURL, "/"),
		username:  username,
		password:  password,
		readLevel: "none",
	}
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

func (h *httpConn) post(ctx context.Context, path string, body any) (*rqliteResponse, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.username != "" {
		req.SetBasicAuth(h.username, h.password)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("rqlite HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	// UseNumber so integer columns decode as json.Number (asInt64 handles it) rather
	// than lossy float64.
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var out rqliteResponse
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("rqlite decode: %w", err)
	}
	return &out, nil
}
