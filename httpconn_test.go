package rqlitestorage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeRqlite is a minimal stand-in for rqlite's HTTP data API that records the last
// request body and returns a canned response per path.
type fakeRqlite struct {
	srv         *httptest.Server
	lastPath    string
	lastBody    string
	lastAuthOK  bool
	execResp    string
	queryResp   string
	statusCode  int
}

func newFakeRqlite(t *testing.T) *fakeRqlite {
	t.Helper()
	f := &fakeRqlite{statusCode: http.StatusOK}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.lastPath = r.URL.Path + "?" + r.URL.RawQuery
		f.lastBody = string(b)
		if u, p, ok := r.BasicAuth(); ok && u == "user" && p == "pass" {
			f.lastAuthOK = true
		}
		if f.statusCode != http.StatusOK {
			w.WriteHeader(f.statusCode)
			_, _ = w.Write([]byte("upstream error"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/db/execute"):
			_, _ = io.WriteString(w, f.execResp)
		case strings.HasPrefix(r.URL.Path, "/db/query"):
			_, _ = io.WriteString(w, f.queryResp)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestHTTPConnExec(t *testing.T) {
	f := newFakeRqlite(t)
	f.execResp = `{"results":[{"rows_affected":1},{"rows_affected":0}]}`
	c := newHTTPConn(f.srv.URL, "user", "pass")

	affected, err := c.Exec(context.Background(),
		Statement{SQL: "INSERT INTO t(a) VALUES(?)", Args: []any{"x"}},
		Statement{SQL: "DELETE FROM t WHERE a=?", Args: []any{"y"}},
	)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if len(affected) != 2 || affected[0] != 1 || affected[1] != 0 {
		t.Fatalf("rows-affected: %v", affected)
	}
	if !f.lastAuthOK {
		t.Fatal("basic auth header was not sent")
	}
	if !strings.Contains(f.lastPath, "/db/execute") || !strings.Contains(f.lastPath, "transaction") {
		t.Fatalf("path: %s", f.lastPath)
	}
	// Body must be rqlite's array-of-[sql, args...] form.
	var body [][]any
	if err := json.Unmarshal([]byte(f.lastBody), &body); err != nil {
		t.Fatalf("body not a statement array: %v (%s)", err, f.lastBody)
	}
	if len(body) != 2 || body[0][0] != "INSERT INTO t(a) VALUES(?)" || body[0][1] != "x" {
		t.Fatalf("encoded statements: %s", f.lastBody)
	}
}

func TestHTTPConnQuery(t *testing.T) {
	f := newFakeRqlite(t)
	f.queryResp = `{"results":[{"columns":["value"],"types":["text"],"values":[["aGVsbG8="]]}]}`
	c := newHTTPConn(f.srv.URL, "", "")

	cols, rows, err := c.Query(context.Background(), Statement{SQL: "SELECT value FROM t WHERE k=?", Args: []any{"k"}})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(cols) != 1 || cols[0] != "value" {
		t.Fatalf("cols: %v", cols)
	}
	if len(rows) != 1 || rows[0][0] != "aGVsbG8=" {
		t.Fatalf("rows: %v", rows)
	}
	if !strings.Contains(f.lastPath, "/db/query") || !strings.Contains(f.lastPath, "level=weak") {
		t.Fatalf("path: %s", f.lastPath) // default read level is weak (see setReadLevel)
	}
}

func TestHTTPConnReadLevel(t *testing.T) {
	f := newFakeRqlite(t)
	f.queryResp = `{"results":[{"columns":["value"],"types":["text"],"values":[["x"]]}]}`
	c := newHTTPConn(f.srv.URL, "", "")

	if err := c.setReadLevel("bogus"); err == nil {
		t.Fatal("setReadLevel(bogus): want error, got nil")
	}
	if err := c.setReadLevel("none"); err != nil {
		t.Fatalf("setReadLevel(none): %v", err)
	}
	if _, _, err := c.Query(context.Background(), Statement{SQL: "SELECT 1"}); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(f.lastPath, "level=none") {
		t.Fatalf("path after setReadLevel(none): %s", f.lastPath)
	}
}

func TestHTTPConnQueryEmpty(t *testing.T) {
	f := newFakeRqlite(t)
	f.queryResp = `{"results":[{"columns":["value"],"types":["text"]}]}` // no rows
	c := newHTTPConn(f.srv.URL, "", "")
	_, rows, err := c.Query(context.Background(), Statement{SQL: "SELECT value FROM t WHERE k=?", Args: []any{"nope"}})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows, got %v", rows)
	}
}

func TestHTTPConnIntegerDecoding(t *testing.T) {
	// Integers arrive as JSON numbers; UseNumber + asInt64 must recover them exactly,
	// including values beyond float64's exact-integer range.
	f := newFakeRqlite(t)
	big := int64(1_700_000_000_123_456_789)
	f.queryResp = `{"results":[{"columns":["size","modified"],"values":[[5,` + itoa(big) + `]]}]}`
	c := newHTTPConn(f.srv.URL, "", "")
	_, rows, err := c.Query(context.Background(), Statement{SQL: "SELECT size, modified FROM t"})
	if err != nil {
		t.Fatal(err)
	}
	size, err := asInt64(rows[0][0])
	if err != nil || size != 5 {
		t.Fatalf("size: %v err=%v", size, err)
	}
	mod, err := asInt64(rows[0][1])
	if err != nil || mod != big {
		t.Fatalf("modified: got %d want %d err=%v", mod, big, err)
	}
}

func TestHTTPConnResultError(t *testing.T) {
	f := newFakeRqlite(t)
	f.execResp = `{"results":[{"error":"near \"FROM\": syntax error"}]}`
	c := newHTTPConn(f.srv.URL, "", "")
	_, err := c.Exec(context.Background(), Statement{SQL: "bad"})
	if err == nil || !strings.Contains(err.Error(), "syntax error") {
		t.Fatalf("want statement error surfaced, got %v", err)
	}
}

func TestHTTPConnHTTPError(t *testing.T) {
	f := newFakeRqlite(t)
	f.statusCode = http.StatusServiceUnavailable
	c := newHTTPConn(f.srv.URL, "", "")
	_, err := c.Exec(context.Background(), Statement{SQL: "x"})
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("want HTTP 503 surfaced, got %v", err)
	}
}

// TestHTTPConnAgainstStore round-trips the Store through the HTTP transport, with a
// tiny in-memory SQLite behind the fake server — proving the wire encoding and the
// Store logic compose end to end.
func TestHTTPConnAgainstStore(t *testing.T) {
	back := newFakeConn(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var stmts [][]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &stmts)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/db/execute") {
			results := make([]map[string]any, 0, len(stmts))
			for _, st := range stmts {
				n, err := back.Exec(r.Context(), toStatement(st))
				if err != nil {
					results = append(results, map[string]any{"error": err.Error()})
					continue
				}
				results = append(results, map[string]any{"rows_affected": n[0]})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
			return
		}
		// query: single statement
		cols, rows, err := back.Query(r.Context(), toStatement(stmts[0]))
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"error": err.Error()}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{map[string]any{"columns": cols, "values": rows}},
		})
	}))
	t.Cleanup(srv.Close)

	s := NewStore(newHTTPConn(srv.URL, "", ""))
	s.noKeeper = true
	ctx := context.Background()
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema over HTTP: %v", err)
	}
	if err := s.Put(ctx, "acme/a/cert", []byte("PEMDATA")); err != nil {
		t.Fatalf("Put over HTTP: %v", err)
	}
	got, err := s.Get(ctx, "acme/a/cert")
	if err != nil || string(got) != "PEMDATA" {
		t.Fatalf("Get over HTTP: %q err=%v", got, err)
	}
	if err := s.Lock(ctx, "L"); err != nil {
		t.Fatalf("Lock over HTTP: %v", err)
	}
	if err := s.Unlock(ctx, "L"); err != nil {
		t.Fatalf("Unlock over HTTP: %v", err)
	}
}

func toStatement(arr []any) Statement {
	sql, _ := arr[0].(string)
	return Statement{SQL: sql, Args: arr[1:]}
}

func itoa(v int64) string {
	return strings.TrimSpace(jsonNumber(v))
}
func jsonNumber(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
