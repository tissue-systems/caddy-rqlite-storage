package rqlitestorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// fakeConn implements conn against an in-memory SQLite database, so the tests run
// the *exact* SQL the Store emits (real LIKE, real ON CONFLICT semantics, real
// rows-affected) without a live rqlite server. A single pinned connection + mutex
// models rqlite's transactional write serialization.
type fakeConn struct {
	db      *sql.DB
	mu      sync.Mutex
	failNow error // when set, the next Exec/Query returns it (one-shot)
}

func newFakeConn(t *testing.T) *fakeConn {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1) // :memory: lives in one connection
	t.Cleanup(func() { _ = db.Close() })
	return &fakeConn{db: db}
}

func (f *fakeConn) Exec(ctx context.Context, stmts ...Statement) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNow != nil {
		err := f.failNow
		f.failNow = nil
		return nil, err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(stmts))
	for i, s := range stmts {
		res, err := tx.ExecContext(ctx, s.SQL, s.Args...)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		out[i], _ = res.RowsAffected()
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fakeConn) Query(ctx context.Context, stmt Statement) ([]string, [][]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNow != nil {
		err := f.failNow
		f.failNow = nil
		return nil, nil, err
	}
	rows, err := f.db.QueryContext(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		out = append(out, vals)
	}
	return cols, out, rows.Err()
}

// newTestStore returns a Store wired to a fresh in-memory db with the schema applied,
// the keepalive goroutine disabled, and a controllable clock.
func newTestStore(t *testing.T) (*Store, *fakeConn, *fakeClock) {
	t.Helper()
	fc := newFakeConn(t)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	s := NewStore(fc)
	s.now = clk.Now
	s.noKeeper = true
	s.lockTTL = 30 * time.Second
	s.poll = 2 * time.Millisecond
	if err := s.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return s, fc, clk
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestEnsureSchemaIdempotent(t *testing.T) {
	s, _, _ := newTestStore(t)
	if err := s.EnsureSchema(ctxT(t)); err != nil {
		t.Fatalf("second EnsureSchema: %v", err)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	s, _, _ := newTestStore(t)
	ctx := ctxT(t)
	// Arbitrary binary, including a NUL and high bytes — must survive base64 transport.
	val := []byte{0x00, 0x01, 0xff, 0xfe, 'h', 'i', 0x80}
	if err := s.Put(ctx, "acme/example.com/cert.pem", val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "acme/example.com/cert.pem")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("round-trip mismatch: got %v want %v", got, val)
	}
}

func TestGetNotExist(t *testing.T) {
	s, _, _ := newTestStore(t)
	_, err := s.Get(ctxT(t), "missing")
	if !errors.Is(err, ErrNotExist) {
		t.Fatalf("want ErrNotExist, got %v", err)
	}
}

func TestPutOverwrite(t *testing.T) {
	s, _, clk := newTestStore(t)
	ctx := ctxT(t)
	if err := s.Put(ctx, "k", []byte("first")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	if err := s.Put(ctx, "k", []byte("second-longer")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second-longer" {
		t.Fatalf("overwrite: got %q", got)
	}
	info, err := s.Stat(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len("second-longer")) {
		t.Fatalf("size after overwrite: got %d", info.Size)
	}
}

func TestExists(t *testing.T) {
	s, _, _ := newTestStore(t)
	ctx := ctxT(t)
	if s.Has(ctx, "k") {
		t.Fatal("Has should be false before Put")
	}
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !s.Has(ctx, "k") {
		t.Fatal("Has should be true after Put")
	}
}

func TestDelete(t *testing.T) {
	s, _, _ := newTestStore(t)
	ctx := ctxT(t)
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Del(ctx, "k"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if s.Has(ctx, "k") {
		t.Fatal("key should be gone after Del")
	}
	if err := s.Del(ctx, "k"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("second Del: want ErrNotExist, got %v", err)
	}
}

func TestStat(t *testing.T) {
	s, _, clk := newTestStore(t)
	ctx := ctxT(t)
	want := clk.Now()
	if err := s.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	info, err := s.Stat(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != "k" || info.Size != 5 {
		t.Fatalf("stat: %+v", info)
	}
	if !info.Modified.Equal(want) {
		t.Fatalf("modified: got %v want %v", info.Modified, want)
	}
	if _, err := s.Stat(ctx, "nope"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("stat missing: want ErrNotExist, got %v", err)
	}
}

func seed(t *testing.T, s *Store, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := s.Put(context.Background(), k, []byte("x")); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}
}

func TestListRecursive(t *testing.T) {
	s, _, _ := newTestStore(t)
	seed(t, s,
		"acme/example.com/sites/a", "acme/example.com/sites/b",
		"acme/example.com/keys/k", "acme/other.com/sites/c")
	got, err := s.List(ctxT(t), "acme/example.com", true)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"acme/example.com/keys/k", "acme/example.com/sites/a", "acme/example.com/sites/b",
	}
	assertSameSet(t, got, want)
}

func TestListNonRecursive(t *testing.T) {
	s, _, _ := newTestStore(t)
	seed(t, s,
		"acme/example.com/sites/a", "acme/example.com/sites/b",
		"acme/example.com/keys/k", "acme/example.com/meta")
	got, err := s.List(ctxT(t), "acme/example.com", false)
	if err != nil {
		t.Fatal(err)
	}
	// Immediate children only, deduped: "sites" and "keys" collapse, "meta" is a leaf.
	want := []string{
		"acme/example.com/keys", "acme/example.com/meta", "acme/example.com/sites",
	}
	assertSameSet(t, got, want)
}

func TestListEmptyPrefix(t *testing.T) {
	s, _, _ := newTestStore(t)
	seed(t, s, "a/x", "a/y", "b")
	rec, err := s.List(ctxT(t), "", true)
	if err != nil {
		t.Fatal(err)
	}
	assertSameSet(t, rec, []string{"a/x", "a/y", "b"})
	top, err := s.List(ctxT(t), "", false)
	if err != nil {
		t.Fatal(err)
	}
	assertSameSet(t, top, []string{"a", "b"})
}

// TestListLikeEscaping ensures a prefix containing LIKE metacharacters matches
// literally and does not pull in unrelated keys.
func TestListLikeEscaping(t *testing.T) {
	s, _, _ := newTestStore(t)
	seed(t, s, "a%b/child", "axb/child", "a_b/child", "aZb/child")
	got, err := s.List(ctxT(t), "a%b", true)
	if err != nil {
		t.Fatal(err)
	}
	assertSameSet(t, got, []string{"a%b/child"})

	got2, err := s.List(ctxT(t), "a_b", true)
	if err != nil {
		t.Fatal(err)
	}
	assertSameSet(t, got2, []string{"a_b/child"})
}

func TestListPrefixIsAlsoKey(t *testing.T) {
	s, _, _ := newTestStore(t)
	seed(t, s, "p", "p/a", "p/b/c")
	got, err := s.List(ctxT(t), "p", true)
	if err != nil {
		t.Fatal(err)
	}
	assertSameSet(t, got, []string{"p", "p/a", "p/b/c"})
}

// ---- locking ----

func TestLockUnlockBasic(t *testing.T) {
	s, _, _ := newTestStore(t)
	ctx := ctxT(t)
	if err := s.Lock(ctx, "issuer:example.com"); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := s.Unlock(ctx, "issuer:example.com"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// Re-acquire after unlock must succeed promptly.
	if err := s.Lock(ctx, "issuer:example.com"); err != nil {
		t.Fatalf("re-Lock: %v", err)
	}
}

// TestLockBlocksWhileHeld: a second holder cannot acquire while the lock is fresh,
// and gives up when its context deadline passes.
func TestLockBlocksWhileHeld(t *testing.T) {
	fc := newFakeConn(t)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	a := NewStore(fc)
	a.now, a.noKeeper, a.lockTTL, a.poll = clk.Now, true, time.Hour, 2*time.Millisecond
	if err := a.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := NewStore(fc) // same underlying store, different in-process holder
	b.now, b.noKeeper, b.lockTTL, b.poll = clk.Now, true, time.Hour, 2*time.Millisecond

	if err := a.Lock(context.Background(), "x"); err != nil {
		t.Fatalf("A Lock: %v", err)
	}
	bctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := b.Lock(bctx, "x")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("B Lock should time out while A holds it, got %v", err)
	}
	if err := a.Unlock(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(ctxT(t), "x"); err != nil {
		t.Fatalf("B Lock after A unlock: %v", err)
	}
}

// TestLockStealAfterExpiry: once a held lock's TTL elapses, another holder steals it.
func TestLockStealAfterExpiry(t *testing.T) {
	fc := newFakeConn(t)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0).UTC()}
	a := NewStore(fc)
	a.now, a.noKeeper, a.lockTTL, a.poll = clk.Now, true, 30*time.Second, 2*time.Millisecond
	if err := a.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := NewStore(fc)
	b.now, b.noKeeper, b.lockTTL, b.poll = clk.Now, true, 30*time.Second, 2*time.Millisecond

	if err := a.Lock(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	// A "crashes" without unlocking; time passes beyond the TTL.
	clk.Advance(31 * time.Second)
	if err := b.Lock(ctxT(t), "x"); err != nil {
		t.Fatalf("B should steal expired lock, got %v", err)
	}
}

// TestTryAcquireRefreshRelease drives the lock primitives directly.
func TestTryAcquireRefreshRelease(t *testing.T) {
	s, _, clk := newTestStore(t)
	ctx := ctxT(t)

	got, err := s.tryAcquire(ctx, "n", "tokA")
	if err != nil || !got {
		t.Fatalf("first acquire: got=%v err=%v", got, err)
	}
	// A different token cannot acquire while fresh.
	got, err = s.tryAcquire(ctx, "n", "tokB")
	if err != nil || got {
		t.Fatalf("acquire while held should fail: got=%v err=%v", got, err)
	}
	// Owner can refresh; non-owner cannot.
	if ok, err := s.refreshLock(ctx, "n", "tokA"); err != nil || !ok {
		t.Fatalf("owner refresh: ok=%v err=%v", ok, err)
	}
	if ok, err := s.refreshLock(ctx, "n", "tokB"); err != nil || ok {
		t.Fatalf("non-owner refresh should fail: ok=%v err=%v", ok, err)
	}
	// Non-owner cannot release; owner can.
	if ok, err := s.releaseLock(ctx, "n", "tokB"); err != nil || ok {
		t.Fatalf("non-owner release should fail: ok=%v err=%v", ok, err)
	}
	if ok, err := s.releaseLock(ctx, "n", "tokA"); err != nil || !ok {
		t.Fatalf("owner release: ok=%v err=%v", ok, err)
	}
	// After release a fresh acquire (even pre-expiry) succeeds.
	clk.Advance(time.Second)
	if ok, err := s.tryAcquire(ctx, "n", "tokC"); err != nil || !ok {
		t.Fatalf("acquire after release: ok=%v err=%v", ok, err)
	}
}

// TestLockMutualExclusion is the correctness test that matters: many goroutines
// contend for one lock; under it they read-modify-write a shared counter. If the
// lock weren't mutually exclusive, the final count would be wrong.
func TestLockMutualExclusion(t *testing.T) {
	fc := newFakeConn(t)
	if err := NewStore(fc).EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}

	const workers, iters = 8, 25
	var counter int
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := NewStore(fc)
			s.noKeeper, s.lockTTL, s.poll = true, time.Hour, time.Millisecond
			for i := 0; i < iters; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := s.Lock(ctx, "counter"); err != nil {
					cancel()
					errs <- err
					return
				}
				v := counter // read-modify-write a non-atomic shared var
				time.Sleep(50 * time.Microsecond)
				counter = v + 1
				if err := s.Unlock(ctx, "counter"); err != nil {
					cancel()
					errs <- err
					return
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker error: %v", err)
	}
	if counter != workers*iters {
		t.Fatalf("mutual exclusion violated: counter=%d want %d", counter, workers*iters)
	}
}

func TestExecErrorPropagates(t *testing.T) {
	s, fc, _ := newTestStore(t)
	fc.failNow = fmt.Errorf("boom")
	if err := s.Put(ctxT(t), "k", []byte("v")); err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}
}

func assertSameSet(t *testing.T, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if fmt.Sprint(g) != fmt.Sprint(w) {
		t.Fatalf("set mismatch:\n got  %v\n want %v", g, w)
	}
}
