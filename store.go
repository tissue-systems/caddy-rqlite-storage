// Package rqlitestorage implements a CertMagic/Caddy storage backend on top of
// rqlite (https://rqlite.io) — distributed, Raft-replicated SQLite.
//
// It lets a fleet of Caddy instances share one certificate store: certificates and
// keys are shared, and ACME challenge tokens live in rqlite, so whichever instance a
// challenge lands on can answer it, and a cert issued by one instance is immediately
// usable by the others. rqlite's Raft consensus provides the linearizable distributed
// locking an ACME cluster needs — two instances must never solve or issue the same
// certificate concurrently — without standing up a separate datastore for it.
//
// This file is the transport-agnostic core: all storage and locking logic, exercised
// in full by the unit tests against an in-memory SQLite. The production rqlite HTTP
// transport is in httpconn.go; the Caddy module / certmagic.Storage adapter lives in
// the ./caddymodule subpackage so this core stays dependency-light.
package rqlitestorage

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotExist is returned by Get/Stat/Del when the key is absent. The certmagic
// adapter maps it to fs.ErrNotExist so CertMagic's errors.Is checks succeed.
var ErrNotExist = errors.New("rqlitestorage: does not exist")

// Statement is one parameterized SQL statement (`?` placeholders).
type Statement struct {
	SQL  string
	Args []any
}

// conn is the minimal transport the Store needs. Production is *httpConn (rqlite's
// HTTP API); tests use an in-memory SQLite fake. Implementations must run Exec's
// statements as a single atomic transaction (rqlite does this for a statement array).
type conn interface {
	// Exec runs the statements as one transaction and returns rows-affected per statement.
	Exec(ctx context.Context, stmts ...Statement) ([]int64, error)
	// Query runs one read statement and returns column names and the result rows.
	Query(ctx context.Context, stmt Statement) (cols []string, rows [][]any, err error)
}

// KeyInfo mirrors the subset of certmagic.KeyInfo the store produces.
type KeyInfo struct {
	Key      string
	Modified time.Time
	Size     int64
}

const (
	defaultLockTTL     = 60 * time.Second
	defaultLockPoll    = 1 * time.Second
	lockRefreshDivisor = 3 // refresh a held lock every TTL/3
)

// Store holds the storage + locking logic over a conn.
type Store struct {
	conn    conn
	lockTTL time.Duration
	poll    time.Duration

	// Injectable for deterministic tests; default to wall clock / crypto-random.
	now      func() time.Time
	newToken func() string
	noKeeper bool // when true, Lock does not spawn the keepalive goroutine (tests)

	mu      sync.Mutex
	keepers map[string]*keeper // active held locks, by name
}

type keeper struct {
	token  string
	cancel context.CancelFunc
}

// NewStore builds a Store over c with default lock TTL/poll.
func NewStore(c conn) *Store {
	return &Store{
		conn:     c,
		lockTTL:  defaultLockTTL,
		poll:     defaultLockPoll,
		now:      time.Now,
		newToken: randomToken,
		keepers:  make(map[string]*keeper),
	}
}

func randomToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// EnsureSchema creates the storage and lock tables if absent. Idempotent.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.conn.Exec(ctx,
		Statement{SQL: `CREATE TABLE IF NOT EXISTS caddy_storage (
			key      TEXT PRIMARY KEY,
			value    TEXT    NOT NULL,
			size     INTEGER NOT NULL,
			modified INTEGER NOT NULL
		)`},
		Statement{SQL: `CREATE TABLE IF NOT EXISTS caddy_locks (
			name    TEXT PRIMARY KEY,
			token   TEXT    NOT NULL,
			expires INTEGER NOT NULL
		)`},
	)
	return err
}

// Put stores value under key (upsert). Values are base64-encoded so arbitrary
// binary survives rqlite's JSON transport untouched.
func (s *Store) Put(ctx context.Context, key string, value []byte) error {
	enc := base64.StdEncoding.EncodeToString(value)
	_, err := s.conn.Exec(ctx, Statement{
		SQL: `INSERT INTO caddy_storage (key, value, size, modified) VALUES (?, ?, ?, ?)
		      ON CONFLICT(key) DO UPDATE SET value=excluded.value, size=excluded.size, modified=excluded.modified`,
		Args: []any{key, enc, int64(len(value)), s.now().UnixNano()},
	})
	return err
}

// Get returns the value stored under key, or ErrNotExist.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	_, rows, err := s.conn.Query(ctx, Statement{
		SQL: `SELECT value FROM caddy_storage WHERE key = ?`, Args: []any{key},
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotExist
	}
	enc, err := asString(rows[0][0])
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(enc)
}

// Has reports whether key exists.
func (s *Store) Has(ctx context.Context, key string) bool {
	_, rows, err := s.conn.Query(ctx, Statement{
		SQL: `SELECT 1 FROM caddy_storage WHERE key = ?`, Args: []any{key},
	})
	return err == nil && len(rows) > 0
}

// Del removes key, or returns ErrNotExist if it was absent.
func (s *Store) Del(ctx context.Context, key string) error {
	affected, err := s.conn.Exec(ctx, Statement{
		SQL: `DELETE FROM caddy_storage WHERE key = ?`, Args: []any{key},
	})
	if err != nil {
		return err
	}
	if len(affected) == 0 || affected[0] == 0 {
		return ErrNotExist
	}
	return nil
}

// Stat returns size/modified metadata for key, or ErrNotExist.
func (s *Store) Stat(ctx context.Context, key string) (KeyInfo, error) {
	_, rows, err := s.conn.Query(ctx, Statement{
		SQL: `SELECT size, modified FROM caddy_storage WHERE key = ?`, Args: []any{key},
	})
	if err != nil {
		return KeyInfo{}, err
	}
	if len(rows) == 0 {
		return KeyInfo{}, ErrNotExist
	}
	size, err := asInt64(rows[0][0])
	if err != nil {
		return KeyInfo{}, err
	}
	modNanos, err := asInt64(rows[0][1])
	if err != nil {
		return KeyInfo{}, err
	}
	return KeyInfo{Key: key, Size: size, Modified: time.Unix(0, modNanos).UTC()}, nil
}

// List returns the keys under prefix. With recursive=false only immediate children
// (one path segment beyond prefix, '/'-separated) are returned, deduplicated.
// An empty prefix lists the whole store.
func (s *Store) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var stmt Statement
	if prefix == "" {
		stmt = Statement{SQL: `SELECT key FROM caddy_storage ORDER BY key`}
	} else {
		stmt = Statement{
			SQL:  `SELECT key FROM caddy_storage WHERE key = ? OR key LIKE ? ESCAPE '\' ORDER BY key`,
			Args: []any{prefix, escapeLike(prefix) + `/%`},
		}
	}
	_, rows, err := s.conn.Query(ctx, stmt)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		k, err := asString(r[0])
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if recursive {
		return keys, nil
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		child := immediateChild(prefix, k)
		if child == "" {
			continue
		}
		if _, ok := seen[child]; ok {
			continue
		}
		seen[child] = struct{}{}
		out = append(out, child)
	}
	return out, nil
}

// Lock blocks until the named lock is acquired or ctx is done. Acquisition is a
// single atomic upsert (insert-if-absent, else steal-if-expired) serialized by
// rqlite's Raft, so at most one caller across the fleet holds the lock.
func (s *Store) Lock(ctx context.Context, name string) error {
	token := s.newToken()
	for {
		got, err := s.tryAcquire(ctx, name, token)
		if err != nil {
			return err
		}
		if got {
			s.startKeeper(name, token)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.poll):
		}
	}
}

// Unlock releases a lock previously acquired by this Store (matched by its token,
// so it can never delete another holder's lock). A lock whose keeper was lost
// (e.g. process crash) is left to expire via its TTL.
func (s *Store) Unlock(ctx context.Context, name string) error {
	s.mu.Lock()
	k, ok := s.keepers[name]
	if ok {
		delete(s.keepers, name)
		k.cancel()
	}
	s.mu.Unlock()
	if !ok {
		return nil
	}
	_, err := s.releaseLock(ctx, name, k.token)
	return err
}

// tryAcquire performs one atomic acquire-or-steal. Returns true if this caller now
// holds the lock. The ON CONFLICT ... WHERE only steals a lock whose expiry has
// passed; a fresh lock yields 0 rows-affected (not acquired).
func (s *Store) tryAcquire(ctx context.Context, name, token string) (bool, error) {
	now := s.now()
	affected, err := s.conn.Exec(ctx, Statement{
		SQL: `INSERT INTO caddy_locks (name, token, expires) VALUES (?, ?, ?)
		      ON CONFLICT(name) DO UPDATE SET token=excluded.token, expires=excluded.expires
		      WHERE caddy_locks.expires < ?`,
		Args: []any{name, token, now.Add(s.lockTTL).UnixNano(), now.UnixNano()},
	})
	if err != nil {
		return false, err
	}
	return len(affected) > 0 && affected[0] == 1, nil
}

// refreshLock extends a held lock's expiry, but only if we still own it (token match).
// Returns false if the lock was stolen or removed.
func (s *Store) refreshLock(ctx context.Context, name, token string) (bool, error) {
	affected, err := s.conn.Exec(ctx, Statement{
		SQL:  `UPDATE caddy_locks SET expires = ? WHERE name = ? AND token = ?`,
		Args: []any{s.now().Add(s.lockTTL).UnixNano(), name, token},
	})
	if err != nil {
		return false, err
	}
	return len(affected) > 0 && affected[0] == 1, nil
}

// releaseLock deletes a lock we own (token match). Returns false if nothing was deleted.
func (s *Store) releaseLock(ctx context.Context, name, token string) (bool, error) {
	affected, err := s.conn.Exec(ctx, Statement{
		SQL:  `DELETE FROM caddy_locks WHERE name = ? AND token = ?`,
		Args: []any{name, token},
	})
	if err != nil {
		return false, err
	}
	return len(affected) > 0 && affected[0] == 1, nil
}

// startKeeper records the held lock and (unless disabled) periodically refreshes its
// expiry until Unlock, so long-running ACME operations don't have their lock stolen.
// The keepalive uses a background context — the lock outlives the Lock() ctx by design.
func (s *Store) startKeeper(name, token string) {
	kctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	if old, ok := s.keepers[name]; ok {
		old.cancel()
	}
	s.keepers[name] = &keeper{token: token, cancel: cancel}
	s.mu.Unlock()

	if s.noKeeper {
		return
	}
	refresh := s.lockTTL / lockRefreshDivisor
	if refresh <= 0 {
		refresh = time.Second
	}
	go func() {
		ticker := time.NewTicker(refresh)
		defer ticker.Stop()
		for {
			select {
			case <-kctx.Done():
				return
			case <-ticker.C:
				ok, err := s.refreshLock(kctx, name, token)
				if err != nil {
					continue // transient; try again next tick
				}
				if !ok {
					return // lost the lock; stop refreshing
				}
			}
		}
	}()
}

// immediateChild returns the single path segment of key directly under prefix
// (joined back onto prefix), or "" if key is not under prefix.
func immediateChild(prefix, key string) string {
	if prefix == "" {
		if i := strings.IndexByte(key, '/'); i >= 0 {
			return key[:i]
		}
		return key
	}
	if key == prefix {
		return key
	}
	rel := strings.TrimPrefix(key, prefix+"/")
	if rel == key {
		return "" // not under prefix
	}
	if i := strings.IndexByte(rel, '/'); i >= 0 {
		return prefix + "/" + rel[:i]
	}
	return prefix + "/" + rel
}

// escapeLike escapes LIKE metacharacters so a prefix is matched literally
// (paired with `ESCAPE '\'` in the query).
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func asString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("rqlitestorage: expected string, got %T", v)
	}
}

func asInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case string:
		return strconv.ParseInt(t, 10, 64)
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	default:
		return 0, fmt.Errorf("rqlitestorage: expected integer, got %T", v)
	}
}
