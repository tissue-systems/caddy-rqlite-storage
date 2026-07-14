package rqlitestorage

import "time"

// NewHTTPStore builds a Store backed by the rqlite node reachable at baseURL
// (e.g. "http://127.0.0.1:4001"). username/password may be empty when rqlite has
// no HTTP basic auth. This is the constructor the Caddy module uses.
func NewHTTPStore(baseURL, username, password string) *Store {
	return NewStore(newHTTPConn(baseURL, username, password))
}

// SetLockTTL overrides how long an acquired lock is held before it may be stolen
// by another instance (and how often a held lock is refreshed: TTL/3). No-op for d <= 0.
func (s *Store) SetLockTTL(d time.Duration) {
	if d > 0 {
		s.lockTTL = d
	}
}

// SetReadLevel sets the rqlite read-consistency level used for key lookups:
// "weak" (default), "none", "linearizable", or "strong". "weak" routes reads
// through the Raft leader, so the store always sees its own writes. "none"
// reads the local node's FSM — faster, but on a multi-node cluster a
// non-leader node can lag right after a write forwarded from it, which fails
// CertMagic's write-then-read storage preflight and aborts certificate
// obtains; only use "none" on single-node deployments.
// No-op (returns nil) when the Store is not backed by the rqlite HTTP transport.
func (s *Store) SetReadLevel(level string) error {
	if h, ok := s.conn.(*httpConn); ok {
		return h.setReadLevel(level)
	}
	return nil
}
