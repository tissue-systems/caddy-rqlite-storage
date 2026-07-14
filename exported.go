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
// "none" (default), "weak", "linearizable", or "strong". The default reads the
// local node's FSM, which can lag the leader right after a write forwarded from
// a follower — CertMagic's write-then-read storage preflight fails on that lag
// whenever the local node is not the Raft leader, aborting certificate obtains.
// Use "weak" on multi-node clusters: reads route through the leader, so the
// store always sees its own writes, at the cost of one intra-cluster hop.
// No-op (returns nil) when the Store is not backed by the rqlite HTTP transport.
func (s *Store) SetReadLevel(level string) error {
	if h, ok := s.conn.(*httpConn); ok {
		return h.setReadLevel(level)
	}
	return nil
}
