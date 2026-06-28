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
