// Package caddymodule registers the rqlite CertMagic storage backend as a Caddy
// module (`caddy.storage.rqlite`) and adapts the transport-agnostic core in the
// parent package to certmagic.Storage. This is the package xcaddy compiles in:
//
//	xcaddy build --with github.com/tissue-systems/caddy-rqlite-storage/caddymodule
//
// Caddyfile usage (global options):
//
//	{
//	    storage rqlite {
//	        url      http://127.0.0.1:4001
//	        username {$RQLITE_USER}
//	        password {$RQLITE_PASSWORD}
//	        lock_ttl 60s
//	    }
//	}
package caddymodule

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	rqlitestorage "github.com/tissue-systems/caddy-rqlite-storage"
)

func init() {
	caddy.RegisterModule(RqliteStorage{})
}

// RqliteStorage is the Caddy module configuration for rqlite-backed cert storage.
type RqliteStorage struct {
	// URL is the rqlite node's HTTP API base, e.g. http://127.0.0.1:4001.
	URL string `json:"url,omitempty"`
	// Username/Password are optional rqlite HTTP basic-auth credentials.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	// LockTTL is how long an acquired ACME lock is held before it may be stolen.
	LockTTL caddy.Duration `json:"lock_ttl,omitempty"`
	// ReadLevel is the rqlite read-consistency level for key lookups: "weak"
	// (default; leader-routed, always sees its own writes), "none" (fast local
	// reads — single-node only: on a cluster a non-leader's stale read fails
	// CertMagic's storage preflight), "linearizable", or "strong".
	ReadLevel string `json:"read_level,omitempty"`

	store *rqlitestorage.Store
}

// CaddyModule returns the Caddy module information.
func (RqliteStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.rqlite",
		New: func() caddy.Module { return new(RqliteStorage) },
	}
}

// Provision builds the store and ensures the backing tables exist.
func (s *RqliteStorage) Provision(ctx caddy.Context) error {
	repl := caddy.NewReplacer()
	url := repl.ReplaceAll(s.URL, "")
	if url == "" {
		url = "http://127.0.0.1:4001"
	}
	st := rqlitestorage.NewHTTPStore(url, repl.ReplaceAll(s.Username, ""), repl.ReplaceAll(s.Password, ""))
	if s.LockTTL > 0 {
		st.SetLockTTL(time.Duration(s.LockTTL))
	}
	if lvl := repl.ReplaceAll(s.ReadLevel, ""); lvl != "" {
		if err := st.SetReadLevel(lvl); err != nil {
			return fmt.Errorf("rqlite storage: %w", err)
		}
	}
	if err := st.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("rqlite storage: ensure schema: %w", err)
	}
	s.store = st
	return nil
}

// CertMagicStorage returns the certmagic.Storage implementation.
func (s *RqliteStorage) CertMagicStorage() (certmagic.Storage, error) {
	if s.store == nil {
		return nil, errors.New("rqlite storage: not provisioned")
	}
	return &storage{s.store}, nil
}

// UnmarshalCaddyfile parses the `storage rqlite { ... }` block.
func (s *RqliteStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() { // optional inline: storage rqlite <url>
			s.URL = d.Val()
		}
		for d.NextBlock(0) {
			switch d.Val() {
			case "url":
				if !d.Args(&s.URL) {
					return d.ArgErr()
				}
			case "username":
				if !d.Args(&s.Username) {
					return d.ArgErr()
				}
			case "password":
				if !d.Args(&s.Password) {
					return d.ArgErr()
				}
			case "lock_ttl":
				var raw string
				if !d.Args(&raw) {
					return d.ArgErr()
				}
				dur, err := caddy.ParseDuration(raw)
				if err != nil {
					return d.Errf("invalid lock_ttl: %v", err)
				}
				s.LockTTL = caddy.Duration(dur)
			case "read_level":
				if !d.Args(&s.ReadLevel) {
					return d.ArgErr()
				}
			default:
				return d.Errf("unrecognized rqlite storage option: %s", d.Val())
			}
		}
	}
	return nil
}

// storage adapts the core Store to certmagic.Storage, mapping the core's ErrNotExist
// to fs.ErrNotExist (which CertMagic detects via errors.Is).
type storage struct {
	s *rqlitestorage.Store
}

func (a *storage) Store(ctx context.Context, key string, value []byte) error {
	return a.s.Put(ctx, key, value)
}

func (a *storage) Load(ctx context.Context, key string) ([]byte, error) {
	v, err := a.s.Get(ctx, key)
	return v, mapErr(err)
}

func (a *storage) Delete(ctx context.Context, key string) error {
	return mapErr(a.s.Del(ctx, key))
}

func (a *storage) Exists(ctx context.Context, key string) bool {
	return a.s.Has(ctx, key)
}

func (a *storage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	return a.s.List(ctx, path, recursive)
}

func (a *storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	ki, err := a.s.Stat(ctx, key)
	if err != nil {
		return certmagic.KeyInfo{}, mapErr(err)
	}
	return certmagic.KeyInfo{
		Key:        ki.Key,
		Modified:   ki.Modified,
		Size:       ki.Size,
		IsTerminal: true, // we only store leaf keys
	}, nil
}

func (a *storage) Lock(ctx context.Context, name string) error {
	return a.s.Lock(ctx, name)
}

func (a *storage) Unlock(ctx context.Context, name string) error {
	return a.s.Unlock(ctx, name)
}

func mapErr(err error) error {
	if errors.Is(err, rqlitestorage.ErrNotExist) {
		return fs.ErrNotExist
	}
	return err
}

// Interface guards.
var (
	_ caddy.Module           = (*RqliteStorage)(nil)
	_ caddy.Provisioner      = (*RqliteStorage)(nil)
	_ caddy.StorageConverter = (*RqliteStorage)(nil)
	_ caddyfile.Unmarshaler  = (*RqliteStorage)(nil)
	_ certmagic.Storage      = (*storage)(nil)
)
