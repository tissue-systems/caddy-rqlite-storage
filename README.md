# caddy-rqlite-storage

A [CertMagic](https://github.com/caddyserver/certmagic) / [Caddy](https://caddyserver.com) storage
backend on top of [rqlite](https://rqlite.io) — distributed, Raft-replicated SQLite.

For a fleet of Caddy instances that need a *shared* certificate store: certificates and keys are shared,
and ACME challenge tokens live in rqlite, so an ACME challenge landing on any instance can be solved, and a
cert issued by one instance is immediately usable by the others.

## Why rqlite

The hard requirement of a multi-instance Caddy cluster is **correct distributed locking** — two instances
must never solve or issue the same certificate concurrently. rqlite's Raft consensus provides linearizable
locking for that. If you already run an rqlite cluster, this backend reuses it — **no separate datastore**
to operate — and gets stronger locking than a single Redis would.

## Layout

| Package | Imports | Purpose |
|---|---|---|
| `.` (`rqlitestorage`) | stdlib only | Transport-agnostic core: storage + locking logic, rqlite HTTP transport. **Fully unit-tested.** |
| `./caddymodule` | caddy/v2, certmagic | Registers the `caddy.storage.rqlite` Caddy module and adapts the core to `certmagic.Storage`. |

The split keeps the core's tests fast and dependency-light (only an in-memory SQLite for the test fake),
while the Caddy glue stays isolated.

## Build into Caddy

```sh
xcaddy build \
  --with github.com/caddy-dns/rfc2136 \
  --with github.com/mholt/caddy-ratelimit \
  --with github.com/tissue-systems/caddy-rqlite-storage/caddymodule
```

## Configure (Caddyfile global options)

```caddyfile
{
    storage rqlite {
        url      http://127.0.0.1:4001   # this instance's local rqlite node
        username {$RQLITE_USER}           # optional HTTP basic-auth
        password {$RQLITE_PASSWORD}
        lock_ttl 60s                      # how long a held ACME lock survives before it may be stolen
    }
}
```

Each instance points at its **local** rqlite node; Raft replication makes the store identical everywhere.
Cert/key lookups use `level=none` reads (fast, local); lock acquisition is a leader write (consistent).

## Schema

Created automatically on provision (idempotent):

```sql
CREATE TABLE caddy_storage (key TEXT PRIMARY KEY, value TEXT /*base64*/, size INTEGER, modified INTEGER);
CREATE TABLE caddy_locks   (name TEXT PRIMARY KEY, token TEXT, expires INTEGER);
```

## Test

```sh
go test ./... -race
```

The core tests run the exact SQL the store emits against an in-memory SQLite, covering storage
round-trips (incl. binary), not-found semantics, listing (recursive / non-recursive / `LIKE`-escaping),
the lock primitives (acquire / steal-after-expiry / refresh / release / token-ownership), an
N-goroutine **mutual-exclusion** contention test, and the rqlite HTTP wire format end-to-end.

## License

[MIT](LICENSE)
