package caddymodule

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

// The Caddyfile block is a deploy-time surface: an option this parser does not
// recognise fails `caddy validate` and stops a rolling edge deploy, so every
// option the Caddyfile template may carry is asserted here.
func TestUnmarshalCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser(`rqlite {
		url      http://10.0.0.1:4001
		username u
		password p
		lock_ttl 60s
		read_level weak
		leader_retry 3s
	}`)
	var s RqliteStorage
	if err := s.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("UnmarshalCaddyfile: %v", err)
	}
	if s.URL != "http://10.0.0.1:4001" || s.Username != "u" || s.Password != "p" {
		t.Fatalf("connection fields: %+v", s)
	}
	if s.ReadLevel != "weak" {
		t.Fatalf("read_level: %q", s.ReadLevel)
	}
	if time.Duration(s.LockTTL) != time.Minute {
		t.Fatalf("lock_ttl: %s", time.Duration(s.LockTTL))
	}
	if time.Duration(s.LeaderRetry) != 3*time.Second {
		t.Fatalf("leader_retry: %s", time.Duration(s.LeaderRetry))
	}
}

func TestUnmarshalCaddyfileRejectsBadInput(t *testing.T) {
	for name, input := range map[string]string{
		"unknown option":       "rqlite {\n\tnope 1\n}",
		"unparsable duration":  "rqlite {\n\tleader_retry burrito\n}",
		"missing option value": "rqlite {\n\tleader_retry\n}",
	} {
		t.Run(name, func(t *testing.T) {
			var s RqliteStorage
			if err := s.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); err == nil {
				t.Fatalf("want an error for %s, got nil", name)
			}
		})
	}
}

// A retry budget is only settable when it survives the JSON round trip Caddy
// does between adapting the Caddyfile and provisioning the module.
func TestLeaderRetrySurvivesJSON(t *testing.T) {
	d := caddyfile.NewTestDispenser("rqlite {\n\tleader_retry 250ms\n}")
	var s RqliteStorage
	if err := s.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var back RqliteStorage
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatal(err)
	}
	if want := caddy.Duration(250 * time.Millisecond); back.LeaderRetry != want {
		t.Fatalf("leader_retry after JSON: got %s want %s (json: %s)",
			time.Duration(back.LeaderRetry), time.Duration(want), blob)
	}
}
