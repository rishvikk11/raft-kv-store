package main

import "testing"

func TestOrdinalFromHostname(t *testing.T) {
	good := map[string]int{"kv-0": 0, "kv-2": 2, "kv-10": 10, "my-raft-kv-3": 3}
	for host, want := range good {
		if got, err := ordinalFromHostname(host); err != nil || got != want {
			t.Errorf("ordinalFromHostname(%q) = %d, %v; want %d", host, got, err, want)
		}
	}
	for _, host := range []string{"kv", "kv-", "kv-x", "kv--1", "kv-+1", "kv- 1", "-1", "", "-"} {
		if got, err := ordinalFromHostname(host); err == nil {
			t.Errorf("ordinalFromHostname(%q) = %d; want an error", host, got)
		}
	}
}