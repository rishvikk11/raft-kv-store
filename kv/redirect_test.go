package kv

import (
	"net/http"
	"testing"
	"time"
)

// Nodes may know each other by names only they can resolve (inside a container
// network). A follower must redirect clients to the CLIENT-facing address.
func TestRedirectGoesToTheClientFacingAddress(t *testing.T) {
	public := []string{"http://node0.public.example:9000", "http://node1.public.example:9000", "http://node2.public.example:9000"}
	lc := startClusterOpts(t, 3, false, public)
	leader := lc.leader()
	follower := (leader + 1) % 3

	noFollow := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var location string
	waitFor(t, 5*time.Second, "the follower to learn who the leader is", func() bool {
		resp, err := noFollow.Get(lc.urls[follower] + "/kv/somekey")
		if err != nil {
			return false
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusTemporaryRedirect {
			return false
		}
		location = resp.Header.Get("Location")
		return true
	})
	if want := public[leader] + "/kv/somekey"; location != want {
		t.Fatalf("redirected to %q; want the client-facing address %q", location, want)
	}
}