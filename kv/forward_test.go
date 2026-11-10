package kv

// A follower that passes requests on to the leader, so a client (or a
// load-balancing Service) can talk to ANY node without being redirected.

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// noFollow reports exactly what the node we asked answered, without chasing redirects.
var noFollow = &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func rawDo(t *testing.T, url, method, body string, hdr map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := noFollow.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// waitServes retries while the cluster is between leaders (a 503), then returns the answer.
func waitServes(t *testing.T, url, method, body string, hdr map[string]string) (int, string) {
	t.Helper()
	var code int
	var out string
	waitFor(t, 10*time.Second, method+" "+url, func() bool {
		code, out = rawDo(t, url, method, body, hdr)
		return code != http.StatusServiceUnavailable
	})
	return code, out
}

func TestFollowerForwardsInsteadOfRedirecting(t *testing.T) {
	lc := startClusterOpts(t, 3, clusterOpts{forward: true})
	leader := lc.leader()
	follower := (leader + 1) % 3

	// Writing through a FOLLOWER must simply work: 204 straight from the follower, not a 307.
	if code, _ := waitServes(t, lc.urls[follower]+"/kv/fruit", "PUT", "mango", nil); code != http.StatusNoContent {
		t.Fatalf("PUT via follower = %d; want 204 (forwarded, not redirected)", code)
	}
	// The body must have survived the trip: an empty body would store "".
	for i := 0; i < 3; i++ {
		if code, body := waitServes(t, lc.urls[i]+"/kv/fruit", "GET", "", nil); code != 200 || body != "mango" {
			t.Fatalf("GET via node %d = %d %q; want 200 \"mango\"", i, code, body)
		}
	}
	if code, _ := waitServes(t, lc.urls[follower]+"/kv/fruit", "DELETE", "", nil); code != http.StatusNoContent {
		t.Fatalf("DELETE via follower = %d; want 204", code)
	}
	if code, _ := waitServes(t, lc.urls[follower]+"/kv/fruit", "GET", "", nil); code != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %d; want 404", code)
	}
}

// The headers that make retries safe must survive forwarding, or a retried
// write sent through a follower would be applied twice.
func TestRequestIDsSurviveForwarding(t *testing.T) {
	lc := startClusterOpts(t, 3, clusterOpts{forward: true})
	leader := lc.leader()
	f1, f2 := (leader+1)%3, (leader+2)%3

	if code, _ := waitServes(t, lc.urls[f1]+"/kv/x", "PUT", "from-A", id("A", 1)); code != http.StatusNoContent {
		t.Fatalf("A's PUT = %d", code)
	}
	if code, _ := waitServes(t, lc.urls[f2]+"/kv/x", "PUT", "from-B", id("B", 1)); code != http.StatusNoContent {
		t.Fatalf("B's PUT = %d", code)
	}
	// A's retry arrives through the OTHER follower. It must not drag x back.
	if code, _ := waitServes(t, lc.urls[f2]+"/kv/x", "PUT", "from-A", id("A", 1)); code != http.StatusNoContent {
		t.Fatalf("A's retry = %d; want 204", code)
	}
	if code, body := waitServes(t, lc.urls[f1]+"/kv/x", "GET", "", nil); code != 200 || body != "from-B" {
		t.Fatalf("GET x = %d %q; want from-B (the retry was applied again)", code, body)
	}
	// An older request number is still refused, through forwarding too.
	if code, _ := waitServes(t, lc.urls[f1]+"/kv/y", "PUT", "1", id("C", 5)); code != http.StatusNoContent {
		t.Fatalf("C's PUT #5 = %d", code)
	}
	if code, _ := waitServes(t, lc.urls[f2]+"/kv/y", "PUT", "old", id("C", 3)); code != http.StatusConflict {
		t.Fatalf("C's PUT #3 after #5 = %d; want 409 relayed from the leader", code)
	}
}

// A node that gets a request already passed on once must not pass it on again,
// otherwise two nodes with different ideas of who leads could bounce it forever.
func TestAForwardedRequestIsNeverForwardedAgain(t *testing.T) {
	lc := startClusterOpts(t, 3, clusterOpts{forward: true})
	leader := lc.leader()
	follower := (leader + 1) % 3

	// Wait until the follower knows who leads (otherwise it answers 503 for that reason instead).
	waitServes(t, lc.urls[follower]+"/kv/warm", "PUT", "x", nil)
	code, body := rawDo(t, lc.urls[follower]+"/kv/loop", "PUT", "x", map[string]string{forwardedHeader: "2"})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("an already-forwarded request to a follower = %d %q; want 503 (no second hop)", code, body)
	}
	// The same request sent straight to the leader is fine: the header only stops further hops.
	if code, _ := rawDo(t, lc.urls[leader]+"/kv/loop", "PUT", "x", map[string]string{forwardedHeader: "2"}); code != http.StatusNoContent {
		t.Fatalf("an already-forwarded request to the leader = %d; want 204", code)
	}
}

// Without -forward nothing changes: followers still redirect.
func TestWithoutForwardingFollowersStillRedirect(t *testing.T) {
	lc := startCluster(t, 3)
	leader := lc.leader()
	follower := (leader + 1) % 3
	var code int
	waitFor(t, 5*time.Second, "the follower to learn the leader", func() bool {
		code, _ = rawDo(t, lc.urls[follower]+"/kv/k", "GET", "", nil)
		return code == http.StatusTemporaryRedirect
	})
}

// Forwarding must also survive the leader dying: clients keep getting answers
// (or a retryable 503), never a wrong one.
func TestForwardingSurvivesLeaderDeath(t *testing.T) {
	lc := startClusterOpts(t, 3, clusterOpts{forward: true})
	leader := lc.leader()
	survivor := (leader + 1) % 3
	if code, _ := waitServes(t, lc.urls[survivor]+"/kv/before", "PUT", "1", id("A", 1)); code != http.StatusNoContent {
		t.Fatalf("PUT = %d", code)
	}
	lc.stop(leader)
	if code, _ := waitServes(t, lc.urls[survivor]+"/kv/after", "PUT", "2", id("A", 2)); code != http.StatusNoContent {
		t.Fatalf("PUT after the leader died = %d", code)
	}
	if code, body := waitServes(t, lc.urls[survivor]+"/kv/before", "GET", "", nil); code != 200 || body != "1" {
		t.Fatalf("GET before = %d %q; an acknowledged write was lost", code, body)
	}
}