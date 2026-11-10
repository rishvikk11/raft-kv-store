package kv

// End-to-end tests: three real servers on real TCP ports, each with its own
// data directory on disk, talking Raft over HTTP.

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rishvikk11/raft-kv-store/raft"
)

type liveCluster struct {
	t    *testing.T
	urls []string
	dirs []string
	reps []*Replicated
	srvs []*http.Server
	http *http.Client
}

func startCluster(t *testing.T, n int) *liveCluster {
	t.Helper()
	lc := &liveCluster{
		t: t, urls: make([]string, n), dirs: make([]string, n),
		reps: make([]*Replicated, n), srvs: make([]*http.Server, n),
		http: &http.Client{Timeout: 8 * time.Second}, // follows 307 redirects, re-sending the body
	}
	lns := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		lc.urls[i] = "http://" + ln.Addr().String()
		lc.dirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i))
	}
	for i := 0; i < n; i++ {
		lc.boot(i, lns[i])
	}
	t.Cleanup(func() {
		for i := range lc.reps {
			lc.stop(i)
		}
	})
	return lc
}

func (lc *liveCluster) boot(i int, ln net.Listener) {
	rep, err := NewReplicated(raft.Config{
		ID: i, ClusterSize: len(lc.urls), Transport: raft.NewHTTPTransport(lc.urls), DataDir: lc.dirs[i],
		DisableFsync:       true,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
	}, lc.urls)
	if err != nil {
		lc.t.Fatal(err)
	}
	srv := &http.Server{Handler: rep.Handler()}
	go srv.Serve(ln)
	rep.Start()
	lc.reps[i], lc.srvs[i] = rep, srv
}

// stop is a crash: sockets close, the process state is dropped, only the data dir remains.
func (lc *liveCluster) stop(i int) {
	if lc.srvs[i] != nil {
		lc.srvs[i].Close()
		lc.srvs[i] = nil
	}
	if lc.reps[i] != nil {
		lc.reps[i].Stop()
		lc.reps[i] = nil
	}
}

func (lc *liveCluster) restart(i int) {
	ln, err := net.Listen("tcp", strings.TrimPrefix(lc.urls[i], "http://"))
	if err != nil {
		lc.t.Fatal(err)
	}
	lc.boot(i, ln)
}

func (lc *liveCluster) leader() int {
	lc.t.Helper()
	var id = -1
	waitFor(lc.t, 10*time.Second, "a leader", func() bool {
		for i, r := range lc.reps {
			if r != nil && r.node.Status().Role == raft.Leader {
				id = i
				return true
			}
		}
		return false
	})
	return id
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// do sends a request to node `via`, retrying while the cluster is between leaders.
func (lc *liveCluster) do(via int, method, key, body string) (int, string) {
	return lc.doHdr(via, method, key, body, nil)
}

func (lc *liveCluster) doHdr(via int, method, key, body string, hdr map[string]string) (int, string) {
	lc.t.Helper()
	var code int
	var out string
	waitFor(lc.t, 15*time.Second, method+" "+key, func() bool {
		req, _ := http.NewRequest(method, lc.urls[via]+"/kv/"+key, strings.NewReader(body))
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := lc.http.Do(req)
		if err != nil {
			return false
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			return false
		}
		code, out = resp.StatusCode, string(b)
		return true
	})
	return code, out
}

func TestClusterPutGetDelete(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()

	// Write through every node, including followers (they redirect to the leader).
	for i := 0; i < 3; i++ {
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		if code, _ := lc.do(i, "PUT", key, val); code != http.StatusNoContent {
			t.Fatalf("PUT via node %d = %d; want 204", i, code)
		}
	}
	for i := 0; i < 3; i++ {
		code, body := lc.do((i+1)%3, "GET", fmt.Sprintf("k%d", i), "")
		if code != 200 || body != fmt.Sprintf("v%d", i) {
			t.Fatalf("GET k%d = %d %q", i, code, body)
		}
	}
	if code, _ := lc.do(0, "DELETE", "k0", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d; want 204", code)
	}
	if code, _ := lc.do(0, "DELETE", "k0", ""); code != http.StatusNotFound {
		t.Fatalf("second DELETE = %d; want 404", code)
	}
	if code, _ := lc.do(1, "GET", "k0", ""); code != http.StatusNotFound {
		t.Fatalf("GET after DELETE = %d; want 404", code)
	}

	// Every node's Store must end up identical, not just the leader's.
	waitFor(t, 5*time.Second, "all three stores to match", func() bool {
		for i := 0; i < 3; i++ {
			if _, ok := lc.reps[i].store.Get("k0"); ok {
				return false
			}
			for _, k := range []string{"k1", "k2"} {
				if v, ok := lc.reps[i].store.Get(k); !ok || v != "v"+k[1:] {
					return false
				}
			}
		}
		return true
	})
}

func TestClusterSurvivesKillingTheLeader(t *testing.T) {
	lc := startCluster(t, 3)
	leader := lc.leader()
	if code, _ := lc.do(leader, "PUT", "before", "1"); code != http.StatusNoContent {
		t.Fatalf("PUT = %d", code)
	}

	lc.stop(leader) // kill the leader, not just a follower
	survivor := (leader + 1) % 3
	if code, _ := lc.do(survivor, "PUT", "during", "2"); code != http.StatusNoContent {
		t.Fatalf("PUT with leader down = %d; want 204", code)
	}
	if code, body := lc.do(survivor, "GET", "before", ""); code != 200 || body != "1" {
		t.Fatalf("GET before = %d %q; a committed write was lost", code, body)
	}

	lc.restart(leader) // it must recover from disk and catch up
	waitFor(t, 10*time.Second, "restarted node to catch up", func() bool {
		a, okA := lc.reps[leader].store.Get("before")
		b, okB := lc.reps[leader].store.Get("during")
		return okA && okB && a == "1" && b == "2"
	})
}

func TestClusterRecoversFromFullRestart(t *testing.T) {
	lc := startCluster(t, 3)
	lc.leader()
	for i := 0; i < 5; i++ {
		if code, _ := lc.do(0, "PUT", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)); code != http.StatusNoContent {
			t.Fatalf("PUT = %d", code)
		}
	}
	for i := 0; i < 3; i++ {
		lc.stop(i)
	}
	for i := 0; i < 3; i++ {
		lc.restart(i)
	}
	lc.leader()
	for i := 0; i < 5; i++ {
		key, want := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		waitFor(t, 10*time.Second, "recovered "+key, func() bool {
			req, _ := http.NewRequest("GET", lc.urls[0]+"/kv/"+key, nil)
			resp, err := lc.http.Do(req)
			if err != nil {
				return false
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp.StatusCode == 200 && string(b) == want
		})
	}
}