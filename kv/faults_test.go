package kv

// Stage 4: fault injection. A real 3-node cluster (real sockets, real disks)
// keeps serving clients while we break it: kill the leader, cut it off from the
// others, crash and restart nodes. Afterwards the referee in history_test.go
// checks every answer any client ever received, and we check that all three
// nodes ended with identical data.

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rishvikk11/raft-kv-store/raft"
)

// ---- a switchable network partition between nodes ----

type faultNet struct {
	mu  sync.Mutex
	cut map[[2]int]bool // directed: messages from -> to are dropped
}

func newFaultNet() *faultNet { return &faultNet{cut: map[[2]int]bool{}} }

// isolate cuts node i off from every other node, in both directions. Clients
// can still reach it; only Raft's own messages are blocked.
func (f *faultNet) isolate(i, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for j := 0; j < n; j++ {
		if j != i {
			f.cut[[2]int{i, j}] = true
			f.cut[[2]int{j, i}] = true
		}
	}
}

func (f *faultNet) healAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cut = map[[2]int]bool{}
}

func (f *faultNet) blocked(from, to int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cut[[2]int{from, to}]
}

var errPartitioned = errors.New("network partition")

type faultTransport struct {
	inner raft.Transport
	from  int
	net   *faultNet
}

func (t faultTransport) RequestVote(to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if t.net.blocked(t.from, to) {
		return nil, errPartitioned
	}
	return t.inner.RequestVote(to, a)
}

func (t faultTransport) AppendEntries(to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if t.net.blocked(t.from, to) {
		return nil, errPartitioned
	}
	return t.inner.AppendEntries(to, a)
}

// findLeader is like leader() but never fails the test, so fault injectors can call it.
func (lc *liveCluster) findLeader() (int, bool) {
	best, bestTerm := -1, -1
	for i, r := range lc.reps {
		if r == nil {
			continue
		}
		if st := r.node.Status(); st.Role == raft.Leader && st.Term > bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	return best, best >= 0
}

// ---- the workload: clients that keep going while things break ----

type workload struct {
	lc     *liveCluster
	client *http.Client
	keys   []string
	stop   chan struct{}
	wg     sync.WaitGroup

	mu         sync.Mutex
	writes     map[string][]writeRec
	reads      map[string][]readRec
	dupSent    int
	dupRefused int
	errs       []string
}

func newWorkload(lc *liveCluster, keys []string) *workload {
	return &workload{
		lc: lc, keys: keys, stop: make(chan struct{}),
		// A short timeout matters: a request stuck on a cut-off leader must give up
		// quickly and retry elsewhere, or every writer sits blocked for the whole
		// partition and nothing happens that a stale reader could be stale about.
		client: &http.Client{Timeout: 600 * time.Millisecond, Transport: &http.Transport{MaxIdleConnsPerHost: 32}},
		writes: map[string][]writeRec{}, reads: map[string][]readRec{},
	}
}

func (w *workload) fail(format string, a ...any) {
	w.mu.Lock()
	w.errs = append(w.errs, fmt.Sprintf(format, a...))
	w.mu.Unlock()
}

func (w *workload) stopped() bool {
	select {
	case <-w.stop:
		return true
	default:
		return false
	}
}

func (w *workload) put(node int, key string, val, seq int) (int, error) {
	req, _ := http.NewRequest("PUT", w.lc.urls[node]+"/kv/"+key, strings.NewReader(strconv.Itoa(val)))
	req.Header.Set("Client-Id", "w-"+key)
	req.Header.Set("Request-Id", strconv.Itoa(seq))
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

func (w *workload) start(readers int) {
	for _, k := range w.keys {
		w.wg.Add(2)
		go w.writer(k)
		go w.duplicator(k)
	}
	for r := 0; r < readers; r++ {
		w.wg.Add(1)
		go w.reader()
	}
}

func (w *workload) finish() {
	close(w.stop)
	w.wg.Wait()
	w.client.CloseIdleConnections()
}

// writer writes 1, 2, 3, ... to its key. Each write is retried, with the SAME
// request number, against random nodes until the cluster acknowledges it.
func (w *workload) writer(key string) {
	defer w.wg.Done()
	rng := rand.New(rand.NewSource(rand.Int63()))
	for v := 1; !w.stopped(); v++ {
		invoke := time.Now()
		acked := false
		for deadline := time.Now().Add(45 * time.Second); time.Now().Before(deadline); {
			code, err := w.put(rng.Intn(len(w.lc.urls)), key, v, v)
			if err == nil && code == http.StatusNoContent {
				acked = true
				break
			}
			if err == nil && code != http.StatusServiceUnavailable {
				w.fail("write %s=%d got unexpected status %d", key, v, code)
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
		if !acked {
			w.fail("write %s=%d was never acknowledged, even with retries", key, v)
			return
		}
		w.mu.Lock()
		w.writes[key] = append(w.writes[key], writeRec{v: v, invoke: invoke, ack: time.Now()})
		w.mu.Unlock()
		time.Sleep(time.Duration(rng.Intn(8)) * time.Millisecond)
	}
}

// duplicator models a delayed or duplicated network message: it re-sends an
// OLD, already-acknowledged write long after the writer moved on. A correct
// cluster refuses it (409). One that applied it would drag the key backwards,
// and the referee would see the stale value.
func (w *workload) duplicator(key string) {
	defer w.wg.Done()
	rng := rand.New(rand.NewSource(rand.Int63()))
	for !w.stopped() {
		time.Sleep(time.Duration(20+rng.Intn(40)) * time.Millisecond)
		w.mu.Lock()
		n := len(w.writes[key])
		var old int
		if n >= 3 {
			old = w.writes[key][rng.Intn(n-2)].v // at least two writes behind the newest
		}
		w.mu.Unlock()
		if old == 0 {
			continue
		}
		code, err := w.put(rng.Intn(len(w.lc.urls)), key, old, old)
		if err != nil {
			continue
		}
		w.mu.Lock()
		w.dupSent++
		if code == http.StatusConflict {
			w.dupRefused++
		}
		w.mu.Unlock()
		if code == http.StatusNoContent {
			w.fail("an old request (%s=%d) was accepted again after newer ones completed", key, old)
		}
	}
}

// reader asks random nodes for random keys. A read that gets refused or times
// out is simply skipped: refusing is allowed, a wrong answer is not.
func (w *workload) reader() {
	defer w.wg.Done()
	rng := rand.New(rand.NewSource(rand.Int63()))
	for !w.stopped() {
		key := w.keys[rng.Intn(len(w.keys))]
		invoke := time.Now()
		resp, err := w.client.Get(w.lc.urls[rng.Intn(len(w.lc.urls))] + "/kv/" + key)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			respAt := time.Now()
			v, ok := 0, true
			switch resp.StatusCode {
			case http.StatusOK:
				var perr error
				if v, perr = strconv.Atoi(string(body)); perr != nil {
					w.fail("read of %q returned a value that is not a number: %q", key, body)
					ok = false
				}
			case http.StatusNotFound:
				v = 0
			default:
				ok = false
			}
			if ok {
				w.mu.Lock()
				w.reads[key] = append(w.reads[key], readRec{v: v, invoke: invoke, resp: respAt})
				w.mu.Unlock()
			}
		}
		time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
	}
}

// ---- running a scenario ----

type injector func(lc *liveCluster, rng *rand.Rand, stopped func() bool)

// nap sleeps for d but wakes early (returning false) once the injector is told to stop.
func nap(stopped func() bool, d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if stopped() {
			return false
		}
	}
	return !stopped()
}

func runScenario(t *testing.T, duration time.Duration, inject injector) {
	t.Helper()
	runScenarioWith(t, false, duration, inject)
}

// runScenarioWith can switch off leader step-down, so a cut-off leader keeps
// believing it leads and clients can keep talking to it. That is the dangerous
// case for reads, and the one step-down would otherwise hide from this test.
func runScenarioWith(t *testing.T, noStepDown bool, duration time.Duration, inject injector) {
	t.Helper()
	seed := time.Now().UnixNano()
	if v := os.Getenv("CHAOS_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("seed %d (CHAOS_SEED=%d replays the same fault choices)", seed, seed)

	lc := startClusterWith(t, 3, noStepDown)
	lc.leader()
	w := newWorkload(lc, []string{"a", "b", "c"})
	w.start(4)

	halt := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		inject(lc, rand.New(rand.NewSource(seed)), func() bool {
			select {
			case <-halt:
				return true
			default:
				return false
			}
		})
	}()
	time.Sleep(duration)
	close(halt)
	<-done

	// Heal everything: reconnect the network and restart whatever is down.
	lc.faults.healAll()
	for i := range lc.reps {
		if lc.reps[i] == nil {
			lc.restart(i)
		}
	}
	lc.leader()
	w.finish()

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.errs) > 0 {
		t.Fatalf("workload problems: %v", w.errs)
	}
	nw, nr := 0, 0
	for _, k := range w.keys {
		nw += len(w.writes[k])
		nr += len(w.reads[k])
	}
	if nw < 30 || nr < 50 {
		t.Fatalf("only %d writes and %d reads got through; too little for this test to mean anything", nw, nr)
	}
	for _, msg := range checkHistory(w.writes, w.reads) {
		t.Errorf("wrong answer given to a client: %s", msg)
	}
	t.Logf("%d acknowledged writes, %d reads checked, %d late duplicates sent (%d refused)", nw, nr, w.dupSent, w.dupRefused)

	// Durability and agreement: once healed, ALL THREE nodes hold the last
	// acknowledged value of every key. Nothing acknowledged was lost.
	waitFor(t, 20*time.Second, "all replicas to hold every key's last acknowledged value", func() bool {
		for _, r := range lc.reps {
			for _, k := range w.keys {
				ws := w.writes[k]
				if len(ws) == 0 {
					continue
				}
				if got, ok := r.store.Get(k); !ok || got != strconv.Itoa(ws[len(ws)-1].v) {
					return false
				}
			}
		}
		return true
	})
}

// ---- the faults ----

// killLeaders: crash whoever leads, wait, bring it back, repeat.
func killLeaders(lc *liveCluster, _ *rand.Rand, stopped func() bool) {
	for !stopped() {
		leader, ok := lc.findLeader()
		if !ok {
			nap(stopped, 50*time.Millisecond)
			continue
		}
		lc.stop(leader)
		if !nap(stopped, 700*time.Millisecond) {
			return
		}
		lc.restart(leader)
		nap(stopped, 900*time.Millisecond)
	}
}

// partitionLeaders: cut the leader off from the others (clients can still
// reach it, which is exactly the dangerous case), then heal, repeat.
func partitionLeaders(lc *liveCluster, _ *rand.Rand, stopped func() bool) {
	for !stopped() {
		leader, ok := lc.findLeader()
		if !ok {
			nap(stopped, 50*time.Millisecond)
			continue
		}
		lc.faults.isolate(leader, len(lc.urls))
		nap(stopped, 900*time.Millisecond)
		lc.faults.healAll()
		nap(stopped, 900*time.Millisecond)
	}
}

// crashAndRestart: restart each node one at a time, then crash ALL nodes at once.
func crashAndRestart(lc *liveCluster, _ *rand.Rand, stopped func() bool) {
	for !stopped() {
		for i := range lc.reps {
			lc.stop(i)
			if !nap(stopped, 300*time.Millisecond) {
				return
			}
			lc.restart(i)
			if !nap(stopped, 500*time.Millisecond) {
				return
			}
		}
		for i := range lc.reps {
			lc.stop(i)
		}
		if !nap(stopped, 300*time.Millisecond) {
			return
		}
		for i := range lc.reps {
			lc.restart(i)
		}
		nap(stopped, 800*time.Millisecond)
	}
}

// randomChaos: a random mix of all of the above, including losing a majority.
func randomChaos(lc *liveCluster, rng *rand.Rand, stopped func() bool) {
	for !stopped() {
		i := rng.Intn(len(lc.reps))
		switch rng.Intn(5) {
		case 0:
			lc.stop(i)
		case 1:
			if lc.reps[i] == nil {
				lc.restart(i)
			}
		case 2:
			lc.faults.isolate(i, len(lc.urls))
		case 3:
			lc.faults.healAll()
		case 4:
			if leader, ok := lc.findLeader(); ok {
				lc.stop(leader)
			}
		}
		nap(stopped, time.Duration(100+rng.Intn(300))*time.Millisecond)
	}
}

func TestFaultKillLeaderDuringTraffic(t *testing.T) { runScenario(t, 5*time.Second, killLeaders) }
func TestFaultPartitionLeaderDuringTraffic(t *testing.T) {
	runScenario(t, 5*time.Second, partitionLeaders)
}
func TestFaultPartitionedLeaderThatNeverNoticesDuringTraffic(t *testing.T) {
	runScenarioWith(t, true, 5*time.Second, partitionLeaders)
}
func TestFaultCrashAndRestartDuringTraffic(t *testing.T) {
	runScenario(t, 6*time.Second, crashAndRestart)
}
func TestFaultRandomChaosDuringTraffic(t *testing.T) { runScenario(t, 6*time.Second, randomChaos) }