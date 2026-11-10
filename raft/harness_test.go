package raft

// Test harness: an in-memory network plus a cluster of real Nodes with real
// WAL files. The network can cut off nodes, drop messages and delay them, so a
// "partition" or "crash" in a test is repeatable instead of a lucky accident.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var errNet = errors.New("network: message lost")

type testNet struct {
	mu       sync.Mutex
	nodes    map[int]*Node
	isolated map[int]bool
	cut      map[[2]int]bool // directed: from -> to
	dropRate float64
	maxDelay time.Duration
}

func newTestNet() *testNet {
	return &testNet{nodes: map[int]*Node{}, isolated: map[int]bool{}, cut: map[[2]int]bool{}}
}

func (tn *testNet) setNode(i int, nd *Node) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if nd == nil {
		delete(tn.nodes, i)
	} else {
		tn.nodes[i] = nd
	}
}

func (tn *testNet) node(i int) *Node {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	return tn.nodes[i]
}

// path decides whether a message from -> to gets through right now.
func (tn *testNet) path(from, to int) error {
	tn.mu.Lock()
	ok := tn.nodes[from] != nil && tn.nodes[to] != nil &&
		!tn.isolated[from] && !tn.isolated[to] && !tn.cut[[2]int{from, to}]
	drop, delay := tn.dropRate, tn.maxDelay
	tn.mu.Unlock()
	if !ok {
		return errNet
	}
	if drop > 0 && rand.Float64() < drop {
		return errNet
	}
	if delay > 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(delay))))
	}
	return nil
}

type endpoint struct {
	net  *testNet
	from int
}

func (e endpoint) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if err := e.net.path(e.from, to); err != nil {
		return nil, err
	}
	nd := e.net.node(to)
	if nd == nil {
		return nil, errNet
	}
	reply := nd.HandleRequestVote(*args)
	if err := e.net.path(to, e.from); err != nil { // the reply can be lost too
		return nil, err
	}
	return &reply, nil
}

func (e endpoint) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if err := e.net.path(e.from, to); err != nil {
		return nil, err
	}
	nd := e.net.node(to)
	if nd == nil {
		return nil, errNet
	}
	reply := nd.HandleAppendEntries(*args)
	if err := e.net.path(to, e.from); err != nil {
		return nil, err
	}
	return &reply, nil
}

type appliedEntry struct {
	index, term int
	cmd         string
}

type cluster struct {
	t             *testing.T
	n             int
	net           *testNet
	dirs          []string
	noCheckQuorum bool

	mu         sync.Mutex
	nodes      []*Node
	applied    [][]appliedEntry // per node, what its (volatile) state machine has seen
	termLeader map[int]int      // term -> the node that was leader in it
	violation  string           // set if two leaders ever existed in one term

	stopMon chan struct{}
	monDone chan struct{}
}

func newCluster(t *testing.T, n int) *cluster { return newClusterWith(t, n, false) }

// newClusterWith can switch off the leader's timer-based step-down. That models
// a leader that is paused or slow and has not noticed yet that it was replaced.
func newClusterWith(t *testing.T, n int, noCheckQuorum bool) *cluster {
	t.Helper()
	c := &cluster{
		t: t, n: n, net: newTestNet(), noCheckQuorum: noCheckQuorum,
		dirs: make([]string, n), nodes: make([]*Node, n), applied: make([][]appliedEntry, n),
		termLeader: map[int]int{}, stopMon: make(chan struct{}), monDone: make(chan struct{}),
	}
	for i := range c.dirs {
		c.dirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i))
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	go c.monitor()
	t.Cleanup(c.shutdown)
	return c
}

// start boots node i from whatever is in its data directory. After a crash the
// state machine is empty again; it is rebuilt by re-applying committed entries.
func (c *cluster) start(i int) {
	c.mu.Lock()
	c.applied[i] = nil
	c.mu.Unlock()
	nd, err := NewNode(Config{
		ID: i, ClusterSize: c.n, Transport: endpoint{c.net, i}, DataDir: c.dirs[i],
		DisableFsync:       true,
		disableCheckQuorum: c.noCheckQuorum,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
		Apply: func(index, term int, cmd []byte) {
			c.mu.Lock()
			c.applied[i] = append(c.applied[i], appliedEntry{index, term, string(cmd)})
			c.mu.Unlock()
		},
	})
	if err != nil {
		c.t.Fatalf("NewNode(%d): %v", i, err)
	}
	c.mu.Lock()
	c.nodes[i] = nd
	c.mu.Unlock()
	c.net.setNode(i, nd)
	nd.Start()
}

// kill simulates a crash: the process stops and its in-memory state is gone.
// Only what it wrote to its data directory survives.
func (c *cluster) kill(i int) {
	c.net.setNode(i, nil)
	c.mu.Lock()
	nd := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if nd != nil {
		nd.Stop()
	}
}

func (c *cluster) restart(i int) { c.start(i) }

func (c *cluster) isolate(i int) {
	c.net.mu.Lock()
	c.net.isolated[i] = true
	c.net.mu.Unlock()
}

func (c *cluster) reconnect(i int) {
	c.net.mu.Lock()
	delete(c.net.isolated, i)
	c.net.mu.Unlock()
}

func (c *cluster) shutdown() {
	close(c.stopMon)
	<-c.monDone
	for i := 0; i < c.n; i++ {
		c.kill(i)
	}
	c.mu.Lock()
	v := c.violation
	c.mu.Unlock()
	if v != "" {
		c.t.Errorf("election safety violated: %s", v)
	}
}

func (c *cluster) node(i int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *cluster) live() []int {
	var ids []int
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, nd := range c.nodes {
		if nd != nil {
			ids = append(ids, i)
		}
	}
	return ids
}

func (c *cluster) idsOrLive(ids []int) []int {
	if len(ids) == 0 {
		return c.live()
	}
	return ids
}

// monitor watches for the one thing that must NEVER happen: two different
// leaders in the same term.
func (c *cluster) monitor() {
	defer close(c.monDone)
	tick := time.NewTicker(3 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.stopMon:
			return
		case <-tick.C:
		}
		for i := 0; i < c.n; i++ {
			nd := c.node(i)
			if nd == nil {
				continue
			}
			st := nd.Status()
			if st.Role != Leader {
				continue
			}
			c.mu.Lock()
			if prev, seen := c.termLeader[st.Term]; seen && prev != st.ID {
				c.violation = fmt.Sprintf("nodes %d and %d were both leader in term %d", prev, st.ID, st.Term)
			} else {
				c.termLeader[st.Term] = st.ID
			}
			c.mu.Unlock()
		}
	}
}

// leaderAmong returns the leader (highest term wins) among the given nodes.
func (c *cluster) leaderAmong(ids []int) (int, bool) {
	best, bestTerm := -1, -1
	for _, i := range ids {
		nd := c.node(i)
		if nd == nil {
			continue
		}
		if st := nd.Status(); st.Role == Leader && st.Term > bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	return best, best >= 0
}

func (c *cluster) waitLeader(ids ...int) int {
	c.t.Helper()
	ids = c.idsOrLive(ids)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if id, ok := c.leaderAmong(ids); ok {
			return id
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader emerged among nodes %v", ids)
	return -1
}

func (c *cluster) appliedAt(i, index int) (appliedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index < 1 || index > len(c.applied[i]) {
		return appliedEntry{}, false
	}
	return c.applied[i][index-1], true
}

func (c *cluster) waitApplied(ids []int, index int, cmd string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		all := true
		for _, i := range ids {
			if e, ok := c.appliedAt(i, index); !ok || e.cmd != cmd {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// submit hands cmd to the current leader among ids and waits until EVERY node
// in ids has applied it. It retries with a new leader if the first attempt is lost.
func (c *cluster) submit(cmd string, ids ...int) int {
	c.t.Helper()
	ids = c.idsOrLive(ids)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		id, ok := c.leaderAmong(ids)
		if !ok {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		nd := c.node(id)
		if nd == nil {
			continue
		}
		index, _, ok := nd.Propose([]byte(cmd))
		if !ok {
			continue
		}
		if c.waitApplied(ids, index, cmd, 2*time.Second) {
			return index
		}
	}
	c.t.Fatalf("command %q was never applied on all of %v", cmd, ids)
	return -1
}

func (c *cluster) appliedCmds(i int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.applied[i] {
		if e.cmd != "" {
			out = append(out, e.cmd)
		}
	}
	return out
}

// checkSafety verifies State Machine Safety: no two nodes ever applied
// different commands at the same ledger index, and each node applied entries
// in order with no gaps.
func (c *cluster) checkSafety() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.n; i++ {
		for pos, e := range c.applied[i] {
			if e.index != pos+1 {
				c.t.Errorf("node %d applied index %d at position %d (gap or reorder)", i, e.index, pos)
			}
		}
		for j := i + 1; j < c.n; j++ {
			for pos := 0; pos < len(c.applied[i]) && pos < len(c.applied[j]); pos++ {
				a, b := c.applied[i][pos], c.applied[j][pos]
				if a != b {
					c.t.Errorf("nodes %d and %d disagree at index %d: %+v vs %+v", i, j, pos+1, a, b)
				}
			}
		}
	}
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// latest is the toy "state machine" used in read tests: the value of the most
// recent command node i has applied ("" if none).
func (c *cluster) latest(i int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := len(c.applied[i]) - 1; k >= 0; k-- {
		if c.applied[i][k].cmd != "" {
			return c.applied[i][k].cmd
		}
	}
	return ""
}

// naiveRead is what Stage 2 did: if I believe I am the leader, answer from my own data.
func (c *cluster) naiveRead(i int) (string, bool) {
	if c.node(i).Status().Role != Leader {
		return "", false
	}
	return c.latest(i), true
}

// safeRead asks the node to confirm the read first (see Node.ConfirmRead).
func (c *cluster) safeRead(i int, within time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := c.node(i).ConfirmRead(ctx); err != nil {
		return "", err
	}
	return c.latest(i), nil
}