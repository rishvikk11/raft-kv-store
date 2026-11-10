package raft

// White-box tests for ConfirmRead. A transport that I control by hand decides
// exactly when each reply arrives, so each step of the read-index recipe can
// be checked on its own.

import (
	"context"
	"sync"
	"testing"
	"time"
)

type gateTransport struct {
	started   chan struct{} // peer 1 calls announce themselves here
	release   chan struct{} // the test sends one token to let one peer-1 call return
	done      chan struct{} // closed at cleanup
	peer2Acks bool          // peer 2 answers instantly ("I follow you, but my log doesn't match")
}

func newGate(peer2Acks bool) *gateTransport {
	return &gateTransport{started: make(chan struct{}, 64), release: make(chan struct{}), done: make(chan struct{}), peer2Acks: peer2Acks}
}

func (g *gateTransport) RequestVote(int, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errNet
}

func (g *gateTransport) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	switch to {
	case 1:
		g.started <- struct{}{}
		select {
		case <-g.release:
			return &AppendEntriesReply{Term: args.Term, Success: true}, nil
		case <-g.done:
		}
	case 2:
		if g.peer2Acks {
			return &AppendEntriesReply{Term: args.Term, Success: false, ConflictIndex: 1}, nil
		}
	}
	return nil, errNet
}

func (g *gateTransport) await(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func (g *gateTransport) let(t *testing.T, what string) {
	t.Helper()
	select {
	case g.release <- struct{}{}:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out releasing %s", what)
	}
}

// forcedLeader builds node 0 of a 3-node cluster and makes it leader of term 1
// without an election, so the test controls everything that happens next.
func forcedLeader(t *testing.T, tr Transport) *Node {
	t.Helper()
	nd, err := NewNode(Config{
		ID: 0, ClusterSize: 3, Transport: tr, DataDir: t.TempDir(), DisableFsync: true,
		Apply: func(int, int, []byte) {}, DisableStepDown: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	nd.mu.Lock()
	nd.term, nd.votedFor = 1, 0
	nd.becomeLeaderLocked()
	nd.mu.Unlock()
	nd.Start()
	t.Cleanup(nd.Stop)
	return nd
}

func startRead(nd *Node) <-chan error {
	res := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res <- nd.ConfirmRead(ctx)
	}()
	return res
}

func mustStayPending(t *testing.T, res <-chan error, why string) {
	t.Helper()
	select {
	case err := <-res:
		t.Fatalf("ConfirmRead returned (%v) but must still be waiting: %s", err, why)
	case <-time.After(200 * time.Millisecond):
	}
}

func mustFinish(t *testing.T, res <-chan error, why string) {
	t.Helper()
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("ConfirmRead failed: %v (%s)", err, why)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("ConfirmRead did not finish: %s", why)
	}
}

// Step 1 of the recipe. A brand-new leader has not committed anything in its
// own term, so it does not yet know the true commit point. Even if everyone
// answers heartbeats, it must wait for its no-op to commit first.
func TestReadWaitsUntilLeaderCommitsInOwnTerm(t *testing.T) {
	g := newGate(true) // peer 2 acknowledges heartbeats instantly; peer 1 is held back
	nd := forcedLeader(t, g)
	t.Cleanup(func() { close(g.done) }) // registered last, so it runs BEFORE nd.Stop and frees any held-back call
	g.await(t, "the no-op to reach peer 1")

	res := startRead(nd)
	mustStayPending(t, res, "the leader's no-op is not committed yet")
	g.let(t, "the reply that commits the no-op")
	mustFinish(t, res, "no-op committed and a majority acknowledged a fresh heartbeat")
}

// Step 3 of the recipe. A reply to a heartbeat that was sent BEFORE the read
// arrived proves nothing about whether we are still the leader NOW. Only a
// heartbeat sent after the read counts.
func TestReadIgnoresRepliesToOlderHeartbeats(t *testing.T) {
	g := newGate(false) // only peer 1 can confirm
	nd := forcedLeader(t, g)
	t.Cleanup(func() { close(g.done) })

	g.await(t, "the no-op")
	g.let(t, "the no-op reply")
	waitUntil(t, 2*time.Second, "no-op to commit", func() bool { return nd.Status().CommitIndex == 1 })

	g.await(t, "a heartbeat built before the read exists") // call 2: now in flight, blocked
	res := startRead(nd)
	waitUntil(t, 2*time.Second, "the read to register", func() bool {
		nd.mu.Lock()
		defer nd.mu.Unlock()
		return nd.readSeq == 1
	})
	g.let(t, "the reply to the OLD heartbeat")
	mustStayPending(t, res, "that reply answers a heartbeat sent before the read")

	g.await(t, "a heartbeat built after the read")
	g.let(t, "the reply to the NEW heartbeat")
	mustFinish(t, res, "a heartbeat sent after the read was acknowledged")
}

// Step 4 of the recipe. Confirming leadership is not enough: the local data
// must also have caught up to the read index, or the answer would still be old.
func TestReadWaitsForStateMachineToCatchUp(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }

	nd, err := NewNode(Config{
		ID: 0, ClusterSize: 1, DataDir: t.TempDir(), DisableFsync: true,
		ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond,
		Apply: func(index, term int, cmd []byte) {
			if index == 1 {
				<-gate // the state machine is slow to apply the first entry
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	nd.Start()
	t.Cleanup(nd.Stop)
	t.Cleanup(release) // registered last, so it runs BEFORE nd.Stop and frees the blocked Apply
	waitUntil(t, 2*time.Second, "the single node to become leader", func() bool {
		st := nd.Status()
		return st.Role == Leader && st.CommitIndex == 1
	})

	res := startRead(nd)
	mustStayPending(t, res, "entry 1 is committed but not yet applied")
	release()
	mustFinish(t, res, "the state machine caught up")
}