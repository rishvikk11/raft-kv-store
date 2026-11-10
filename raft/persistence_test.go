package raft

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// A node that voted, then crashed, must still remember the vote. Otherwise it
// could vote twice in one term and two leaders could win.
func TestVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	a := newBareNode(t, dir, 0, 3)
	if r := a.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("setup: vote not granted")
	}
	a.Stop() // "crash"

	b := newBareNode(t, dir, 0, 3)
	if st := b.Status(); st.Term != 5 {
		t.Fatalf("term after restart = %d; want 5", st.Term)
	}
	if r := b.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}); r.VoteGranted {
		t.Fatal("restarted node voted for a second candidate in the same term")
	}
	if r := b.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("restarted node forgot it had voted for candidate 1")
	}
}

func TestLogSurvivesRestartIncludingOverwrites(t *testing.T) {
	dir := t.TempDir()
	a := newBareNode(t, dir, 0, 3)
	a.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: ents(1, 1, 2, 2)})
	a.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 1, Entries: ents(3, 3)})
	want := logTerms(a)
	a.Stop()

	b := newBareNode(t, dir, 0, 3)
	if got := logTerms(b); !slices.Equal(got, want) || !slices.Equal(got, []int{1, 1, 3, 3}) {
		t.Fatalf("log after restart = %v; before = %v; want [1 1 3 3]", got, want)
	}
	if st := b.Status(); st.Term != 3 {
		t.Fatalf("term after restart = %d; want 3", st.Term)
	}
}

func TestFullClusterRestart(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	var want []string
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("durable-%d", i)
		c.submit(cmd)
		want = append(want, cmd)
	}
	termBefore := c.node(c.waitLeader()).Status().Term

	for i := 0; i < 3; i++ { // everything crashes at once
		c.kill(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	leader := c.waitLeader()
	if tm := c.node(leader).Status().Term; tm <= termBefore {
		t.Fatalf("term went backwards or stood still across the restart: %d -> %d", termBefore, tm)
	}
	c.submit("after-restart")
	want = append(want, "after-restart")
	for i := 0; i < 3; i++ {
		i := i
		waitUntil(t, 5*time.Second, "node to rebuild its state machine", func() bool {
			return slices.Equal(c.appliedCmds(i), want)
		})
	}
	c.checkSafety()
}

func TestKillLeaderThenRestartIt(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	c.submit("before-1")
	c.submit("before-2")

	c.kill(leader)
	survivors := c.live()
	c.waitLeader(survivors...)
	c.submit("during-1", survivors...)
	c.submit("during-2", survivors...)

	c.restart(leader) // comes back from disk and must catch up
	waitUntil(t, 5*time.Second, "restarted leader to catch up", func() bool {
		return slices.Equal(c.appliedCmds(leader), []string{"before-1", "before-2", "during-1", "during-2"})
	})
	c.submit("after-1")
	c.checkSafety()
}

// Random crashes, partitions and message loss for a few seconds while clients
// keep writing. We assert only safety properties, which must hold no matter
// what; then we heal everything and check the cluster converges.
func TestChaos(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("CHAOS_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("seed %d (set CHAOS_SEED=%d to replay the same sequence of faults)", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	c := newCluster(t, 3)
	c.net.mu.Lock()
	c.net.dropRate = 0.05
	c.net.maxDelay = 4 * time.Millisecond
	c.net.mu.Unlock()
	c.waitLeader()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // a background client that fires writes at whoever leads
		defer close(done)
		for k := 0; ; k++ {
			select {
			case <-stop:
				return
			default:
			}
			for _, i := range c.live() {
				if nd := c.node(i); nd != nil {
					nd.Propose([]byte(fmt.Sprintf("chaos-%d", k)))
				}
			}
			time.Sleep(8 * time.Millisecond)
		}
	}()

	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		i := rng.Intn(3)
		switch rng.Intn(4) {
		case 0:
			c.isolate(i)
		case 1:
			c.reconnect(i)
		case 2:
			if len(c.live()) > 1 { // keep at least one node up so something is always recoverable
				c.kill(i)
			}
		case 3:
			if c.node(i) == nil {
				c.restart(i)
			}
		}
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
	}
	close(stop)
	<-done

	// Heal everything.
	c.net.mu.Lock()
	c.net.dropRate = 0
	c.net.maxDelay = 0
	c.net.mu.Unlock()
	for i := 0; i < 3; i++ {
		c.reconnect(i)
		if c.node(i) == nil {
			c.restart(i)
		}
	}
	c.submit("final-marker") // proves the healed cluster still works, and waits for all 3 to apply it
	c.checkSafety()
}

// A leader counts ITS OWN copy of an entry toward the majority, so that copy
// must be on disk too. Here "x" is committed on the leader and ONE follower.
// Then that follower dies for good and the leader crashes and restarts, so the
// only nodes left are the leader and the follower that never saw "x". If the
// leader had not saved its own copy, those two would see equal ledgers, elect
// a leader that lacks "x", and an acknowledged write would vanish.
func TestLeaderOwnCopyCountsSoItMustBeOnDisk(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	withX := (leader + 1) % 3
	withoutX := (leader + 2) % 3

	c.isolate(withoutX)
	index := c.submit("x", leader, withX) // committed on a majority: leader + withX

	c.kill(withX)  // the only other holder of "x" is gone for good
	c.kill(leader) // and the leader crashes...
	c.restart(leader)
	c.reconnect(withoutX)

	survivors := []int{leader, withoutX}
	c.waitLeader(survivors...)
	if !c.waitApplied(survivors, index, "x", 5*time.Second) {
		t.Fatal("an acknowledged write was lost: the leader's own copy was not on disk when it counted toward the majority")
	}
	c.checkSafety()
}

// A restarted node reports what it read from its OWN disk. This is what lets an
// outside observer tell "remembered it" apart from "was refilled by the leader".
func TestStatusReportsWhatWasRecoveredFromDisk(t *testing.T) {
	dir := t.TempDir()
	a := newBareNode(t, dir, 0, 3)
	if st := a.Status(); st.RecoveredEntries != 0 || st.RecoveredTerm != 0 {
		t.Fatalf("a brand-new node reports it recovered %d entries, term %d; want 0, 0", st.RecoveredEntries, st.RecoveredTerm)
	}
	a.HandleAppendEntries(AppendEntriesArgs{Term: 4, LeaderID: 1, Entries: ents(1, 2, 4)})
	a.Stop()

	b := newBareNode(t, dir, 0, 3)
	if st := b.Status(); st.RecoveredEntries != 3 || st.RecoveredTerm != 4 {
		t.Fatalf("restarted node reports it recovered %d entries, term %d; want 3, 4", st.RecoveredEntries, st.RecoveredTerm)
	}
}