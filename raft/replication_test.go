package raft

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestBasicReplication(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	var want []string
	for i := 0; i < 20; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		c.submit(cmd) // returns only once all 3 nodes applied it
		want = append(want, cmd)
	}
	for i := 0; i < 3; i++ {
		if got := c.appliedCmds(i); !slices.Equal(got, want) {
			t.Fatalf("node %d applied %v; want %v", i, got, want)
		}
	}
	c.checkSafety()
}

func TestFollowerCatchesUpAfterBeingAway(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	away := (leader + 1) % 3
	var majority []int
	for i := 0; i < 3; i++ {
		if i != away {
			majority = append(majority, i)
		}
	}
	c.isolate(away)
	for i := 0; i < 15; i++ {
		c.submit(fmt.Sprintf("while-away-%d", i), majority...) // two of three is enough
	}
	if n := len(c.appliedCmds(away)); n != 0 {
		t.Fatalf("isolated node applied %d commands", n)
	}
	c.reconnect(away)
	last := "while-away-14"
	waitUntil(t, 5*time.Second, "returning follower to catch up", func() bool {
		cmds := c.appliedCmds(away)
		return len(cmds) == 15 && cmds[14] == last
	})
	c.checkSafety()
}

// A leader that cannot reach a majority may accept entries, but must never
// commit them. This is the quorum rule.
func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	c.submit("committed-before-partition")
	for i := 0; i < 3; i++ {
		if i != leader {
			c.isolate(i)
		}
	}
	index, _, ok := c.node(leader).Propose([]byte("lonely"))
	if !ok {
		t.Fatal("leader refused a proposal")
	}
	time.Sleep(700 * time.Millisecond)
	if st := c.node(leader).Status(); st.CommitIndex >= index {
		t.Fatalf("leader committed index %d with no majority (commitIndex=%d)", index, st.CommitIndex)
	}
	for i := 0; i < 3; i++ {
		if slices.Contains(c.appliedCmds(i), "lonely") {
			t.Fatalf("node %d applied an entry that was never on a majority", i)
		}
	}
	for i := 0; i < 3; i++ {
		c.reconnect(i)
	}
	c.submit("after-heal")
	c.checkSafety()
}

// Paper Figure 7: an old leader holds entries nobody else has. After the
// cluster moves on, those entries must be erased and never applied anywhere.
func TestConflictingEntriesAreOverwritten(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	c.submit("shared-1")
	c.submit("shared-2")

	c.isolate(old) // the old leader keeps believing it leads
	for i := 0; i < 3; i++ {
		if _, _, ok := c.node(old).Propose([]byte(fmt.Sprintf("stale-%d", i))); !ok {
			t.Fatal("isolated leader refused proposal")
		}
	}
	var others []int
	for i := 0; i < 3; i++ {
		if i != old {
			others = append(others, i)
		}
	}
	c.waitLeader(others...)
	c.submit("new-1", others...)
	c.submit("new-2", others...)

	c.reconnect(old)
	waitUntil(t, 5*time.Second, "old leader to adopt the new ledger", func() bool {
		cmds := c.appliedCmds(old)
		return slices.Contains(cmds, "new-2")
	})
	want := []string{"shared-1", "shared-2", "new-1", "new-2"}
	for i := 0; i < 3; i++ {
		got := c.appliedCmds(i)
		if len(got) < len(want) || !slices.Equal(got[:len(want)], want) {
			t.Fatalf("node %d applied %v; want prefix %v", i, got, want)
		}
		for _, cmd := range got {
			if cmd == "stale-0" || cmd == "stale-1" || cmd == "stale-2" {
				t.Fatalf("node %d applied %q, which was never committed", i, cmd)
			}
		}
	}
	c.checkSafety()
}

// The election restriction: a node that missed committed entries cannot win,
// because the up-to-date nodes refuse to vote for it.
func TestStaleNodeCannotBecomeLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	stale := (leader + 1) % 3
	good := (leader + 2) % 3

	c.isolate(stale)
	for i := 0; i < 3; i++ {
		c.submit(fmt.Sprintf("committed-%d", i), leader, good)
	}
	c.kill(leader) // the old leader dies for good
	c.reconnect(stale)

	winner := c.waitLeader(stale, good)
	if winner != good {
		t.Fatalf("node %d won, but it was missing committed entries", winner)
	}
	waitUntil(t, 5*time.Second, "stale node to receive the missing entries", func() bool {
		return len(c.appliedCmds(stale)) == 3
	})
	c.checkSafety()
}

func TestWorksOnUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 3)
	c.net.mu.Lock()
	c.net.dropRate = 0.2
	c.net.maxDelay = 8 * time.Millisecond
	c.net.mu.Unlock()
	for i := 0; i < 15; i++ {
		c.submit(fmt.Sprintf("lossy-%d", i))
	}
	c.checkSafety()
}