package raft

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	term := c.node(leader).Status().Term

	// Let heartbeats flow, then check everyone agrees on who leads.
	waitUntil(t, 3*time.Second, "followers to learn the leader", func() bool {
		for i := 0; i < 3; i++ {
			st := c.node(i).Status()
			if st.Term != term || st.LeaderID != leader {
				return false
			}
		}
		return true
	})
	leaders := 0
	for i := 0; i < 3; i++ {
		if c.node(i).Status().Role == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders; want exactly 1", leaders)
	}

	// With heartbeats flowing, nobody should call a new election.
	time.Sleep(800 * time.Millisecond)
	if st := c.node(leader).Status(); st.Role != Leader || st.Term != term {
		t.Fatalf("leadership changed with no failure: %+v (was node %d, term %d)", st, leader, term)
	}
}

func TestReElectionAfterLeaderIsCutOff(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	oldTerm := c.node(old).Status().Term

	c.isolate(old)
	var others []int
	for i := 0; i < 3; i++ {
		if i != old {
			others = append(others, i)
		}
	}
	next := c.waitLeader(others...)
	if nt := c.node(next).Status().Term; nt <= oldTerm {
		t.Fatalf("new leader's term %d is not higher than old term %d", nt, oldTerm)
	}

	// The old leader rejoins, sees the higher term, and must step down.
	c.reconnect(old)
	waitUntil(t, 5*time.Second, "old leader to step down", func() bool {
		st := c.node(old).Status()
		return st.Role == Follower && st.Term == c.node(next).Status().Term
	})
	c.submit("after-healing")
	c.checkSafety()
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	// Cut off both followers from everyone, including each other.
	for i := 0; i < 3; i++ {
		if i != old {
			c.isolate(i)
		}
	}
	time.Sleep(1200 * time.Millisecond) // many election timeouts
	for i := 0; i < 3; i++ {
		if i == old {
			continue
		}
		if c.node(i).Status().Role == Leader {
			t.Fatalf("node %d became leader without a majority of votes", i)
		}
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1)
	c.waitLeader()
	c.submit("solo-1")
	c.submit("solo-2")
	if got := c.appliedCmds(0); len(got) != 2 || got[0] != "solo-1" || got[1] != "solo-2" {
		t.Fatalf("applied %v", got)
	}
}