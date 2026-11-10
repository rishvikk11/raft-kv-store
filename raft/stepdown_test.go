package raft

import (
	"testing"
	"time"
)

func countLeaders(c *cluster) int {
	n := 0
	for i := 0; i < c.n; i++ {
		if nd := c.node(i); nd != nil && nd.Status().Role == Leader {
			n++
		}
	}
	return n
}

// A leader that is cut off from everyone must notice and stop acting like the
// leader, instead of believing it is in charge forever.
func TestCutOffLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	c.isolate(old)

	waitUntil(t, 3*time.Second, "cut-off leader to step down", func() bool {
		return c.node(old).Status().Role != Leader
	})
	if _, _, ok := c.node(old).Propose([]byte("should-be-refused")); ok {
		t.Fatal("a leader that stepped down accepted a write")
	}
}

// While one leader is cut off and another is elected, the cluster must settle
// to exactly one node claiming to be leader, not two.
func TestOnlyOneLeaderClaimedAfterPartition(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	c.isolate(old)

	waitUntil(t, 5*time.Second, "exactly one node to claim leadership", func() bool {
		return countLeaders(c) == 1 && c.node(old).Status().Role != Leader
	})
	// ...and it stays that way.
	time.Sleep(500 * time.Millisecond)
	if n := countLeaders(c); n != 1 {
		t.Fatalf("%d nodes claim to be leader; want 1", n)
	}
}

// Losing ONE follower still leaves a majority (leader + the other follower),
// so the leader must keep going. Stepping down here would be a false alarm.
func TestLeaderKeepsLeadingWithMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	term := c.node(leader).Status().Term
	lost := (leader + 1) % 3
	c.isolate(lost)

	var majority []int
	for i := 0; i < 3; i++ {
		if i != lost {
			majority = append(majority, i)
		}
	}
	time.Sleep(1200 * time.Millisecond) // many timeouts
	if st := c.node(leader).Status(); st.Role != Leader || st.Term != term {
		t.Fatalf("leader changed with a majority still reachable: %+v", st)
	}
	c.submit("still-works", majority...)
	c.checkSafety()
}