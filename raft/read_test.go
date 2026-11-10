package raft

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// partitionedLeaderScenario sets up the Amy/Ben/Cara story: the leader is cut
// off, the others elect a new leader and commit a newer value.
func partitionedLeaderScenario(t *testing.T) (c *cluster, oldLeader, newLeader int) {
	t.Helper()
	c = newClusterWith(t, 3, true) // true: the old leader does not notice it was cut off
	oldLeader = c.waitLeader()
	c.submit("balance=100")

	c.isolate(oldLeader)
	var others []int
	for i := 0; i < 3; i++ {
		if i != oldLeader {
			others = append(others, i)
		}
	}
	newLeader = c.waitLeader(others...)
	c.submit("balance=150", others...) // acknowledged to a client: this is now the truth
	return c, oldLeader, newLeader
}

// THE BUG, on purpose. This test shows why a naive leader read is wrong: the
// cut-off leader still believes it is the leader and answers with old data,
// even though a newer value was already acknowledged to a client.
func TestNaiveLeaderReadIsStaleAfterPartition(t *testing.T) {
	c, old, _ := partitionedLeaderScenario(t)

	got, answered := c.naiveRead(old)
	if !answered {
		t.Fatal("setup problem: the old leader no longer thinks it is the leader")
	}
	if got != "balance=100" {
		t.Fatalf("naive read returned %q; this demo expects the stale value balance=100", got)
	}
	t.Logf("naive read from the cut-off leader returned %q, but the committed value is balance=150", got)
}

// THE FIX. The same situation, but the leader must confirm with a majority
// before answering. It cannot, so it refuses instead of returning stale data.
func TestSafeReadRefusesOnCutOffLeader(t *testing.T) {
	c, old, newLeader := partitionedLeaderScenario(t)

	if got, err := c.safeRead(old, 600*time.Millisecond); err == nil {
		t.Fatalf("cut-off leader answered a read with %q instead of refusing", got)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want it to give up waiting for a majority", err)
	}

	got, err := c.safeRead(newLeader, 2*time.Second)
	if err != nil || got != "balance=150" {
		t.Fatalf("read from the real leader = %q, %v; want balance=150", got, err)
	}
}

func TestConfirmReadOnFollowerFails(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	follower := (leader + 1) % 3
	if _, err := c.safeRead(follower, time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v; want ErrNotLeader", err)
	}
}

// A brand-new leader has not yet committed anything in its own term, so it does
// not know the true commit point. ConfirmRead must wait for that, not guess.
func TestConfirmReadWorksRightAfterElection(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader()
	if _, err := c.safeRead(leader, 3*time.Second); err != nil {
		t.Fatalf("ConfirmRead right after election: %v", err)
	}
}

func TestConfirmReadSingleNode(t *testing.T) {
	c := newCluster(t, 1)
	c.waitLeader()
	c.submit("solo")
	if got, err := c.safeRead(0, time.Second); err != nil || got != "solo" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// If the leader loses its leadership while a read is waiting, the read must

func TestConfirmReadFailsWhenLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3) // step-down ON
	leader := c.waitLeader()
	c.submit("x")
	c.isolate(leader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.node(leader).ConfirmRead(ctx)
	if err == nil {
		t.Fatal("ConfirmRead succeeded on a cut-off leader")
	}
	if !errors.Is(err, ErrLostLeadership) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v; want the node to report it lost leadership", err)
	}
}

// The real property, under churn. A writer acknowledges writes 1,2,3,... one
// at a time. Readers note the latest acknowledged number BEFORE they ask for a
// read, so whatever a confirmed read returns must be at least that new.
// Midway, the leader is cut off for a while.
func TestConfirmedReadsNeverMissAcknowledgedWrites(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()

	var acked atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var failure atomic.Value

	wg.Add(1)
	go func() { // writer
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			cmd := fmt.Sprintf("n-%d", i)
			for done := false; !done; {
				select {
				case <-stop:
					return
				default:
				}
				id, ok := c.leaderAmong(c.live())
				if !ok {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				index, _, ok := c.node(id).Propose([]byte(cmd))
				if ok && c.waitApplied([]int{id}, index, cmd, time.Second) {
					acked.Store(int64(i)) // a client would now have been told "done"
					done = true
				}
			}
		}
	}()

	var confirmed atomic.Int64
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() { // readers
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				before := acked.Load()
				id, ok := c.leaderAmong(c.live())
				if !ok {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				got, err := c.safeRead(id, 300*time.Millisecond)
				if err != nil {
					continue // refusing is allowed; stale answers are not
				}
				confirmed.Add(1)
				n, _ := strconv.Atoi(strings.TrimPrefix(got, "n-"))
				if int64(n) < before {
					failure.Store(fmt.Sprintf("read on node %d returned %q after write %d had been acknowledged", id, got, before))
					return
				}
			}
		}()
	}

	time.Sleep(700 * time.Millisecond)
	cut := c.waitLeader()
	c.isolate(cut) // leader cut off mid-traffic
	time.Sleep(900 * time.Millisecond)
	c.reconnect(cut)
	time.Sleep(700 * time.Millisecond)
	close(stop)
	wg.Wait()

	if f := failure.Load(); f != nil {
		t.Fatal(f)
	}
	if confirmed.Load() < 20 {
		t.Fatalf("only %d reads were confirmed; the test did not exercise much", confirmed.Load())
	}
	t.Logf("%d confirmed reads, %d acknowledged writes, no stale answer", confirmed.Load(), acked.Load())
	c.checkSafety()
}