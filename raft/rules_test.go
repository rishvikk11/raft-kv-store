package raft

// Direct tests of the rules in Figure 2, calling the message handlers by hand
// so each rule is checked in isolation.

import (
	"slices"
	"testing"
)

func newBareNode(t *testing.T, dir string, id, n int) *Node {
	t.Helper()
	nd, err := NewNode(Config{
		ID: id, ClusterSize: n, DataDir: dir, DisableFsync: true,
		Transport: endpoint{newTestNet(), id},
		Apply:     func(int, int, []byte) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nd.Stop)
	return nd
}

func ents(terms ...int) []Entry {
	out := make([]Entry, len(terms))
	for i, tm := range terms {
		out[i] = Entry{Term: tm, Command: []byte{byte('a' + i)}}
	}
	return out
}

func logTerms(nd *Node) []int {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	var out []int
	for _, e := range nd.log[1:] {
		out = append(out, e.Term)
	}
	return out
}

func TestRequestVoteRules(t *testing.T) {
	nd := newBareNode(t, t.TempDir(), 0, 3)

	if r := nd.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 1}); !r.VoteGranted || r.Term != 5 {
		t.Fatalf("first vote in term 5: %+v; want granted", r)
	}
	if r := nd.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}); r.VoteGranted {
		t.Fatal("granted a second vote in the same term to a different candidate")
	}
	if r := nd.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 1}); !r.VoteGranted {
		t.Fatal("refused to repeat its vote for the same candidate")
	}
	if r := nd.HandleRequestVote(RequestVoteArgs{Term: 4, CandidateID: 2}); r.VoteGranted || r.Term != 5 {
		t.Fatalf("vote for an old term: %+v; want denied with term 5", r)
	}
	if r := nd.HandleRequestVote(RequestVoteArgs{Term: 6, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("a newer term should free the node to vote again")
	}
}

func TestElectionRestrictionOnVotes(t *testing.T) {
	nd := newBareNode(t, t.TempDir(), 0, 3)
	// Give this node a ledger: [t1, t1, t2].
	nd.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: 1, Entries: ents(1, 1, 2)})

	cases := []struct {
		name              string
		term              int
		lastIdx, lastTerm int
		want              bool
	}{
		{"candidate has older last term", 3, 9, 1, false},
		{"same last term but shorter ledger", 4, 2, 2, false},
		{"same last term, same length", 5, 3, 2, true},
		{"newer last term, even if shorter", 6, 1, 3, true},
	}
	for _, c := range cases {
		r := nd.HandleRequestVote(RequestVoteArgs{Term: c.term, CandidateID: 2, LastLogIndex: c.lastIdx, LastLogTerm: c.lastTerm})
		if r.VoteGranted != c.want {
			t.Errorf("%s: granted=%v; want %v", c.name, r.VoteGranted, c.want)
		}
	}
}

func TestAppendEntriesRules(t *testing.T) {
	nd := newBareNode(t, t.TempDir(), 0, 3)

	// Stale leader is rejected.
	nd.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: 1})
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: 2}); r.Success || r.Term != 3 {
		t.Fatalf("old-term leader: %+v; want rejected with term 3", r)
	}

	// Gap: the follower has nothing at PrevLogIndex 5.
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 5, PrevLogTerm: 3}); r.Success || r.ConflictIndex != 1 {
		t.Fatalf("gap: %+v; want failure with ConflictIndex 1", r)
	}

	// Build [t1, t1, t2, t2].
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, Entries: ents(1, 1, 2, 2)}); !r.Success {
		t.Fatalf("initial append failed: %+v", r)
	}

	// A duplicate of an old message must NOT truncate the entries that follow it.
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, Entries: ents(1, 1)}); !r.Success {
		t.Fatalf("duplicate append failed: %+v", r)
	}
	if got := logTerms(nd); !slices.Equal(got, []int{1, 1, 2, 2}) {
		t.Fatalf("duplicate message changed the log to %v", got)
	}

	// Same index, different term at PrevLogIndex: reject and point at the first entry of the clashing term.
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 4, PrevLogTerm: 3}); r.Success || r.ConflictIndex != 3 {
		t.Fatalf("term clash: %+v; want failure with ConflictIndex 3", r)
	}

	// A real conflict: leader's entries at index 3+ are term 3, so ours (t2,t2) are replaced.
	if r := nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, Entries: ents(3, 3, 3)}); !r.Success {
		t.Fatalf("overwrite failed: %+v", r)
	}
	if got := logTerms(nd); !slices.Equal(got, []int{1, 1, 3, 3, 3}) {
		t.Fatalf("log after overwrite = %v; want [1 1 3 3 3]", got)
	}

	// Commit index follows the leader but never runs past what the message proved.
	nd.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, Entries: ents(3), LeaderCommit: 99})
	if st := nd.Status(); st.CommitIndex != 3 {
		t.Fatalf("commitIndex = %d; want 3 (prev 2 + 1 new entry)", st.CommitIndex)
	}
}

// Paper Figure 8: an entry from an OLD term must not be committed just because
// a majority holds it; it can still be overwritten. Only an entry from the
// leader's own term is committed by counting, and it carries the older ones along.
func TestOnlyCurrentTermEntriesCommitByCounting(t *testing.T) {
	nd := newBareNode(t, t.TempDir(), 0, 3)
	nd.mu.Lock()
	defer nd.mu.Unlock()

	nd.term = 4
	nd.role = Leader
	nd.log = append(nd.log, Entry{Term: 2}) // index 1: from an old term

	nd.matchIndex = []int{1, 1, 0} // a majority (nodes 0 and 1) holds index 1
	nd.advanceCommitLocked()
	if nd.commitIndex != 0 {
		t.Fatalf("committed an old-term entry by counting replicas (commitIndex=%d)", nd.commitIndex)
	}

	nd.log = append(nd.log, Entry{Term: 4}) // index 2: this leader's own entry
	nd.matchIndex = []int{2, 2, 0}
	nd.advanceCommitLocked()
	if nd.commitIndex != 2 {
		t.Fatalf("commitIndex = %d; want 2 (committing index 2 also commits index 1)", nd.commitIndex)
	}
}