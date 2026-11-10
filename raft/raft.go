// Package raft is an implementation of the Raft consensus algorithm, written
// from the Raft paper ("In Search of an Understandable Consensus Algorithm",
// Ongaro & Ousterhout), mainly its Figure 2.
//
// Included: leader election with randomized timeouts, log replication, the
// election restriction and the "only count current-term entries" commit rule,
// and durable state (currentTerm, votedFor, log) written before any reply.
// Not included (separate sections of the paper): log compaction/snapshots and
// cluster membership changes.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Role is what a node currently believes it is
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

func (r Role) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

// Entry is one line of the ledger. A new leader appends an Entry with an empty
// Command (a "no-op") as its first act, so entries from earlier terms can be
// committed safely.
type Entry struct {
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesArgs struct {
	Term         int     `json:"term"`
	LeaderID     int     `json:"leaderId"`
	PrevLogIndex int     `json:"prevLogIndex"`
	PrevLogTerm  int     `json:"prevLogTerm"`
	Entries      []Entry `json:"entries"`
	LeaderCommit int     `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// On failure: the leader should retry from ConflictIndex instead of
	// stepping back one entry at a time.
	ConflictIndex int `json:"conflictIndex"`
}

// Transport delivers messages to other nodes. A nil reply with an error means
// the message or its response was lost. Tests use an in-memory implementation;
// real clusters use HTTPTransport.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

type Config struct {
	ID          int // this node's number, 0..ClusterSize-1
	ClusterSize int
	Transport   Transport
	DataDir     string // directory holding this node's WAL

	// Apply is called once for every committed entry, in index order, from a
	// single goroutine. Entries with an empty command are leader no-ops.
	Apply func(index, term int, command []byte)

	ElectionTimeoutMin time.Duration // default 300ms
	ElectionTimeoutMax time.Duration // default 600ms
	HeartbeatInterval  time.Duration // default 75ms
	DisableFsync       bool          // tests only: skip forcing writes to disk
	Logf               func(format string, args ...any)
}

const (
	tickInterval     = 10 * time.Millisecond
	maxEntriesPerRPC = 64
)

type Node struct {
	cfg Config
	id  int
	n   int
	wal *wal

	mu        sync.Mutex // guards everything below
	stopped   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
	applyCond *sync.Cond

	// Persistent state (Figure 2). Saved to the WAL before we reply to anyone.
	term     int
	votedFor int // -1 = none this term
	log      []Entry
	// what the WAL currently holds for term/votedFor, so we only write on change
	savedTerm, savedVotedFor int

	// Volatile state.
	role             Role
	leaderID         int // -1 = unknown
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time

	// Leader-only state, rebuilt at each election win.
	nextIndex  []int // per peer: next ledger index to send
	matchIndex []int // per peer: highest index known stored there (self included)
	kicks      []chan struct{}
}

func NewNode(cfg Config) (*Node, error) {
	switch {
	case cfg.ClusterSize < 1 || cfg.ID < 0 || cfg.ID >= cfg.ClusterSize:
		return nil, errors.New("raft: ID must be in [0, ClusterSize)")
	case cfg.ClusterSize > 1 && cfg.Transport == nil:
		return nil, errors.New("raft: Transport required for a multi-node cluster")
	case cfg.Apply == nil:
		return nil, errors.New("raft: Apply callback required")
	case cfg.DataDir == "":
		return nil, errors.New("raft: DataDir required")
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 75 * time.Millisecond
	}

	w, st, err := openWAL(cfg.DataDir, !cfg.DisableFsync)
	if err != nil {
		return nil, fmt.Errorf("raft node %d: %w", cfg.ID, err)
	}
	nd := &Node{
		cfg: cfg, id: cfg.ID, n: cfg.ClusterSize, wal: w,
		stopCh: make(chan struct{}),
		term:   st.term, votedFor: st.votedFor, log: st.log,
		savedTerm: st.term, savedVotedFor: st.votedFor,
		role: Follower, leaderID: -1,
		nextIndex: make([]int, cfg.ClusterSize), matchIndex: make([]int, cfg.ClusterSize),
	}
	nd.applyCond = sync.NewCond(&nd.mu)
	return nd, nil
}

// Start launches the node's background goroutines.
func (nd *Node) Start() {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.stopped {
		return
	}
	nd.resetElectionTimerLocked()
	nd.wg.Add(2)
	go nd.ticker()
	go nd.applier()
}

// Stop shuts the node down and closes its WAL. Safe to call more than once.
func (nd *Node) Stop() {
	nd.mu.Lock()
	if nd.stopped {
		nd.mu.Unlock()
		return
	}
	nd.stopped = true
	close(nd.stopCh)
	nd.applyCond.Broadcast()
	nd.mu.Unlock()

	nd.wg.Wait()

	nd.mu.Lock()
	nd.wal.close()
	nd.mu.Unlock()
}

type Status struct {
	ID           int  `json:"id"`
	Term         int  `json:"term"`
	Role         Role `json:"role"`
	LeaderID     int  `json:"leaderId"` // -1 if unknown
	CommitIndex  int  `json:"commitIndex"`
	LastApplied  int  `json:"lastApplied"`
	LastLogIndex int  `json:"lastLogIndex"`
}

func (nd *Node) Status() Status {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return Status{
		ID: nd.id, Term: nd.term, Role: nd.role, LeaderID: nd.leaderID,
		CommitIndex: nd.commitIndex, LastApplied: nd.lastApplied, LastLogIndex: nd.lastIndex(),
	}
}

// Propose asks the cluster to add command to the ledger. It returns at once:
// ok=false means this node is not the leader. ok=true only means the leader
// accepted and saved the entry, NOT that it is committed. Watch for the entry
// in the Apply callback (and check that the term matches) to learn its fate.
// command must not be empty.
func (nd *Node) Propose(command []byte) (index, term int, ok bool) {
	if len(command) == 0 {
		return 0, 0, false
	}
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.stopped || nd.role != Leader {
		return 0, 0, false
	}
	index = nd.lastIndex() + 1
	nd.log = append(nd.log, Entry{Term: nd.term, Command: command})
	nd.persistLocked(record{kind: recEntry, index: index, term: nd.term, command: command})
	nd.matchIndex[nd.id] = index
	nd.advanceCommitLocked() // matters only for a 1-node cluster
	nd.kickAllLocked()
	return index, nd.term, true
}

// ---- small helpers (callers hold nd.mu) ----

func (nd *Node) lastIndex() int { return len(nd.log) - 1 }
func (nd *Node) lastTerm() int  { return nd.log[len(nd.log)-1].Term }

func (nd *Node) logf(format string, args ...any) {
	if nd.cfg.Logf != nil {
		nd.cfg.Logf(fmt.Sprintf("raft node %d (term %d, %s): ", nd.id, nd.term, nd.role)+format, args...)
	}
}

func (nd *Node) resetElectionTimerLocked() {
	span := int64(nd.cfg.ElectionTimeoutMax - nd.cfg.ElectionTimeoutMin)
	d := nd.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(span))
	nd.electionDeadline = time.Now().Add(d)
}

// persistLocked writes any changed term/votedFor plus the given ledger records
// to the WAL in one fsync. If the disk fails we cannot keep our promises, so we
// crash rather than carry on with state that is not saved.
func (nd *Node) persistLocked(recs ...record) {
	if nd.term != nd.savedTerm || nd.votedFor != nd.savedVotedFor {
		recs = append([]record{{kind: recHardState, term: nd.term, votedFor: nd.votedFor}}, recs...)
	}
	if len(recs) == 0 {
		return
	}
	if err := nd.wal.commit(recs); err != nil {
		panic(fmt.Sprintf("raft node %d: cannot write WAL: %v", nd.id, err))
	}
	nd.savedTerm, nd.savedVotedFor = nd.term, nd.votedFor
}

// becomeFollowerLocked moves to follower. If the term is newer, we forget our
// vote (each term allows one vote). Callers must persistLocked before replying.
func (nd *Node) becomeFollowerLocked(term int) {
	if term > nd.term {
		nd.term = term
		nd.votedFor = -1
		nd.leaderID = -1
	}
	if nd.role != Follower {
		nd.logf("stepping down to follower")
		nd.role = Follower
		nd.resetElectionTimerLocked()
	}
}

// ---- background loops ----

func (nd *Node) ticker() {
	defer nd.wg.Done()
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-nd.stopCh:
			return
		case <-t.C:
		}
		nd.mu.Lock()
		if !nd.stopped && nd.role != Leader && time.Now().After(nd.electionDeadline) {
			nd.startElectionLocked()
		}
		nd.mu.Unlock()
	}
}

// applier feeds committed entries to the state machine, in order, outside the lock.
func (nd *Node) applier() {
	defer nd.wg.Done()
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for {
		for !nd.stopped && nd.lastApplied >= nd.commitIndex {
			nd.applyCond.Wait()
		}
		if nd.stopped {
			return
		}
		start := nd.lastApplied + 1
		entries := append([]Entry(nil), nd.log[start:nd.commitIndex+1]...)
		nd.mu.Unlock()
		for i, e := range entries {
			nd.cfg.Apply(start+i, e.Term, e.Command)
		}
		nd.mu.Lock()
		nd.lastApplied = start + len(entries) - 1
	}
}

// ---- elections ----

func (nd *Node) startElectionLocked() {
	nd.term++
	nd.role = Candidate
	nd.votedFor = nd.id // vote for myself
	nd.leaderID = -1
	nd.persistLocked() // my vote must be on disk before I ask anyone else for theirs
	nd.resetElectionTimerLocked()
	nd.logf("starting election")

	term := nd.term
	args := RequestVoteArgs{Term: term, CandidateID: nd.id, LastLogIndex: nd.lastIndex(), LastLogTerm: nd.lastTerm()}
	votes := 1
	if votes*2 > nd.n { // single-node cluster
		nd.becomeLeaderLocked()
		return
	}
	for p := 0; p < nd.n; p++ {
		if p == nd.id {
			continue
		}
		nd.wg.Add(1)
		go func(p int) {
			defer nd.wg.Done()
			reply, err := nd.cfg.Transport.RequestVote(p, &args)
			if err != nil {
				return
			}
			nd.mu.Lock()
			defer nd.mu.Unlock()
			if nd.stopped || nd.role != Candidate || nd.term != term {
				return // stale: the election this vote belongs to is over
			}
			if reply.Term > nd.term {
				nd.becomeFollowerLocked(reply.Term)
				nd.persistLocked()
				return
			}
			if reply.VoteGranted {
				votes++
				if votes*2 > nd.n {
					nd.becomeLeaderLocked()
				}
			}
		}(p)
	}
}

func (nd *Node) becomeLeaderLocked() {
	nd.logf("won the election, becoming leader")
	nd.role = Leader
	nd.leaderID = nd.id
	for p := range nd.nextIndex {
		nd.nextIndex[p] = nd.lastIndex() + 1
		nd.matchIndex[p] = 0
	}
	// First act: a no-op entry in the new term. Once it commits, everything
	// before it commits too (see advanceCommitLocked for why we need this).
	nd.log = append(nd.log, Entry{Term: nd.term})
	nd.persistLocked(record{kind: recEntry, index: nd.lastIndex(), term: nd.term})
	nd.matchIndex[nd.id] = nd.lastIndex()

	nd.kicks = make([]chan struct{}, nd.n)
	for p := 0; p < nd.n; p++ {
		if p == nd.id {
			continue
		}
		nd.kicks[p] = make(chan struct{}, 1)
		nd.wg.Add(1)
		go nd.replicate(p, nd.term, nd.kicks[p])
	}
	nd.advanceCommitLocked() // matters only for a 1-node cluster
}

// HandleRequestVote is the receiver side of RequestVote (Figure 2).
func (nd *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.stopped || args.Term < nd.term {
		return RequestVoteReply{Term: nd.term}
	}
	if args.Term > nd.term {
		nd.becomeFollowerLocked(args.Term)
	}
	reply := RequestVoteReply{Term: nd.term}

	// Election restriction: only vote for a candidate whose ledger is at least
	// as up to date as ours. This is what stops a node that missed committed
	// entries from becoming leader and erasing them.
	upToDate := args.LastLogTerm > nd.lastTerm() ||
		(args.LastLogTerm == nd.lastTerm() && args.LastLogIndex >= nd.lastIndex())

	if (nd.votedFor == -1 || nd.votedFor == args.CandidateID) && upToDate {
		nd.votedFor = args.CandidateID
		reply.VoteGranted = true
		nd.resetElectionTimerLocked()
	}
	nd.persistLocked() // the vote is saved before the "yes" leaves this function
	return reply
}

// ---- replication: leader side ----

// replicate runs on the leader, one goroutine per follower, for one term.
func (nd *Node) replicate(peer, term int, kick <-chan struct{}) {
	defer nd.wg.Done()
	for {
		nd.mu.Lock()
		if nd.stopped || nd.role != Leader || nd.term != term {
			nd.mu.Unlock()
			return
		}
		next := nd.nextIndex[peer]
		args := AppendEntriesArgs{
			Term: term, LeaderID: nd.id,
			PrevLogIndex: next - 1, PrevLogTerm: nd.log[next-1].Term,
			LeaderCommit: nd.commitIndex,
		}
		if last := nd.lastIndex(); next <= last {
			end := min(last, next+maxEntriesPerRPC-1)
			args.Entries = append([]Entry(nil), nd.log[next:end+1]...)
		}
		nd.mu.Unlock()

		reply, err := nd.cfg.Transport.AppendEntries(peer, &args)

		nd.mu.Lock()
		if nd.stopped || nd.role != Leader || nd.term != term {
			nd.mu.Unlock()
			return
		}
		sendAgain := false
		if err == nil {
			switch {
			case reply.Term > nd.term:
				nd.becomeFollowerLocked(reply.Term)
				nd.persistLocked()
				nd.mu.Unlock()
				return
			case reply.Success:
				match := args.PrevLogIndex + len(args.Entries)
				if match > nd.matchIndex[peer] {
					nd.matchIndex[peer] = match
				}
				nd.nextIndex[peer] = nd.matchIndex[peer] + 1
				nd.advanceCommitLocked()
				sendAgain = nd.nextIndex[peer] <= nd.lastIndex()
			default:
				// The follower's ledger doesn't line up at PrevLogIndex: back up.
				ni := max(1, min(reply.ConflictIndex, next-1, nd.lastIndex()+1))
				nd.nextIndex[peer] = ni
				sendAgain = ni < next
			}
		}
		nd.mu.Unlock()
		if sendAgain {
			continue
		}
		select {
		case <-kick:
		case <-time.After(nd.cfg.HeartbeatInterval):
		case <-nd.stopCh:
			return
		}
	}
}

func (nd *Node) kickAllLocked() {
	for _, ch := range nd.kicks {
		if ch == nil {
			continue
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// advanceCommitLocked raises commitIndex to the highest entry stored on a
// majority, but ONLY if that entry is from the leader's own term. Counting
// replicas of an older-term entry is unsafe (paper, Figure 8): such an entry
// can still be overwritten even when a majority holds it. Committing a
// current-term entry commits everything before it as a side effect.
func (nd *Node) advanceCommitLocked() {
	for idx := nd.lastIndex(); idx > nd.commitIndex; idx-- {
		if nd.log[idx].Term != nd.term {
			return // everything lower is from an older term too
		}
		count := 0
		for p := 0; p < nd.n; p++ {
			if nd.matchIndex[p] >= idx {
				count++
			}
		}
		if count*2 > nd.n {
			nd.commitIndex = idx
			nd.applyCond.Broadcast()
			return
		}
	}
}

// ---- replication: follower side ----

// HandleAppendEntries is the receiver side of AppendEntries (Figure 2).
func (nd *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.stopped {
		return AppendEntriesReply{Term: nd.term}
	}
	reply, recs := nd.appendEntriesLocked(&args)
	nd.persistLocked(recs...) // saved before we say "success"
	return reply
}

func (nd *Node) appendEntriesLocked(args *AppendEntriesArgs) (AppendEntriesReply, []record) {
	if args.Term < nd.term {
		return AppendEntriesReply{Term: nd.term}, nil // message from a stale leader
	}
	if args.Term > nd.term || nd.role != Follower {
		nd.becomeFollowerLocked(args.Term)
	}
	reply := AppendEntriesReply{Term: nd.term}
	nd.leaderID = args.LeaderID
	nd.resetElectionTimerLocked() // a real leader is alive

	// Consistency check: do we hold the entry that comes right before the new ones?
	last := nd.lastIndex()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1
		return reply, nil
	}
	if t := nd.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		i := args.PrevLogIndex
		for i > 1 && nd.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i // first index of the conflicting term
		return reply, nil
	}
	reply.Success = true

	var recs []record
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= nd.lastIndex() {
			if nd.log[idx].Term == e.Term {
				continue // already have it (duplicate or reordered message)
			}
			if idx <= nd.commitIndex {
				panic(fmt.Sprintf("raft node %d: leader asked to overwrite committed entry %d", nd.id, idx))
			}
			nd.log = nd.log[:idx] // conflict: drop this entry and everything after it
			recs = append(recs, record{kind: recTruncate, index: idx})
		}
		nd.log = append(nd.log, e)
		recs = append(recs, record{kind: recEntry, index: idx, term: e.Term, command: e.Command})
	}

	if args.LeaderCommit > nd.commitIndex {
		// Never commit past what this message proved matches the leader's ledger.
		if c := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); c > nd.commitIndex {
			nd.commitIndex = c
			nd.applyCond.Broadcast()
		}
	}
	return reply, recs
}