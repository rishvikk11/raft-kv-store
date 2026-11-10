package kv

// Replicated puts Raft in front of the Store. A write is not applied when it
// arrives; it is proposed to Raft, and the Store changes only when Raft reports
// the entry COMMITTED (stored on a majority). Every node applies the same
// committed entries in the same order, so every node's Store ends up identical.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rishvikk11/raft-kv-store/raft"
)

const (
	commitTimeout = 5 * time.Second
	readTimeout   = 2 * time.Second
)

// Command is what goes into the Raft ledger. ClientID and Seq identify a
// request so that a retry of the same request is recognised and not applied twice.
type Command struct {
	Op       string `json:"op"` // "put" or "delete"
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID string `json:"clientId,omitempty"`
	Seq      int64  `json:"seq,omitempty"`
}

type applyResult struct {
	term    int  // term of the entry that was actually applied at this index
	existed bool // for delete: did the key exist?
	stale   bool // the request was older than one this client already completed
}

// session is what the cluster remembers about one client: the newest request
// it completed and the answer given, so a retry gets the same answer.
type session struct {
	seq     int64
	existed bool
}

type Replicated struct {
	store *Store
	peers []string // base URL of each node, indexed by node ID
	node  *raft.Node

	// Part of the replicated state, like store: rebuilt identically on every node
	// by applying the same entries in the same order. Only apply() touches it.
	sessions map[string]session

	mu      sync.Mutex
	waiters map[int]chan applyResult // HTTP requests waiting for a ledger index to be applied
}

// NewReplicated builds the node. cfg.Apply is set here; leave it nil.
func NewReplicated(cfg raft.Config, peers []string) (*Replicated, error) {
	r := &Replicated{store: NewStore(), peers: peers, sessions: map[string]session{}, waiters: map[int]chan applyResult{}}
	cfg.Apply = r.apply
	node, err := raft.NewNode(cfg)
	if err != nil {
		return nil, err
	}
	r.node = node
	return r, nil
}

func (r *Replicated) Start() { r.node.Start() }
func (r *Replicated) Stop()  { r.node.Stop() }

// execute changes the Store. It is the only place a write takes effect.
func (r *Replicated) execute(c Command) (existed bool) {
	switch c.Op {
	case "put":
		r.store.Put(c.Key, c.Value)
	case "delete":
		return r.store.Delete(c.Key)
	}
	return false
}

// apply is called by Raft, once per committed entry, in order, on every node.
// This is where duplicates are caught: it is the one place where all nodes see
// the same requests in the same order, so all nodes make the same decision.
func (r *Replicated) apply(index, term int, command []byte) {
	res := applyResult{term: term}
	if len(command) > 0 { // empty = a new leader's no-op: nothing for the Store to do
		var c Command
		switch err := json.Unmarshal(command, &c); {
		case err != nil:
			log.Printf("kv: skipping undecodable command at index %d: %v", index, err)
		case c.ClientID == "":
			res.existed = r.execute(c) // no request ID: applied every time it arrives
		default:
			s := r.sessions[c.ClientID]
			switch {
			case c.Seq > s.seq: // a new request: do it and remember the answer
				res.existed = r.execute(c)
				r.sessions[c.ClientID] = session{seq: c.Seq, existed: res.existed}
			case c.Seq == s.seq: // a retry of the last one: repeat the answer, change nothing
				res.existed = s.existed
			default: // older than the last one: the client has moved on
				res.stale = true
			}
		}
	}
	r.mu.Lock()
	ch := r.waiters[index]
	delete(r.waiters, index)
	r.mu.Unlock()
	if ch != nil {
		ch <- res
	}
}

func (r *Replicated) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/raft/", raft.Handler(r.node))
	mux.HandleFunc("PUT /kv/{key}", r.handlePut)
	mux.HandleFunc("GET /kv/{key}", r.handleGet)
	mux.HandleFunc("DELETE /kv/{key}", r.handleDelete)
	return mux
}

// notLeader sends the client to the leader if we know it, else asks it to retry.
func (r *Replicated) notLeader(w http.ResponseWriter, req *http.Request) {
	st := r.node.Status()
	if st.LeaderID >= 0 && st.LeaderID != st.ID && st.LeaderID < len(r.peers) {
		http.Redirect(w, req, r.peers[st.LeaderID]+req.URL.RequestURI(), http.StatusTemporaryRedirect)
		return
	}
	w.Header().Set("Retry-After", "1")
	http.Error(w, "no leader known yet; retry shortly", http.StatusServiceUnavailable)
}

// submit proposes c and waits until it has been committed and applied.
// On failure it writes the HTTP error itself and returns ok=false.
func (r *Replicated) submit(w http.ResponseWriter, req *http.Request, c Command) (res applyResult, ok bool) {
	data, _ := json.Marshal(c)
	ch := make(chan applyResult, 1)

	// Hold r.mu across Propose and registering the waiter so apply() cannot run
	// for this index before we are listening for it.
	r.mu.Lock()
	index, term, isLeader := r.node.Propose(data)
	if isLeader {
		if old := r.waiters[index]; old != nil {
			old <- applyResult{term: -1} // an earlier proposal at this index was overwritten
		}
		r.waiters[index] = ch
	}
	r.mu.Unlock()
	if !isLeader {
		r.notLeader(w, req)
		return applyResult{}, false
	}

	timer := time.NewTimer(commitTimeout)
	defer timer.Stop()
	select {
	case res = <-ch:
		if res.term != term {
			// A different leader's entry took this slot: ours was never committed.
			http.Error(w, "leadership changed before the write committed; retry", http.StatusServiceUnavailable)
			return applyResult{}, false
		}
		return res, true
	case <-timer.C:
	case <-req.Context().Done():
	}
	r.mu.Lock()
	if r.waiters[index] == ch {
		delete(r.waiters, index)
	}
	r.mu.Unlock()
	http.Error(w, "timed out waiting for the write to commit; it may or may not have been applied", http.StatusServiceUnavailable)
	return applyResult{}, false
}

// requestID reads the optional Client-Id and Request-Id headers. A client that
// wants retries to be safe sends both, numbering its requests 1, 2, 3, ... and
// re-sending the SAME number when it retries.
func requestID(req *http.Request) (clientID string, seq int64, err error) {
	clientID, raw := req.Header.Get("Client-Id"), req.Header.Get("Request-Id")
	if clientID == "" && raw == "" {
		return "", 0, nil
	}
	if clientID == "" || raw == "" {
		return "", 0, errors.New("send both Client-Id and Request-Id, or neither")
	}
	if len(clientID) > 128 {
		return "", 0, errors.New("Client-Id is too long (max 128 bytes)")
	}
	seq, err = strconv.ParseInt(raw, 10, 64)
	if err != nil || seq < 1 {
		return "", 0, errors.New("Request-Id must be a whole number, 1 or higher")
	}
	return clientID, seq, nil
}

func (r *Replicated) handlePut(w http.ResponseWriter, req *http.Request) {
	clientID, seq, err := requestID(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxValueBytes))
	if err != nil {
		http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	res, ok := r.submit(w, req, Command{Op: "put", Key: req.PathValue("key"), Value: string(body), ClientID: clientID, Seq: seq})
	if !ok {
		return
	}
	if res.stale {
		http.Error(w, "an older request than one this client already completed", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Replicated) handleDelete(w http.ResponseWriter, req *http.Request) {
	clientID, seq, err := requestID(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, ok := r.submit(w, req, Command{Op: "delete", Key: req.PathValue("key"), ClientID: clientID, Seq: seq})
	if !ok {
		return
	}
	switch {
	case res.stale:
		http.Error(w, "an older request than one this client already completed", http.StatusConflict)
	case !res.existed:
		http.Error(w, "key not found", http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleGet answers from the local Store, but only after Raft confirms that
// this node is STILL the leader and has applied everything committed so far.
// Without that check a leader that was cut off from the cluster would keep
// answering from old data (see TestNaiveLeaderReadIsStaleAfterPartition).
func (r *Replicated) handleGet(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), readTimeout)
	defer cancel()
	switch err := r.node.ConfirmRead(ctx); {
	case err == nil:
	case errors.Is(err, raft.ErrNotLeader):
		r.notLeader(w, req)
		return
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "could not confirm this node is still the leader; retry", http.StatusServiceUnavailable)
		return
	}
	v, ok := r.store.Get(req.PathValue("key"))
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	io.WriteString(w, v)
}