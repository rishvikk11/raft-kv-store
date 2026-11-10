package kv

// Replicated puts Raft in front of the Store. A write is not applied when it
// arrives; it is proposed to Raft, and the Store changes only when Raft reports
// the entry COMMITTED (stored on a majority). Every node applies the same
// committed entries in the same order, so every node's Store ends up identical.

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/rishvikk11/raft-kv-store/raft"
)

const commitTimeout = 5 * time.Second

// Command is what goes into the Raft ledger.
type Command struct {
	Op    string `json:"op"` // "put" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type applyResult struct {
	term    int  // term of the entry that was actually applied at this index
	existed bool // for delete: did the key exist?
}

type Replicated struct {
	store *Store
	peers []string // base URL of each node, indexed by node ID
	node  *raft.Node

	mu      sync.Mutex
	waiters map[int]chan applyResult // HTTP requests waiting for a ledger index to be applied
}

// NewReplicated builds the node. cfg.Apply is set here; leave it nil.
func NewReplicated(cfg raft.Config, peers []string) (*Replicated, error) {
	r := &Replicated{store: NewStore(), peers: peers, waiters: map[int]chan applyResult{}}
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

// apply is called by Raft, once per committed entry, in order.
func (r *Replicated) apply(index, term int, command []byte) {
	var existed bool
	if len(command) > 0 { // empty = a new leader's no-op: nothing for the Store to do
		var c Command
		if err := json.Unmarshal(command, &c); err != nil {
			log.Printf("kv: skipping undecodable command at index %d: %v", index, err)
		} else {
			switch c.Op {
			case "put":
				r.store.Put(c.Key, c.Value)
			case "delete":
				existed = r.store.Delete(c.Key)
			}
		}
	}
	r.mu.Lock()
	ch := r.waiters[index]
	delete(r.waiters, index)
	r.mu.Unlock()
	if ch != nil {
		ch <- applyResult{term: term, existed: existed}
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

func (r *Replicated) handlePut(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxValueBytes))
	if err != nil {
		http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	if _, ok := r.submit(w, req, Command{Op: "put", Key: req.PathValue("key"), Value: string(body)}); ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (r *Replicated) handleDelete(w http.ResponseWriter, req *http.Request) {
	res, ok := r.submit(w, req, Command{Op: "delete", Key: req.PathValue("key")})
	if !ok {
		return
	}
	if !res.existed {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGet is the NAIVE read: the leader answers from its own Store.
// This is knowingly incomplete and is replaced in Stage 3. A leader that has
// been cut off from the cluster, or whose Store has not yet applied the latest
// committed writes, can return stale data here.
func (r *Replicated) handleGet(w http.ResponseWriter, req *http.Request) {
	if r.node.Status().Role != raft.Leader {
		r.notLeader(w, req)
		return
	}
	v, ok := r.store.Get(req.PathValue("key"))
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	io.WriteString(w, v)
}