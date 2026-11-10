package raft

// HTTP transport: each node sends Raft messages to its peers as JSON POSTs and
// serves the same three endpoints. Deliberately simple and easy to inspect with curl.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxRPCBody = 128 << 20 // an AppendEntries batch can carry many entries

type HTTPTransport struct {
	peers  []string // base URL of every node, indexed by node ID
	client *http.Client
}

func NewHTTPTransport(peers []string) *HTTPTransport {
	clean := make([]string, len(peers))
	for i, p := range peers {
		clean[i] = strings.TrimRight(p, "/")
	}
	return &HTTPTransport{
		peers: clean,
		// A short timeout keeps a dead or unreachable peer from stalling its sender.
		client: &http.Client{Timeout: 500 * time.Millisecond},
	}
}

func (t *HTTPTransport) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := t.call(to, "/raft/vote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := t.call(to, "/raft/append", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) call(to int, path string, args, reply any) error {
	if to < 0 || to >= len(t.peers) {
		return fmt.Errorf("raft: no peer %d", to)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(t.peers[to]+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("raft: %s on node %d: %s", path, to, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}

// Handler serves the Raft endpoints for nd. Mount it at /raft/.
func Handler(nd *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRPCBody)).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(nd.HandleRequestVote(args))
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRPCBody)).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(nd.HandleAppendEntries(args))
	})
	mux.HandleFunc("GET /raft/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(nd.Status())
	})
	return mux
}