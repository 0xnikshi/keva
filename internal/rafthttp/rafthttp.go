// Package rafthttp carries Raft RPCs between nodes over HTTP + JSON: a
// Transport sends them to peers, and Handler serves incoming ones.
package rafthttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/0xnikshi/keva/internal/raft"
)

// Transport sends Raft RPCs to peers over HTTP, addressing each by ID.
type Transport struct {
	peers  map[string]string // peer ID → base URL
	client *http.Client
}

// New returns a Transport that reaches the given peers (id → base URL).
func New(peers map[string]string) *Transport {
	return &Transport{
		peers:  peers,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// RequestVote sends a RequestVote RPC to peer.
func (t *Transport) RequestVote(peer string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := t.post(peer, "/raft/vote", args, &reply)
	return reply, err
}

// AppendEntries sends an AppendEntries RPC to peer.
func (t *Transport) AppendEntries(peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := t.post(peer, "/raft/append", args, &reply)
	return reply, err
}

func (t *Transport) post(peer, path string, args, reply any) error {
	base, ok := t.peers[peer]
	if !ok {
		return fmt.Errorf("rafthttp: unknown peer %q", peer)
	}

	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rafthttp: peer %q returned status %d", peer, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}

// Handler returns the HTTP handler that serves incoming Raft RPCs for node.
func Handler(node *raft.Raft) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, node.RequestVote(args))
	})

	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, node.AppendEntries(args))
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
