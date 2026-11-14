// Package rafthttp carries Raft RPCs between nodes over HTTP + JSON: a
// Transport sends them to peers, and Handler serves incoming ones.
package rafthttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/0xnikshi/keva/internal/raft"
)

// Transport sends Raft RPCs to peers over HTTP, addressing each by ID. The
// base peer map is supplied at startup (and read-only thereafter); addresses
// of members added later through a configuration change are kept separately
// under a mutex, since those arrive while the node is running.
type Transport struct {
	peers map[string]string // base peer ID → URL, fixed once the node starts

	mu    sync.RWMutex
	extra map[string]string // addresses learned from configuration changes

	client *http.Client
}

// New returns a Transport that reaches the given peers (id → base URL). The
// map may be populated by the caller up until the node starts.
func New(peers map[string]string) *Transport {
	return &Transport{
		peers:  peers,
		extra:  map[string]string{},
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// SetPeers registers or updates peer addresses, so a node can reach members
// added through a configuration change after startup.
func (t *Transport) SetPeers(peers map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, url := range peers {
		t.extra[id] = url
	}
}

func (t *Transport) peerURL(peer string) (string, bool) {
	if url, ok := t.peers[peer]; ok {
		return url, true
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	url, ok := t.extra[peer]
	return url, ok
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

// InstallSnapshot sends an InstallSnapshot RPC to peer.
func (t *Transport) InstallSnapshot(peer string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	err := t.post(peer, "/raft/snapshot", args, &reply)
	return reply, err
}

// TimeoutNow sends a TimeoutNow RPC to peer, asking it to start an election
// immediately as part of a leadership handoff.
func (t *Transport) TimeoutNow(peer string, args raft.TimeoutNowArgs) error {
	var reply struct{}
	return t.post(peer, "/raft/timeoutnow", args, &reply)
}

func (t *Transport) post(peer, path string, args, reply any) error {
	base, ok := t.peerURL(peer)
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

	mux.HandleFunc("POST /raft/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var args raft.InstallSnapshotArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, node.InstallSnapshot(args))
	})

	mux.HandleFunc("POST /raft/timeoutnow", func(w http.ResponseWriter, r *http.Request) {
		var args raft.TimeoutNowArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		node.TimeoutNow(args)
		writeJSON(w, struct{}{})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
