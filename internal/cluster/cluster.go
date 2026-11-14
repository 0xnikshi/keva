// Package cluster wires a Raft node, its state-machine store, and the HTTP
// handlers for client and peer traffic into one unit.
package cluster

import (
	"context"
	"net/http"
	"time"

	"github.com/0xnikshi/keva/internal/fsm"
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/rafthttp"
	"github.com/0xnikshi/keva/internal/server"
	"github.com/0xnikshi/keva/internal/store"
)

// Config configures a cluster node.
type Config struct {
	ID       string
	PeerIDs  []string          // identifiers of the other nodes
	PeerURLs map[string]string // peer ID → base URL (may be filled after New)

	StoragePath string // file for persistent Raft state; "" → in-memory

	ElectionTimeout   time.Duration // zero → Raft default
	HeartbeatInterval time.Duration // zero → Raft default
}

// Node is a fully wired Keva node: Raft consensus over an HTTP transport, an
// in-memory store as the state machine, and a combined HTTP handler serving
// both client (/v1) and peer (/raft) routes.
type Node struct {
	raft    *raft.Raft
	handler http.Handler
}

// New builds, but does not start, a node from cfg. PeerURLs is held by
// reference, so it may be populated after New and before Start. When
// StoragePath is set, persistent state is loaded from it (recovering a
// crashed node) and saved to it going forward.
func New(cfg Config) (*Node, error) {
	mem := store.NewMemory()

	r := raft.New(raft.Config{
		ID:                cfg.ID,
		Peers:             cfg.PeerIDs,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	}, rafthttp.New(cfg.PeerURLs))
	r.SetStateMachine(fsm.New(mem))

	if cfg.StoragePath != "" {
		if err := r.SetStorage(raft.NewFileStorage(cfg.StoragePath)); err != nil {
			return nil, err
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/v1/", server.New(r, mem, cfg.PeerURLs).Handler())
	mux.Handle("/raft/", rafthttp.Handler(r))

	return &Node{raft: r, handler: mux}, nil
}

// Handler returns the node's HTTP handler.
func (n *Node) Handler() http.Handler { return n.handler }

// Start runs the Raft loop.
func (n *Node) Start() { n.raft.Start() }

// Stop halts the Raft loop.
func (n *Node) Stop() { n.raft.Stop() }

// IsLeader reports whether this node is currently the leader.
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// TransferLeadership hands leadership to an up-to-date follower, returning once
// this node has stepped down. Used on graceful shutdown so the cluster keeps a
// leader without waiting out an election timeout.
func (n *Node) TransferLeadership(ctx context.Context) error {
	return n.raft.TransferLeadership(ctx)
}

// AddNode adds a member to the cluster. It must be called on the leader. The
// address lets every node reach the new member once the change replicates.
func (n *Node) AddNode(id, addr string) error {
	_, err := n.raft.AddNode(keva.Member{ID: id, Addr: addr})
	return err
}

// RemoveNode removes a member from the cluster. It must be called on the leader.
func (n *Node) RemoveNode(id string) error {
	_, err := n.raft.RemoveNode(id)
	return err
}
