package raft

import (
	"sync"

	"github.com/0xnikshi/keva/internal/keva"
)

// Config holds the static configuration of a Raft node.
type Config struct {
	ID    string   // this node's identifier
	Peers []string // identifiers of the other nodes in the cluster
}

// Raft is a single node participating in the consensus protocol.
type Raft struct {
	mu      sync.Mutex
	applyMu sync.Mutex // serializes state-machine application to keep it ordered

	id        string
	peers     []string
	transport Transport
	sm        StateMachine
	storage   Storage

	// Persistent state: must be saved to stable storage before the node
	// responds to an RPC, so it survives a crash. (Disk persistence is
	// wired up in a later step; for now it lives in memory.)
	currentTerm uint64       // latest term the node has seen
	votedFor    string       // candidate voted for in currentTerm; "" if none
	log         []keva.Entry // the replicated command log (1-based indices)

	// Volatile state, rebuilt on restart.
	state       State
	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied to the state machine

	// Leader-only volatile state, reset on each election.
	nextIndex  map[string]uint64 // next log index to send to each peer
	matchIndex map[string]uint64 // highest index known replicated on each peer
}

// New creates a node in the follower state at term 0 with an empty log,
// using tr to reach its peers.
func New(cfg Config, tr Transport) *Raft {
	return &Raft{
		id:        cfg.ID,
		peers:     cfg.Peers,
		transport: tr,
		state:     Follower,
	}
}

// State returns the node's current role.
func (r *Raft) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Term returns the node's current term.
func (r *Raft) Term() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// lastLogIndex returns the index of the last log entry, or 0 when the log
// is empty. Indices are 1-based: slice position i holds entry i+1.
// The caller must hold r.mu.
func (r *Raft) lastLogIndex() uint64 {
	return uint64(len(r.log))
}

// lastLogTerm returns the term of the last log entry, or 0 when empty.
// The caller must hold r.mu.
func (r *Raft) lastLogTerm() uint64 {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

// becomeFollower steps the node down to follower for the given term,
// clearing any vote recorded under a previous term. The caller must hold
// r.mu.
func (r *Raft) becomeFollower(term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.persist()
}
