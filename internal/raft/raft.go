package raft

import (
	"sync"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

// Config holds the static configuration of a Raft node.
type Config struct {
	ID                string        // this node's identifier
	Peers             []string      // identifiers of the other nodes in the cluster
	ElectionTimeout   time.Duration // base election timeout; randomized in [T, 2T)
	HeartbeatInterval time.Duration // leader heartbeat / replication period
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
	log         []keva.Entry // the replicated command log (indices start at logBase+1)

	// Snapshot state. A snapshot folds every entry up to logBase into a
	// state-machine image, letting the log discard that prefix. logBase is 0
	// until the first snapshot, after which the log holds only entries past it.
	logBase     uint64    // index of the last entry captured by the snapshot
	logBaseTerm uint64    // term of that entry
	snapshot    *Snapshot // latest snapshot, nil if none taken yet

	// Cluster membership. The configuration is itself part of the log: a
	// change is a config entry, and a node adopts the latest config entry the
	// moment it appears in the log. members is the full set (self included);
	// peers is the derived set of others. snapMembers is the configuration as
	// of the snapshot boundary, used as the base when the log holds no config
	// entry of its own.
	members     []keva.Member
	snapMembers []keva.Member

	// Volatile state, rebuilt on restart.
	state       State
	leaderID    string // the leader this node currently recognizes, "" if unknown
	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied to the state machine

	// Leader-only volatile state, reset on each election.
	nextIndex  map[string]uint64 // next log index to send to each peer
	matchIndex map[string]uint64 // highest index known replicated on each peer

	// Run-loop timing and lifecycle.
	baseTimeout       time.Duration
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	lastContact       time.Time
	running           bool
	stopCh            chan struct{}
}

// New creates a node in the follower state at term 0 with an empty log,
// using tr to reach its peers.
func New(cfg Config, tr Transport) *Raft {
	base := cfg.ElectionTimeout
	if base <= 0 {
		base = 150 * time.Millisecond
	}
	hb := cfg.HeartbeatInterval
	if hb <= 0 {
		hb = 50 * time.Millisecond
	}
	members := make([]keva.Member, 0, len(cfg.Peers)+1)
	members = append(members, keva.Member{ID: cfg.ID})
	for _, p := range cfg.Peers {
		members = append(members, keva.Member{ID: p})
	}

	return &Raft{
		id:                cfg.ID,
		peers:             cfg.Peers,
		transport:         tr,
		state:             Follower,
		baseTimeout:       base,
		heartbeatInterval: hb,
		members:           members,
		snapMembers:       members,
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

// lastLogIndex returns the index of the last log entry. Entries up to
// logBase have been compacted into the snapshot, so the last index is
// logBase plus the number of entries still held. The caller must hold r.mu.
func (r *Raft) lastLogIndex() uint64 {
	return r.logBase + uint64(len(r.log))
}

// lastLogTerm returns the term of the last log entry. When the log holds no
// entries of its own, it falls back to the snapshot's last included term.
// The caller must hold r.mu.
func (r *Raft) lastLogTerm() uint64 {
	if len(r.log) == 0 {
		return r.logBaseTerm
	}
	return r.log[len(r.log)-1].Term
}

// termAt returns the term of the entry at the given log index. index 0 (no
// entry) has term 0; index == logBase refers to the snapshot's last included
// entry. The index must not be below logBase, as those entries are compacted
// away. The caller must hold r.mu.
func (r *Raft) termAt(index uint64) uint64 {
	switch {
	case index == 0 || index < r.logBase:
		return 0
	case index == r.logBase:
		return r.logBaseTerm
	default:
		return r.log[index-r.logBase-1].Term
	}
}

// becomeFollower steps the node down to follower for the given term,
// clearing any vote recorded under a previous term. The caller must hold
// r.mu.
func (r *Raft) becomeFollower(term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderID = "" // leader unknown until the next heartbeat
	r.persist()
}

// Leader returns the ID of the leader this node currently recognizes, or
// "" if none is known.
func (r *Raft) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// IsLeader reports whether this node is currently the leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}
