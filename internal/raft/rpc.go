package raft

import "github.com/0xnikshi/keva/internal/keva"

// RequestVoteArgs holds the arguments for a RequestVote RPC, sent by a
// candidate soliciting a vote.
type RequestVoteArgs struct {
	Term         uint64 // candidate's term
	CandidateID  string // candidate requesting the vote
	LastLogIndex uint64 // index of the candidate's last log entry
	LastLogTerm  uint64 // term of the candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 // responder's term, so a stale candidate can update itself
	VoteGranted bool   // true if the vote was granted
}

// AppendEntriesArgs holds the arguments for an AppendEntries RPC, sent by
// the leader to replicate log entries and as a heartbeat when empty.
type AppendEntriesArgs struct {
	Term         uint64       // leader's term
	LeaderID     string       // so followers can redirect clients
	PrevLogIndex uint64       // index of the entry immediately before Entries
	PrevLogTerm  uint64       // term of the PrevLogIndex entry
	Entries      []keva.Entry // new entries to store (empty for a heartbeat)
	LeaderCommit uint64       // leader's commit index
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64 // responder's term, for the leader to update itself
	Success bool   // true if the follower's log matched and entries were stored
}

// InstallSnapshotArgs holds the arguments for an InstallSnapshot RPC, sent by
// the leader to a follower that has fallen behind the leader's snapshot
// boundary: the entries the follower still needs are already compacted away,
// so the leader ships the whole state-machine image instead of log entries.
type InstallSnapshotArgs struct {
	Term              uint64        // leader's term
	LeaderID          string        // so the follower can redirect clients
	LastIncludedIndex uint64        // the snapshot replaces the log through this index
	LastIncludedTerm  uint64        // term of the last included entry
	Data              []byte        // serialized state-machine image
	Members           []keva.Member // cluster configuration as of the boundary
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64 // responder's term, for the leader to update itself
}

// Transport sends Raft RPCs to peer nodes, addressed by peer ID. It is an
// interface so the consensus logic can be tested over an in-memory
// transport, with no real network involved.
type Transport interface {
	RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	InstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
	// SetPeers registers or updates peer addresses (id → URL), so a node can
	// reach members added after startup through a configuration change.
	SetPeers(peers map[string]string)
}
