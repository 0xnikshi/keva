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

// Transport sends Raft RPCs to peer nodes, addressed by peer ID. It is an
// interface so the consensus logic can be tested over an in-memory
// transport, with no real network involved.
type Transport interface {
	RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
