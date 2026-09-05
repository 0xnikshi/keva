package raft

import (
	"errors"

	"github.com/0xnikshi/keva/internal/keva"
)

// ErrNotLeader is returned by Submit when the node is not the leader.
var ErrNotLeader = errors.New("raft: not leader")

// AppendEntries handles an incoming AppendEntries RPC. The leader uses it
// both to replicate log entries and, with no entries, as a heartbeat.
func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject a leader from an older term.
	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	// Recognize this leader for the term and (re)assert follower state.
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	} else {
		r.state = Follower
	}

	// Log matching: we must already hold the entry preceding the new ones,
	// with the same term. Otherwise reject so the leader backs up and retries.
	if args.PrevLogIndex > r.lastLogIndex() {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	if args.PrevLogIndex > 0 && r.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	// Append new entries, truncating the first conflicting entry and
	// everything after it.
	for i := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if idx > r.lastLogIndex() {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
		if r.log[idx-1].Term != args.Entries[i].Term {
			r.log = append(r.log[:idx-1], args.Entries[i:]...)
			break
		}
	}

	// Adopt the leader's commit index, bounded by what we now hold.
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, r.lastLogIndex())
	}

	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}

// Submit appends a command to the leader's log and replicates it, returning
// the log index it was stored at. It fails if this node is not the leader.
func (r *Raft) Submit(cmd keva.Command) (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	index := r.lastLogIndex() + 1
	r.log = append(r.log, keva.Entry{Term: r.currentTerm, Index: index, Command: cmd})
	r.mu.Unlock()

	r.replicate()
	return index, nil
}

// replicate pushes outstanding entries to every peer, then advances the
// commit index if a majority now hold them.
func (r *Raft) replicate() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	peers := append([]string(nil), r.peers...)
	r.mu.Unlock()

	for _, peer := range peers {
		r.replicateTo(peer, term)
	}

	r.mu.Lock()
	r.advanceCommit()
	r.mu.Unlock()
}

// replicateTo brings one peer's log up to date, backing off nextIndex on a
// log-matching failure until the follower accepts.
func (r *Raft) replicateTo(peer string, term uint64) {
	for {
		r.mu.Lock()
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		prevIndex := r.nextIndex[peer] - 1
		var prevTerm uint64
		if prevIndex > 0 {
			prevTerm = r.log[prevIndex-1].Term
		}
		entries := append([]keva.Entry(nil), r.log[prevIndex:]...)
		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     r.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		r.mu.Unlock()

		reply, err := r.transport.AppendEntries(peer, args)
		if err != nil {
			return // unreachable; a later round retries
		}

		r.mu.Lock()
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term)
			r.mu.Unlock()
			return
		}
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		if reply.Success {
			r.matchIndex[peer] = prevIndex + uint64(len(entries))
			r.nextIndex[peer] = r.matchIndex[peer] + 1
			r.mu.Unlock()
			return
		}
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]-- // back up one and retry
		}
		r.mu.Unlock()
	}
}

// advanceCommit raises commitIndex to the highest entry from the current
// term that a majority of nodes have stored. Caller holds r.mu.
func (r *Raft) advanceCommit() {
	majority := (len(r.peers)+1)/2 + 1
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		// A leader only commits an entry by counting once it is from the
		// leader's own term (a core Raft safety rule).
		if r.log[n-1].Term != r.currentTerm {
			continue
		}
		count := 1 // the leader holds it
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= majority {
			r.commitIndex = n
			return
		}
	}
}
