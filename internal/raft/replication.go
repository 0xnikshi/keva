package raft

import (
	"context"
	"errors"

	"github.com/0xnikshi/keva/internal/keva"
)

// ErrNotLeader is returned by Submit when the node is not the leader.
var ErrNotLeader = errors.New("raft: not leader")

// AppendEntries handles an incoming AppendEntries RPC. The leader uses it
// both to replicate log entries and, with no entries, as a heartbeat.
func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()

	// Reject a leader from an older term.
	if args.Term < r.currentTerm {
		reply := AppendEntriesReply{Term: r.currentTerm, Success: false}
		r.mu.Unlock()
		return reply
	}
	// Recognize this leader for the term and (re)assert follower state.
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	} else {
		r.state = Follower
	}
	r.resetElectionDeadline() // heard from a live leader
	r.leaderID = args.LeaderID

	// Log matching: we must already hold the entry preceding the new ones,
	// with the same term. Otherwise reject so the leader backs up and retries.
	// Entries at or below logBase are already captured by our snapshot and
	// known good, so the term check only applies from logBase onward.
	if args.PrevLogIndex > r.lastLogIndex() {
		reply := AppendEntriesReply{Term: r.currentTerm, Success: false}
		r.mu.Unlock()
		return reply
	}
	if args.PrevLogIndex >= r.logBase && args.PrevLogIndex > 0 &&
		r.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply := AppendEntriesReply{Term: r.currentTerm, Success: false}
		r.mu.Unlock()
		return reply
	}

	// Append new entries, truncating the first conflicting entry and
	// everything after it. Entries already folded into the snapshot are
	// skipped rather than re-stored.
	for i := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if idx <= r.logBase {
			continue
		}
		pos := idx - r.logBase - 1
		if idx > r.lastLogIndex() {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
		if r.log[pos].Term != args.Entries[i].Term {
			r.log = append(r.log[:pos], args.Entries[i:]...)
			break
		}
	}

	// A config entry may have arrived or been truncated; adopt the latest
	// configuration the log now implies.
	r.refreshConfig()

	// Adopt the leader's commit index, bounded by what we now hold.
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, r.lastLogIndex())
	}

	r.persist() // the log and/or term may have changed
	reply := AppendEntriesReply{Term: r.currentTerm, Success: true}
	r.mu.Unlock()

	// Apply any entries the leader just told us are committed. Done after
	// releasing the lock, since applyCommitted takes it itself.
	r.applyCommitted()
	return reply
}

// Submit appends a command to the leader's log and gets it persisted and
// replicated, returning the log index it was stored at. It fails if this node
// is not the leader. Submit is propose-only: it returns once the entry has
// been through a persist-and-replicate round, not once it has committed —
// callers that need durability wait on AwaitCommit. Concurrent Submits share a
// single persist and replication round (see flush).
func (r *Raft) Submit(cmd keva.Command) (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	index := r.lastLogIndex() + 1
	r.log = append(r.log, keva.Entry{Term: r.currentTerm, Index: index, Command: cmd})
	r.mu.Unlock()

	r.flush()
	return index, nil
}

// flush persists the log and runs one replication round, coalescing concurrent
// callers: the first starts a processor, the rest queue, and one persist +
// round serves the whole batch. A caller is always served by a round that
// began after its own append, so its entry is included.
func (r *Raft) flush() {
	done := make(chan struct{})
	r.flushMu.Lock()
	r.flushWaiters = append(r.flushWaiters, done)
	if !r.flushing {
		r.flushing = true
		go r.flushLoop()
	}
	r.flushMu.Unlock()
	<-done
}

func (r *Raft) flushLoop() {
	for {
		r.flushMu.Lock()
		batch := r.flushWaiters
		r.flushWaiters = nil
		if len(batch) == 0 {
			r.flushing = false
			r.flushMu.Unlock()
			return
		}
		r.flushMu.Unlock()

		r.mu.Lock()
		r.persist() // one fsync covering every entry appended by the batch
		r.mu.Unlock()
		r.replicate() // one round carrying them all

		for _, done := range batch {
			close(done)
		}
	}
}

// AwaitCommit blocks until the entry at index is committed, returning nil, or
// until ctx is done, or until this node stops being the leader for the term
// that proposed the entry — in which case the entry may never commit, so the
// caller must treat it as failed. It lets a server turn Submit's propose-only
// semantics into a durable acknowledgement without polling.
func (r *Raft) AwaitCommit(ctx context.Context, index uint64) error {
	for {
		r.mu.Lock()
		switch {
		case r.state != Leader:
			r.mu.Unlock()
			return ErrNotLeader
		case r.commitIndex >= index:
			r.mu.Unlock()
			return nil
		}
		wait := r.commitWait
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
			// Commit advanced or leadership changed; loop and re-check.
		}
	}
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

	r.applyCommitted()
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
		if prevIndex < r.logBase {
			// The entries this peer needs are compacted away; send it the
			// snapshot to catch up, then let a later round resume from the
			// boundary with ordinary AppendEntries.
			snap := r.snapshot
			r.mu.Unlock()
			r.sendSnapshot(peer, term, snap)
			return
		}
		var prevTerm uint64
		if prevIndex > 0 {
			prevTerm = r.termAt(prevIndex)
		}
		entries := append([]keva.Entry(nil), r.log[prevIndex-r.logBase:]...)
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
	// Only consider entries that have been persisted: the commit index must
	// never pass a write the fsync has not yet covered.
	for n := r.persistedIndex; n > r.commitIndex; n-- {
		// A leader only commits an entry by counting once it is from the
		// leader's own term (a core Raft safety rule).
		if r.termAt(n) != r.currentTerm {
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
			r.notifyCommitWaiters()
			return
		}
	}
}
