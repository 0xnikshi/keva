package raft

import (
	"context"
	"time"
)

// readConfirmTimeout bounds one batched read-index confirmation round, so the
// batch processor never blocks indefinitely on a cluster that cannot commit.
const readConfirmTimeout = 5 * time.Second

type readResult struct {
	idx uint64
	err error
}

// ReadIndex returns a commit index such that, once the state machine has
// applied up to it, a read reflects every write committed before the call — a
// linearizable read. Concurrent callers are coalesced: they queue and share a
// single leadership-confirmation round rather than each heartbeating a
// majority, so read throughput is not bounded by one round per read. It
// returns ErrNotLeader if this node is not the leader, or ctx's error if the
// caller gives up first.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	res := make(chan readResult, 1)

	r.readMu.Lock()
	r.readQueue = append(r.readQueue, res)
	if !r.readProcessing {
		r.readProcessing = true
		go r.processReads()
	}
	r.readMu.Unlock()

	select {
	case rr := <-res:
		return rr.idx, rr.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// processReads serves queued reads in batches. Each pass takes every request
// waiting, confirms the read index once, and delivers that result to all of
// them — so a burst of concurrent reads costs one confirmation round, not one
// per read. A request that arrives mid-round joins the next batch, so it is
// always served by a round whose index reflects its own arrival.
func (r *Raft) processReads() {
	for {
		r.readMu.Lock()
		batch := r.readQueue
		r.readQueue = nil
		if len(batch) == 0 {
			r.readProcessing = false
			r.readMu.Unlock()
			return
		}
		r.readMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), readConfirmTimeout)
		idx, err := r.confirmReadIndex(ctx)
		cancel()

		for _, res := range batch {
			res <- readResult{idx: idx, err: err}
		}
	}
}

// confirmReadIndex performs one read-index confirmation: wait for the read
// barrier, capture the commit index, confirm leadership with a majority, and
// apply up to that index. See ReadIndex for the linearizability rationale.
func (r *Raft) confirmReadIndex(ctx context.Context) (uint64, error) {
	// Raft §6.4: a newly elected leader's commit index can trail the true
	// commit point until it commits an entry from its own term. Serving a read
	// before then risks missing an already-acknowledged write.
	if err := r.awaitReadBarrier(ctx); err != nil {
		return 0, err
	}

	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := r.currentTerm
	readIdx := r.commitIndex
	peers := append([]string(nil), r.peers...)
	r.mu.Unlock()

	acks := 1 // this node
	majority := (len(peers)+1)/2 + 1
	for _, peer := range peers {
		if acks >= majority {
			break
		}
		if r.confirmPeer(peer, term) {
			acks++
		}
	}
	if acks < majority {
		return 0, ErrNotLeader
	}

	// Ensure the state machine has applied everything committed as of readIdx
	// before the caller reads it. Without this, a read can land in the window
	// between an entry being committed and being applied, and miss a write it
	// must reflect — a stale, non-linearizable read.
	r.applyCommitted()
	return readIdx, nil
}

// awaitReadBarrier blocks until a current-term entry (the election no-op) has
// committed, so the commit index reflects the true commit point. It returns
// early if the node stops leading or ctx is done.
func (r *Raft) awaitReadBarrier(ctx context.Context) error {
	for {
		r.mu.Lock()
		switch {
		case r.state != Leader:
			r.mu.Unlock()
			return ErrNotLeader
		case r.commitIndex >= r.readBarrier:
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

// confirmPeer heartbeats peer and reports whether it replied in a term no
// greater than ours — i.e. still recognizes this node as leader. A higher
// term steps us down. Caller must not hold r.mu.
func (r *Raft) confirmPeer(peer string, term uint64) bool {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return false
	}
	prevIndex := r.nextIndex[peer] - 1
	var prevTerm uint64
	if prevIndex >= r.logBase && prevIndex > 0 && prevIndex <= r.lastLogIndex() {
		prevTerm = r.termAt(prevIndex)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.transport.AppendEntries(peer, args)
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return false
	}
	return true
}
