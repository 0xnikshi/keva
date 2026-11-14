package raft

import (
	"context"
	"errors"
	"time"
)

// ErrNoTransferTarget means no follower was caught up enough to receive
// leadership, so the caller should fall back to a plain stop.
var ErrNoTransferTarget = errors.New("raft: no up-to-date follower to receive leadership")

// TimeoutNow handles a TimeoutNow RPC: the current leader is stepping down and
// has chosen this node to take over, so it arms an immediate election instead
// of waiting out its election timeout. The node still has to win on its own
// merits — the leader picked it because its log is up to date.
func (r *Raft) TimeoutNow(args TimeoutNowArgs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if args.Term < r.currentTerm || r.state == Leader {
		return
	}
	r.lastContact = time.Time{} // force the next tick to see a timeout
	r.electionTimeout = 0
}

// TransferLeadership hands leadership to an up-to-date follower and returns
// once this node has stepped down, so a graceful shutdown leaves a leader in
// place with no election-timeout gap. It brings a follower current, stops
// heartbeating so it will not reset that follower's timer, tells it to time
// out now, and waits for it to win. It returns ErrNotLeader if this node is
// not the leader, ErrNoTransferTarget if no follower is caught up, or ctx's
// error if the handoff does not complete in time.
func (r *Raft) TransferLeadership(ctx context.Context) error {
	r.replicate() // best effort: bring followers up to date first

	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	term := r.currentTerm
	last := r.lastLogIndex()
	var target string
	for _, peer := range r.peers {
		if r.matchIndex[peer] >= last {
			target = peer
			break
		}
	}
	if target == "" {
		r.mu.Unlock()
		return ErrNoTransferTarget
	}
	r.transferring = true // stop heartbeating so the target's armed timer fires
	r.mu.Unlock()

	if err := r.transport.TimeoutNow(target, TimeoutNowArgs{Term: term, LeaderID: r.id}); err != nil {
		r.clearTransferring()
		return err
	}

	// Wait until the target's election steps us down, or give up on timeout.
	for {
		r.mu.Lock()
		stepped := r.state != Leader || r.currentTerm != term
		r.mu.Unlock()
		if stepped {
			return nil
		}
		select {
		case <-ctx.Done():
			r.clearTransferring() // handoff failed; resume normal leader duty
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (r *Raft) clearTransferring() {
	r.mu.Lock()
	r.transferring = false
	r.mu.Unlock()
}
