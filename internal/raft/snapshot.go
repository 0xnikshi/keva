package raft

import "github.com/0xnikshi/keva/internal/keva"

// Snapshot is a point-in-time image of the state machine that stands in for
// the log prefix it replaces. LastIncludedIndex and LastIncludedTerm name the
// final entry folded into Data, so the log can resume from the entry after it
// and a restarted node can resolve log-matching against the boundary.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	Members           []keva.Member // cluster configuration as of the boundary
}

// Snapshot compacts the log once it has grown to at least threshold entries,
// folding every applied entry into a state-machine image and discarding that
// prefix. It is a no-op when the log is still short or nothing new has been
// applied since the last snapshot. It reports whether a snapshot was taken.
func (r *Raft) Snapshot(threshold int) (bool, error) {
	// Hold applyMu for the whole operation so no entry is applied between
	// reading lastApplied and capturing the state-machine image: the image
	// and the index it claims to cover stay consistent.
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()
	if r.sm == nil || len(r.log) < threshold || r.lastApplied <= r.logBase {
		r.mu.Unlock()
		return false, nil
	}
	snapIndex := r.lastApplied
	snapTerm := r.termAt(snapIndex)
	r.mu.Unlock()

	// Capture outside r.mu: the store locks itself, and no apply can race us
	// because we hold applyMu.
	data, err := r.sm.Snapshot()
	if err != nil {
		return false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// The configuration as of the boundary is the last config entry at or
	// before snapIndex; entries past it may carry a newer, not-yet-covered
	// config that must not be folded in.
	snapMembers := r.snapMembers
	for _, e := range r.log {
		if e.Index > snapIndex {
			break
		}
		if e.Config != nil {
			snapMembers = e.Config
		}
	}

	// Discard the compacted prefix, keeping entries past snapIndex. The
	// boundary term is remembered so log matching still has it.
	tail := append([]keva.Entry(nil), r.log[snapIndex-r.logBase:]...)
	r.log = tail
	r.logBase = snapIndex
	r.logBaseTerm = snapTerm
	r.snapMembers = snapMembers
	r.snapshot = &Snapshot{
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		Data:              data,
		Members:           snapMembers,
	}
	r.persist()
	return true, nil
}

// InstallSnapshot handles an incoming InstallSnapshot RPC: the leader has
// compacted past entries this follower still lacked, so it sends the image
// directly. The follower adopts it, restores its state machine, and resumes
// its log from the snapshot boundary.
func (r *Raft) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	// Hold applyMu for the whole call so restoring the state machine is
	// serialized against applyCommitted — they must never touch the store at
	// once. This matches the lock order used elsewhere: applyMu before mu.
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()

	// Reject a leader from an older term.
	if args.Term < r.currentTerm {
		reply := InstallSnapshotReply{Term: r.currentTerm}
		r.mu.Unlock()
		return reply
	}
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	} else {
		r.state = Follower
	}
	r.resetElectionDeadline()
	r.leaderID = args.LeaderID

	// A snapshot we already cover adds nothing.
	if args.LastIncludedIndex <= r.logBase {
		reply := InstallSnapshotReply{Term: r.currentTerm}
		r.mu.Unlock()
		return reply
	}

	// If we still hold the boundary entry and it matches, keep the tail past
	// it; otherwise the whole log is superseded and discarded.
	if args.LastIncludedIndex < r.lastLogIndex() &&
		r.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		r.log = append([]keva.Entry(nil), r.log[args.LastIncludedIndex-r.logBase:]...)
	} else {
		r.log = nil
	}
	r.logBase = args.LastIncludedIndex
	r.logBaseTerm = args.LastIncludedTerm
	r.snapMembers = args.Members
	r.snapshot = &Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
		Members:           args.Members,
	}
	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	r.refreshConfig() // adopt the snapshot's configuration
	r.persist()
	sm := r.sm
	r.mu.Unlock()

	if sm != nil {
		_ = sm.Restore(args.Data)
	}

	r.mu.Lock()
	if r.lastApplied < args.LastIncludedIndex {
		r.lastApplied = args.LastIncludedIndex
	}
	reply := InstallSnapshotReply{Term: r.currentTerm}
	r.mu.Unlock()
	return reply
}

// sendSnapshot ships the current snapshot to a peer whose next entry has been
// compacted away, then advances that peer's replication indices on success so
// ordinary AppendEntries can resume from the boundary. Caller must not hold
// r.mu.
func (r *Raft) sendSnapshot(peer string, term uint64, snap *Snapshot) {
	if snap == nil {
		return
	}
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
		Members:           snap.Members,
	}
	reply, err := r.transport.InstallSnapshot(peer, args)
	if err != nil {
		return // unreachable; a later round retries
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}
	if snap.LastIncludedIndex > r.matchIndex[peer] {
		r.matchIndex[peer] = snap.LastIncludedIndex
	}
	if next := snap.LastIncludedIndex + 1; next > r.nextIndex[peer] {
		r.nextIndex[peer] = next
	}
}
