package raft

// ReadIndex confirms this node is still the leader by exchanging heartbeats
// with a majority in the current term, then returns the commit index at the
// time of the call. Once the state machine has applied up to that index, a
// read reflects every write committed before the call — a linearizable read.
// It returns ErrNotLeader if this node is not, or is no longer, the leader.
func (r *Raft) ReadIndex() (uint64, error) {
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
	return readIdx, nil
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
	if prevIndex > 0 && prevIndex <= r.lastLogIndex() {
		prevTerm = r.log[prevIndex-1].Term
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
