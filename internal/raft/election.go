package raft

// RequestVote handles an incoming RequestVote RPC and decides whether to
// grant the candidate this node's vote.
func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	// A candidate from an older term is stale; reject and report our term.
	if args.Term < r.currentTerm {
		return RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	// A newer term means we are behind: adopt it and step down first.
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	// Grant the vote only if we have not already voted for someone else this
	// term and the candidate's log is at least as up-to-date as ours.
	canVote := r.votedFor == "" || r.votedFor == args.CandidateID
	if canVote && r.candidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateID
		r.persist()
		return RequestVoteReply{Term: r.currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
}

// candidateLogUpToDate implements Raft's election restriction: a candidate
// log is at least as up-to-date as ours if its last entry has a higher
// term, or the same term and an index at least as large. Caller holds r.mu.
func (r *Raft) candidateLogUpToDate(lastIndex, lastTerm uint64) bool {
	myIndex, myTerm := r.lastLogIndex(), r.lastLogTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// becomeCandidate starts a new term with this node campaigning, voting for
// itself. Caller holds r.mu.
func (r *Raft) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.persist()
}

// becomeLeader marks the node as leader and initializes per-peer
// replication tracking. Caller holds r.mu.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.nextIndex = make(map[string]uint64, len(r.peers))
	r.matchIndex = make(map[string]uint64, len(r.peers))
	last := r.lastLogIndex()
	for _, peer := range r.peers {
		r.nextIndex[peer] = last + 1 // optimistically assume peers are caught up
		r.matchIndex[peer] = 0
	}
}

// startElection runs one election round: the node becomes a candidate for a
// new term and requests votes from every peer, returning true if it won a
// majority. Votes are gathered sequentially for clarity; production Raft
// fans them out concurrently.
func (r *Raft) startElection() bool {
	r.mu.Lock()
	r.becomeCandidate()
	term := r.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastLogIndex(),
		LastLogTerm:  r.lastLogTerm(),
	}
	peers := append([]string(nil), r.peers...)
	r.mu.Unlock()

	votes := 1 // a candidate always votes for itself
	majority := (len(peers)+1)/2 + 1
	if votes >= majority { // single-node cluster wins immediately
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.state == Candidate && r.currentTerm == term {
			r.becomeLeader()
			return true
		}
		return false
	}

	for _, peer := range peers {
		reply, err := r.transport.RequestVote(peer, args)
		if err != nil {
			continue // unreachable peer counts as no vote
		}

		r.mu.Lock()
		// Abort if we have moved on to a different term or role meanwhile.
		if r.state != Candidate || r.currentTerm != term {
			r.mu.Unlock()
			return false
		}
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term) // we are stale
			r.mu.Unlock()
			return false
		}
		if reply.VoteGranted {
			votes++
			if votes >= majority {
				r.becomeLeader()
				r.mu.Unlock()
				return true
			}
		}
		r.mu.Unlock()
	}
	return false
}
