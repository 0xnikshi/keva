package raft

import (
	"errors"

	"github.com/0xnikshi/keva/internal/keva"
)

// Errors returned by membership changes.
var (
	ErrConfigChangeInProgress = errors.New("raft: a configuration change is already in progress")
	ErrAlreadyMember          = errors.New("raft: node is already a member")
	ErrNotMember              = errors.New("raft: node is not a member")
)

// AddNode proposes adding a member to the cluster and returns the log index of
// the change. Only the leader may propose, and only one change may be in
// flight at a time: changing membership by a single node keeps the old and new
// majorities overlapping, which is what makes the change safe without joint
// consensus.
func (r *Raft) AddNode(m keva.Member) (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	if r.configPending() {
		r.mu.Unlock()
		return 0, ErrConfigChangeInProgress
	}
	for _, existing := range r.members {
		if existing.ID == m.ID {
			r.mu.Unlock()
			return 0, ErrAlreadyMember
		}
	}
	next := append(append([]keva.Member(nil), r.members...), m)
	index := r.appendConfig(next)
	r.mu.Unlock()

	r.replicate()
	return index, nil
}

// RemoveNode proposes removing a member, subject to the same leader-only,
// one-change-at-a-time rules as AddNode.
func (r *Raft) RemoveNode(id string) (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	if r.configPending() {
		r.mu.Unlock()
		return 0, ErrConfigChangeInProgress
	}
	next := make([]keva.Member, 0, len(r.members))
	found := false
	for _, existing := range r.members {
		if existing.ID == id {
			found = true
			continue
		}
		next = append(next, existing)
	}
	if !found {
		r.mu.Unlock()
		return 0, ErrNotMember
	}
	index := r.appendConfig(next)
	r.mu.Unlock()

	r.replicate()
	return index, nil
}

// Members returns a copy of the current cluster membership.
func (r *Raft) Members() []keva.Member {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]keva.Member(nil), r.members...)
}

// appendConfig appends a configuration entry and makes it effective at once:
// Raft adopts a configuration the moment it enters the log, not when it
// commits. Caller holds r.mu and has verified leadership.
func (r *Raft) appendConfig(members []keva.Member) uint64 {
	index := r.lastLogIndex() + 1
	r.log = append(r.log, keva.Entry{Term: r.currentTerm, Index: index, Config: members})
	r.applyConfig(members)
	r.persist()
	return index
}

// applyConfig adopts members as the active configuration: it updates the
// derived peer set, teaches the transport any new peer addresses, and (on a
// leader) begins tracking replication to peers it has not seen before. Caller
// holds r.mu.
func (r *Raft) applyConfig(members []keva.Member) {
	r.members = members
	r.recomputePeers()

	if r.transport != nil {
		urls := make(map[string]string, len(members))
		for _, m := range members {
			if m.Addr != "" {
				urls[m.ID] = m.Addr
			}
		}
		r.transport.SetPeers(urls)
	}

	if r.state == Leader {
		last := r.lastLogIndex()
		if r.nextIndex == nil {
			r.nextIndex = make(map[string]uint64, len(r.peers))
			r.matchIndex = make(map[string]uint64, len(r.peers))
		}
		for _, peer := range r.peers {
			if _, ok := r.nextIndex[peer]; !ok {
				r.nextIndex[peer] = last + 1
				r.matchIndex[peer] = 0
			}
		}
	}
}

// recomputePeers derives the peer set (every member but this node) from the
// current membership. Caller holds r.mu.
func (r *Raft) recomputePeers() {
	peers := make([]string, 0, len(r.members))
	for _, m := range r.members {
		if m.ID != r.id {
			peers = append(peers, m.ID)
		}
	}
	r.peers = peers
}

// configPending reports whether an uncommitted configuration entry sits in the
// log — the condition that blocks starting another change. Caller holds r.mu.
func (r *Raft) configPending() bool {
	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].Index <= r.commitIndex {
			break
		}
		if r.log[i].Config != nil {
			return true
		}
	}
	return false
}

// refreshConfig re-adopts the configuration implied by the current log: the
// most recent config entry, or the snapshot-boundary configuration when the
// log holds none. It is called after the log changes so truncation or new
// entries are reflected. Caller holds r.mu.
func (r *Raft) refreshConfig() {
	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].Config != nil {
			r.applyConfig(r.log[i].Config)
			return
		}
	}
	r.applyConfig(r.snapMembers)
}
