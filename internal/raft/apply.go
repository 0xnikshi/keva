package raft

import "github.com/0xnikshi/keva/internal/keva"

// StateMachine applies committed commands to produce the replicated state.
type StateMachine interface {
	Apply(cmd keva.Command)
}

// SetStateMachine attaches the state machine that committed entries are
// applied to. It is set once, before the node starts serving.
func (r *Raft) SetStateMachine(sm StateMachine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sm = sm
}

// applyCommitted applies every newly committed entry to the state machine,
// in log order. applyMu serializes applies so entries are never applied out
// of order or twice, even when called from concurrent RPC handlers.
func (r *Raft) applyCommitted() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	r.mu.Lock()
	if r.sm == nil || r.commitIndex <= r.lastApplied {
		r.mu.Unlock()
		return
	}
	from, to := r.lastApplied, r.commitIndex
	entries := append([]keva.Entry(nil), r.log[from:to]...)
	r.mu.Unlock()

	for _, e := range entries {
		r.sm.Apply(e.Command)
	}

	r.mu.Lock()
	if to > r.lastApplied {
		r.lastApplied = to
	}
	r.mu.Unlock()
}
