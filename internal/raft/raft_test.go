package raft

import (
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

func TestNewStartsAsFollowerAtTermZero(t *testing.T) {
	r := New(Config{ID: "n1", Peers: []string{"n2", "n3"}})

	if got := r.State(); got != Follower {
		t.Errorf("state = %s, want follower", got)
	}
	if got := r.Term(); got != 0 {
		t.Errorf("term = %d, want 0", got)
	}
}

func TestLastLogIndexAndTerm(t *testing.T) {
	r := New(Config{ID: "n1"})

	if idx, term := r.lastLogIndex(), r.lastLogTerm(); idx != 0 || term != 0 {
		t.Errorf("empty log: index=%d term=%d, want 0, 0", idx, term)
	}

	r.log = []keva.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 3, Index: 3},
	}
	if got := r.lastLogIndex(); got != 3 {
		t.Errorf("lastLogIndex = %d, want 3", got)
	}
	if got := r.lastLogTerm(); got != 3 {
		t.Errorf("lastLogTerm = %d, want 3", got)
	}
}

func TestBecomeFollowerResetsVote(t *testing.T) {
	r := New(Config{ID: "n1"})
	r.state = Candidate
	r.currentTerm = 2
	r.votedFor = "n1"

	r.becomeFollower(5)

	if r.state != Follower {
		t.Errorf("state = %s, want follower", r.state)
	}
	if r.currentTerm != 5 {
		t.Errorf("term = %d, want 5", r.currentTerm)
	}
	if r.votedFor != "" {
		t.Errorf("votedFor = %q, want empty", r.votedFor)
	}
}
