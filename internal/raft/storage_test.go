package raft

import (
	"path/filepath"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

func TestFileStorageRoundTrip(t *testing.T) {
	st := NewFileStorage(filepath.Join(t.TempDir(), "raft.state"))

	// A missing file loads as the zero state.
	if ps, err := st.Load(); err != nil || ps.CurrentTerm != 0 || len(ps.Log) != 0 {
		t.Fatalf("empty load = %+v, err %v", ps, err)
	}

	want := PersistentState{
		CurrentTerm: 4,
		VotedFor:    "n2",
		Log: []keva.Entry{
			{Term: 4, Index: 1, Command: keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("v")}},
		},
	}
	if err := st.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := st.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.CurrentTerm != 4 || got.VotedFor != "n2" || len(got.Log) != 1 {
		t.Fatalf("Load = %+v, want term 4 / n2 / 1 entry", got)
	}
	if got.Log[0].Command.Key != "k" {
		t.Errorf("recovered command key = %q, want k", got.Log[0].Command.Key)
	}
}

func TestNodeRecoversTermAndVote(t *testing.T) {
	st := NewFileStorage(filepath.Join(t.TempDir(), "raft.state"))

	r := New(Config{ID: "n1"}, nil)
	if err := r.SetStorage(st); err != nil {
		t.Fatalf("SetStorage: %v", err)
	}
	// Granting a vote in term 3 persists the term and the vote.
	if reply := r.RequestVote(RequestVoteArgs{Term: 3, CandidateID: "n2"}); !reply.VoteGranted {
		t.Fatal("vote should be granted")
	}

	// "Restart": a fresh node loading the same storage.
	r2 := New(Config{ID: "n1"}, nil)
	if err := r2.SetStorage(st); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if r2.Term() != 3 {
		t.Errorf("recovered term = %d, want 3", r2.Term())
	}
	r2.mu.Lock()
	vf := r2.votedFor
	r2.mu.Unlock()
	if vf != "n2" {
		t.Errorf("recovered votedFor = %q, want n2", vf)
	}
}

func TestNodeRecoversLog(t *testing.T) {
	st := NewFileStorage(filepath.Join(t.TempDir(), "raft.state"))

	r := New(Config{ID: "n1"}, nil)
	if err := r.SetStorage(st); err != nil {
		t.Fatalf("SetStorage: %v", err)
	}
	r.AppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n2",
		Entries: []keva.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
	})

	r2 := New(Config{ID: "n1"}, nil)
	if err := r2.SetStorage(st); err != nil {
		t.Fatalf("reload: %v", err)
	}
	r2.mu.Lock()
	n := r2.lastLogIndex()
	r2.mu.Unlock()
	if n != 2 {
		t.Errorf("recovered log length = %d, want 2", n)
	}
}
