package raft

import (
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

func TestAppendEntriesRejectsStaleLeader(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)
	r.currentTerm = 5

	reply := r.AppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "n2"})
	if reply.Success {
		t.Error("should reject a stale leader")
	}
	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
}

func TestAppendEntriesAppendsAndCommits(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)

	reply := r.AppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []keva.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
		LeaderCommit: 2,
	})
	if !reply.Success {
		t.Fatal("should accept entries with a matching prev")
	}
	if r.lastLogIndex() != 2 {
		t.Errorf("lastLogIndex = %d, want 2", r.lastLogIndex())
	}
	if r.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2", r.commitIndex)
	}
}

func TestAppendEntriesRejectsLogMismatch(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)
	r.currentTerm = 1
	r.log = []keva.Entry{{Term: 1, Index: 1}}

	// PrevLogIndex 2 does not exist in our log.
	reply := r.AppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "n2", PrevLogIndex: 2, PrevLogTerm: 1})
	if reply.Success {
		t.Error("should reject when the preceding entry is missing")
	}
}

func TestLeaderReplicatesAndCommits(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	n1 := nodes["n1"]

	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}

	idx, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("v")})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if idx != 1 {
		t.Errorf("submitted index = %d, want 1", idx)
	}

	n1.mu.Lock()
	leaderCommit := n1.commitIndex
	n1.mu.Unlock()
	if leaderCommit != 1 {
		t.Errorf("leader commitIndex = %d, want 1", leaderCommit)
	}

	for _, id := range []string{"n2", "n3"} {
		n := nodes[id]
		n.mu.Lock()
		last := n.lastLogIndex()
		n.mu.Unlock()
		if last != 1 {
			t.Errorf("%s lastLogIndex = %d, want 1 (entry replicated)", id, last)
		}
	}
}
