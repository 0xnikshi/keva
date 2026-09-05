package raft

import (
	"sync"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// recordingSM records the commands applied to it, in order.
type recordingSM struct {
	mu      sync.Mutex
	applied []keva.Command
}

func (s *recordingSM) Apply(cmd keva.Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, cmd)
}

func (s *recordingSM) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

func TestCommittedEntriesAreApplied(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	sms := make(map[string]*recordingSM)
	for id, n := range nodes {
		sm := &recordingSM{}
		n.SetStateMachine(sm)
		sms[id] = sm
	}

	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}

	if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "a", Value: keva.Value("1")}); err != nil {
		t.Fatalf("Submit a: %v", err)
	}
	if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "b", Value: keva.Value("2")}); err != nil {
		t.Fatalf("Submit b: %v", err)
	}

	// The leader applies committed entries immediately.
	if got := sms["n1"].count(); got != 2 {
		t.Errorf("leader applied %d commands, want 2", got)
	}

	// One more replication round carries the latest commit index to the
	// followers, which then apply the final entry.
	n1.replicate()

	for _, id := range []string{"n2", "n3"} {
		if got := sms[id].count(); got != 2 {
			t.Errorf("%s applied %d commands, want 2", id, got)
		}
	}
}
