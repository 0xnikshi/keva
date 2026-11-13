package raft

import (
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// partition splits the cluster into isolated groups: a node can exchange RPCs
// only with nodes in its own group, modeling a network partition.
func (t *memTransport) partition(groups ...[]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.groups = map[string]int{}
	for i, g := range groups {
		for _, id := range g {
			t.groups[id] = i + 1
		}
	}
}

// heal removes every partition, restoring a single network.
func (t *memTransport) heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.groups = nil
}

// mesh builds a full-mesh cluster of the given IDs over one in-memory
// transport, each node backed by a recording state machine.
func mesh(t *testing.T, ids ...string) (map[string]*Raft, map[string]*snapSM) {
	t.Helper()
	initial := make(map[string][]string, len(ids))
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		initial[id] = peers
	}
	return newMembers(t, initial)
}

// commitPut submits a write on the leader and runs one replication round.
func commitPut(t *testing.T, leader *Raft, k, v string) {
	t.Helper()
	if _, err := leader.Submit(keva.Command{Op: keva.OpPut, Key: keva.Key(k), Value: keva.Value(v)}); err != nil {
		t.Fatalf("submit %s: %v", k, err)
	}
	leader.replicate()
}

// TestMinorityPartitionCannotCommit shows the central safety guarantee under a
// partition: the majority side keeps committing, while the isolated minority
// can neither elect a leader nor observe the new writes.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	nodes, sms := mesh(t, "n1", "n2", "n3", "n4", "n5")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the initial election")
	}
	commitPut(t, n1, "a", "1")

	tr := n1.transport.(*memTransport)
	tr.partition([]string{"n1", "n2", "n3"}, []string{"n4", "n5"})

	// The majority side still has three of five nodes, so it commits.
	commitPut(t, n1, "b", "2")
	if _, ok := sms["n1"].data["b"]; !ok {
		t.Error("majority leader did not apply its own committed write")
	}

	// The minority has only two of five reachable, so no election can succeed.
	if nodes["n4"].startElection() {
		t.Error("a minority node won an election it cannot have a majority for")
	}
	// And it never saw the write the majority committed.
	if _, ok := sms["n4"].data["b"]; ok {
		t.Error("a minority node observed a write it was partitioned away from")
	}
}

// TestClusterConvergesAfterHeal shows there is no lasting divergence: once the
// partition heals, the isolated nodes catch up to exactly the majority's
// committed state.
func TestClusterConvergesAfterHeal(t *testing.T) {
	nodes, sms := mesh(t, "n1", "n2", "n3", "n4", "n5")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the initial election")
	}
	commitPut(t, n1, "a", "1")

	tr := n1.transport.(*memTransport)
	tr.partition([]string{"n1", "n2", "n3"}, []string{"n4", "n5"})

	// The majority commits more writes while the minority is cut off.
	commitPut(t, n1, "b", "2")
	commitPut(t, n1, "c", "3")

	// Heal, and let the leader bring the reconnected nodes current.
	tr.heal()
	for i := 0; i < 3; i++ {
		n1.replicate()
	}

	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for id, sm := range sms {
		for k, v := range want {
			if got, ok := sm.data[keva.Key(k)]; !ok || string(got) != v {
				t.Errorf("node %s: %s=%q (present=%v), want %q", id, k, got, ok, v)
			}
		}
	}
}
