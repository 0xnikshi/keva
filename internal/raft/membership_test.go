package raft

import (
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

// newMembers builds a cluster whose nodes start with the given initial peer
// sets, all sharing one in-memory transport, each with a snapSM state machine.
// Nodes not listed as anyone's peer (but present here) can be added later.
func newMembers(t testing.TB, initial map[string][]string) (map[string]*Raft, map[string]*snapSM) {
	t.Helper()
	tr := &memTransport{nodes: map[string]*Raft{}, down: map[string]bool{}}
	nodes := make(map[string]*Raft, len(initial))
	sms := make(map[string]*snapSM, len(initial))
	for id, peers := range initial {
		r := New(Config{
			ID:                id,
			Peers:             peers,
			ElectionTimeout:   60 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		}, tr)
		sm := newSnapSM()
		r.SetStateMachine(sm)
		tr.nodes[id] = r
		nodes[id] = r
		sms[id] = sm
	}
	return nodes, sms
}

func TestAddNodeJoinsRunningCluster(t *testing.T) {
	// n1, n2 form the cluster; n3 exists on the transport but is not a member.
	nodes, sms := newMembers(t, map[string][]string{
		"n1": {"n2"},
		"n2": {"n1"},
		"n3": nil,
	})
	n1, n3 := nodes["n1"], nodes["n3"]

	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	for _, k := range []string{"a", "b"} {
		if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: keva.Key(k), Value: keva.Value(k)}); err != nil {
			t.Fatalf("submit %s: %v", k, err)
		}
	}

	if _, err := n1.AddNode(keva.Member{ID: "n3"}); err != nil {
		t.Fatalf("add node: %v", err)
	}
	// The leader adopts the new config at once; a couple of rounds let the
	// newcomer catch up from its empty log.
	n1.replicate()
	n1.replicate()

	if got := len(n1.Members()); got != 3 {
		t.Errorf("leader membership = %d, want 3", got)
	}
	if got := len(n3.Members()); got != 3 {
		t.Errorf("new node membership = %d, want 3", got)
	}
	for _, k := range []string{"a", "b"} {
		if got, ok := sms["n3"].data[keva.Key(k)]; !ok || string(got) != k {
			t.Errorf("n3 %s=%q (present=%v), want %q", k, got, ok, k)
		}
	}
}

func TestRemoveNodeShrinksCluster(t *testing.T) {
	nodes, _ := newMembers(t, map[string][]string{
		"n1": {"n2", "n3"},
		"n2": {"n1", "n3"},
		"n3": {"n1", "n2"},
	})
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}

	if _, err := n1.RemoveNode("n3"); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	n1.replicate()

	if got := len(n1.Members()); got != 2 {
		t.Errorf("membership after removal = %d, want 2", got)
	}
	for _, m := range n1.Members() {
		if m.ID == "n3" {
			t.Error("n3 still present after removal")
		}
	}
}

func TestOneConfigChangeAtATime(t *testing.T) {
	nodes, _ := newMembers(t, map[string][]string{
		"n1": {"n2"},
		"n2": {"n1"},
		"n3": nil,
		"n4": nil,
	})
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}

	// Drop every other node so the first change cannot reach a majority and
	// stays pending.
	tr := n1.transport.(*memTransport)
	tr.setDown("n2", true)
	tr.setDown("n3", true)

	if _, err := n1.AddNode(keva.Member{ID: "n3"}); err != nil {
		t.Fatalf("first add: %v", err)
	}
	// With the first change uncommitted, a second must be refused.
	if _, err := n1.AddNode(keva.Member{ID: "n4"}); err != ErrConfigChangeInProgress {
		t.Errorf("second add err = %v, want ErrConfigChangeInProgress", err)
	}
}
