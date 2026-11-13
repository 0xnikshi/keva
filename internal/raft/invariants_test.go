package raft

import (
	"fmt"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// checkInvariants asserts Raft's core safety properties across every node. It
// is meant to run after each step of a scenario, so a violation is caught the
// moment it appears rather than inferred from a wrong final answer.
func checkInvariants(t *testing.T, nodes map[string]*Raft) {
	t.Helper()
	for _, check := range []func(map[string]*Raft) error{
		electionSafety, logMatching, committedAgreement,
	} {
		if err := check(nodes); err != nil {
			t.Fatal(err)
		}
	}
}

// electionSafety: at most one leader may exist in a given term.
func electionSafety(nodes map[string]*Raft) error {
	leaders := map[uint64][]string{}
	for id, n := range nodes {
		n.mu.Lock()
		if n.state == Leader {
			leaders[n.currentTerm] = append(leaders[n.currentTerm], id)
		}
		n.mu.Unlock()
	}
	for term, ids := range leaders {
		if len(ids) > 1 {
			return fmt.Errorf("election safety violated: term %d has leaders %v", term, ids)
		}
	}
	return nil
}

// logMatching: any two entries sharing an index and term must be identical.
// Because at most one leader exists per term, a given (index, term) names
// exactly one entry — so a disagreement is a corrupted log.
func logMatching(nodes map[string]*Raft) error {
	byIndex := map[uint64]map[uint64]string{} // index → term → entry signature
	for id, n := range nodes {
		n.mu.Lock()
		base := n.logBase
		for i, e := range n.log {
			idx := base + uint64(i) + 1
			terms := byIndex[idx]
			if terms == nil {
				terms = map[uint64]string{}
				byIndex[idx] = terms
			}
			sig := entrySig(e)
			if prev, ok := terms[e.Term]; ok && prev != sig {
				n.mu.Unlock()
				return fmt.Errorf("log matching violated at index %d term %d: %q vs %q (node %s)",
					idx, e.Term, prev, sig, id)
			}
			terms[e.Term] = sig
		}
		n.mu.Unlock()
	}
	return nil
}

// committedAgreement: a committed entry is final, so no two nodes may hold a
// different term at the same committed index. With log matching, this means
// every node agrees on the committed prefix — the state-machine safety property.
func committedAgreement(nodes map[string]*Raft) error {
	committed := map[uint64]uint64{} // committed index → term
	for id, n := range nodes {
		n.mu.Lock()
		base := n.logBase
		for i, e := range n.log {
			idx := base + uint64(i) + 1
			if idx > n.commitIndex {
				break
			}
			if prev, ok := committed[idx]; ok && prev != e.Term {
				n.mu.Unlock()
				return fmt.Errorf("committed entry at index %d diverges: term %d vs %d (node %s)",
					idx, prev, e.Term, id)
			}
			committed[idx] = e.Term
		}
		n.mu.Unlock()
	}
	return nil
}

// entrySig is a comparable fingerprint of an entry's payload, so two entries
// can be checked for equality regardless of which node they came from.
func entrySig(e keva.Entry) string {
	if e.Config != nil {
		ids := make([]string, len(e.Config))
		for i, m := range e.Config {
			ids[i] = m.ID
		}
		return fmt.Sprintf("cfg:%v", ids)
	}
	return fmt.Sprintf("%d:%s=%s", e.Command.Op, e.Command.Key, e.Command.Value)
}

// TestSafetyWithStaleLeader exercises the subtlest case the invariants must
// tolerate: a partition that leaves one leader stranded in an old term while
// the majority elects a new one. Two leaders exist at once — legally, in
// different terms — and the stale one must never commit anything that
// contradicts the majority.
func TestSafetyWithStaleLeader(t *testing.T) {
	nodes, sms := mesh(t, "n1", "n2", "n3", "n4", "n5")
	n1, n3 := nodes["n1"], nodes["n3"]

	if !n1.startElection() {
		t.Fatal("n1 should win the initial election")
	}
	commitPut(t, n1, "a", "1")
	checkInvariants(t, nodes)

	// Strand n1 (and n2) in the minority; the majority elects a new leader in
	// a higher term.
	tr := n1.transport.(*memTransport)
	tr.partition([]string{"n1", "n2"}, []string{"n3", "n4", "n5"})

	if !n3.startElection() {
		t.Fatal("n3 should win on the majority side")
	}
	commitPut(t, n3, "b", "2")
	checkInvariants(t, nodes) // n1 is now a stale leader — still safe

	// The stale leader accepts a write but cannot commit it: its append reaches
	// only the minority, never a majority.
	if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "c", Value: keva.Value("3")}); err != nil {
		t.Fatalf("submit c: %v", err)
	}
	n1.replicate()
	checkInvariants(t, nodes)
	for id, sm := range sms {
		if _, ok := sm.data["c"]; ok {
			t.Errorf("node %s applied the uncommitted write c", id)
		}
	}

	// Heal: the real leader reconciles the minority, rolling back c.
	tr.heal()
	for i := 0; i < 3; i++ {
		n3.replicate()
	}
	checkInvariants(t, nodes)

	for id, sm := range sms {
		if got, ok := sm.data["a"]; !ok || string(got) != "1" {
			t.Errorf("node %s: a=%q (present=%v), want 1", id, got, ok)
		}
		if got, ok := sm.data["b"]; !ok || string(got) != "2" {
			t.Errorf("node %s: b=%q (present=%v), want 2", id, got, ok)
		}
		if _, ok := sm.data["c"]; ok {
			t.Errorf("node %s still holds the rolled-back write c", id)
		}
	}
}

// TestInvariantCheckerHasTeeth confirms the checker actually fails on corrupt
// state — a checker that never fires proves nothing. We fabricate each
// violation by hand and assert the matching check catches it.
func TestInvariantCheckerHasTeeth(t *testing.T) {
	// Two leaders in the same term.
	nodes, _ := mesh(t, "n1", "n2", "n3")
	if !nodes["n1"].startElection() {
		t.Fatal("n1 should win")
	}
	term := nodes["n1"].Term()
	n2 := nodes["n2"]
	n2.mu.Lock()
	n2.state = Leader
	n2.currentTerm = term
	n2.mu.Unlock()
	if electionSafety(nodes) == nil {
		t.Error("electionSafety missed two leaders in one term")
	}

	// Same (index, term), different payloads.
	a := map[string]*Raft{"a": New(Config{ID: "a"}, nil), "b": New(Config{ID: "b"}, nil)}
	a["a"].log = []keva.Entry{{Term: 1, Index: 1, Command: keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("x")}}}
	a["b"].log = []keva.Entry{{Term: 1, Index: 1, Command: keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("y")}}}
	if logMatching(a) == nil {
		t.Error("logMatching missed conflicting entries at the same index and term")
	}

	// A committed index holding different terms on two nodes.
	c := map[string]*Raft{"a": New(Config{ID: "a"}, nil), "b": New(Config{ID: "b"}, nil)}
	c["a"].log = []keva.Entry{{Term: 1, Index: 1}}
	c["a"].commitIndex = 1
	c["b"].log = []keva.Entry{{Term: 2, Index: 1}}
	c["b"].commitIndex = 1
	if committedAgreement(c) == nil {
		t.Error("committedAgreement missed a diverging committed entry")
	}
}
