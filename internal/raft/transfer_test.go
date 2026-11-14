package raft

import "testing"

// TestTimeoutNowTriggersElection: a follower told to time out now starts and
// wins an election on its next tick, without waiting out its election timeout.
func TestTimeoutNowTriggersElection(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1, n2 := nodes["n1"], nodes["n2"]
	if !n1.startElection() {
		t.Fatal("n1 should win the first election")
	}
	n1.replicate() // make sure n2's log is current, so it can win
	oldTerm := n1.Term()

	n2.TimeoutNow(TimeoutNowArgs{Term: oldTerm, LeaderID: "n1"})
	n2.tick() // the armed timer fires on this tick

	if got := n2.State(); got != Leader {
		t.Errorf("n2 state = %s, want leader after TimeoutNow + tick", got)
	}
	if n2.Term() <= oldTerm {
		t.Errorf("n2 term = %d, want greater than the old leader's %d", n2.Term(), oldTerm)
	}
}

// TestTimeoutNowIgnoredFromStaleTerm: a TimeoutNow carrying an older term is
// ignored, so a deposed leader cannot provoke a needless election.
func TestTimeoutNowIgnoredFromStaleTerm(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n2 := nodes["n2"]
	n2.mu.Lock()
	n2.currentTerm = 5
	n2.resetElectionDeadline() // a fresh deadline, so only TimeoutNow could arm it
	n2.mu.Unlock()

	n2.TimeoutNow(TimeoutNowArgs{Term: 1, LeaderID: "n1"})
	n2.tick()

	if got := n2.State(); got == Leader {
		t.Error("n2 should not have started an election from a stale TimeoutNow")
	}
}
