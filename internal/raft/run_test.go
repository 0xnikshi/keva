package raft

import (
	"testing"
	"time"
)

func waitForLeader(t *testing.T, nodes map[string]*Raft, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, n := range nodes {
			if n.State() == Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func stopAll(nodes map[string]*Raft) {
	for _, n := range nodes {
		n.Stop()
	}
}

func TestClusterElectsSingleLeader(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	for _, n := range nodes {
		n.Start()
	}
	defer stopAll(nodes)

	if leader := waitForLeader(t, nodes, 2*time.Second); leader == "" {
		t.Fatal("no single leader elected within the timeout")
	}
}

func TestLeaderFailover(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	for _, n := range nodes {
		n.Start()
	}
	defer stopAll(nodes)

	first := waitForLeader(t, nodes, 2*time.Second)
	if first == "" {
		t.Fatal("no initial leader")
	}

	// "Crash" the leader: stop its loop and make it unreachable.
	nodes[first].Stop()
	nodes[first].transport.(*memTransport).setDown(first, true)

	survivors := map[string]*Raft{}
	for id, n := range nodes {
		if id != first {
			survivors[id] = n
		}
	}
	second := waitForLeader(t, survivors, 2*time.Second)
	if second == "" {
		t.Fatal("no new leader elected after failover")
	}
	if second == first {
		t.Fatalf("stopped leader %q should not lead again", first)
	}
}
