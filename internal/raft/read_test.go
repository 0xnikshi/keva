package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

func TestReadIndexOnLeader(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("v")}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	idx, err := n1.ReadIndex(context.Background())
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx < 1 {
		t.Errorf("read index = %d, want >= 1", idx)
	}
}

func TestReadIndexOnFollowerFails(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")

	if _, err := nodes["n2"].ReadIndex(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower ReadIndex = %v, want ErrNotLeader", err)
	}
}

// TestReadIndexWaitsForCurrentTermCommit is the regression test for the
// stale-read-after-election bug: a freshly elected leader must not serve a
// read until it has committed an entry in its own term, because until then its
// commit index can trail a previous leader's committed writes.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the first election")
	}
	commitPut(t, n1, "a", "1")

	// n2 wins a new election: its no-op is appended but not yet committed.
	n2 := nodes["n2"]
	if !n2.startElection() {
		t.Fatal("n2 should win the second election")
	}

	// A read cannot be served yet — it blocks on the barrier and times out
	// rather than returning a commit index that predates n2's leadership.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := n2.ReadIndex(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ReadIndex before current-term commit = %v, want DeadlineExceeded", err)
	}

	// Once n2 replicates and its no-op commits, the read proceeds.
	n2.replicate()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if _, err := n2.ReadIndex(ctx2); err != nil {
		t.Errorf("ReadIndex after current-term commit: %v", err)
	}
}
