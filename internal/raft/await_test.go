package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

// TestAwaitCommitReturnsWhenCommitted: on a healthy cluster the entry commits
// during Submit, so awaiting it returns without blocking.
func TestAwaitCommitReturnsWhenCommitted(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	idx, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "a", Value: keva.Value("1")})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n1.AwaitCommit(ctx, idx); err != nil {
		t.Errorf("AwaitCommit of a committed entry: %v", err)
	}
}

// TestAwaitCommitTimesOut: an isolated leader cannot reach a majority, so its
// write never commits and the wait ends at the deadline.
func TestAwaitCommitTimesOut(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	n1.transport.(*memTransport).partition([]string{"n1"}, []string{"n2", "n3"})

	idx, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "b", Value: keva.Value("2")})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := n1.AwaitCommit(ctx, idx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("AwaitCommit err = %v, want context.DeadlineExceeded", err)
	}
}

// TestAwaitCommitFailsOnLeadershipLoss: if the node steps down while a write is
// waiting, the wait returns ErrNotLeader — the entry's fate is now unknown.
func TestAwaitCommitFailsOnLeadershipLoss(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	n1.transport.(*memTransport).partition([]string{"n1"}, []string{"n2", "n3"})

	idx, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "c", Value: keva.Value("3")})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- n1.AwaitCommit(context.Background(), idx) }()

	// A new leader in a higher term reaches n1, forcing it to step down.
	n1.AppendEntries(AppendEntriesArgs{Term: n1.Term() + 5, LeaderID: "n2"})

	select {
	case err := <-done:
		if !errors.Is(err, ErrNotLeader) {
			t.Errorf("AwaitCommit err = %v, want ErrNotLeader", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitCommit did not return after leadership was lost")
	}
}
