package raft

import (
	"errors"
	"testing"

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

	idx, err := n1.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx < 1 {
		t.Errorf("read index = %d, want >= 1", idx)
	}
}

func TestReadIndexOnFollowerFails(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")

	if _, err := nodes["n2"].ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower ReadIndex = %v, want ErrNotLeader", err)
	}
}
