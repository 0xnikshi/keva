package rafthttp

import (
	"net/http/httptest"
	"testing"

	"github.com/0xnikshi/keva/internal/raft"
)

// A RequestVote travels over real HTTP to a node's handler and back.
func TestRequestVoteOverHTTP(t *testing.T) {
	node := raft.New(raft.Config{ID: "n2"}, nil)
	ts := httptest.NewServer(Handler(node))
	defer ts.Close()

	tr := New(map[string]string{"n2": ts.URL})
	reply, err := tr.RequestVote("n2", raft.RequestVoteArgs{Term: 1, CandidateID: "n1"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Error("expected the vote to be granted over HTTP")
	}
}

func TestAppendEntriesOverHTTP(t *testing.T) {
	node := raft.New(raft.Config{ID: "n2"}, nil)
	ts := httptest.NewServer(Handler(node))
	defer ts.Close()

	tr := New(map[string]string{"n2": ts.URL})
	reply, err := tr.AppendEntries("n2", raft.AppendEntriesArgs{Term: 1, LeaderID: "n1"})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Error("expected the heartbeat to be accepted over HTTP")
	}
}

func TestUnknownPeerErrors(t *testing.T) {
	tr := New(map[string]string{})
	if _, err := tr.RequestVote("ghost", raft.RequestVoteArgs{}); err == nil {
		t.Error("expected an error for an unknown peer")
	}
}
