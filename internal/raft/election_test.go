package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

// memTransport routes RPCs to peers held in memory — no network, so
// elections are deterministic. A downed peer returns an unreachable error,
// modeling a crashed node; groups model a network partition, where only nodes
// in the same group can exchange RPCs (see partition_test.go).
type memTransport struct {
	mu     sync.Mutex
	nodes  map[string]*Raft
	down   map[string]bool
	groups map[string]int // node ID → partition group; empty means one network
}

func (t *memTransport) setDown(id string, d bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.down[id] = d
}

// target resolves the receiver of an RPC sent by from, honoring both downed
// nodes and network partitions.
func (t *memTransport) target(from, peer string) (*Raft, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.down[peer] {
		return nil, false
	}
	if len(t.groups) > 0 && t.groups[from] != t.groups[peer] {
		return nil, false // partitioned: sender and receiver are on opposite sides
	}
	n, ok := t.nodes[peer]
	return n, ok
}

func (t *memTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, ok := t.target(args.CandidateID, peer)
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("peer %q unreachable", peer)
	}
	return n.RequestVote(args), nil
}

func (t *memTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n, ok := t.target(args.LeaderID, peer)
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("peer %q unreachable", peer)
	}
	return n.AppendEntries(args), nil
}

func (t *memTransport) InstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n, ok := t.target(args.LeaderID, peer)
	if !ok {
		return InstallSnapshotReply{}, fmt.Errorf("peer %q unreachable", peer)
	}
	return n.InstallSnapshot(args), nil
}

func (t *memTransport) TimeoutNow(peer string, args TimeoutNowArgs) error {
	n, ok := t.target(args.LeaderID, peer)
	if !ok {
		return fmt.Errorf("peer %q unreachable", peer)
	}
	n.TimeoutNow(args)
	return nil
}

// SetPeers is a no-op: the in-memory transport already holds every node by ID.
func (t *memTransport) SetPeers(map[string]string) {}

func newCluster(ids ...string) map[string]*Raft {
	tr := &memTransport{nodes: map[string]*Raft{}, down: map[string]bool{}}
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		tr.nodes[id] = New(Config{
			ID:                id,
			Peers:             peers,
			ElectionTimeout:   60 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		}, tr)
	}
	return tr.nodes
}

func TestElectionWinsWithMajority(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")

	if won := nodes["n1"].startElection(); !won {
		t.Fatal("expected n1 to win the election")
	}
	if got := nodes["n1"].State(); got != Leader {
		t.Errorf("n1 state = %s, want leader", got)
	}
	if got := nodes["n1"].Term(); got != 1 {
		t.Errorf("term = %d, want 1", got)
	}
}

func TestRequestVoteGrantsToUpToDateCandidate(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)

	reply := r.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !reply.VoteGranted {
		t.Error("expected vote granted")
	}
	if r.votedFor != "n2" {
		t.Errorf("votedFor = %q, want n2", r.votedFor)
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)
	r.currentTerm = 5

	reply := r.RequestVote(RequestVoteArgs{Term: 3, CandidateID: "n2"})
	if reply.VoteGranted {
		t.Error("expected vote denied for stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
}

func TestRequestVoteRejectsSecondCandidateSameTerm(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)

	if reply := r.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"}); !reply.VoteGranted {
		t.Fatal("first vote should be granted")
	}
	if reply := r.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n3"}); reply.VoteGranted {
		t.Error("should not vote for a second candidate in the same term")
	}
}

func TestRequestVoteRejectsBehindLog(t *testing.T) {
	r := New(Config{ID: "n1"}, nil)
	r.currentTerm = 2
	r.log = []keva.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}

	// Candidate's last log term (1) is behind ours (2), despite a higher index.
	reply := r.RequestVote(RequestVoteArgs{Term: 3, CandidateID: "n2", LastLogIndex: 9, LastLogTerm: 1})
	if reply.VoteGranted {
		t.Error("should reject a candidate whose log is less up-to-date")
	}
}
