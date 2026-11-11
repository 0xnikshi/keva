package raft

import (
	"path/filepath"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// snapSM is a minimal state machine that records applied commands as key→value
// and serializes them, so snapshot and restore can be checked end to end.
type snapSM struct {
	data map[keva.Key]keva.Value
}

func newSnapSM() *snapSM { return &snapSM{data: make(map[keva.Key]keva.Value)} }

func (s *snapSM) Apply(cmd keva.Command) {
	switch cmd.Op {
	case keva.OpPut:
		s.data[cmd.Key] = cmd.Value
	case keva.OpDelete:
		delete(s.data, cmd.Key)
	}
}

func (s *snapSM) Snapshot() ([]byte, error) {
	var buf []byte
	for k, v := range s.data {
		buf = append(buf, byte(len(k)))
		buf = append(buf, k...)
		buf = append(buf, byte(len(v)))
		buf = append(buf, v...)
	}
	return buf, nil
}

func (s *snapSM) Restore(data []byte) error {
	s.data = make(map[keva.Key]keva.Value)
	for i := 0; i < len(data); {
		kl := int(data[i])
		i++
		k := keva.Key(data[i : i+kl])
		i += kl
		vl := int(data[i])
		i++
		v := keva.Value(data[i : i+vl])
		i += vl
		s.data[k] = v
	}
	return nil
}

// TestSnapshotCompactsLog drives a single node to commit several entries,
// snapshots, and checks the log prefix is discarded while the index math and
// later appends still line up.
func TestSnapshotCompactsLog(t *testing.T) {
	nodes := newCluster("n1")
	n1 := nodes["n1"]
	n1.SetStateMachine(newSnapSM())
	if !n1.startElection() {
		t.Fatal("single node should win its election")
	}

	for _, k := range []string{"a", "b", "c"} {
		if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: keva.Key(k), Value: keva.Value(k)}); err != nil {
			t.Fatalf("submit %s: %v", k, err)
		}
	}

	beforeLast := n1.lastLogIndex()
	ok, err := n1.Snapshot(1)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if !ok {
		t.Fatal("snapshot should have been taken")
	}

	n1.mu.Lock()
	logBase, logLen, last := n1.logBase, len(n1.log), n1.lastLogIndex()
	n1.mu.Unlock()

	if logBase == 0 {
		t.Error("logBase should have advanced past 0")
	}
	if last != beforeLast {
		t.Errorf("lastLogIndex changed across snapshot: got %d, want %d", last, beforeLast)
	}
	if uint64(logLen) >= beforeLast {
		t.Errorf("log was not compacted: still holds %d entries", logLen)
	}

	// A write after the snapshot lands at the next index and commits.
	idx, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: "d", Value: keva.Value("d")})
	if err != nil {
		t.Fatalf("submit after snapshot: %v", err)
	}
	if idx != last+1 {
		t.Errorf("post-snapshot write got index %d, want %d", idx, last+1)
	}
}

// TestSnapshotRecovery snapshots, then rebuilds a fresh node from the same
// storage file and checks the state machine is restored from the snapshot.
func TestSnapshotRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.state")

	first := New(Config{ID: "n1"}, nil)
	sm1 := newSnapSM()
	first.SetStateMachine(sm1)
	if err := first.SetStorage(NewFileStorage(path)); err != nil {
		t.Fatalf("set storage: %v", err)
	}
	if !first.startElection() {
		t.Fatal("single node should win its election")
	}
	for _, k := range []string{"x", "y", "z"} {
		if _, err := first.Submit(keva.Command{Op: keva.OpPut, Key: keva.Key(k), Value: keva.Value(k)}); err != nil {
			t.Fatalf("submit %s: %v", k, err)
		}
	}
	if _, err := first.Snapshot(1); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Rebuild from the same file: the state machine is restored from the
	// snapshot during SetStorage, with no log replay.
	second := New(Config{ID: "n1"}, nil)
	sm2 := newSnapSM()
	second.SetStateMachine(sm2)
	if err := second.SetStorage(NewFileStorage(path)); err != nil {
		t.Fatalf("reload storage: %v", err)
	}

	for _, k := range []string{"x", "y", "z"} {
		if got, ok := sm2.data[keva.Key(k)]; !ok || string(got) != k {
			t.Errorf("restored %s = %q (present=%v), want %q", k, got, ok, k)
		}
	}

	second.mu.Lock()
	logBase, lastApplied := second.logBase, second.lastApplied
	second.mu.Unlock()
	if lastApplied != logBase {
		t.Errorf("lastApplied=%d, want logBase=%d after snapshot recovery", lastApplied, logBase)
	}
}

// TestInstallSnapshotCatchesUpFollower keeps a follower offline while the
// leader commits and then compacts past the entries it missed, so catching it
// up can only go through InstallSnapshot, not ordinary AppendEntries.
func TestInstallSnapshotCatchesUpFollower(t *testing.T) {
	nodes := newCluster("n1", "n2", "n3")
	sms := make(map[string]*snapSM, len(nodes))
	for id, n := range nodes {
		sm := newSnapSM()
		n.SetStateMachine(sm)
		sms[id] = sm
	}

	tr := nodes["n1"].transport.(*memTransport)
	tr.setDown("n3", true) // n3 misses everything that follows

	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := n1.Submit(keva.Command{Op: keva.OpPut, Key: keva.Key(k), Value: keva.Value(k)}); err != nil {
			t.Fatalf("submit %s: %v", k, err)
		}
	}

	// Compact so the entries n3 is missing no longer exist as log entries.
	if ok, err := n1.Snapshot(1); err != nil || !ok {
		t.Fatalf("snapshot: ok=%v err=%v", ok, err)
	}
	n1.mu.Lock()
	base := n1.logBase
	n1.mu.Unlock()
	if base == 0 {
		t.Fatal("leader log was not compacted")
	}

	// n3 returns. Its nextIndex sits below the snapshot boundary, so the
	// replication round must install the snapshot to catch it up.
	tr.setDown("n3", false)
	n1.replicate()

	for _, k := range []string{"a", "b", "c", "d"} {
		if got, ok := sms["n3"].data[keva.Key(k)]; !ok || string(got) != k {
			t.Errorf("n3 restored %s=%q (present=%v), want %q", k, got, ok, k)
		}
	}

	n3 := nodes["n3"]
	n3.mu.Lock()
	n3base := n3.logBase
	n3.mu.Unlock()
	if n3base != base {
		t.Errorf("n3 logBase=%d, want %d (adopted the leader's snapshot boundary)", n3base, base)
	}
}
