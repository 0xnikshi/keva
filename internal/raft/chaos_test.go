package raft

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// currentLeader returns an up node that believes it is leader, preferring the
// highest term. It may be a stale minority leader — that is fine, since a write
// it cannot commit simply isn't recorded.
func currentLeader(nodes map[string]*Raft, down map[string]bool) *Raft {
	var ldr *Raft
	var maxTerm uint64
	for id, n := range nodes {
		if down[id] {
			continue
		}
		n.mu.Lock()
		isLdr, term := n.state == Leader, n.currentTerm
		n.mu.Unlock()
		if isLdr && (ldr == nil || term > maxTerm) {
			ldr, maxTerm = n, term
		}
	}
	return ldr
}

// mostUpToDate returns the node whose log is most advanced, by (last term, last
// index) — the election restriction guarantees it is a valid leader candidate.
func mostUpToDate(nodes map[string]*Raft) *Raft {
	var best *Raft
	var bt, bi uint64
	for _, n := range nodes {
		n.mu.Lock()
		lt, li := n.lastLogTerm(), n.lastLogIndex()
		n.mu.Unlock()
		if best == nil || lt > bt || (lt == bt && li > bi) {
			best, bt, bi = n, lt, li
		}
	}
	return best
}

func committedAt(r *Raft, idx uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return idx <= r.commitIndex
}

func fullyCommitted(r *Raft) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex == r.lastLogIndex()
}

// settle drives a healed cluster to a single stable leader that has committed
// its whole log, then propagates those commits. It returns that leader.
func settle(t *testing.T, nodes map[string]*Raft) *Raft {
	t.Helper()
	for i := 0; i < 60; i++ {
		mostUpToDate(nodes).startElection()
		for j := 0; j < 8; j++ {
			if ldr := currentLeader(nodes, nil); ldr != nil {
				ldr.replicate()
			}
		}
		if ldr := currentLeader(nodes, nil); ldr != nil && fullyCommitted(ldr) {
			for j := 0; j < 5; j++ {
				ldr.replicate()
			}
			return ldr
		}
	}
	t.Fatal("cluster did not settle on a committed leader")
	return nil
}

// TestChaos throws a randomized storm of writes, elections, partitions, node
// isolation, reconnection, and snapshots at the cluster, checking Raft's safety
// invariants after every single step. After the storm it heals the network,
// settles on a leader, and asserts that every write ever reported committed
// survived — Leader Completeness holds through arbitrary failures.
func TestChaos(t *testing.T) {
	for seed := uint64(1); seed <= 15; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runChaos(t, seed)
		})
	}
}

func runChaos(t *testing.T, seed uint64) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	nodes, sms := mesh(t, ids...)
	tr := nodes["n1"].transport.(*memTransport)
	rng := rand.New(rand.NewPCG(seed, 0x6b657661))

	nodes["n1"].startElection()
	nodes["n1"].replicate()
	checkInvariants(t, nodes)

	committed := map[keva.Key]keva.Value{} // unique-key writes that must survive
	down := map[string]bool{}
	writeSeq := 0

	const steps = 150
	for s := 0; s < steps; s++ {
		switch r := rng.IntN(100); {
		case r < 40: // write to the current leader
			if ldr := currentLeader(nodes, down); ldr != nil {
				writeSeq++
				key := keva.Key(fmt.Sprintf("k%d", writeSeq))
				val := keva.Value(fmt.Sprintf("v%d", writeSeq))
				if idx, err := ldr.Submit(keva.Command{Op: keva.OpPut, Key: key, Value: val}); err == nil {
					ldr.replicate()
					if committedAt(ldr, idx) {
						committed[key] = val
					}
				}
			}
		case r < 55: // extra replication round
			if ldr := currentLeader(nodes, down); ldr != nil {
				ldr.replicate()
			}
		case r < 70: // an election timeout fires somewhere
			id := ids[rng.IntN(len(ids))]
			if !down[id] && nodes[id].startElection() {
				nodes[id].replicate()
			}
		case r < 80: // network splits in two
			order := append([]string(nil), ids...)
			rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			cut := 1 + rng.IntN(len(order)-1)
			tr.partition(order[:cut], order[cut:])
		case r < 88: // network heals
			tr.heal()
		case r < 94: // a node is isolated
			id := ids[rng.IntN(len(ids))]
			down[id] = true
			tr.setDown(id, true)
		default: // a node reconnects
			id := ids[rng.IntN(len(ids))]
			down[id] = false
			tr.setDown(id, false)
		}

		if rng.IntN(8) == 0 { // occasional log compaction
			_, _ = nodes[ids[rng.IntN(len(ids))]].Snapshot(3)
		}

		checkInvariants(t, nodes)
	}

	// Heal everything and let the cluster converge on a leader.
	tr.heal()
	for _, id := range ids {
		down[id] = false
		tr.setDown(id, false)
	}
	leader := settle(t, nodes)
	checkInvariants(t, nodes)

	// Every committed write must still be present on the elected leader:
	// Leader Completeness guarantees a committed entry survives into every
	// future leader's log, and is re-applied once that leader commits.
	for k, v := range committed {
		if got, ok := sms[leader.id].data[k]; !ok || string(got) != string(v) {
			t.Fatalf("committed write %q lost after chaos: got %q (present=%v), want %q",
				k, got, ok, v)
		}
	}
}
