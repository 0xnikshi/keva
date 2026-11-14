package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestReadIndexBatchesConcurrentReads shows the read-path optimization: a burst
// of concurrent linearizable reads shares a handful of leadership-confirmation
// rounds rather than one per read. A small per-round delay holds each round
// open long enough for the others to queue, making the coalescing observable.
func TestReadIndexBatchesConcurrentReads(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	commitPut(t, n1, "k", "v") // commit the read barrier

	tr := n1.transport.(*memTransport)
	tr.appendDelay = 15 * time.Millisecond // keep rounds open so reads pile up
	tr.appendCalls.Store(0)

	const readers = 20
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := n1.ReadIndex(ctx); err != nil {
				t.Errorf("ReadIndex: %v", err)
			}
		}()
	}
	wg.Wait()

	rpcs := tr.appendCalls.Load()
	if rpcs >= readers {
		t.Errorf("%d reads used %d confirmation RPCs; expected far fewer (coalesced)", readers, rpcs)
	}
	t.Logf("%d concurrent reads used %d confirmation RPCs", readers, rpcs)
}
