package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/keva"
)

// countingStorage is durable storage that only counts Save calls, so a test
// can see how many fsync-equivalent persists a batch of writes caused.
type countingStorage struct{ saves atomic.Int64 }

func (c *countingStorage) Save(PersistentState) error   { c.saves.Add(1); return nil }
func (*countingStorage) Load() (PersistentState, error) { return PersistentState{}, nil }

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

// TestWritesShareGroupCommit shows the write-path optimization: a burst of
// concurrent proposals shares one persist (fsync) and one replication round
// rather than each paying its own. A small per-round delay holds a round open
// so the proposals pile into one batch, making the coalescing observable.
func TestWritesShareGroupCommit(t *testing.T) {
	nodes, _ := mesh(t, "n1", "n2", "n3")
	n1 := nodes["n1"]
	storage := &countingStorage{}
	if err := n1.SetStorage(storage); err != nil {
		t.Fatalf("set storage: %v", err)
	}
	if !n1.startElection() {
		t.Fatal("n1 should win the election")
	}
	n1.replicate() // commit the election no-op

	tr := n1.transport.(*memTransport)
	tr.appendDelay = 15 * time.Millisecond // keep a round open so writes pile up
	storage.saves.Store(0)

	const writers = 20
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := keva.Command{Op: keva.OpPut, Key: keva.Key(fmt.Sprintf("k%d", i)), Value: keva.Value("v")}
			if _, err := n1.Submit(cmd); err != nil {
				t.Errorf("submit: %v", err)
			}
		}(i)
	}
	wg.Wait()

	saves := storage.saves.Load()
	if saves >= writers {
		t.Errorf("%d writes caused %d persists; group commit did not coalesce", writers, saves)
	}
	t.Logf("%d concurrent writes used %d persists (fsyncs)", writers, saves)
}
