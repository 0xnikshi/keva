package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// benchLeader builds a three-node in-memory cluster and returns its leader,
// ready for proposals and reads.
func benchLeader(b *testing.B) *Raft {
	b.Helper()
	nodes, _ := mesh(b, "n1", "n2", "n3")
	n1 := nodes["n1"]
	if !n1.startElection() {
		b.Fatal("n1 should win the election")
	}
	n1.replicate() // commit the election no-op so reads and commits can proceed
	return n1
}

// BenchmarkSubmitParallel measures write throughput under concurrency, where
// group commit coalesces proposals into shared persist/replication rounds.
func BenchmarkSubmitParallel(b *testing.B) {
	leader := benchLeader(b)
	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1)
			cmd := keva.Command{Op: keva.OpPut, Key: keva.Key(fmt.Sprintf("k%d", i)), Value: keva.Value("v")}
			if _, err := leader.Submit(cmd); err != nil {
				b.Fatalf("submit: %v", err)
			}
		}
	})
}

// BenchmarkReadIndexParallel measures read throughput under concurrency, where
// reads share leadership-confirmation rounds.
func BenchmarkReadIndexParallel(b *testing.B) {
	leader := benchLeader(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := leader.ReadIndex(ctx); err != nil {
				b.Fatalf("read index: %v", err)
			}
		}
	})
}

// BenchmarkSubmitSerial measures single-threaded write latency — the cost of
// one proposal's own persist and replication round, with nothing to batch.
func BenchmarkSubmitSerial(b *testing.B) {
	leader := benchLeader(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := keva.Command{Op: keva.OpPut, Key: keva.Key(fmt.Sprintf("k%d", i)), Value: keva.Value("v")}
		if _, err := leader.Submit(cmd); err != nil {
			b.Fatalf("submit: %v", err)
		}
	}
}
