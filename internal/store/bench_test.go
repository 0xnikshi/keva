package store

import (
	"fmt"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// BenchmarkMemoryPut and BenchmarkMemoryGet measure the raw store with no
// consensus in the path — the ceiling the replicated layer works below.
func BenchmarkMemoryPut(b *testing.B) {
	m := NewMemory()
	val := keva.Value("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Put(keva.Key(fmt.Sprintf("k%d", i%1000)), val)
	}
}

func BenchmarkMemoryGet(b *testing.B) {
	m := NewMemory()
	for i := 0; i < 1000; i++ {
		_ = m.Put(keva.Key(fmt.Sprintf("k%d", i)), keva.Value("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get(keva.Key(fmt.Sprintf("k%d", i%1000)))
	}
}
