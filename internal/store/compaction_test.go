package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Compaction must discard redundant history (overwrites, deletes) while
// preserving current state, both live and after a reopen.
func TestCompactShrinksLogAndPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	d, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}

	// Lots of redundancy: 500 overwrites of one key, plus a delete.
	for i := 0; i < 500; i++ {
		mustPut(t, d, "counter", fmt.Sprintf("%d", i))
	}
	mustPut(t, d, "keep", "yes")
	mustPut(t, d, "gone", "temp")
	if err := d.Delete("gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	sizeBefore := fileSize(t, path)
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	sizeAfter := fileSize(t, path)
	if sizeAfter >= sizeBefore {
		t.Errorf("log did not shrink: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// State is unchanged by compaction.
	assertValue(t, d, "counter", "499")
	assertValue(t, d, "keep", "yes")
	if _, err := d.Get("gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted key present after compaction")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery from the compacted log rebuilds the same state.
	d2, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	assertValue(t, d2, "counter", "499")
	assertValue(t, d2, "keep", "yes")
	if _, err := d2.Get("gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted key present after reopen")
	}
}

// Writes after compaction append to the new log and are recovered too.
func TestWritesAfterCompactionAreDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	d, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	mustPut(t, d, "a", "1")
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	mustPut(t, d, "b", "2") // appended to the compacted log
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	assertValue(t, d2, "a", "1")
	assertValue(t, d2, "b", "2")
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return info.Size()
}
