package store

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// The headline guarantee: data written by one instance is recovered by a
// fresh instance opened on the same file — a process "restart".
func TestDurablePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	d, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	mustPut(t, d, "name", "nikhil")
	mustPut(t, d, "city", "pune")
	mustPut(t, d, "temp", "scratch")
	if err := d.Delete("temp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: a brand-new instance recovering purely from the log.
	d2, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()

	assertValue(t, d2, "name", "nikhil")
	assertValue(t, d2, "city", "pune")
	if _, err := d2.Get("temp"); !errors.Is(err, ErrNotFound) {
		t.Errorf(`Get("temp") = %v; want ErrNotFound (it was deleted before close)`, err)
	}
}

// Replay must reflect the last write to a key, not the first.
func TestDurableReplayReflectsLastWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	d, _ := OpenDurable(path)
	mustPut(t, d, "k", "first")
	mustPut(t, d, "k", "second")
	_ = d.Close()

	d2, _ := OpenDurable(path)
	defer func() { _ = d2.Close() }()
	assertValue(t, d2, "k", "second")
}

// Because every Append fsyncs, writes survive even without a clean Close
// — the crash case. We drop the first instance without closing it.
func TestDurableRecoversWithoutClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	d, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	mustPut(t, d, "k", "v")
	// No Close: simulate a crash after the fsync'd append.

	d2, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	assertValue(t, d2, "k", "v")
}

// A fresh store on a fresh path starts empty.
func TestDurableEmptyStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")
	d, err := OpenDurable(path)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.Get("nothing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get on empty store = %v; want ErrNotFound", err)
	}
}

func mustPut(t *testing.T, s Store, key, val string) {
	t.Helper()
	if err := s.Put(keva.Key(key), keva.Value(val)); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func assertValue(t *testing.T, s Store, key, want string) {
	t.Helper()
	got, err := s.Get(keva.Key(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, keva.Value(want)) {
		t.Errorf("Get(%q) = %q; want %q", key, got, want)
	}
}
