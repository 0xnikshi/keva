package store

import (
	"bytes"
	"errors"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

func TestMemoryPutGet(t *testing.T) {
	m := NewMemory()

	if err := m.Put("k1", keva.Value("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := m.Get("k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, keva.Value("v1")) {
		t.Errorf("Get = %q, want %q", got, "v1")
	}
}

func TestMemoryGetMissing(t *testing.T) {
	m := NewMemory()

	_, err := m.Get("absent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing = %v, want ErrNotFound", err)
	}
}

func TestMemoryOverwrite(t *testing.T) {
	m := NewMemory()
	_ = m.Put("k", keva.Value("first"))
	_ = m.Put("k", keva.Value("second"))

	got, _ := m.Get("k")
	if !bytes.Equal(got, keva.Value("second")) {
		t.Errorf("Get = %q, want %q", got, "second")
	}
}

func TestMemoryDelete(t *testing.T) {
	m := NewMemory()
	_ = m.Put("k", keva.Value("v"))

	if err := m.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

// The store must not alias caller-owned slices: mutating the input after
// Put, or the result after Get, must not change stored data.
func TestMemoryDoesNotAliasCallerSlices(t *testing.T) {
	m := NewMemory()

	in := keva.Value("original")
	_ = m.Put("k", in)
	in[0] = 'X' // mutate the caller's slice after Put

	got, _ := m.Get("k")
	if !bytes.Equal(got, keva.Value("original")) {
		t.Errorf("stored value changed via input slice: got %q", got)
	}

	got[0] = 'Y' // mutate the returned slice
	again, _ := m.Get("k")
	if !bytes.Equal(again, keva.Value("original")) {
		t.Errorf("stored value changed via returned slice: got %q", again)
	}
}
