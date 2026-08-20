package store

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

func walPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestWALAppendAndReplay(t *testing.T) {
	path := walPath(t)

	want := []keva.Command{
		{Op: keva.OpPut, Key: "a", Value: keva.Value("1")},
		{Op: keva.OpPut, Key: "b", Value: keva.Value("longer value")},
		{Op: keva.OpDelete, Key: "a"},
	}

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	for _, cmd := range want {
		if err := w.Append(cmd); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replay mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func TestReadAllMissingFileIsEmpty(t *testing.T) {
	got, err := ReadAll(filepath.Join(t.TempDir(), "does-not-exist.wal"))
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d commands, want 0", len(got))
	}
}

// A crash mid-append leaves a partial record at the tail. Replay must
// return every complete record and stop, not error.
func TestReadAllToleratesTornTail(t *testing.T) {
	path := walPath(t)

	w, _ := OpenWAL(path)
	_ = w.Append(keva.Command{Op: keva.OpPut, Key: "a", Value: keva.Value("1")})
	_ = w.Append(keva.Command{Op: keva.OpPut, Key: "b", Value: keva.Value("2")})
	_ = w.Close()

	// Chop the last byte to simulate a torn final record.
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 1 || got[0].Key != "a" {
		t.Errorf("got %+v, want only the first record", got)
	}
}

// A flipped byte inside a complete record must be caught by the CRC.
func TestReadAllDetectsCorruption(t *testing.T) {
	path := walPath(t)

	w, _ := OpenWAL(path)
	_ = w.Append(keva.Command{Op: keva.OpPut, Key: "a", Value: keva.Value("1")})
	_ = w.Close()

	data, _ := os.ReadFile(path)
	data[recordHeaderSize] ^= 0xFF // corrupt the first payload byte
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := ReadAll(path)
	if !errors.Is(err, errCorruptRecord) {
		t.Errorf("ReadAll = %v, want errCorruptRecord", err)
	}
}
