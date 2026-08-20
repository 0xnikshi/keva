package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// A corrupt length prefix must be rejected as corruption rather than
// driving an allocation of its claimed size. Without the maxRecordSize
// guard this would attempt a multi-gigabyte allocation.
func TestReadAllRejectsOversizedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.wal")

	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], math.MaxUint32) // claim ~4 GiB
	if err := os.WriteFile(path, hdr[:], 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := ReadAll(path); !errors.Is(err, errCorruptRecord) {
		t.Errorf("ReadAll = %v; want errCorruptRecord", err)
	}
}

// FuzzReadRecords asserts the replay decoder never panics or hangs on
// arbitrary input: it must always return either commands or an error.
func FuzzReadRecords(f *testing.F) {
	f.Add(encodeRecord(keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("v")}))
	f.Add(encodeRecord(keva.Command{Op: keva.OpDelete, Key: "k"}))
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = readRecords(bytes.NewReader(data))
	})
}
