package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/0xnikshi/keva/internal/keva"
)

// recordHeaderSize is the fixed prefix on every WAL record: a uint32
// payload length followed by a uint32 CRC32 of the payload.
const recordHeaderSize = 8

// errCorruptRecord is returned during replay when a record's checksum
// does not match its payload.
var errCorruptRecord = errors.New("store: corrupt WAL record")

// WAL is an append-only, crash-recoverable log of commands. Each Append
// is framed with a length and CRC and flushed to stable storage.
type WAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
}

// OpenWAL opens (creating if needed) the write-ahead log at path for
// appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes cmd to the log and flushes it to stable storage before
// returning, so a committed append survives a crash.
func (w *WAL) Append(cmd keva.Command) error {
	frame := encodeRecord(cmd)

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.w.Write(frame); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes any buffered data and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// rewrite atomically replaces the log's contents with cmds, then reopens
// it for appending. It writes a temp file, fsyncs it, renames it over the
// live path, and fsyncs the directory — so a crash during compaction
// leaves either the old complete log or the new one, never a partial file.
func (w *WAL) rewrite(cmds []keva.Command) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmp := w.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	bw := bufio.NewWriter(f)
	for _, cmd := range cmds {
		if _, err := bw.Write(encodeRecord(cmd)); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// The atomic swap: rename replaces the log in a single step, and the
	// directory fsync makes that rename itself durable.
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	if err := syncDir(w.path); err != nil {
		return err
	}

	// Reopen the freshly compacted file for subsequent appends.
	if err := w.f.Close(); err != nil {
		return err
	}
	nf, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = nf
	w.w = bufio.NewWriter(nf)
	return nil
}

// syncDir fsyncs the directory containing path, making a rename within it
// durable across a crash.
func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

// ReadAll replays the log at path, returning its commands in order. A
// missing file yields no commands. A torn record at the tail (from a
// crash mid-append) ends replay cleanly; a checksum mismatch elsewhere
// is reported as corruption.
func ReadAll(path string) ([]keva.Command, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReader(f)
	var cmds []keva.Command
	var hdr [recordHeaderSize]byte

	for {
		_, err := io.ReadFull(r, hdr[:])
		if errors.Is(err, io.EOF) {
			break // clean record boundary: end of log
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break // torn header at tail: crashed mid-append
		}
		if err != nil {
			return cmds, err
		}

		length := binary.BigEndian.Uint32(hdr[0:4])
		crc := binary.BigEndian.Uint32(hdr[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break // torn payload at tail
			}
			return cmds, err
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return cmds, errCorruptRecord
		}

		cmd, err := decodeRecord(payload)
		if err != nil {
			return cmds, err
		}
		cmds = append(cmds, cmd)
	}

	return cmds, nil
}

// encodeRecord serializes cmd into a length- and CRC-framed record.
func encodeRecord(cmd keva.Command) []byte {
	key := []byte(cmd.Key)
	val := []byte(cmd.Value)

	payload := make([]byte, 0, 1+4+len(key)+4+len(val))
	payload = append(payload, byte(cmd.Op))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(key)))
	payload = append(payload, key...)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(val)))
	payload = append(payload, val...)

	frame := make([]byte, recordHeaderSize, recordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	return append(frame, payload...)
}

// decodeRecord parses a payload produced by encodeRecord.
func decodeRecord(payload []byte) (keva.Command, error) {
	off := 0
	need := func(n int) bool { return off+n <= len(payload) }

	if !need(1) {
		return keva.Command{}, errCorruptRecord
	}
	op := keva.Op(payload[off])
	off++

	if !need(4) {
		return keva.Command{}, errCorruptRecord
	}
	keyLen := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	if !need(keyLen) {
		return keva.Command{}, errCorruptRecord
	}
	key := keva.Key(payload[off : off+keyLen])
	off += keyLen

	if !need(4) {
		return keva.Command{}, errCorruptRecord
	}
	valLen := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	if !need(valLen) {
		return keva.Command{}, errCorruptRecord
	}
	var val keva.Value
	if valLen > 0 {
		val = append(keva.Value(nil), payload[off:off+valLen]...)
	}

	return keva.Command{Op: op, Key: key, Value: val}, nil
}
