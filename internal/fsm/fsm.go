// Package fsm adapts the key-value store to the Raft state machine, so a
// committed log entry becomes a store mutation.
package fsm

import (
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/store"
)

// KV is a key-value store that can also snapshot and restore its contents,
// which Raft needs to compact its log.
type KV interface {
	store.Store
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Store applies committed commands to an underlying key-value store and
// exposes its snapshot/restore for Raft log compaction.
type Store struct {
	store KV
}

// New returns a state machine backed by s.
func New(s KV) *Store {
	return &Store{store: s}
}

// Apply executes a committed command against the store.
func (f *Store) Apply(cmd keva.Command) {
	switch cmd.Op {
	case keva.OpPut:
		_ = f.store.Put(cmd.Key, cmd.Value)
	case keva.OpDelete:
		_ = f.store.Delete(cmd.Key)
	}
}

// Snapshot serializes the current state.
func (f *Store) Snapshot() ([]byte, error) { return f.store.Snapshot() }

// Restore replaces the state with a snapshot.
func (f *Store) Restore(data []byte) error { return f.store.Restore(data) }
