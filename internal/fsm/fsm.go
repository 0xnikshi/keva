// Package fsm adapts the key-value store to the Raft state machine, so a
// committed log entry becomes a store mutation.
package fsm

import (
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/store"
)

// Store applies committed commands to an underlying key-value store.
type Store struct {
	store store.Store
}

// New returns a state machine backed by s.
func New(s store.Store) *Store {
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
