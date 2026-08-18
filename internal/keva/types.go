// Package keva defines the core domain types shared across the store,
// raft, and server packages.
package keva

// Key identifies a value in the store.
type Key string

// Value is the opaque payload stored under a Key.
type Value []byte

// Op is the kind of mutation a Command performs.
type Op uint8

// Supported mutation operations.
const (
	OpPut Op = iota + 1
	OpDelete
)

// Command is a single mutation applied to the store and replicated
// through the Raft log.
type Command struct {
	Op    Op
	Key   Key
	Value Value
}

// Entry is one record in the Raft log.
type Entry struct {
	Term    uint64
	Index   uint64
	Command Command
}
