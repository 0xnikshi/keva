package store

import (
	"sync"

	"github.com/0xnikshi/keva/internal/keva"
)

// Memory is an in-memory Store. It holds all data in a map guarded by a
// read-write mutex and does not persist across restarts.
type Memory struct {
	mu   sync.RWMutex
	data map[keva.Key]keva.Value
}

// compile-time check that *Memory satisfies Store.
var _ Store = (*Memory)(nil)

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{data: make(map[keva.Key]keva.Value)}
}

// Get returns the value stored at key, or ErrNotFound if it is absent.
func (m *Memory) Get(key keva.Key) (keva.Value, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneValue(v), nil
}

// Put stores value under key, overwriting any existing value.
func (m *Memory) Put(key keva.Key, value keva.Value) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = cloneValue(value)
	return nil
}

// Delete removes key. Deleting an absent key is not an error.
func (m *Memory) Delete(key keva.Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

// Close releases resources held by the store. The in-memory store holds
// none, so this is a no-op.
func (m *Memory) Close() error { return nil }

// cloneValue returns an independent copy of v so that data held in the
// store cannot be mutated through a slice the caller still references.
func cloneValue(v keva.Value) keva.Value {
	if v == nil {
		return nil
	}
	c := make(keva.Value, len(v))
	copy(c, v)
	return c
}
