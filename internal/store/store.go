package store

import (
	"errors"

	"github.com/0xnikshi/keva/internal/keva"
)

// ErrNotFound is returned by Get when the key is absent.
var ErrNotFound = errors.New("store: key not found")

// Store is a key-value store. Implementations must be safe for
// concurrent use by multiple goroutines.
type Store interface {
	Get(key keva.Key) (keva.Value, error)
	Put(key keva.Key, value keva.Value) error
	Delete(key keva.Key) error
	Close() error
}
