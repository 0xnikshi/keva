package store

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
)

// TestDurableMatchesModel drives random sequences of Put / Delete /
// Compact / Restart against the store and a plain-map reference model,
// asserting the two never diverge — including across compaction and
// reopen. A small key space forces frequent overwrites and deletes.
func TestDurableMatchesModel(t *testing.T) {
	const (
		operations = 1000
		keySpace   = 16
	)

	for _, seed := range []int64{1, 42, 7} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			path := filepath.Join(t.TempDir(), "state.wal")

			d, err := OpenDurable(path)
			if err != nil {
				t.Fatalf("OpenDurable: %v", err)
			}
			model := make(map[keva.Key]keva.Value)

			for i := 0; i < operations; i++ {
				key := keva.Key(fmt.Sprintf("k%d", rng.Intn(keySpace)))
				switch n := rng.Intn(12); {
				case n < 7: // Put (most common)
					val := randValue(rng)
					if err := d.Put(key, val); err != nil {
						t.Fatalf("Put: %v", err)
					}
					model[key] = val
				case n < 10: // Delete
					if err := d.Delete(key); err != nil {
						t.Fatalf("Delete: %v", err)
					}
					delete(model, key)
				case n == 10: // Compact
					if err := d.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				default: // Restart: close and recover from the log
					if err := d.Close(); err != nil {
						t.Fatalf("Close: %v", err)
					}
					if d, err = OpenDurable(path); err != nil {
						t.Fatalf("reopen: %v", err)
					}
				}
			}

			checkModel(t, d, model, keySpace)

			// A final restart: recovered state must still match the model.
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if d, err = OpenDurable(path); err != nil {
				t.Fatalf("final reopen: %v", err)
			}
			defer func() { _ = d.Close() }()
			checkModel(t, d, model, keySpace)
		})
	}
}

func checkModel(t *testing.T, d *Durable, model map[keva.Key]keva.Value, keySpace int) {
	t.Helper()
	for i := 0; i < keySpace; i++ {
		key := keva.Key(fmt.Sprintf("k%d", i))
		got, err := d.Get(key)
		want, present := model[key]
		if present {
			if err != nil {
				t.Fatalf("key %s: Get error %v; want value %q", key, err, want)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("key %s: got %q; want %q", key, got, want)
			}
		} else if !errors.Is(err, ErrNotFound) {
			t.Fatalf("key %s: Get = %v; want ErrNotFound", key, err)
		}
	}
}

func randValue(rng *rand.Rand) keva.Value {
	b := make([]byte, rng.Intn(24))
	_, _ = rng.Read(b)
	return keva.Value(b)
}
