package fsm

import (
	"errors"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/store"
)

func TestApplyPutThenDelete(t *testing.T) {
	mem := store.NewMemory()
	f := New(mem)

	f.Apply(keva.Command{Op: keva.OpPut, Key: "k", Value: keva.Value("v")})
	if got, err := mem.Get("k"); err != nil || string(got) != "v" {
		t.Fatalf("after Put: got %q, err %v", got, err)
	}

	f.Apply(keva.Command{Op: keva.OpDelete, Key: "k"})
	if _, err := mem.Get("k"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after Delete: err = %v, want ErrNotFound", err)
	}
}
