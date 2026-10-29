package main

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/server"
	"github.com/0xnikshi/keva/internal/store"
)

// applyProposer applies commands straight to a store, standing in for Raft
// in the client's end-to-end tests.
type applyProposer struct{ s store.Store }

func (p applyProposer) Submit(cmd keva.Command) (uint64, error) {
	switch cmd.Op {
	case keva.OpPut:
		_ = p.s.Put(cmd.Key, cmd.Value)
	case keva.OpDelete:
		_ = p.s.Delete(cmd.Key)
	}
	return 1, nil
}
func (applyProposer) ReadIndex() (uint64, error) { return 1, nil }
func (applyProposer) Leader() string             { return "n1" }
func (applyProposer) IsLeader() bool             { return true }

func testClient(t *testing.T) *client {
	t.Helper()
	mem := store.NewMemory()
	ts := httptest.NewServer(server.New(applyProposer{mem}, mem, nil).Handler())
	t.Cleanup(ts.Close)
	return &client{addr: ts.URL, http: ts.Client()}
}

func TestClientRoundTrip(t *testing.T) {
	c := testClient(t)

	if err := c.put("greeting", "hello"); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := c.get("greeting")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("get = %q, want %q", got, "hello")
	}

	if err := c.delete("greeting"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.get("greeting"); !errors.Is(err, errNotFound) {
		t.Errorf("get after delete = %v, want errNotFound", err)
	}
}

func TestClientGetMissing(t *testing.T) {
	c := testClient(t)

	if _, err := c.get("nope"); !errors.Is(err, errNotFound) {
		t.Errorf("get missing = %v, want errNotFound", err)
	}
}
