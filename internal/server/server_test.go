package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/fsm"
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/store"
)

func init() {
	// Silence the request-logging middleware during tests.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// directProposer applies commands straight to a store, standing in for Raft
// in handler unit tests.
type directProposer struct{ store store.Store }

func (p directProposer) Submit(cmd keva.Command) (uint64, error) {
	switch cmd.Op {
	case keva.OpPut:
		_ = p.store.Put(cmd.Key, cmd.Value)
	case keva.OpDelete:
		_ = p.store.Delete(cmd.Key)
	}
	return 1, nil
}

// notLeaderProposer always reports that this node is not the leader.
type notLeaderProposer struct{}

func (notLeaderProposer) Submit(keva.Command) (uint64, error) { return 0, raft.ErrNotLeader }

func newTestServer() http.Handler {
	mem := store.NewMemory()
	return New(directProposer{mem}, mem).Handler()
}

func do(t *testing.T, h http.Handler, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, target, bytes.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestPutThenGet(t *testing.T) {
	h := newTestServer()

	body, _ := json.Marshal(api.PutRequest{Value: []byte("hello")})
	if w := do(t, h, http.MethodPut, "/v1/kv/greeting", body); w.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", w.Code)
	}

	w := do(t, h, http.MethodGet, "/v1/kv/greeting", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", w.Code)
	}
	var resp api.ValueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(resp.Value, []byte("hello")) {
		t.Errorf("value = %q, want %q", resp.Value, "hello")
	}
}

func TestGetMissingReturns404(t *testing.T) {
	h := newTestServer()

	w := do(t, h, http.MethodGet, "/v1/kv/absent", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestDeleteThenGet(t *testing.T) {
	h := newTestServer()

	body, _ := json.Marshal(api.PutRequest{Value: []byte("x")})
	do(t, h, http.MethodPut, "/v1/kv/k", body)

	if w := do(t, h, http.MethodDelete, "/v1/kv/k", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", w.Code)
	}
	if w := do(t, h, http.MethodGet, "/v1/kv/k", nil); w.Code != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want 404", w.Code)
	}
}

func TestPutInvalidJSONReturns400(t *testing.T) {
	h := newTestServer()

	w := do(t, h, http.MethodPut, "/v1/kv/k", []byte("{not valid json"))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestPutBodyTooLargeReturns413(t *testing.T) {
	h := newTestServer()

	body, _ := json.Marshal(api.PutRequest{Value: make([]byte, 2<<20)}) // ~2 MiB
	w := do(t, h, http.MethodPut, "/v1/kv/big", body)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", w.Code)
	}
}

func TestPutWhenNotLeaderReturns503(t *testing.T) {
	h := New(notLeaderProposer{}, store.NewMemory()).Handler()

	body, _ := json.Marshal(api.PutRequest{Value: []byte("v")})
	if w := do(t, h, http.MethodPut, "/v1/kv/k", body); w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

// A write proposed through a real Raft node is committed, applied to the
// state machine, and then readable.
func TestWriteGoesThroughRaft(t *testing.T) {
	mem := store.NewMemory()
	node := raft.New(raft.Config{
		ID:                "n1",
		ElectionTimeout:   20 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
	}, nil)
	node.SetStateMachine(fsm.New(mem))
	node.Start()
	defer node.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for node.State() != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		t.Fatal("single node did not become leader")
	}

	h := New(node, mem).Handler()

	body, _ := json.Marshal(api.PutRequest{Value: []byte("hello")})
	if w := do(t, h, http.MethodPut, "/v1/kv/greeting", body); w.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", w.Code)
	}

	w := do(t, h, http.MethodGet, "/v1/kv/greeting", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", w.Code)
	}
	var resp api.ValueResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if !bytes.Equal(resp.Value, []byte("hello")) {
		t.Errorf("value = %q, want hello", resp.Value)
	}
}
