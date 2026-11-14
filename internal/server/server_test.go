package server

import (
	"bytes"
	"context"
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
func (directProposer) AwaitCommit(context.Context, uint64) error { return nil }
func (directProposer) ReadIndex(context.Context) (uint64, error) { return 1, nil }
func (directProposer) Leader() string                            { return "n1" }
func (directProposer) IsLeader() bool                            { return true }
func (directProposer) Status() raft.Status                       { return raft.Status{ID: "n1", Role: "leader"} }

// notLeaderProposer always reports that this node is a follower with no
// known leader.
type notLeaderProposer struct{}

func (notLeaderProposer) Submit(keva.Command) (uint64, error)       { return 0, raft.ErrNotLeader }
func (notLeaderProposer) AwaitCommit(context.Context, uint64) error { return raft.ErrNotLeader }
func (notLeaderProposer) ReadIndex(context.Context) (uint64, error) { return 0, raft.ErrNotLeader }
func (notLeaderProposer) Leader() string                            { return "" }
func (notLeaderProposer) IsLeader() bool                            { return false }
func (notLeaderProposer) Status() raft.Status                       { return raft.Status{Role: "follower"} }

func newTestServer() http.Handler {
	mem := store.NewMemory()
	return New(directProposer{mem}, mem, nil).Handler()
}

// blockingProposer parks every write in AwaitCommit until release is closed,
// signalling on entered each time, so a test can hold request slots open and
// exercise the in-flight limiter.
type blockingProposer struct {
	entered chan struct{}
	release chan struct{}
}

func (blockingProposer) Submit(keva.Command) (uint64, error) { return 1, nil }
func (p blockingProposer) AwaitCommit(ctx context.Context, _ uint64) error {
	p.entered <- struct{}{}
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (blockingProposer) ReadIndex(context.Context) (uint64, error) { return 1, nil }
func (blockingProposer) Leader() string                            { return "n1" }
func (blockingProposer) IsLeader() bool                            { return true }
func (blockingProposer) Status() raft.Status                       { return raft.Status{ID: "n1", Role: "leader"} }

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
	h := New(notLeaderProposer{}, store.NewMemory(), nil).Handler()

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

	h := New(node, mem, nil).Handler()

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

func TestBackpressureShedsExcessLoad(t *testing.T) {
	mem := store.NewMemory()
	bp := blockingProposer{entered: make(chan struct{}), release: make(chan struct{})}
	ts := httptest.NewServer(NewWithLimit(bp, mem, nil, 1).Handler())
	defer ts.Close()

	put := func(key string) (int, string) {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/"+key, bytes.NewReader([]byte("{}")))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("PUT %s: %v", key, err)
			return 0, ""
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode, resp.Header.Get("Retry-After")
	}

	// Request A claims the only slot and parks in AwaitCommit.
	aCode := make(chan int, 1)
	go func() { code, _ := put("a"); aCode <- code }()
	<-bp.entered

	// Request B arrives with the node full and is shed at once.
	code, retryAfter := put("b")
	if code != http.StatusServiceUnavailable {
		t.Errorf("overflow request status = %d, want 503", code)
	}
	if retryAfter == "" {
		t.Error("overload response missing Retry-After header")
	}

	// Releasing A lets it finish normally, freeing the slot.
	close(bp.release)
	if got := <-aCode; got != http.StatusNoContent {
		t.Errorf("released request status = %d, want 204", got)
	}
}

func TestStatusReportsStateAndCounts(t *testing.T) {
	h := newTestServer()
	body, _ := json.Marshal(api.PutRequest{Value: []byte("1")})
	do(t, h, http.MethodPut, "/v1/kv/a", body)
	do(t, h, http.MethodGet, "/v1/kv/a", nil)
	do(t, h, http.MethodDelete, "/v1/kv/a", nil)

	w := do(t, h, http.MethodGet, "/v1/status", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", w.Code)
	}
	var resp statusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if resp.Node.Role != "leader" {
		t.Errorf("node role = %q, want leader", resp.Node.Role)
	}
	if resp.Requests.Writes < 1 || resp.Requests.Reads < 1 || resp.Requests.Deletes < 1 {
		t.Errorf("request counts = %+v, want >=1 read/write/delete", resp.Requests)
	}
}

func TestStatusAvailableWhileOverloaded(t *testing.T) {
	mem := store.NewMemory()
	bp := blockingProposer{entered: make(chan struct{}), release: make(chan struct{})}
	ts := httptest.NewServer(NewWithLimit(bp, mem, nil, 1).Handler())
	defer ts.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/a", bytes.NewReader([]byte("{}")))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-bp.entered // the only slot is now held

	// The status endpoint is ungated, so it still answers.
	resp, err := http.Get(ts.URL + "/v1/status")
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status while overloaded = %d, want 200", resp.StatusCode)
	}

	close(bp.release)
	<-done
}
