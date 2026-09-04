package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/store"
)

func init() {
	// Silence the request-logging middleware during tests.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newTestServer() http.Handler {
	return New(store.NewMemory()).Handler()
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
	var resp api.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == "" {
		t.Error("expected an error message in the body")
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
