package main

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/0xnikshi/keva/internal/server"
	"github.com/0xnikshi/keva/internal/store"
)

func testClient(t *testing.T) *client {
	t.Helper()
	ts := httptest.NewServer(server.New(store.NewMemory()).Handler())
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
