package cluster_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/cluster"
)

type harness struct {
	node *cluster.Node
	ts   *httptest.Server
	urls map[string]string
}

// startCluster builds and starts an in-process cluster of the given IDs,
// wired to each other over real HTTP (httptest servers).
func startCluster(t *testing.T, ids ...string) map[string]*harness {
	t.Helper()
	nodes := map[string]*harness{}

	for _, id := range ids {
		var peerIDs []string
		for _, other := range ids {
			if other != id {
				peerIDs = append(peerIDs, other)
			}
		}
		urls := map[string]string{} // filled below, shared by reference
		n := cluster.New(cluster.Config{
			ID:                id,
			PeerIDs:           peerIDs,
			PeerURLs:          urls,
			ElectionTimeout:   60 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		})
		nodes[id] = &harness{node: n, ts: httptest.NewServer(n.Handler()), urls: urls}
	}

	for id, h := range nodes {
		for other, oh := range nodes {
			if other != id {
				h.urls[other] = oh.ts.URL
			}
		}
	}
	for _, h := range nodes {
		h.node.Start()
	}
	t.Cleanup(func() {
		for _, h := range nodes {
			h.node.Stop()
			h.ts.Close()
		}
	})
	return nodes
}

func waitForLeader(t *testing.T, nodes map[string]*harness, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, h := range nodes {
			if h.node.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func put(t *testing.T, base, key, value string) int {
	t.Helper()
	body, _ := json.Marshal(api.PutRequest{Value: []byte(value)})
	req, _ := http.NewRequest(http.MethodPut, base+"/v1/kv/"+key, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func get(t *testing.T, base, key string) (string, int) {
	t.Helper()
	resp, err := http.Get(base + "/v1/kv/" + key)
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var vr api.ValueResponse
	_ = json.NewDecoder(resp.Body).Decode(&vr)
	return string(vr.Value), resp.StatusCode
}

func TestClusterWriteForwardAndRead(t *testing.T) {
	nodes := startCluster(t, "n1", "n2", "n3")
	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == "" {
		t.Fatal("no leader elected")
	}

	// Pick a follower and write to it: it must forward to the leader.
	var follower string
	for id := range nodes {
		if id != leader {
			follower = id
			break
		}
	}
	if code := put(t, nodes[follower].ts.URL, "city", "pune"); code != http.StatusNoContent {
		t.Fatalf("write to follower = %d, want 204 (forwarded)", code)
	}

	// Every node returns the same value.
	for id, h := range nodes {
		got, code := get(t, h.ts.URL, "city")
		if code != http.StatusOK || got != "pune" {
			t.Errorf("get from %s = %q (%d), want pune (200)", id, got, code)
		}
	}
}

func TestClusterSurvivesLeaderFailure(t *testing.T) {
	nodes := startCluster(t, "n1", "n2", "n3")
	first := waitForLeader(t, nodes, 3*time.Second)
	if first == "" {
		t.Fatal("no initial leader")
	}
	if code := put(t, nodes[first].ts.URL, "k", "v1"); code != http.StatusNoContent {
		t.Fatalf("initial write = %d, want 204", code)
	}

	// Crash the leader: stop its loop and make it unreachable.
	nodes[first].node.Stop()
	nodes[first].ts.Close()

	survivors := map[string]*harness{}
	for id, h := range nodes {
		if id != first {
			survivors[id] = h
		}
	}

	second := waitForLeader(t, survivors, 3*time.Second)
	if second == "" {
		t.Fatal("no new leader after failure")
	}

	// The cluster still serves: write via a survivor, read it back.
	if code := put(t, nodes[second].ts.URL, "k", "v2"); code != http.StatusNoContent {
		t.Fatalf("write after failover = %d, want 204", code)
	}
	for id, h := range survivors {
		got, code := get(t, h.ts.URL, "k")
		if code != http.StatusOK || got != "v2" {
			t.Errorf("get from survivor %s = %q (%d), want v2 (200)", id, got, code)
		}
	}
}
