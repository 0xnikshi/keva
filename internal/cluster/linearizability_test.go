package cluster_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xnikshi/keva/internal/api"
)

// An operation is one client request as observed from outside: its kind, the
// value it wrote or read, and the logical-clock window [call, ret] during which
// it was in flight. Linearizability asks whether all operations can be placed
// at a single instant inside their window, consistent with sequential KV
// semantics and with real time.
type opKind int

const (
	opPut opKind = iota
	opGet
	opDelete
)

type operation struct {
	kind    opKind
	key     string
	value   string // written value (put) or observed value (get)
	present bool   // get: whether the key was found; put: true; delete: false
	call    int64
	ret     int64
}

// kvState is the sequential model of a single key.
type kvState struct {
	value   string
	present bool
}

// apply runs one operation against the model, reporting whether its observed
// result is consistent with that state and returning the resulting state.
func apply(s kvState, op operation) (bool, kvState) {
	switch op.kind {
	case opPut:
		return true, kvState{value: op.value, present: true}
	case opDelete:
		return true, kvState{}
	default: // opGet
		ok := op.present == s.present && (!op.present || op.value == s.value)
		return ok, s
	}
}

// linearizable reports whether a single key's operation history is
// linearizable, using the Wing–Gong search: repeatedly linearize an operation
// that no pending operation must precede, backtracking when stuck. The memo
// prunes (completed-set, state) pairs already shown hopeless, and the
// real-time pruning keeps the search frontier as small as the concurrency.
func linearizable(ops []operation) bool {
	n := len(ops)
	full := (1 << n) - 1
	memo := map[string]bool{}

	var search func(done int, state kvState) bool
	search = func(done int, state kvState) bool {
		if done == full {
			return true
		}
		memoKey := fmt.Sprintf("%d|%t|%s", done, state.present, state.value)
		if v, ok := memo[memoKey]; ok {
			return v
		}

		result := false
		for i := 0; i < n && !result; i++ {
			if done&(1<<i) != 0 {
				continue
			}
			// Candidate only if no still-pending operation must come first:
			// one that completed in real time before ops[i] began.
			blocked := false
			for j := 0; j < n; j++ {
				if j == i || done&(1<<j) != 0 {
					continue
				}
				if ops[j].ret < ops[i].call {
					blocked = true
					break
				}
			}
			if blocked {
				continue
			}
			if ok, next := apply(state, ops[i]); ok {
				if search(done|(1<<i), next) {
					result = true
				}
			}
		}

		memo[memoKey] = result
		return result
	}
	return search(0, kvState{})
}

// TestLinearizableUnderConcurrency drives several clients at the cluster at
// once over real HTTP, records what each operation observed, and checks that
// the resulting concurrent history could have come from one consistent
// register — exercising the whole stack, not just Raft's internals.
func TestLinearizableUnderConcurrency(t *testing.T) {
	nodes := startCluster(t, "n1", "n2", "n3")
	if waitForLeader(t, nodes, 3*time.Second) == "" {
		t.Fatal("no leader elected")
	}

	var bases []string
	for _, h := range nodes {
		bases = append(bases, h.ts.URL)
	}
	keys := []string{"x", "y"}

	const clients = 3
	const opsPerClient = 20
	var clock int64 // logical clock: a strict, tie-free real-time order

	histories := make([][]operation, clients)
	errs := make([]error, clients)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(c)+1, 42))
			seq := 0
			var hist []operation
			for i := 0; i < opsPerClient; i++ {
				key := keys[rng.IntN(len(keys))]
				base := bases[rng.IntN(len(bases))]
				switch r := rng.IntN(10); {
				case r < 6: // put a globally unique value
					seq++
					val := fmt.Sprintf("c%d-%d", c, seq)
					call := atomic.AddInt64(&clock, 1)
					if err := retryPut(base, key, val); err != nil {
						errs[c] = err
						return
					}
					hist = append(hist, operation{opPut, key, val, true, call, atomic.AddInt64(&clock, 1)})
				case r < 9: // read
					call := atomic.AddInt64(&clock, 1)
					val, present, err := retryGet(base, key)
					if err != nil {
						errs[c] = err
						return
					}
					hist = append(hist, operation{opGet, key, val, present, call, atomic.AddInt64(&clock, 1)})
				default: // delete
					call := atomic.AddInt64(&clock, 1)
					if err := retryDelete(base, key); err != nil {
						errs[c] = err
						return
					}
					hist = append(hist, operation{opDelete, key, "", false, call, atomic.AddInt64(&clock, 1)})
				}
			}
			histories[c] = hist
		}(c)
	}
	wg.Wait()

	for c, err := range errs {
		if err != nil {
			t.Fatalf("client %d: %v", c, err)
		}
	}

	// Keys are independent registers, so linearizability holds iff each key's
	// sub-history does.
	byKey := map[string][]operation{}
	for _, h := range histories {
		for _, op := range h {
			byKey[op.key] = append(byKey[op.key], op)
		}
	}
	for key, ops := range byKey {
		if !linearizable(ops) {
			t.Errorf("history for key %q (%d ops) is not linearizable", key, len(ops))
		}
	}
}

// TestLinearizabilityCheckerHasTeeth confirms the checker rejects a history
// that violates real time: a read that returns an overwritten value even
// though the overwrite completed before the read began.
func TestLinearizabilityCheckerHasTeeth(t *testing.T) {
	legal := []operation{
		{opPut, "x", "1", true, 1, 2},
		{opPut, "x", "2", true, 3, 4},
		{opGet, "x", "2", true, 5, 6},
	}
	if !linearizable(legal) {
		t.Error("checker rejected a linearizable history")
	}

	// Same history, but the final read observes the stale "1" after "2" has
	// already been committed — impossible for a single register.
	illegal := []operation{
		{opPut, "x", "1", true, 1, 2},
		{opPut, "x", "2", true, 3, 4},
		{opGet, "x", "1", true, 5, 6},
	}
	if linearizable(illegal) {
		t.Error("checker accepted a non-linearizable history")
	}
}

// httpClient bounds each attempt so a request to a momentarily wedged node
// fails fast and is retried elsewhere, rather than blocking the whole run.
var httpClient = &http.Client{Timeout: 2 * time.Second}

func retryPut(base, key, value string) error {
	body, _ := json.Marshal(api.PutRequest{Value: []byte(value)})
	return retryUntil("PUT "+key, func() bool {
		req, _ := http.NewRequest(http.MethodPut, base+"/v1/kv/"+key, bytes.NewReader(body))
		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusNoContent
	})
}

func retryDelete(base, key string) error {
	return retryUntil("DELETE "+key, func() bool {
		req, _ := http.NewRequest(http.MethodDelete, base+"/v1/kv/"+key, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusNoContent
	})
}

func retryGet(base, key string) (value string, present bool, err error) {
	err = retryUntil("GET "+key, func() bool {
		resp, e := httpClient.Get(base + "/v1/kv/" + key)
		if e != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		switch resp.StatusCode {
		case http.StatusOK:
			var vr api.ValueResponse
			_ = json.NewDecoder(resp.Body).Decode(&vr)
			value, present = string(vr.Value), true
			return true
		case http.StatusNotFound:
			value, present = "", false
			return true
		default: // 503 while leadership settles, etc.
			return false
		}
	})
	return value, present, err
}

// retryUntil calls do until it reports success, so a transient non-leader
// response never leaves an ambiguous operation in the recorded history. It
// returns an error instead of failing the test, so it is safe to call from a
// client goroutine (t.Fatalf must only be used on the test's own goroutine).
func retryUntil(what string, do func() bool) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if do() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not succeed before deadline", what)
		}
		time.Sleep(3 * time.Millisecond)
	}
}
