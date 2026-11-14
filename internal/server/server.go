package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/store"
)

// maxBodySize caps a request body so a client cannot exhaust memory with
// an oversized payload.
const maxBodySize = 1 << 20 // 1 MiB

// Proposer submits commands to the replication layer and reports leadership.
// Submit fails when this node is not the leader.
type Proposer interface {
	Submit(cmd keva.Command) (uint64, error)
	AwaitCommit(ctx context.Context, index uint64) error // block until the entry commits
	ReadIndex(ctx context.Context) (uint64, error)       // confirm leadership for a linearizable read
	Leader() string                                      // ID of the current leader, "" if unknown
	IsLeader() bool
	Status() raft.Status // observable Raft state for the status endpoint
}

// requestTimeout bounds how long a write waits for its entry to commit, or a
// read waits for the leader to be confirmed, before the client gets a timeout
// — so a stalled cluster never hangs a request.
const requestTimeout = 5 * time.Second

// Server serves the Keva HTTP API. Writes are proposed through Raft (and
// forwarded to the leader when this node is a follower); reads are served
// from the local store (the replicated state machine).
type Server struct {
	raft    Proposer
	store   store.Store
	peers   map[string]string // peer ID → base URL, for forwarding to the leader
	client  *http.Client
	sem     chan struct{} // bounds in-flight client requests (backpressure)
	metrics metrics
}

// metrics counts client activity for the status endpoint. The fields are
// touched from many request goroutines, so they are atomic.
type metrics struct {
	reads      atomic.Int64
	writes     atomic.Int64
	deletes    atomic.Int64
	forwarded  atomic.Int64
	overloaded atomic.Int64
}

// maxInflight caps concurrent client requests a node will serve at once.
// Beyond it, load is shed rather than queued, so an overloaded or stalled
// node stays responsive instead of exhausting goroutines and memory.
const maxInflight = 256

// New returns a Server that proposes writes through p, reads from s, and
// forwards writes to the leader using the peers map (id → base URL).
func New(p Proposer, s store.Store, peers map[string]string) *Server {
	return NewWithLimit(p, s, peers, maxInflight)
}

// NewWithLimit is New with an explicit in-flight request cap, mainly so tests
// can exercise the backpressure path with a small limit.
func NewWithLimit(p Proposer, s store.Store, peers map[string]string, inflight int) *Server {
	return &Server{
		raft:   p,
		store:  s,
		peers:  peers,
		client: &http.Client{Timeout: 5 * time.Second},
		sem:    make(chan struct{}, inflight),
	}
}

// Handler builds the HTTP routes and returns the root handler. The key-value
// API is gated by the in-flight limiter; the status endpoint is not, so a node
// stays observable even while it is shedding load. Everything is wrapped in the
// request-logging middleware.
func (s *Server) Handler() http.Handler {
	kv := http.NewServeMux()
	kv.HandleFunc("GET /v1/kv/{key}", s.handleGet)
	kv.HandleFunc("PUT /v1/kv/{key}", s.handlePut)
	kv.HandleFunc("DELETE /v1/kv/{key}", s.handleDelete)

	root := http.NewServeMux()
	root.Handle("/v1/kv/", s.limit(kv))
	root.HandleFunc("GET /v1/status", s.handleStatus)
	return logging(root)
}

// limit sheds load once maxInflight requests are already in flight: it tries
// to claim a slot without waiting and, failing that, returns 503 immediately
// rather than letting work queue up without bound. Raft's own peer RPCs run on
// a separate handler and are never gated here, so consensus is not starved.
func (s *Server) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
			next.ServeHTTP(w, r)
		default:
			s.metrics.overloaded.Add(1)
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "server overloaded")
		}
	})
}

// handleStatus reports this node's Raft state and request counters. It is not
// gated by the limiter, so it answers even when the node is shedding load.
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		Node: s.raft.Status(),
		Requests: requestCounts{
			Reads:      s.metrics.reads.Load(),
			Writes:     s.metrics.writes.Load(),
			Deletes:    s.metrics.deletes.Load(),
			Forwarded:  s.metrics.forwarded.Load(),
			Overloaded: s.metrics.overloaded.Load(),
		},
		Inflight: len(s.sem),
	})
}

// statusResponse is the body of GET /v1/status.
type statusResponse struct {
	Node     raft.Status   `json:"node"`
	Requests requestCounts `json:"requests"`
	Inflight int           `json:"inflight"`
}

type requestCounts struct {
	Reads      int64 `json:"reads"`
	Writes     int64 `json:"writes"`
	Deletes    int64 `json:"deletes"`
	Forwarded  int64 `json:"forwarded"`
	Overloaded int64 `json:"overloaded"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.metrics.reads.Add(1)
	// Linearizable reads are served by the leader. Followers forward, and
	// the leader confirms it still leads before reading its local store.
	if !s.raft.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	if _, err := s.raft.ReadIndex(ctx); err != nil {
		s.writeSubmitError(w, err)
		return
	}

	key := keva.Key(r.PathValue("key"))

	val, err := s.store.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, api.ValueResponse{Value: val})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	s.metrics.writes.Add(1)
	if !s.raft.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	key := keva.Key(r.PathValue("key"))

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req api.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	index, err := s.raft.Submit(keva.Command{Op: keva.OpPut, Key: key, Value: req.Value})
	if err != nil {
		s.writeSubmitError(w, err)
		return
	}
	if err := s.awaitCommit(r, index); err != nil {
		s.writeSubmitError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.metrics.deletes.Add(1)
	if !s.raft.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	key := keva.Key(r.PathValue("key"))

	index, err := s.raft.Submit(keva.Command{Op: keva.OpDelete, Key: key})
	if err != nil {
		s.writeSubmitError(w, err)
		return
	}
	if err := s.awaitCommit(r, index); err != nil {
		s.writeSubmitError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// awaitCommit blocks until the proposed entry commits, bounded by both the
// client's request lifetime and commitTimeout — so a success response means
// the write is durably replicated, not merely proposed.
func (s *Server) awaitCommit(r *http.Request, index uint64) error {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	return s.raft.AwaitCommit(ctx, index)
}

// forwardToLeader proxies a write to the current leader and relays its
// response, so a client can write to any node. It returns 503 when no
// leader is known.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) {
	s.metrics.forwarded.Add(1)
	leader := s.raft.Leader()
	base, ok := s.peers[leader]
	if leader == "" || !ok {
		writeError(w, http.StatusServiceUnavailable, "no leader available")
		return
	}

	req, err := http.NewRequest(r.Method, base+r.URL.Path, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "forward failed")
		return
	}
	req.Header = r.Header.Clone()

	resp, err := s.client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "leader unreachable")
		return
	}
	defer func() { _ = resp.Body.Close() }()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// writeSubmitError maps a Raft submission or commit-wait failure to an HTTP
// status. Leadership loss and client cancellation are retryable (503); a
// commit that did not land in time is a gateway timeout (504); anything else
// is a 500.
func (s *Server) writeSubmitError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		writeError(w, http.StatusServiceUnavailable, "not the leader")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "write did not commit in time")
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "request canceled")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, api.ErrorResponse{Error: msg})
}
