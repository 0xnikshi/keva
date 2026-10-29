package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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
	Leader() string // ID of the current leader, "" if unknown
	IsLeader() bool
}

// Server serves the Keva HTTP API. Writes are proposed through Raft (and
// forwarded to the leader when this node is a follower); reads are served
// from the local store (the replicated state machine).
type Server struct {
	raft   Proposer
	store  store.Store
	peers  map[string]string // peer ID → base URL, for forwarding to the leader
	client *http.Client
}

// New returns a Server that proposes writes through p, reads from s, and
// forwards writes to the leader using the peers map (id → base URL).
func New(p Proposer, s store.Store, peers map[string]string) *Server {
	return &Server{
		raft:   p,
		store:  s,
		peers:  peers,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Handler builds the HTTP routes and returns the root handler, wrapped in
// the request-logging middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key}", s.handleDelete)
	return logging(mux)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
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

	if _, err := s.raft.Submit(keva.Command{Op: keva.OpPut, Key: key, Value: req.Value}); err != nil {
		s.writeSubmitError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	key := keva.Key(r.PathValue("key"))

	if _, err := s.raft.Submit(keva.Command{Op: keva.OpDelete, Key: key}); err != nil {
		s.writeSubmitError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// forwardToLeader proxies a write to the current leader and relays its
// response, so a client can write to any node. It returns 503 when no
// leader is known.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) {
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

// writeSubmitError maps a Raft submission failure to an HTTP status. A
// non-leader is retryable, so it returns 503; anything else is a 500.
func (s *Server) writeSubmitError(w http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		writeError(w, http.StatusServiceUnavailable, "not the leader")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, api.ErrorResponse{Error: msg})
}
