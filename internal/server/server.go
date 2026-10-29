package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/store"
)

// maxBodySize caps a request body so a client cannot exhaust memory with
// an oversized payload.
const maxBodySize = 1 << 20 // 1 MiB

// Proposer submits a command to the replication layer, returning the log
// index it was assigned. It fails when this node is not the leader.
type Proposer interface {
	Submit(cmd keva.Command) (uint64, error)
}

// Server serves the Keva HTTP API. Writes are proposed through Raft; reads
// are served from the local store (the replicated state machine).
type Server struct {
	raft  Proposer
	store store.Store
}

// New returns a Server that proposes writes through p and reads from s.
func New(p Proposer, s store.Store) *Server {
	return &Server{raft: p, store: s}
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
	key := keva.Key(r.PathValue("key"))

	if _, err := s.raft.Submit(keva.Command{Op: keva.OpDelete, Key: key}); err != nil {
		s.writeSubmitError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
