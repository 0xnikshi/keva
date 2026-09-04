package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/0xnikshi/keva/internal/api"
	"github.com/0xnikshi/keva/internal/keva"
	"github.com/0xnikshi/keva/internal/store"
)

// Server serves the Keva HTTP API backed by a Store.
type Server struct {
	store store.Store
}

// New returns a Server backed by s.
func New(s store.Store) *Server {
	return &Server{store: s}
}

// maxBodySize caps a request body so a client cannot exhaust memory with
// an oversized payload.
const maxBodySize = 1 << 20 // 1 MiB

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
	if err := s.store.Put(key, req.Value); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := keva.Key(r.PathValue("key"))

	if err := s.store.Delete(key); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, api.ErrorResponse{Error: msg})
}
