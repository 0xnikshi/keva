package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"

	"github.com/0xnikshi/keva/internal/keva"
)

// PersistentState is the subset of a node's state that must survive a
// crash: without it, a restarted node could vote twice in one term or lose
// committed entries.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []keva.Entry
}

// Storage persists and restores a node's persistent Raft state.
type Storage interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}

// SetStorage attaches durable storage and restores any previously saved
// state. Call once, before the node starts serving.
func (r *Raft) SetStorage(s Storage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ps, err := s.Load()
	if err != nil {
		return err
	}
	r.storage = s
	r.currentTerm = ps.CurrentTerm
	r.votedFor = ps.VotedFor
	r.log = ps.Log
	return nil
}

// persist writes the node's persistent state to stable storage. Caller
// holds r.mu. A real deployment must treat a persist failure as fatal —
// acting on unsaved state is unsafe; we keep it best-effort here.
func (r *Raft) persist() {
	if r.storage == nil {
		return
	}
	_ = r.storage.Save(PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	})
}

// FileStorage persists Raft state to a single file, replaced atomically so
// a crash leaves either the old complete state or the new one.
type FileStorage struct {
	path string
}

// NewFileStorage returns storage backed by the file at path.
func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

// Load reads the saved state, returning the zero state if none exists yet.
func (s *FileStorage) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	var ps PersistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil {
		return PersistentState{}, err
	}
	return ps, nil
}

// Save writes state durably: encode, write to a temp file, fsync, rename
// over the live path, and fsync the directory.
func (s *FileStorage) Save(state PersistentState) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	return syncDir(s.path)
}

// syncDir fsyncs the directory containing path, making a rename durable.
func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
