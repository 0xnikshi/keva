// Command kevad is the Keva server daemon: it runs a Raft node backing the
// key-value store and serves the HTTP API until it receives a shutdown
// signal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xnikshi/keva/internal/fsm"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/server"
	"github.com/0xnikshi/keva/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("kevad exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", ":7070", "address to listen on")
	id := flag.String("id", "n1", "this node's identifier")
	flag.Parse()

	// The replicated state machine: an in-memory store driven by the Raft
	// log. (Multi-node networking and persistence come in later steps.)
	mem := store.NewMemory()
	node := raft.New(raft.Config{ID: *id}, nil)
	node.SetStateMachine(fsm.New(mem))
	node.Start()
	defer node.Stop()

	srv := &http.Server{
		Addr:         *addr,
		Handler:      server.New(node, mem).Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("kevad listening", "addr", *addr, "id", *id)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received, draining in-flight requests")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	slog.Info("kevad stopped cleanly")
	return nil
}
