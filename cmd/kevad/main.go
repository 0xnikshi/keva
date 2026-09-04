// Command kevad is the Keva server daemon: it opens the durable store and
// serves the HTTP API until it receives a shutdown signal.
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
	data := flag.String("data", "keva.wal", "path to the write-ahead log file")
	flag.Parse()

	st, err := store.OpenDurable(*data)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			slog.Error("closing store", "err", err)
		}
	}()

	srv := &http.Server{
		Addr:         *addr,
		Handler:      server.New(st).Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// The context is cancelled when an interrupt or termination signal
	// arrives, which is our cue to begin a graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ListenAndServe blocks, so it runs in its own goroutine and reports
	// any startup/serving error back through a buffered channel.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("kevad listening", "addr", *addr, "data", *data)
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

	// Stop accepting new connections and give in-flight requests a bounded
	// window to finish before forcing the close.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	slog.Info("kevad stopped cleanly")
	return nil
}
