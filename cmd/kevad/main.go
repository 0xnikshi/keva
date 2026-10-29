// Command kevad is the Keva server daemon: it runs a Raft node backing the
// key-value store and serves both the client API and the Raft RPC API until
// it receives a shutdown signal.
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
	"strings"
	"syscall"
	"time"

	"github.com/0xnikshi/keva/internal/fsm"
	"github.com/0xnikshi/keva/internal/raft"
	"github.com/0xnikshi/keva/internal/rafthttp"
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
	peersFlag := flag.String("peers", "", "comma-separated id=addr of the other nodes")
	flag.Parse()

	peers, peerIDs, err := parsePeers(*peersFlag)
	if err != nil {
		return err
	}

	// The replicated state machine: an in-memory store driven by the Raft
	// log, reached over an HTTP transport. (Persistence comes in a later step.)
	mem := store.NewMemory()
	node := raft.New(raft.Config{ID: *id, Peers: peerIDs}, rafthttp.New(peers))
	node.SetStateMachine(fsm.New(mem))
	node.Start()
	defer node.Stop()

	mux := http.NewServeMux()
	mux.Handle("/v1/", server.New(node, mem, peers).Handler())
	mux.Handle("/raft/", rafthttp.Handler(node))

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("kevad listening", "addr", *addr, "id", *id, "peers", peerIDs)
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

// parsePeers parses a "id=addr,id=addr" string into a peer map (id → base
// URL) and the list of peer IDs.
func parsePeers(s string) (map[string]string, []string, error) {
	peers := map[string]string{}
	var ids []string
	if s == "" {
		return peers, ids, nil
	}
	for _, entry := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(entry, "=")
		if !ok || id == "" || addr == "" {
			return nil, nil, fmt.Errorf("invalid --peers entry %q (want id=addr)", entry)
		}
		peers[id] = normalizeBase(addr)
		ids = append(ids, id)
	}
	return peers, ids, nil
}

func normalizeBase(addr string) string {
	addr = strings.TrimRight(addr, "/")
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return addr
}
