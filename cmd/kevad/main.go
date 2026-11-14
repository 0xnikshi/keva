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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/0xnikshi/keva/internal/cluster"
	"github.com/0xnikshi/keva/internal/metrics"
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
	dataDir := flag.String("data", "", "directory for persistent Raft state (empty → in-memory)")
	flag.Parse()

	peerURLs, peerIDs, err := parsePeers(*peersFlag)
	if err != nil {
		return err
	}

	var storagePath string
	if *dataDir != "" {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			return fmt.Errorf("create data dir: %w", err)
		}
		storagePath = filepath.Join(*dataDir, *id+".raft")
	}

	node, err := cluster.New(cluster.Config{
		ID: *id, PeerIDs: peerIDs, PeerURLs: peerURLs, StoragePath: storagePath,
	})
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}
	node.Start()
	defer node.Stop()

	// Expose Prometheus metrics at /metrics and instrument the client/peer
	// handler so request rate, latency, and response codes are recorded. The
	// Raft-state collector reads node.Status() on each scrape.
	m := metrics.New(func() metrics.RaftStats {
		s := node.Status()
		return metrics.RaftStats{
			Term:        s.Term,
			CommitIndex: s.CommitIndex,
			LastApplied: s.LastApplied,
			IsLeader:    s.Role == "leader",
		}
	})
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("/", m.InstrumentHandler(node.Handler()))

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

	// In-flight requests have drained while this node was still leader; now
	// hand leadership to a follower so the cluster keeps a leader without
	// waiting out an election timeout. Best effort — if it fails, the plain
	// stop below still lets the survivors elect.
	if node.IsLeader() {
		transferCtx, tcancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := node.TransferLeadership(transferCtx); err != nil {
			slog.Warn("leadership transfer skipped", "err", err)
		} else {
			slog.Info("leadership transferred before shutdown")
		}
		tcancel()
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
