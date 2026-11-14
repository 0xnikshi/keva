// Command kevaload is a small, long-running load generator for a Keva cluster.
// It drives a steady mix of reads and writes across the given nodes and
// exposes client-observed metrics (throughput, latency, errors) at /metrics
// for Prometheus to scrape — the client's view of the cluster, which the
// servers' own metrics cannot provide.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/0xnikshi/keva/internal/api"
)

func main() {
	if err := run(); err != nil {
		slog.Error("kevaload exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	targetsFlag := flag.String("targets", "http://localhost:7070", "comma-separated node base URLs to spread load across")
	qps := flag.Int("qps", 50, "target operations per second")
	keys := flag.Int("keys", 100, "size of the key space")
	readRatio := flag.Float64("read-ratio", 0.8, "fraction of operations that are reads")
	metricsAddr := flag.String("metrics-addr", ":9100", "address to serve /metrics on")
	flag.Parse()

	targets := strings.Split(*targetsFlag, ",")
	for i := range targets {
		targets[i] = strings.TrimRight(strings.TrimSpace(targets[i]), "/")
	}
	if len(targets) == 0 || *qps <= 0 {
		return fmt.Errorf("need at least one target and qps > 0")
	}

	// Client-observed metrics, on this process's own registry.
	ops := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kevaload_ops_total",
		Help: "Client operations attempted, by op and outcome.",
	}, []string{"op", "outcome"})
	dur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kevaload_op_duration_seconds",
		Help:    "Client-observed operation latency in seconds, by op.",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})
	inflight := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kevaload_inflight",
		Help: "Operations currently in flight.",
	})
	reg := prometheus.NewRegistry()
	reg.MustRegister(ops, dur, inflight)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ms := &http.Server{Addr: *metricsAddr, Handler: metricsMux, ReadTimeout: 5 * time.Second}
	go func() {
		if err := ms.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: 3 * time.Second}
	// A bounded worker pool so a slow cluster cannot pile up unbounded
	// goroutines — excess ticks are shed, which also keeps load honest.
	sem := make(chan struct{}, 2**qps)
	ticker := time.NewTicker(time.Second / time.Duration(*qps))
	defer ticker.Stop()

	gen := &generator{client: client, targets: targets, keys: *keys, readRatio: *readRatio, ops: ops, dur: dur, inflight: inflight}

	slog.Info("kevaload running", "targets", targets, "qps", *qps, "metrics", *metricsAddr)
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = ms.Shutdown(shutdownCtx)
			slog.Info("kevaload stopped")
			return nil
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
				go func() { defer func() { <-sem }(); gen.op() }()
			default:
				// Saturated: skip this tick rather than queue without bound.
			}
		}
	}
}

type generator struct {
	client    *http.Client
	targets   []string
	keys      int
	readRatio float64
	ops       *prometheus.CounterVec
	dur       *prometheus.HistogramVec
	inflight  prometheus.Gauge
}

// op performs one read or write against a randomly chosen node and records the
// client-observed latency and outcome.
func (g *generator) op() {
	g.inflight.Inc()
	defer g.inflight.Dec()

	base := g.targets[rand.IntN(len(g.targets))]
	key := fmt.Sprintf("k%d", rand.IntN(g.keys))
	url := base + "/v1/kv/" + key

	start := time.Now()
	if rand.Float64() < g.readRatio {
		resp, err := g.client.Get(url)
		g.dur.WithLabelValues("get").Observe(time.Since(start).Seconds())
		g.ops.WithLabelValues("get", getOutcome(resp, err)).Inc()
		drain(resp)
		return
	}

	body, _ := json.Marshal(api.PutRequest{Value: []byte("v:" + key)})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	resp, err := g.client.Do(req)
	g.dur.WithLabelValues("put").Observe(time.Since(start).Seconds())
	g.ops.WithLabelValues("put", putOutcome(resp, err)).Inc()
	drain(resp)
}

// getOutcome treats a found value and a legitimate miss as success; anything
// else (no leader, timeout, error) is a failure the dashboard should show.
func getOutcome(resp *http.Response, err error) string {
	switch {
	case err != nil:
		return "error"
	case resp.StatusCode == http.StatusOK, resp.StatusCode == http.StatusNotFound:
		return "ok"
	default:
		return "error"
	}
}

func putOutcome(resp *http.Response, err error) string {
	switch {
	case err != nil:
		return "error"
	case resp.StatusCode == http.StatusNoContent:
		return "ok"
	default:
		return "error"
	}
}

func drain(resp *http.Response) {
	if resp != nil {
		_ = resp.Body.Close()
	}
}
