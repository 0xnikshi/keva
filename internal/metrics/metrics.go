// Package metrics exposes a node's activity in the Prometheus text format. It
// is deliberately isolated here so the consensus and server packages stay free
// of any metrics dependency: the daemon wires request instrumentation and a
// Raft-state collector onto its own registry, and serves them at /metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RaftStats is a snapshot of the Raft state worth exporting. The daemon adapts
// its node's status into this, so this package never imports the raft package.
type RaftStats struct {
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	IsLeader    bool
}

// Metrics owns a private registry and the collectors registered on it. A
// private registry (rather than the global default) lets several nodes run in
// one process — as the tests do — without colliding on metric registration.
type Metrics struct {
	reg          *prometheus.Registry
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

// New builds a registry carrying Go-runtime and process metrics, HTTP request
// counters and latency histograms, and a collector that reads live Raft state
// via raftStats on every scrape.
func New(raftStats func() RaftStats) *Metrics {
	reg := prometheus.NewRegistry()

	// Counter: monotonically increasing. Labelled by method and response code,
	// so a rate() in PromQL yields per-status request throughput.
	httpRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "keva_http_requests_total",
		Help: "Total HTTP requests handled, by method and response code.",
	}, []string{"method", "code"})

	// Histogram: bucketed latencies, so PromQL can derive p50/p95/p99. The
	// label names "method" and "code" are what promhttp fills in for us.
	httpDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "keva_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds, by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		httpRequests,
		httpDuration,
		newRaftCollector(raftStats),
	)

	return &Metrics{reg: reg, httpRequests: httpRequests, httpDuration: httpDuration}
}

// Handler serves the metrics in the Prometheus exposition format for scraping.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// InstrumentHandler wraps next so every request it serves is counted and timed.
// promhttp fills the method/code labels from the request and response.
func (m *Metrics) InstrumentHandler(next http.Handler) http.Handler {
	return promhttp.InstrumentHandlerCounter(m.httpRequests,
		promhttp.InstrumentHandlerDuration(m.httpDuration, next))
}

// raftCollector reports Raft state by calling raftStats at scrape time, so the
// exported values are always current without a background updater. This is the
// idiomatic "collect on scrape" custom collector.
type raftCollector struct {
	stats                             func() RaftStats
	term, commit, applied, leaderDesc *prometheus.Desc
}

func newRaftCollector(stats func() RaftStats) *raftCollector {
	return &raftCollector{
		stats:      stats,
		term:       prometheus.NewDesc("keva_raft_term", "Current Raft term.", nil, nil),
		commit:     prometheus.NewDesc("keva_raft_commit_index", "Highest committed log index.", nil, nil),
		applied:    prometheus.NewDesc("keva_raft_last_applied", "Highest applied log index.", nil, nil),
		leaderDesc: prometheus.NewDesc("keva_raft_is_leader", "1 if this node is the leader, else 0.", nil, nil),
	}
}

func (c *raftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.term
	ch <- c.commit
	ch <- c.applied
	ch <- c.leaderDesc
}

func (c *raftCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.stats()
	ch <- prometheus.MustNewConstMetric(c.term, prometheus.GaugeValue, float64(s.Term))
	ch <- prometheus.MustNewConstMetric(c.commit, prometheus.GaugeValue, float64(s.CommitIndex))
	ch <- prometheus.MustNewConstMetric(c.applied, prometheus.GaugeValue, float64(s.LastApplied))
	leader := 0.0
	if s.IsLeader {
		leader = 1
	}
	ch <- prometheus.MustNewConstMetric(c.leaderDesc, prometheus.GaugeValue, leader)
}
