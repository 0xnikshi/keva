#!/usr/bin/env bash
#
# monitor-local.sh — run the whole observability stack natively, no Docker:
# three Keva nodes, the load generator, Prometheus, and Grafana (with the
# provisioned dashboard). Everything runs as a background process under one
# temp directory and is torn down on Ctrl-C.
#
#   make monitor-local      # or: scripts/monitor-local.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KEVAD="$ROOT/bin/kevad"
KEVALOAD="$ROOT/bin/kevaload"
RUN="$(mktemp -d "${TMPDIR:-/tmp}/keva-monitor.XXXXXX")"
PIDS=()

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

cleanup() {
  trap - EXIT INT TERM # disarm so cleanup runs exactly once
  say "Shutting down"
  for pid in "${PIDS[@]:-}"; do
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$RUN"
}
# Ctrl-C / TERM just exit; the EXIT trap then runs cleanup a single time and
# the script actually terminates (rather than looping back into the wait).
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- dependencies ---------------------------------------------------------
# Prefer the self-contained binaries fetched into ./tools; fall back to PATH
# (e.g. a Homebrew install) if present.
PROM_BIN="$(find "$ROOT/tools" -maxdepth 2 -type f -name prometheus 2>/dev/null | head -1 || true)"
[ -n "$PROM_BIN" ] || PROM_BIN="$(command -v prometheus 2>/dev/null || true)"
[ -n "$PROM_BIN" ] || {
  echo "Prometheus not found. Download it with:  make monitor-fetch"
  exit 1
}

GRAFANA_HOME="$(find "$ROOT/tools" -maxdepth 1 -type d -name 'grafana-*' 2>/dev/null | head -1 || true)"
if [ -n "$GRAFANA_HOME" ]; then
  GRAFANA_BIN="$GRAFANA_HOME/bin/grafana"
  [ -x "$GRAFANA_BIN" ] || GRAFANA_BIN="$GRAFANA_HOME/bin/grafana-server"
else
  GRAFANA_BIN="$(command -v grafana 2>/dev/null || command -v grafana-server 2>/dev/null || true)"
  GRAFANA_HOME="$(brew --prefix 2>/dev/null)/opt/grafana/share/grafana"
fi
[ -x "$GRAFANA_BIN" ] || {
  echo "Grafana not found. Download it with:  make monitor-fetch"
  exit 1
}

[ -x "$KEVAD" ] && [ -x "$KEVALOAD" ] || make -C "$ROOT" build

# --- cluster + load generator --------------------------------------------
say "Starting 3 Keva nodes + load generator"
"$KEVAD" --id n1 --addr :7070 --peers n2=localhost:7071,n3=localhost:7072 --data "$RUN/n1" >"$RUN/n1.log" 2>&1 &
PIDS+=($!)
"$KEVAD" --id n2 --addr :7071 --peers n1=localhost:7070,n3=localhost:7072 --data "$RUN/n2" >"$RUN/n2.log" 2>&1 &
PIDS+=($!)
"$KEVAD" --id n3 --addr :7072 --peers n1=localhost:7070,n2=localhost:7071 --data "$RUN/n3" >"$RUN/n3.log" 2>&1 &
PIDS+=($!)
sleep 2
"$KEVALOAD" --targets http://localhost:7070,http://localhost:7071,http://localhost:7072 --qps 80 --metrics-addr :9100 >"$RUN/load.log" 2>&1 &
PIDS+=($!)

# --- Prometheus -----------------------------------------------------------
say "Starting Prometheus on :9090"
"$PROM_BIN" --config.file="$ROOT/deploy/prometheus.local.yml" \
  --storage.tsdb.path="$RUN/prometheus" \
  --web.listen-address=:9090 >"$RUN/prometheus.log" 2>&1 &
PIDS+=($!)

# --- Grafana --------------------------------------------------------------
# Generate provisioning that points at localhost Prometheus and the repo's
# dashboard by absolute path (the committed provisioning targets Docker DNS).
PROV="$RUN/provisioning"
mkdir -p "$PROV/datasources" "$PROV/dashboards"
cat >"$PROV/datasources/ds.yml" <<YML
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://localhost:9090
    isDefault: true
YML
cat >"$PROV/dashboards/prov.yml" <<YML
apiVersion: 1
providers:
  - name: Keva
    type: file
    options:
      path: $ROOT/deploy/grafana/dashboards
YML

export GF_PATHS_PROVISIONING="$PROV"
export GF_PATHS_DATA="$RUN/grafana-data"
export GF_PATHS_LOGS="$RUN/grafana-logs"
export GF_SERVER_HTTP_PORT=3000
export GF_AUTH_ANONYMOUS_ENABLED=true
export GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
export GF_AUTH_DISABLE_LOGIN_FORM=true

say "Starting Grafana on :3000"
if [ "$(basename "$GRAFANA_BIN")" = "grafana" ]; then
  "$GRAFANA_BIN" server --homepath "$GRAFANA_HOME" >"$RUN/grafana.log" 2>&1 &
else
  "$GRAFANA_BIN" --homepath="$GRAFANA_HOME" >"$RUN/grafana.log" 2>&1 &
fi
PIDS+=($!)

# --- wait for Grafana to be ready -----------------------------------------
printf '   waiting for Grafana'
for _ in $(seq 1 40); do
  if curl -fs http://localhost:3000/api/health >/dev/null 2>&1; then break; fi
  printf '.'
  sleep 0.5
done
printf '\n'

say "Up! Open these in your browser:"
echo "   Grafana      → http://localhost:3000   (dashboard: Keva Cluster)"
echo "   Prometheus   → http://localhost:9090"
echo "   Node metrics → http://localhost:7070/metrics"
echo
echo "   Logs:  $RUN"
echo "   Tip:   stop the current leader to watch failover on the dashboard —"
echo "          curl -s localhost:7070/v1/status | grep leader_id"
echo
echo "   Press Ctrl-C to stop everything."

# Block until a signal arrives; the traps then tear everything down. A trapped
# signal makes `wait` return immediately, so Ctrl-C stops the stack at once.
wait
