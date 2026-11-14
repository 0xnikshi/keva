#!/usr/bin/env bash
#
# fetch-tools.sh — download the official prebuilt Prometheus and Grafana
# binaries into ./tools, so the native monitoring stack works without Homebrew
# or a working build toolchain. These are self-contained release archives from
# the projects' canonical download hosts.
#
#   make monitor-fetch
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLS="$ROOT/tools"
mkdir -p "$TOOLS"

PROM_VER="2.55.1"
GRAFANA_VER="11.3.0"

os="$(uname -s | tr '[:upper:]' '[:lower:]')" # darwin / linux
case "$(uname -m)" in
  arm64 | aarch64) arch="arm64" ;;
  x86_64 | amd64) arch="amd64" ;;
  *) echo "unsupported architecture: $(uname -m)"; exit 1 ;;
esac

fetch() { # fetch URL DEST
  echo "  downloading $(basename "$1")"
  curl -fL --retry 3 "$1" -o "$2"
}

# --- Prometheus -----------------------------------------------------------
PROM_DIR="$TOOLS/prometheus-${PROM_VER}.${os}-${arch}"
if [ ! -x "$PROM_DIR/prometheus" ]; then
  echo "Prometheus ${PROM_VER} (${os}-${arch}):"
  fetch "https://github.com/prometheus/prometheus/releases/download/v${PROM_VER}/prometheus-${PROM_VER}.${os}-${arch}.tar.gz" "$TOOLS/prometheus.tar.gz"
  tar -xzf "$TOOLS/prometheus.tar.gz" -C "$TOOLS"
  rm -f "$TOOLS/prometheus.tar.gz"
fi

# --- Grafana --------------------------------------------------------------
if [ -z "$(find "$TOOLS" -maxdepth 1 -type d -name 'grafana-*' 2>/dev/null)" ]; then
  echo "Grafana ${GRAFANA_VER} (${os}-${arch}):"
  fetch "https://dl.grafana.com/oss/release/grafana-${GRAFANA_VER}.${os}-${arch}.tar.gz" "$TOOLS/grafana.tar.gz"
  tar -xzf "$TOOLS/grafana.tar.gz" -C "$TOOLS"
  rm -f "$TOOLS/grafana.tar.gz"
fi

echo "Tools ready in ./tools — now run: make monitor-local"
