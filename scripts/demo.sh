#!/usr/bin/env bash
#
# demo.sh — stand up a three-node Keva cluster, show consistent reads and
# writes through any node, then kill the leader and watch the cluster elect a
# new one and keep serving. Everything runs locally and cleans up on exit.
#
#   scripts/demo.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KEVAD="$ROOT/bin/kevad"
KEVA="$ROOT/bin/keva"
DATA="$(mktemp -d "${TMPDIR:-/tmp}/keva-demo.XXXXXX")"

NODES=(n1 n2 n3)
PORTS=(7070 7071 7072)
PIDS=("" "" "")

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

cleanup() {
  for pid in "${PIDS[@]}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT INT TERM

# index_of NODE -> echoes the array index, or returns 1.
index_of() {
  for i in "${!NODES[@]}"; do
    [ "${NODES[$i]}" = "$1" ] && { echo "$i"; return 0; }
  done
  return 1
}

# peers_for NODE -> "other=host:port,other=host:port"
peers_for() {
  local self="$1" out=""
  for i in "${!NODES[@]}"; do
    [ "${NODES[$i]}" = "$self" ] && continue
    out="${out:+$out,}${NODES[$i]}=localhost:${PORTS[$i]}"
  done
  echo "$out"
}

status_role() { # status_role PORT -> role string ("" if unreachable)
  curl -s "http://localhost:$1/v1/status" 2>/dev/null \
    | grep -o '"role":"[^"]*"' | cut -d'"' -f4
}

# leader_id -> echoes the node currently reporting itself leader, or "".
leader_id() {
  for i in "${!NODES[@]}"; do
    [ -z "${PIDS[$i]}" ] && continue
    [ "$(status_role "${PORTS[$i]}")" = "leader" ] && { echo "${NODES[$i]}"; return 0; }
  done
  echo ""
}

wait_for_leader() { # wait up to ~10s for a single leader; echoes its id
  for _ in $(seq 1 50); do
    local l; l="$(leader_id)"
    [ -n "$l" ] && { echo "$l"; return 0; }
    sleep 0.2
  done
  echo ""
}

# kv_put / kv_get retry briefly, since a write right after an election may be
# redirected while the cluster settles — exactly what a real client does.
kv_put() { # kv_put PORT KEY VALUE
  for _ in $(seq 1 25); do
    "$KEVA" --addr "http://localhost:$1" put "$2" "$3" 2>/dev/null && return 0
    sleep 0.2
  done
  return 1
}
kv_get() { # kv_get PORT KEY
  for _ in $(seq 1 25); do
    out="$("$KEVA" --addr "http://localhost:$1" get "$2" 2>/dev/null)" && { echo "$out"; return 0; }
    sleep 0.2
  done
  return 1
}

[ -x "$KEVAD" ] && [ -x "$KEVA" ] || { say "Building binaries"; make -C "$ROOT" build; }

say "Starting a 3-node cluster"
for i in "${!NODES[@]}"; do
  "$KEVAD" --id "${NODES[$i]}" --addr ":${PORTS[$i]}" \
    --peers "$(peers_for "${NODES[$i]}")" --data "$DATA/${NODES[$i]}" \
    >"$DATA/${NODES[$i]}.log" 2>&1 &
  PIDS[$i]=$!
  note "${NODES[$i]} on :${PORTS[$i]} (pid ${PIDS[$i]})"
done

leader="$(wait_for_leader)"
[ -n "$leader" ] || { echo "no leader elected"; exit 1; }
say "Leader elected: $leader"

say "Write to a follower — it forwards to the leader"
follower="n1"; [ "$leader" = "n1" ] && follower="n2"
fi=$(index_of "$follower")
kv_put "${PORTS[$fi]}" city pune
note "put city=pune via $follower"

say "Read the key back from every node"
for i in "${!NODES[@]}"; do
  note "${NODES[$i]}: city = $(kv_get "${PORTS[$i]}" city)"
done

say "Kill the leader ($leader) and watch failover"
li=$(index_of "$leader")
kill "${PIDS[$li]}" 2>/dev/null || true
PIDS[$li]=""
note "stopped $leader"

new_leader="$(wait_for_leader)"
[ -n "$new_leader" ] || { echo "no new leader elected after failure"; exit 1; }
say "New leader elected: $new_leader"

say "The surviving cluster still serves"
si=""
for i in "${!NODES[@]}"; do [ -n "${PIDS[$i]}" ] && { si=$i; break; }; done
kv_put "${PORTS[$si]}" city mumbai
note "put city=mumbai via ${NODES[$si]}"
for i in "${!NODES[@]}"; do
  [ -z "${PIDS[$i]}" ] && continue
  note "${NODES[$i]}: city = $(kv_get "${PORTS[$i]}" city)"
done

say "Done — the cluster survived a leader failure with no data loss"
