# Architecture

Keva is a distributed, strongly consistent key-value store. Writes are
replicated across a cluster using the Raft consensus algorithm, so the
system stays available and consistent as long as a majority of nodes are
reachable.

## Goals

- **Linearizable** reads and writes on top of Raft.
- **Durable** storage that survives process crashes and restarts.
- **Fault tolerant**: a cluster of `2f+1` nodes tolerates `f` failures.
- **Operable**: metrics, structured logs, and graceful shutdown.
- **Understandable**: a codebase that can be read end to end.

## Non-goals

- Not a general-purpose database — no secondary indexes, no query language.
- No multi-key transactions in the initial design.
- No cross-datacenter / multi-region replication.
- No pluggable storage backends; one engine, built in-tree.

## Components

| Package            | Responsibility                                            |
| ------------------ | --------------------------------------------------------- |
| `internal/keva`    | Core domain types shared across packages.                 |
| `internal/store`   | Durable storage engine: WAL, on-disk state, recovery.     |
| `internal/raft`    | Consensus: leader election, log replication, persistence. |
| `internal/server`  | Network layer; routes client requests to Raft and store.  |
| `internal/cluster` | Node membership, bootstrapping, discovery.                |
| `cmd/kevad`        | Server daemon.                                             |
| `cmd/keva`         | Command-line client.                                      |

## Request flow

A write travels: client → `server` → `raft` (propose) → replicate to a
majority → commit → each node's `store` applies the committed command. A
write is acknowledged only once it has committed. Reads go through the
leader's ReadIndex confirmation, so they reflect every acknowledged write
rather than reading possibly-stale local state.

## Status

Complete. The storage engine, Raft consensus, replicated KV, cluster
operations (snapshots, dynamic membership), correctness testing, operational
hardening, and performance work are all in place. The design is documented
chapter by chapter in [`docs/`](docs/); see [README.md](README.md) for a
feature overview, quickstart, and benchmarks.
