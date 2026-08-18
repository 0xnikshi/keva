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

## Request flow (target design)

A write travels: client → `server` → `raft` (propose) → replicate to a
majority → commit → each node's `store` applies the committed command.
Reads are served with a linearizable guarantee rather than reading local
state directly.

## Status

Phase 0 — project foundation and scaffolding. Package boundaries and the
domain types are in place; the engine, consensus, and network layers are
stubs to be implemented in subsequent phases.
