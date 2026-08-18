# Keva

A distributed, strongly consistent key-value store written in Go, backed
by the Raft consensus algorithm.

> **Status:** early development. The project foundation is in place; the
> storage engine, consensus, and network layers are being built out phase
> by phase. See [ARCHITECTURE.md](ARCHITECTURE.md).

## Getting started

Requires Go (see the version in [`go.mod`](go.mod)).

```bash
make build        # compile both binaries into ./bin
./bin/kevad       # start the server daemon
./bin/keva        # run the client
```

## Development

```bash
make test         # run the test suite
make test-race    # run tests with the race detector
make vet          # go vet
make fmt          # format the code
make lint         # golangci-lint
make cover        # test coverage total
make help         # list all targets
```

## Project layout

```
cmd/
  keva/        client CLI
  kevad/       server daemon
internal/
  keva/        core domain types
  store/       storage engine
  raft/        consensus
  server/      network layer
  cluster/     membership & discovery
```

## Roadmap

Development proceeds in phases: storage engine → network layer →
Raft consensus → replicated KV → cluster operations → correctness
testing → hardening → performance. Details in
[ARCHITECTURE.md](ARCHITECTURE.md).
