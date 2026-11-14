BIN_DIR := bin
PKGS    := ./...
GO      := go

.PHONY: all build build-daemon build-cli build-load demo monitor monitor-down monitor-deps monitor-fetch monitor-local test test-race cover vet fmt fmt-check lint tidy clean help

all: build

## build: compile all binaries into bin/
build: build-daemon build-cli build-load

build-daemon:
	$(GO) build -o $(BIN_DIR)/kevad ./cmd/kevad

build-cli:
	$(GO) build -o $(BIN_DIR)/keva ./cmd/keva

build-load:
	$(GO) build -o $(BIN_DIR)/kevaload ./cmd/kevaload

## demo: run the fault-tolerance demo (3-node cluster, then kill the leader)
demo: build
	@./scripts/demo.sh

## monitor: run the full stack — cluster + load gen + Prometheus + Grafana
monitor:
	docker compose up --build

## monitor-down: stop the stack and remove its volumes
monitor-down:
	docker compose down -v

## monitor-fetch: download official Prometheus & Grafana binaries into ./tools
monitor-fetch:
	@./scripts/fetch-tools.sh

## monitor-deps: install Prometheus and Grafana via Homebrew (alternative to fetch)
monitor-deps:
	brew install prometheus grafana

## monitor-local: run the full stack natively, no Docker (Ctrl-C to stop)
monitor-local: build
	@./scripts/monitor-local.sh

## test: run the test suite
test:
	$(GO) test $(PKGS)

## test-race: run the test suite with the race detector
test-race:
	$(GO) test -race $(PKGS)

## cover: run tests and print total coverage
cover:
	$(GO) test -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

## vet: run go vet
vet:
	$(GO) vet $(PKGS)

## fmt: format all Go code
fmt:
	$(GO) fmt $(PKGS)

## fmt-check: fail if any file is not gofmt-clean
fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "unformatted files:"; gofmt -l .; exit 1; }

## lint: run golangci-lint
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed — https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	fi

## tidy: sync go.mod and go.sum
tidy:
	$(GO) mod tidy

## clean: remove build and coverage artifacts
clean:
	rm -rf $(BIN_DIR) coverage.out

## help: list available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
