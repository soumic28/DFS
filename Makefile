# DFS build and development tasks.
#
# Every target works without a local Go toolchain by running inside Docker.
# If you do install Go, set LOCAL_GO=1 to use it directly — it is much faster.

SHELL := /bin/sh

MODULE      := github.com/soumi/dfs
GO_VERSION  := 1.25
SERVICES    := dfs-meta dfs-node dfs-gateway dfsctl
COMPOSE_DEV := deploy/compose/docker-compose.dev.yml

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# Run the Go toolchain in a container, caching modules and build output on the
# host so repeated runs are not repeatedly slow.
DOCKER_GO := docker run --rm \
	-v "$(CURDIR)":/src -w /src \
	-v dfs-gocache:/go/pkg/mod \
	-v dfs-gobuild:/root/.cache/go-build \
	-e CGO_ENABLED=0 \
	golang:$(GO_VERSION)-alpine go

ifeq ($(LOCAL_GO),1)
GO := go
else
GO := $(DOCKER_GO)
endif

.DEFAULT_GOAL := help

## help: list available targets
help:
	@echo "DFS targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | awk -F': ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

## tidy: resolve dependencies and write go.sum
tidy:
	$(GO) mod tidy

## build: compile all binaries into ./bin
build:
	@mkdir -p bin
	@for svc in $(SERVICES); do \
		echo "building $$svc"; \
		$(GO) build -trimpath \
			-ldflags="-s -w -X $(MODULE)/internal/app.Version=$(VERSION) -X $(MODULE)/internal/app.Commit=$(COMMIT)" \
			-o bin/$$svc ./cmd/$$svc || exit 1; \
	done

## test: run unit tests with the race detector
test:
	$(GO) test -race -count=1 ./...

## cover: run tests and write coverage.out
cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1

## vet: run go vet
vet:
	$(GO) vet ./...

## lint: run golangci-lint (in Docker; no local install needed)
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src \
		-v dfs-gocache:/go/pkg/mod \
		golangci/golangci-lint:v2.1-alpine golangci-lint run --timeout 5m

## proto: generate Go code from api/proto into api/gen
proto:
	docker run --rm -v "$(CURDIR)":/src -w /src/api/proto \
		bufbuild/buf:latest generate

## proto-lint: lint the protobuf definitions
proto-lint:
	docker run --rm -v "$(CURDIR)":/src -w /src/api/proto \
		bufbuild/buf:latest lint

## images: build all service images tagged with the current version
images:
	@for svc in dfs-meta dfs-node dfs-gateway; do \
		echo "building image $$svc"; \
		docker build -f deploy/docker/Dockerfile \
			--build-arg SERVICE=$$svc \
			--build-arg VERSION=$(VERSION) \
			--build-arg COMMIT=$(COMMIT) \
			-t dfs/$$svc:$(VERSION) -t dfs/$$svc:latest . || exit 1; \
	done

## dev: start the local stack
dev:
	docker compose -f $(COMPOSE_DEV) up --build -d
	@echo
	@echo "gateway   http://localhost:8080/v1/version"
	@echo "meta      http://localhost:9100/healthz"
	@echo "node-1    http://localhost:9101/healthz"
	@echo "gateway   http://localhost:9102/metrics"

## dev-logs: follow logs from the local stack
dev-logs:
	docker compose -f $(COMPOSE_DEV) logs -f

## ps: show the local stack's status
ps:
	docker compose -f $(COMPOSE_DEV) ps

## smoke: verify every service reports healthy (the Phase 0 gate)
smoke:
	@sh deploy/scripts/smoke.sh

## phase1-gate: 1 GiB round trip, dedup and live corruption against a real node
# The node's data volume is mounted so the corruption test can rot a chunk file
# underneath the running node, exactly as a failing disk would.
phase1-gate:
	docker run --rm --network dfs-dev_dfs \
		-v "$(CURDIR)":/src -w /src \
		-v dfs-gocache:/go/pkg/mod -v dfs-gobuild:/root/.cache/go-build \
		-v dfs-dev_node1-data:/nodedata \
		-e NODE_ADDR=dfs-node-1:9091 \
		-e NODE_DATA_DIR=/nodedata \
		-e SIZE_MB=$(or $(SIZE_MB),1024) \
		golang:$(GO_VERSION)-alpine \
		go test -tags gate -v -count=1 -timeout 20m ./test/gate/

## down: stop the local stack and delete its volumes
down:
	docker compose -f $(COMPOSE_DEV) down -v

## clean: remove build output and caches
clean:
	rm -rf bin dist coverage.out
	-docker volume rm dfs-gocache dfs-gobuild

.PHONY: help tidy build test cover vet lint proto proto-lint images dev dev-logs ps smoke phase1-gate down clean
