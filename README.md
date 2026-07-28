# DFS

A distributed file storage system built from scratch in Go: content-addressed chunking,
rendezvous-hashed placement, quorum replication, erasure coding, and self-healing repair —
behind an S3-compatible API.

> **Status:** Phases 0–2 complete. **It stores files.** Upload through the gateway and the
> object is chunked, hashed, deduplicated, written to a storage node, and committed in a
> single transaction; download reassembles it byte-identically with Range support. A 200 MiB
> file round-trips at 79 MiB/s up and 261 MiB/s down, and re-uploading identical content
> transfers zero bytes. Phase 3 (real distribution across multiple nodes) is next.

## Quick start

Requires Docker. A local Go toolchain is optional — every Make target runs in a container.

```bash
make dev          # start postgres + meta + node + gateway
make smoke        # Phase 0 gate: health, readiness, metrics
make phase1-gate  # Phase 1 gate: 1 GiB blob store round trip, live corruption
make phase2-gate  # Phase 2 gate: file round trip, dedup, ranged reads
make test         # unit tests with the race detector
make down         # stop and remove volumes
```

## Using it

```bash
make build                                   # builds ./bin/dfsctl
export DFS_ENDPOINT=http://localhost:8080

./bin/dfsctl mb photos
./bin/dfsctl cp ./holiday.jpg dfs://photos/2026/holiday.jpg
./bin/dfsctl ls photos/2026/
./bin/dfsctl stat dfs://photos/2026/holiday.jpg
./bin/dfsctl cp dfs://photos/2026/holiday.jpg ./restored.jpg
./bin/dfsctl rm dfs://photos/2026/holiday.jpg
```

Or over plain HTTP:

```bash
curl -X PUT localhost:8080/v1/b/photos
curl -X PUT --data-binary @holiday.jpg localhost:8080/v1/b/photos/o/2026/holiday.jpg
curl -r 0-1023 localhost:8080/v1/b/photos/o/2026/holiday.jpg   # ranged read
curl localhost:8080/v1/b/photos/o?prefix=2026/&delimiter=/     # listing
```

| Endpoint | URL |
|---|---|
| Gateway API | http://localhost:8080/v1/version |
| Coordinator admin | http://localhost:9100/healthz · /readyz · /metrics |
| Node 1 admin | http://localhost:9101/healthz |
| Gateway admin | http://localhost:9102/metrics |
| PostgreSQL | `make psql` |

## Documentation

| Doc | What's in it |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design principles, components, read/write paths, placement, failure handling, capacity plan, honest limitations |
| [ROADMAP.md](docs/ROADMAP.md) | 10 phases with acceptance gates, ~11–13 weeks part-time |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | VPS hardening, Compose topology, CI/CD, backups, runbook |

## What it will do

- **Chunked & content-addressed** — files split into 8 MiB chunks keyed by BLAKE3 hash; identical data is stored once, cluster-wide
- **Replicated** — `R=3` with `W=2` write quorum; any node can die with no data loss and no downtime
- **Erasure coded** — Reed–Solomon 4+2 for larger objects: 1.5× storage overhead instead of 3×, still tolerating 2 simultaneous failures
- **Self-healing** — heartbeat failure detection, automatic re-replication, background scrubbing that catches bitrot before it becomes loss
- **S3-compatible** — SigV4 auth, multipart uploads, presigned URLs; works with `aws-cli`, `rclone`, and every S3 SDK unmodified
- **Observable** — Prometheus metrics, Grafana dashboards, and a live cluster-topology UI

## Stack

Go · gRPC · PostgreSQL · BLAKE3 · Reed–Solomon · Docker Compose · Caddy · Next.js · Prometheus/Grafana/Loki

## Target deployment

Single Hostinger KVM2 VPS (2 vCPU · 8 GB · 100 GB NVMe · Debian) running 6 storage nodes,
2 gateways, coordinator, Postgres, and the observability stack as containers on an internal
Docker network. ~52 GB usable object capacity with erasure coding.

Designed for multi-host from day one — no localhost defaults anywhere, so adding a second
machine is a config change, not a rewrite.

## Honest limitations

On a single VPS, all six storage nodes share one physical disk. Replication therefore
protects against **process and container failure, not disk failure** — real durability comes
from off-site backups (restic → Backblaze B2, drilled quarterly). PostgreSQL is a single
point of failure: the metadata is what makes the chunks meaningful, so it is backed up hourly
and is the first thing to restore. Both are addressed by the multi-host and HA-metadata work
in Phase 9.

See [ARCHITECTURE.md §8](docs/ARCHITECTURE.md) for the full consistency and durability model.
