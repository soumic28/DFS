# DFS — Build Roadmap

Ten phases. Each has a **hard acceptance test** — a thing you can run that either passes or
doesn't. Do not move to the next phase until the current one's test passes, because every
phase's correctness depends on the one below it being trustworthy.

Estimates assume **~10–15 hours/week**. Full-time, divide by three.

| Phase | Theme | Est. | Gate |
|---|---|---|---|
| 0 | Foundations & dev loop | ✅ **done** | `docker compose up` → 3 healthy services |
| 1 | Single-node blob store | ✅ **done** | 1 GB round-trips byte-identical, survives kill -9 |
| 2 | Metadata + first E2E | ✅ **done** | `dfsctl cp` uploads and downloads a real file |
| 3 | Real distribution | 1.5–2 wk | Kill a node mid-download; read still succeeds |
| 4 | Self-healing | 1–1.5 wk | Cluster returns to R=3 alone; scrubber fixes bitrot |
| 5 | Auth + S3 API | 2 wk | `aws s3 cp` and `rclone sync` work unmodified |
| 6 | Erasure coding | 1–1.5 wk | Overhead 3.0× → 1.5×; survives 2 simultaneous node kills |
| 7 | Dashboard + observability | 1.5 wk | Grafana shows the cluster; UI uploads with progress |
| 8 | Production deployment | 1.5 wk | Live on HTTPS; restore drill passes from backup alone |
| 9 | Hardening & stretch | ongoing | Chaos suite green |

**Total to a live, credible system: ~11–13 weeks part-time.**

---

## Phase 0 — Foundations & dev loop ✅ DONE
*Boring, and it pays for itself by week three.*

- [x] `go mod init github.com/soumi/dfs`, Go 1.25, repo layout per ARCHITECTURE §12
- [x] `buf` configured; `api/proto/storage/v1` and `metadata/v1` define the full internal contract; `make proto` generates via Docker
- [x] Config from environment (`internal/config`) — **no localhost defaults anywhere**; every peer address uses `Required`. This one rule is what makes the multi-host move painless later.
- [x] `internal/obs`: `log/slog` JSON handler, request-ID correlation, RED metrics, `/healthz` + `/readyz` + `/metrics` on every binary
- [x] Graceful shutdown: SIGTERM → fail readiness → drain listener → cleanup in reverse order → exit 0
- [x] Multi-stage Dockerfile, `gcr.io/distroless/static` final stage, non-root, **~19 MB images**
- [x] `deploy/compose/docker-compose.dev.yml`
- [x] GitHub Actions: tidy check, vet, race tests, golangci-lint, buf lint + breaking, image build, smoke test
- [x] `Makefile`: every target runs in Docker, so no local Go toolchain is required

**Gate: PASSING** — `make dev` && `make smoke`:

```
Health:      dfs-meta OK · dfs-node-1 OK · dfs-gateway OK
Readiness:   dfs-meta OK · dfs-node-1 OK · dfs-gateway OK
API:         gateway /v1/version → {"commit":"unknown","version":"dev"}
Metrics:     dfs_build_info present on all three
Phase 0 gate: PASS
```

**Notes from the build, worth remembering:**

- Distroless has no shell, so the binaries probe themselves: `/service probe` hits the
  admin endpoint and exits 0/1. That is what Compose `healthcheck` invokes.
- The metrics route label is written by an inner handler and read by an outer middleware,
  which does *not* work through a plain context value — `r.WithContext` derives a new
  request the outer layer never sees. `internal/obs` uses a mutable slot instead. Without
  it every series collapses to `route="unmatched"` and nothing fails loudly.
  See `TestMetricsSeesRouteLabelSetDownstream`.
- `/data` is created in the build stage and `COPY --chown=nonroot`'d into the image, because
  Docker seeds a new named volume's ownership from the image path. Otherwise the volume is
  root-owned and a non-root storage node cannot write a single chunk (this bites in Phase 1).
- Inbound `X-Request-Id` is validated as hex before use — it lands in every log line, so an
  unvalidated one is a log-injection vector.

---

## Phase 1 — Single-node blob store ✅ DONE
*The foundation. Everything else stands on it.*

`dfs-node` is complete, with no cluster awareness at all.

- [x] `internal/blobstore`: content-addressed disk layout (`chunks/ab/cd/<hash>.chunk`)
- [x] `internal/chunk`: BLAKE3 streaming hasher, `VerifyingReader`, `HashingWriter`
- [x] **Crash-safe write:** temp file → `fsync(file)` → verify hash → `rename(2)` → `fsync(dir)`
- [x] BoltDB index: `chunk_id → {size, created_at, last_scrubbed_at}`, fixed 24-byte records
- [x] gRPC `PutChunk` / `GetChunk` / `StatChunk` / `DeleteChunk` / `PullChunk`, streamed in 256 KiB frames
- [x] Checksum verification on every full read; mismatch → `codes.DataLoss`
- [x] Capacity enforced by **reservation**, not a bare check → `RESOURCE_EXHAUSTED`
- [x] Boot recovery: purge `tmp/`, reconcile the index against disk **in both directions**
- [x] Scrubber goroutine: byte-paced sweep, least-recently-scrubbed first, reports without deleting

**Gate: PASSING** — `make dev && make phase1-gate`:

```
TestGigabyteRoundTrip           PASS   1.00 GiB round-tripped byte-identical
TestDeduplicationOverTheWire    PASS   second upload transferred 0 bytes
TestNodeRejectsMisdeclaredChunk PASS   InvalidArgument
TestNodeReportsCapacity         PASS
TestLiveCorruptionIsDetected    PASS   DataLoss; refused to serve rotted bytes
```

Plus `go test -race ./...`, including `TestSurvivesSIGKILLDuringWrite`, which re-executes
the test binary as a child, kills it with SIGKILL mid-write, and asserts the reopened store
is consistent. Simulating a crash with `Close()` would prove nothing — `Close` is exactly
what a real crash never runs.

**Measured throughput** (Docker Desktop on Windows, volume through a VM filesystem):
cold write **50–65 MB/s**, read **~400 MB/s**. Writes are fsync-bound — two fsyncs per
chunk, one for the file and one for the directory — and will be materially faster on the
VPS's native NVMe. Re-measure there before quoting a number anywhere. A repeat run reports
300+ MB/s "upload", but that is deduplication returning without writing; only a run against
an empty volume measures anything real.

**Notes from the build, worth remembering:**

- **Recovery must run in both directions.** A crash can land between `rename(2)` and the
  index write, leaving a valid chunk file with no index entry. That file was hash-verified
  before the rename, so it is real data and gets *adopted*. Discarding it would be silent
  data loss on a routine restart. The reverse — an index entry with no file — is a lie, so
  the entry is dropped and reads report a miss and fail over. See `recovery_test.go`.
- **Fault attribution in error codes is load-bearing.** A hash mismatch on `put` means the
  *uploader* sent bytes not matching the name it declared → `InvalidArgument`. The same
  mismatch on `get` means *this node's disk* has rotted → `DataLoss`. Returning `DataLoss`
  for a bad upload would make the coordinator suspect a healthy node and schedule repairs it
  does not need. This was a real bug, caught by the gate.
- **Capacity is reserved, not checked.** A plain `used + size > capacity` test lets N
  concurrent uploads all pass a check only one could satisfy. Reservations are taken before
  the write and released after, and `TestFailedWriteReleasesReservation` guards the leak that
  would otherwise slowly wedge a node into refusing everything.
- **A corrupt chunk is reported, never deleted.** It still proves the chunk was placed here;
  removing it before a replacement exists lowers durability rather than restoring it.
- Losing `index.db` entirely costs a rescan, not the data — the chunk files carry their own
  identity in their names. `TestRecoveryRebuildsAfterIndexLoss` proves it.

---

## Phase 2 — Metadata service + first end-to-end path ✅ DONE
*The moment it became a real system.*

Still a single storage node — the whole pipeline works before distribution is added.

- [x] Postgres 16 schema + embedded goose migrations, applied at coordinator startup
- [x] `sqlc` generating the type-safe query layer from `db/queries/` into `internal/meta/dbgen`
- [x] `dfs-meta` gRPC: `CreateBucket`, `AllocateChunk`, `CommitObject`, `LookupObject`, `ListObjects`, `DeleteObject`, `RegisterNode`, `ClusterStatus`, `ReportBadReplica`
- [x] `CommitObject` is **one transaction**: demote previous version, insert object, insert `object_chunks`, upsert placements, bump refcounts
- [x] `internal/chunk`: streaming splitter at 8 MiB that never buffers the whole object
- [x] `dfs-gateway` native REST: `PUT/GET/HEAD/DELETE /v1/b/{bucket}/o/{key...}`, `GET /v1/b/{bucket}/o`
- [x] HTTP Range support — chunk-selective, only fetches chunks overlapping the range
- [x] Dedup: `AllocateChunk` returns `already_exists`, gateway skips the upload **and the transfer**
- [x] `dfsctl`: `mb`, `cp`, `cat`, `ls`, `rm`, `stat`

**Gate: PASSING** — `make dev && make phase2-gate`:

```
1. bucket creation                PASS
2. 200 MiB round trip             PASS  byte-identical (sha256 52013f38...)
3. deduplication on re-upload     PASS  25/25 chunks deduplicated, 0 B transferred
4. ranged reads                   PASS  crossing a chunk boundary, leading, suffix, 206
5. listing                        PASS  prefix + delimiter rollup
6. stat and delete                PASS  404 after delete; shared chunks survived
```

Throughput through the full pipeline: **79 MiB/s up, 261 MiB/s down** (Docker Desktop on
Windows). A re-upload of identical content completes at 287 MiB/s having sent **zero bytes**.

**Notes from the build, worth remembering:**

- **The unique partial index is the consistency mechanism.**
  `CREATE UNIQUE INDEX ... ON objects (bucket_id, key) WHERE is_latest` means the database,
  not the application, guarantees exactly one current version per key. `CommitObject` must
  demote the old version *before* inserting the new one, inside the same transaction — so
  there is no instant where a key has zero or two current versions.
- **The fan-out has to copy the chunk buffer.** The splitter reuses one buffer per chunk;
  the parallel writers read it concurrently while the next iteration overwrites it. Skipping
  that copy corrupts data only under concurrency and only under load — the worst kind of bug
  to find in production. It is the single copy the pipeline pays for.
- **Never fail over to another replica after bytes have been sent.** `readChunk` retries a
  different node only when zero bytes have reached the client; otherwise it aborts. Retrying
  mid-stream would splice two partial responses into one body and silently corrupt the
  download.
- **A read that fails its checksum is the fastest corruption signal available** — much
  faster than the scrubber's next sweep. The gateway reports `DataLoss` to the coordinator
  before failing over, which marks the placement corrupt for Phase 4's repair queue.
- **Deletes are tombstones and refcounts are per version.** Deleting one object must never
  dereference chunks another object shares — the gate proves this by deleting an object and
  then re-reading its deduplicated twin.
- Keyset pagination on `key`, not `OFFSET`: stable under concurrent writes, and the
  continuation token is just the last key returned.

---

## Phase 3 — Real distribution
*1.5–2 weeks* · **The heart of the project.**

- [ ] Node registration + bidirectional heartbeat stream (status up, commands down)
- [ ] `nodes` table with lifecycle state machine; `dfsctl cluster status` renders it
- [ ] `internal/placement`: weighted rendezvous hashing, unit-tested for:
      - determinism, capacity-proportional distribution
      - **minimal movement** — remove one node from a 6-node cluster, assert < ~20% of keys move
- [ ] Gateway fan-out: parallel `PutChunk` to R=3, ack at W=2, third continues in background
- [ ] Partial-failure handling: < W acks → fail the write cleanly, no dangling metadata
- [ ] Parallel/pipelined reads with automatic failover to the next replica
- [ ] Bad-replica reporting: checksum mismatch on read → mark `corrupt`, enqueue repair, retry elsewhere
- [ ] Scale Compose to 6 `dfs-node` replicas, each with its own volume and hard cap

**Gate — the money demo:** start a 5 GB download, `docker stop dfs-node-3` halfway through,
download completes with a correct checksum and the client never sees an error. Confirm chunks
are actually spread across nodes (`dfsctl cluster status` shows balanced `used_bytes`).

---

## Phase 4 — Self-healing
*1–1.5 weeks*

- [ ] Failure detector: `live → suspect (10s) → dead (30s)`, with hysteresis so a GC pause doesn't trigger a repair storm
- [ ] `repair_queue` with priorities and exponential backoff on `attempts`
- [ ] Repair workers: pick target via rendezvous excluding current holders, issue `PullChunk` over the heartbeat stream
- [ ] `PullChunk` on the node: **node-to-node transfer**, bytes never touch gateway or meta
- [ ] Rate limiting: bounded concurrent transfers + aggregate bandwidth cap — non-negotiable on a 2-vCPU box
- [ ] Rebalancer: on node join, migrate chunks whose ideal placement changed (low priority, slow drip)
- [ ] `dfsctl node drain <id>`: evacuate all chunks, then allow safe removal
- [ ] GC: chunks at `refcount = 0` older than a 24 h grace period get deleted
- [ ] Metrics: `dfs_chunks_under_replicated`, `dfs_repair_queue_depth`, `dfs_repair_bytes_total`

**Gate:** `docker stop dfs-node-2` → within ~60 s `dfs_chunks_under_replicated` returns to 0
with no client-visible errors. Corrupt a chunk file by hand → the scrubber detects it and the
repair pipeline restores a good copy. Start a 7th node → it organically fills to its fair share.

---

## Phase 5 — Auth, multi-tenancy, and the S3 API
*2 weeks* · **The biggest single chunk of work. Split it.**

**5a — Identity & native auth (3–4 days)**
- [ ] Users, argon2id password hashing, JWT access + refresh
- [ ] Access keys (`access_key_id` / `secret`) for programmatic use
- [ ] Bucket ownership + per-bucket policy (private / public-read)
- [ ] Per-bucket and per-user quotas, enforced at `AllocateChunk`
- [ ] Presigned URLs for the native API (HMAC, expiry, method-scoped)

**5b — S3 protocol (7–10 days)**
- [ ] **AWS SigV4 verification** — do this first and get it exactly right; everything else is
      blocked on it. Canonical request → string-to-sign → signing key → compare. Support both
      header auth and query-string (presigned) auth. Test against real `aws-cli` requests.
- [ ] XML request/response types (S3 is XML, not JSON — encoding details matter to clients)
- [ ] `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`
- [ ] `ListObjectsV2` with `prefix`, `delimiter`, `max-keys`, continuation tokens, `CommonPrefixes`
- [ ] `PutObject`, `GetObject` (+ Range), `HeadObject`, `DeleteObject`, `DeleteObjects` (batch)
- [ ] `CopyObject` — server-side, and with content addressing it's *metadata-only*, a nice win
- [ ] **Multipart upload:** `Create`, `UploadPart`, `Complete`, `Abort`, `ListParts` — required for large files; every SDK uses it above ~100 MB
- [ ] S3-format ETags (see ARCHITECTURE §3)
- [ ] S3-shaped error responses (`NoSuchKey`, `NoSuchBucket`, `SignatureDoesNotMatch`, ...) — clients parse these codes
- [ ] Presigned S3 URLs

**Gate:**
```bash
aws --endpoint-url https://dfs.yourdomain.com s3 mb s3://demo
aws --endpoint-url https://dfs.yourdomain.com s3 cp ./big.zip s3://demo/   # multipart path
aws --endpoint-url https://dfs.yourdomain.com s3 ls s3://demo/
rclone sync ./localdir dfs:demo/backup                                      # the real test
```
If `rclone sync` works unmodified, your S3 implementation is genuinely good — it exercises
listing pagination, multipart, HEAD, ranges, and error codes all at once.

---

## Phase 6 — Erasure coding
*1–1.5 weeks*

- [ ] `internal/erasure` wrapping `klauspost/reedsolomon`, `k=4 m=6`
- [ ] Shard placement: 6 shards on 6 distinct nodes; `chunk_placements.shard_index` 0..5
- [ ] Encode on write for chunks belonging to objects ≥ 1 MiB; `storage_class = 'ec_4_2'`
- [ ] **Normal read:** fetch any 4 shards, concatenate data shards
- [ ] **Degraded read:** on shard unavailability, fetch 4 of the survivors and `Reconstruct()`
- [ ] EC repair: rebuild only the missing shard(s), not the whole chunk
- [ ] Background migration tool: convert existing replicated chunks to EC
- [ ] Metric: `dfs_storage_overhead_ratio` — graph it before and after

**Gate:** `docker stop` **two** nodes simultaneously → all reads still succeed (degraded).
Storage overhead metric drops from ~3.0 to ~1.6. Usable capacity roughly doubles.

---

## Phase 7 — Dashboard & observability
*1.5 weeks*

**Observability first** — you want it before you're debugging production, not after.
- [ ] RED metrics on every HTTP/gRPC handler (rate, errors, duration histograms)
- [ ] Storage metrics: capacity used/free per node, chunk counts, dedup ratio, repair queue depth, scrub progress
- [ ] Prometheus + Grafana in Compose, dashboards committed as JSON in `deploy/grafana/`
- [ ] Loki + promtail; every log line carries `request_id`, `bucket`, `key`
- [ ] Alert rules: node down, under-replication sustained > 5 min, disk > 85 %, error rate spike

**Dashboard**
- [ ] Next.js + Tailwind + TanStack Query, auth via the native JWT flow
- [ ] File browser: breadcrumbs, prefix navigation, sort, search
- [ ] Multipart upload with per-file progress, drag-and-drop, direct-to-gateway streaming
- [ ] Download, delete, version history, share-link generation (presigned)
- [ ] **Cluster view:** node topology with live health, per-node capacity bars, replication
      status, repair activity, throughput charts. This is the screen that makes the project
      land in a portfolio — it makes the distributed behavior *visible*.

**Gate:** stop a node from the terminal and watch the dashboard reflect it within seconds,
with the repair queue draining live on the Grafana graph.

---

## Phase 8 — Production deployment
*1.5 weeks* · See `DEPLOYMENT.md` for the exact commands.

- [ ] VPS hardening: SSH keys only, root login off, non-standard SSH port, `ufw`, `fail2ban`, `unattended-upgrades`
- [ ] Docker CE from Docker's official apt repo (not Debian's `docker.io`)
- [ ] 2 GB swapfile, `vm.swappiness=10`
- [ ] `docker-compose.prod.yml`: **only Caddy publishes ports (80/443)** — every other service is internal-only on the Docker network. Postgres, meta, and nodes must not be reachable from the internet.
- [ ] `mem_limit` / `cpus` on every container; `restart: unless-stopped`; healthchecks with real readiness probes
- [ ] Caddy: automatic TLS for `dfs.yourdomain.com`, `s3.yourdomain.com`, `app.yourdomain.com`
- [ ] Secrets via Docker secrets or a root-only `.env` — never in the repo, never in the image
- [ ] CI/CD: GitHub Actions → build → push to GHCR → SSH deploy → health-gated rollout with rollback
- [ ] **Backups (the most important item in this phase):**
      - Postgres: WAL archiving + nightly base backup → Backblaze B2 via `restic`
      - Chunk store: nightly `restic` snapshot (its dedup pairs well with content-addressed data)
      - Retention: 7 daily / 4 weekly / 6 monthly
- [ ] **Restore drill:** wipe a staging stack and rebuild it *from backups alone*. A backup you
      have never restored is not a backup. Do this before you put anything you care about in.
- [ ] Alertmanager → Telegram or email
- [ ] `k6` load test; record real throughput numbers for the README
- [ ] `RUNBOOK.md`: node down, disk full, Postgres down, restore procedure, deploy rollback

**Gate:** `https://dfs.yourdomain.com` serves real traffic over TLS with an A rating, a full
restore drill succeeds from backups alone, and killing any container self-heals with no manual
intervention.

---

## Phase 9 — Hardening & stretch goals
*Ongoing — pick by what you want to learn*

**Chaos suite** (do this one; it validates every claim above)
- [ ] `toxiproxy` between components: latency injection, packet loss, partitions
- [ ] Automated scenarios: random node kill, disk-full, corrupt chunk, slow node, Postgres failover
- [ ] Run in CI nightly against an ephemeral stack

**High-value additions**
- [ ] **Multi-host** — a second VPS + WireGuard mesh. Almost free if you followed the
      "no localhost defaults" rule in Phase 0. Add `zone` to placement and enforce
      cross-zone replica spread. *This is what turns your replication into real durability.*
- [ ] **HA metadata** — Postgres streaming replication + Patroni, or (much more interesting to
      build) reimplement the metadata store on Raft with `hashicorp/raft`
- [ ] **FastCDC** content-defined chunking → dedup that survives byte insertions
- [ ] Server-side encryption at rest, per-object keys
- [ ] Lifecycle policies (expire after N days, transition storage class)
- [ ] Compression before chunking (zstd) for compressible content
- [ ] Read cache in the gateway for hot chunks
- [ ] Bucket event notifications (webhook on PUT/DELETE)

---

## Sequencing rules

1. **Never build two hard things at once.** Distribution (Phase 3) and erasure coding
   (Phase 6) are deliberately far apart — debugging a placement bug and a Reed–Solomon bug
   simultaneously is genuinely miserable.
2. **Deploy early, deploy often.** Push Phase 2's minimal system to the VPS as soon as it
   works. Discovering Docker networking and TLS problems at Phase 8 with 10k lines of code in
   flight is much worse than discovering them at Phase 2 with 1k.
3. **Every failure claim needs a test that proves it.** "Survives node failure" is a chaos
   test, not a sentence in a README.
4. **Write the README as you go.** Architecture diagram, real benchmark numbers, an honest
   limitations section (ARCHITECTURE §8). It's what people actually read.

---

## Start here — your next three actions

```bash
# 1. Scaffold
cd ~/Desktop/DFS
git init && go mod init github.com/<you>/dfs
mkdir -p api/proto/{metadata,storage}/v1 cmd/{dfs-gateway,dfs-meta,dfs-node,dfsctl} \
         internal/{auth,chunk,placement,erasure,meta,blobstore,cluster,repair,s3,restapi,obs} \
         db/migrations deploy/compose test/{integration,chaos,load}

# 2. Phase 1 first, before any distributed logic:
#    internal/blobstore — content-addressed, crash-safe, checksum-verified.
#    Write the property test before the implementation.

# 3. Buy the domain and point an A record at the VPS now.
#    DNS propagation is the one thing you can't speed up later.
```

Build order within Phase 1: `internal/chunk` (hashing) → `internal/blobstore` (disk) →
`cmd/dfs-node` (gRPC wrapper). Bottom-up, each layer fully tested before the next.
