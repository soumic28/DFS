# DFS — Deployment Guide

Target: **Hostinger KVM2** — 2 vCPU · 8 GB RAM · 100 GB NVMe · ~8 TB bandwidth · Debian 12/13.

---

## 1. VPS bootstrap

Run once, from your local machine, right after provisioning.

### 1.1 SSH hardening

```bash
# From your laptop — get your key onto the box before locking anything down
ssh-copy-id root@<vps-ip>
ssh root@<vps-ip>

# Create a non-root user
adduser dfs && usermod -aG sudo dfs
rsync --archive --chown=dfs:dfs ~/.ssh /home/dfs/
```

Edit `/etc/ssh/sshd_config`:

```
Port 2222
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
AllowUsers dfs
```

```bash
systemctl restart sshd
# KEEP THIS SESSION OPEN. Verify from a second terminal before you close it:
#   ssh -p 2222 dfs@<vps-ip>
```

### 1.2 Firewall and intrusion protection

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y ufw fail2ban unattended-upgrades curl git htop ncdu

sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 2222/tcp    # SSH
sudo ufw allow 80/tcp      # HTTP → Caddy redirects to HTTPS
sudo ufw allow 443/tcp     # HTTPS
sudo ufw enable
```

> **Docker bypasses UFW.** Docker writes its own iptables rules and a published port is
> reachable regardless of what `ufw` says. The defence that actually works is: **never publish
> a port you don't want on the internet.** In `docker-compose.prod.yml`, only Caddy gets a
> `ports:` mapping. Postgres, `dfs-meta`, and every `dfs-node` use `expose:` and are reachable
> only on the internal Docker network. If you must publish something for debugging, bind it to
> loopback explicitly: `127.0.0.1:5432:5432`.

```bash
sudo dpkg-reconfigure -plow unattended-upgrades   # security patches, automatic
```

### 1.3 Swap and kernel tuning

```bash
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

sudo tee /etc/sysctl.d/99-dfs.conf <<'EOF'
vm.swappiness=10
vm.max_map_count=262144
net.core.somaxconn=4096
fs.file-max=200000
EOF
sudo sysctl --system
```

### 1.4 Docker

Use Docker's official repo — Debian's `docker.io` package lags badly.

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker dfs
newgrp docker
docker compose version   # v2 plugin, should be present
```

Cap log growth so a chatty container can't eat the disk:

```bash
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "3" },
  "storage-driver": "overlay2"
}
EOF
sudo systemctl restart docker
```

### 1.5 Data directories

```bash
sudo mkdir -p /srv/dfs/{postgres,nodes,caddy,prometheus,grafana,loki,backups}
for i in 1 2 3 4 5 6; do sudo mkdir -p /srv/dfs/nodes/node$i; done
sudo chown -R dfs:dfs /srv/dfs
```

Everything shares one 100 GB filesystem, so **capacity is enforced in the application**: each
`dfs-node` gets `DFS_NODE_CAPACITY_BYTES=13958643712` (13 GiB) and refuses writes past it.
6 × 13 GiB = 78 GiB raw, leaving ~7 GiB of slack after the OS and support services. Never let
the disk be the limit — a full `/` takes Postgres down, and Postgres is the one component
whose loss is unrecoverable.

Watch it: `ncdu /srv/dfs`, and alert at 85 %.

---

## 2. DNS

Point three A records at the VPS IP. Do this early — propagation is the one delay you can't
engineer around.

| Host | Serves |
|---|---|
| `dfs.yourdomain.com` | Native REST API |
| `s3.yourdomain.com` | S3-compatible endpoint |
| `app.yourdomain.com` | Next.js dashboard |

For S3 **virtual-hosted-style** addressing (`bucket.s3.yourdomain.com`) also add
`*.s3.yourdomain.com`. That requires a DNS-01 ACME challenge for the wildcard cert — Caddy
supports it with a provider plugin. Path-style (`s3.yourdomain.com/bucket/key`) needs no
wildcard, and `aws-cli` supports it via `--endpoint-url` plus
`s3.addressing_style = path`. **Start path-style**; add the wildcard later if you want it.

---

## 3. Caddyfile

```caddy
{
    email you@yourdomain.com
}

# Native REST API
dfs.yourdomain.com {
    encode zstd gzip
    reverse_proxy dfs-gateway:8080 {
        health_uri /healthz
        lb_policy least_conn
    }
    request_body { max_size 5GB }
}

# S3-compatible endpoint
s3.yourdomain.com {
    reverse_proxy dfs-gateway:8080

    # Do NOT compress here — S3 clients checksum the raw body and
    # transfer-encoding changes will break SigV4 payload verification.
    request_body { max_size 5GB }
}

# Dashboard
app.yourdomain.com {
    encode zstd gzip
    reverse_proxy dfs-web:3000
}

# Grafana — lock this down, it is not public
grafana.yourdomain.com {
    reverse_proxy grafana:3000
    @not_allowed not remote_ip <your-home-ip>/32
    respond @not_allowed 403
}
```

Caddy obtains and renews Let's Encrypt certificates automatically. Persist `/data` in the
Caddy container or you'll re-issue certificates on every deploy and hit rate limits.

---

## 4. Production Compose — shape

Full file lives at `deploy/compose/docker-compose.prod.yml`. The parts that matter:

```yaml
services:
  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports: ["80:80", "443:443"]        # the ONLY published ports in this file
    volumes:
      - ../caddy/Caddyfile:/etc/caddy/Caddyfile:ro
      - /srv/dfs/caddy:/data
    networks: [edge, internal]

  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    expose: ["5432"]                    # internal only — no `ports:`
    environment:
      POSTGRES_DB: dfs
      POSTGRES_USER: dfs
      POSTGRES_PASSWORD_FILE: /run/secrets/pg_password
    volumes:
      - /srv/dfs/postgres:/var/lib/postgresql/data
    command: >
      postgres -c shared_buffers=512MB -c effective_cache_size=2GB
               -c max_connections=100 -c wal_level=replica
               -c archive_mode=on -c archive_command='test ! -f /wal/%f && cp %p /wal/%f'
    deploy:
      resources: { limits: { memory: 1536M, cpus: "0.8" } }
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dfs"]
      interval: 10s
    networks: [internal]

  dfs-meta:
    image: ghcr.io/<you>/dfs-meta:${TAG}
    restart: unless-stopped
    expose: ["9090"]
    environment:
      DFS_DATABASE_URL: postgres://dfs:${PG_PASSWORD}@postgres:5432/dfs?sslmode=disable
      DFS_REPLICATION_FACTOR: "3"
      DFS_WRITE_QUORUM: "2"
      DFS_REPAIR_MAX_CONCURRENT: "2"
      DFS_REPAIR_BANDWIDTH_BPS: "20971520"    # 20 MB/s ceiling — protects live traffic
      GOMEMLIMIT: 460MiB
    depends_on: { postgres: { condition: service_healthy } }
    deploy: { resources: { limits: { memory: 512M, cpus: "0.5" } } }
    networks: [internal]

  dfs-node:
    image: ghcr.io/<you>/dfs-node:${TAG}
    restart: unless-stopped
    expose: ["9091"]
    environment:
      DFS_META_ADDR: dfs-meta:9090
      DFS_NODE_CAPACITY_BYTES: "13958643712"   # 13 GiB hard cap, enforced in-process
      DFS_DATA_DIR: /data
      DFS_SCRUB_INTERVAL: 168h                 # full sweep weekly
      GOMEMLIMIT: 230MiB
    deploy:
      replicas: 6
      resources: { limits: { memory: 256M, cpus: "0.25" } }
    networks: [internal]
    # Per-replica volumes: use one service block per node, or a Swarm volume template.
    # Simplest with plain Compose: six explicit dfs-node-1..6 blocks generated by a template.

  dfs-gateway:
    image: ghcr.io/<you>/dfs-gateway:${TAG}
    restart: unless-stopped
    expose: ["8080"]
    environment:
      DFS_META_ADDR: dfs-meta:9090
      DFS_CHUNK_SIZE_BYTES: "8388608"
      DFS_EC_THRESHOLD_BYTES: "1048576"
      GOMEMLIMIT: 460MiB
    deploy: { replicas: 2, resources: { limits: { memory: 512M, cpus: "0.5" } } }
    networks: [internal]

networks:
  edge:      {}
  internal:  { internal: true }    # no route to the internet from this network
```

Two networks is the key security control: `internal: true` means containers on it cannot
reach the internet and the internet cannot reach them. Only Caddy straddles both.

**Note on the six nodes:** plain Compose `replicas` share a volume definition, which is wrong
here — each node needs its own disk. Either generate six explicit service blocks from a
template in your Makefile, or move to Docker Swarm (`docker swarm init` on the single host)
where volume templating per replica is supported. Generating the Compose file is simpler and
has fewer moving parts; do that.

---

## 5. CI/CD

`.github/workflows/deploy.yml`:

```
push to main
  → go vet · golangci-lint · go test ./... (with Postgres service container)
  → docker buildx build 4 images, tag with git SHA
  → push to ghcr.io
  → ssh -p 2222 dfs@vps:
        docker compose pull
        docker compose up -d --no-deps --wait dfs-meta
        docker compose up -d --no-deps --wait dfs-node-1 ... dfs-node-6
        docker compose up -d --no-deps --wait dfs-gateway
  → smoke test: upload → download → checksum compare against the live endpoint
  → on failure: redeploy previous SHA
```

Deploy order matters: **meta first, then nodes, then gateways.** Nodes re-register on
reconnect and gateways are stateless, so a rolling restart in that order is invisible to
clients — provided your Phase 0 graceful shutdown actually drains in-flight requests.

Store `SSH_KEY`, `VPS_HOST`, `GHCR_TOKEN` as GitHub repository secrets. Deploy with
`--wait` so a container that fails its healthcheck fails the pipeline instead of silently
sitting broken.

---

## 6. Backups — the part that actually protects you

Priority order, and it is not intuitive:

**1. Postgres metadata — critical, irreplaceable.** Chunks without metadata are undecodable
noise: you'd have hashed blobs with no record of which object they compose or in what order.
Lose this and you have lost everything, even with every byte of chunk data intact.

**2. Chunk data — important, large.**

**3. Configuration — trivial to back up, it's in git.**

```bash
# /srv/dfs/scripts/backup.sh
set -euo pipefail
export RESTIC_REPOSITORY="b2:dfs-backups:/"
export RESTIC_PASSWORD_FILE=/srv/dfs/.restic-pass

# Metadata — a consistent logical dump, hourly
docker compose exec -T postgres pg_dump -U dfs -Fc dfs \
  | restic backup --stdin --stdin-filename dfs-meta.dump --tag metadata

# Chunk data — nightly. restic dedups, which pairs beautifully with a
# content-addressed store: unchanged chunks cost nothing on subsequent runs.
restic backup /srv/dfs/nodes --tag chunks

restic forget --keep-daily 7 --keep-weekly 4 --keep-monthly 6 --prune
```

Backblaze B2 is ~$6/TB/month and egress to restore is cheap. Your 8 TB of VPS bandwidth makes
the transfer itself free.

```
# crontab -e
0 * * * *  /srv/dfs/scripts/backup.sh metadata  >> /var/log/dfs-backup.log 2>&1
0 3 * * *  /srv/dfs/scripts/backup.sh full      >> /var/log/dfs-backup.log 2>&1
```

### The restore drill — do this before you store anything you care about

```bash
# On a throwaway box or a separate Compose project:
restic restore latest --target /tmp/restore --tag metadata
docker compose -p dfs-restore up -d postgres
pg_restore -U dfs -d dfs /tmp/restore/dfs-meta.dump
restic restore latest --target /srv/dfs-restore/nodes --tag chunks
docker compose -p dfs-restore up -d
dfsctl --endpoint http://localhost:8081 ls dfs://demo/
# Download a known file. Compare checksums against the original.
```

Schedule this quarterly. A backup you have never restored is a hypothesis, not a backup —
and silent backup failure is the single most common way people lose data on a VPS.

---

## 7. Operational limits to keep in view

| Limit | Value | What to do when you hit it |
|---|---|---|
| Usable capacity | ~26 GB (R=3) → ~52 GB (EC 4+2) | Ship Phase 6; then add a second VPS or block storage |
| CPU | 2 vCPU | Cap repair bandwidth; set `GOMAXPROCS`; EC encode is cheap, container count is not |
| RAM | 8 GB, ~6 GB allocated | Enforce `GOMEMLIMIT` per service so Go's GC respects the container limit |
| Bandwidth | ~8 TB/mo | ~2.6 TB of transfer at 3× replication write amplification; monitor it |
| Real durability | Off-site backup only | One disk means `R=3` protects against process failure, not disk failure |

That last row is the one to be honest about in your README. On a single host, replication
gives you availability and correctness, not disk durability — the backup does. Adding a
second VPS with WireGuard (Phase 9) is what converts it into genuine durability, and because
Phase 0 forbids localhost defaults, that migration is a config change rather than a rewrite.

---

## 8. Runbook stubs

Write these out properly in `RUNBOOK.md` as you build.

| Symptom | First check | Action |
|---|---|---|
| Node down | `dfsctl cluster status`, `docker ps -a` | Repair auto-triggers at 30 s. Check for OOM: `dmesg -T \| grep -i oom` |
| Disk > 85 % | `ncdu /srv/dfs` | Force GC; lower per-node caps; migrate to EC |
| Under-replication stuck | `dfs_repair_queue_depth`, repair worker logs | Check for insufficient live nodes to satisfy R=3 |
| Postgres down | `docker compose logs postgres` | System is read-unavailable. Restore from hourly dump; never `--force` past a corrupt data dir |
| Upload failures | Gateway logs by `request_id` | Usually quota, capacity exhaustion, or < W nodes live |
| Cert renewal failed | `docker compose logs caddy` | Verify port 80 reachable and DNS still points at the box |
| Slow reads | Grafana p99 by node | Find the slow node, `dfsctl node drain` it, investigate |
