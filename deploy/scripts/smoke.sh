#!/bin/sh
# Phase 0 gate: every service is up, healthy, exporting metrics, and the
# gateway answers a real request.
#
# Run with: make smoke
set -eu

fail=0

check() {
  name=$1
  url=$2
  printf '  %-28s ' "$name"
  if body=$(curl -fsS --max-time 5 "$url" 2>/dev/null); then
    printf 'OK   %s\n' "$(printf '%s' "$body" | head -c 60 | tr -d '\n')"
  else
    printf 'FAIL %s\n' "$url"
    fail=1
  fi
}

echo "Health:"
check "dfs-meta    /healthz" "http://localhost:9100/healthz"
check "dfs-node-1  /healthz" "http://localhost:9101/healthz"
check "dfs-gateway /healthz" "http://localhost:9102/healthz"

echo
echo "Readiness:"
check "dfs-meta    /readyz"  "http://localhost:9100/readyz"
check "dfs-node-1  /readyz"  "http://localhost:9101/readyz"
check "dfs-gateway /readyz"  "http://localhost:9102/readyz"

echo
echo "API:"
check "gateway     /v1/version" "http://localhost:8080/v1/version"

echo
echo "Metrics:"
for port in 9100 9101 9102; do
  printf '  %-28s ' "port $port dfs_build_info"
  if curl -fsS --max-time 5 "http://localhost:$port/metrics" 2>/dev/null | grep -q '^dfs_build_info'; then
    echo 'OK'
  else
    echo 'FAIL'
    fail=1
  fi
done

echo
if [ "$fail" -eq 0 ]; then
  echo 'Phase 0 gate: PASS'
else
  echo 'Phase 0 gate: FAIL — see docker compose logs'
  exit 1
fi
