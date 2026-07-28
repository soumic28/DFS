#!/bin/sh
# Phase 2 gate: a real file makes it through the whole pipeline and comes back
# byte-identical, deduplication is observable, and Range requests are correct.
#
# Run with: make phase2-gate
set -eu

ENDPOINT=${DFS_ENDPOINT:-http://localhost:8080}
BUCKET=${BUCKET:-gate-phase2}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

DFSCTL=${DFSCTL:-./bin/dfsctl}
export DFS_ENDPOINT="$ENDPOINT"

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }

echo "1. bucket creation"
$DFSCTL mb "$BUCKET" >/dev/null
pass "created bucket $BUCKET"

echo
echo "2. round trip of a 200 MiB file"
# Incompressible, so nothing along the path can cheat by compressing it, and
# larger than one chunk so the multi-chunk assembly path is exercised.
dd if=/dev/urandom of="$WORK/original.bin" bs=1M count=200 2>/dev/null
BEFORE=$(sha256sum "$WORK/original.bin" | cut -d' ' -f1)

$DFSCTL cp "$WORK/original.bin" "dfs://$BUCKET/data/original.bin"
$DFSCTL cp "dfs://$BUCKET/data/original.bin" "$WORK/restored.bin"

AFTER=$(sha256sum "$WORK/restored.bin" | cut -d' ' -f1)
if [ "$BEFORE" = "$AFTER" ]; then
  pass "200 MiB round-tripped byte-identical ($BEFORE)"
else
  fail "checksum mismatch: $BEFORE != $AFTER"
fi

echo
echo "3. deduplication on re-upload"
OUT=$($DFSCTL cp "$WORK/original.bin" "dfs://$BUCKET/data/copy.bin")
echo "$OUT" | sed 's/^/  /'
if echo "$OUT" | grep -q "deduplicated"; then
  TRANSFERRED=$(echo "$OUT" | grep -o '[0-9.]* [KMG]*i*B actually transferred' || echo "")
  pass "identical content deduplicated (${TRANSFERRED:-0 B transferred})"
else
  fail "re-upload of identical bytes was not deduplicated"
fi

echo
echo "4. ranged reads"
URL="$ENDPOINT/v1/b/$BUCKET/o/data/original.bin"

# A range spanning a chunk boundary is the interesting case: 8 MiB chunks mean
# byte 8388600 is in chunk 0 and 8388620 is in chunk 1.
curl -fsS -r 8388600-8388619 "$URL" -o "$WORK/range-cross.bin"
dd if="$WORK/original.bin" of="$WORK/range-cross-want.bin" bs=1 skip=8388600 count=20 2>/dev/null
if cmp -s "$WORK/range-cross.bin" "$WORK/range-cross-want.bin"; then
  pass "range crossing a chunk boundary is correct"
else
  fail "range crossing a chunk boundary returned wrong bytes"
fi

curl -fsS -r 0-99 "$URL" -o "$WORK/range-head.bin"
dd if="$WORK/original.bin" of="$WORK/range-head-want.bin" bs=1 count=100 2>/dev/null
cmp -s "$WORK/range-head.bin" "$WORK/range-head-want.bin" \
  && pass "leading range is correct" || fail "leading range returned wrong bytes"

curl -fsS -r -100 "$URL" -o "$WORK/range-tail.bin"
tail -c 100 "$WORK/original.bin" > "$WORK/range-tail-want.bin"
cmp -s "$WORK/range-tail.bin" "$WORK/range-tail-want.bin" \
  && pass "suffix range is correct" || fail "suffix range returned wrong bytes"

STATUS=$(curl -s -o /dev/null -w '%{http_code}' -r 0-99 "$URL")
[ "$STATUS" = "206" ] && pass "ranged read returns 206" || fail "ranged read returned $STATUS, want 206"

echo
echo "5. listing"
$DFSCTL cp "$WORK/original.bin" "dfs://$BUCKET/other/nested.bin" >/dev/null
LIST=$($DFSCTL ls "$BUCKET/data/")
echo "$LIST" | sed 's/^/  /'
echo "$LIST" | grep -q "data/original.bin" \
  && pass "prefix listing returns the expected keys" || fail "prefix listing"

echo
echo "6. stat and delete"
$DFSCTL stat "dfs://$BUCKET/data/original.bin" | sed 's/^/  /'
$DFSCTL rm "dfs://$BUCKET/data/original.bin" >/dev/null

STATUS=$(curl -s -o /dev/null -w '%{http_code}' "$URL")
[ "$STATUS" = "404" ] && pass "deleted object returns 404" || fail "deleted object returned $STATUS, want 404"

# The copy shares every chunk with the deleted original, so it must still read
# back perfectly — deleting one object must never dereference another's data.
$DFSCTL cp "dfs://$BUCKET/data/copy.bin" "$WORK/after-delete.bin" >/dev/null
AFTER_DELETE=$(sha256sum "$WORK/after-delete.bin" | cut -d' ' -f1)
[ "$BEFORE" = "$AFTER_DELETE" ] \
  && pass "chunks shared with a deleted object survived" \
  || fail "deleting an object damaged another object sharing its chunks"

echo
echo 'Phase 2 gate: PASS'
