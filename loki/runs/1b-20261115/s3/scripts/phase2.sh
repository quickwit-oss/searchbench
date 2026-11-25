#!/usr/bin/env bash
# HOST (and local dry run). Phase 2: the standard lib/benchmark.sh query phase,
# query-only, over the loaded S3 destination: full loki/queries.logql, 3 tries,
# 60/60 s caps, SEARCHBENCH_RESTART=yes (stop + drop_caches + start per query),
# SEARCHBENCH_CPU=on. lib/benchmark.sh and the adapter are unchanged; only for this
# phase ./cpu-usage and ./query are wrapped (sbwrap-*, originals kept as *.orig,
# restored afterwards): GetObject scrapes outside the CPU window + every try's stdout.
#
#   phase2.sh LOKI_DIR OUT_DIR DEADLINE_EPOCH
# Requires SEARCHBENCH_* / LOKI_* env (remote.env). Exit: collate_phase2.py's status
# (2 = answer mismatch P0), or the harness status if it failed first.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOKI="$(cd "$1" && pwd)"; OUT=$2; DEADLINE=$3
mkdir -p "$OUT/gets/raw" "$OUT/answers"
OUT="$(cd "$OUT" && pwd)"
cp "$HERE/getscrape.py" "$OUT/gets/"
for f in cpu-usage query; do
  [[ -e "$LOKI/$f.orig" ]] && { echo "wrapper already installed: $LOKI/$f.orig" >&2; exit 1; }
done
restore() {
  for f in cpu-usage query; do
    [[ -e "$LOKI/$f.orig" ]] && mv -f "$LOKI/$f.orig" "$LOKI/$f"
  done
}
trap restore EXIT
for f in cpu-usage query; do
  mv "$LOKI/$f" "$LOKI/$f.orig"
  cp "$HERE/sbwrap-$f" "$LOKI/$f"; chmod +x "$LOKI/$f"
done
sha256sum "$HERE"/sbwrap-* "$LOKI"/cpu-usage.orig "$LOKI"/query.orig > "$OUT/wrappers.sha256" 2>/dev/null \
  || shasum -a 256 "$HERE"/sbwrap-* "$LOKI"/cpu-usage.orig "$LOKI"/query.orig > "$OUT/wrappers.sha256"
export SB_GETS_DIR="$OUT/gets" SB_ANSWERS_DIR="$OUT/answers" SB_HARNESS_LOG="$OUT/harness.log"
export SEARCHBENCH_RESTART=yes SEARCHBENCH_CPU=on SEARCHBENCH_TRIES=3
export SEARCHBENCH_QUERY_TIMEOUT=60 SEARCHBENCH_COLD_TIMEOUT=60 SEARCHBENCH_SKIP_DOWNLOAD=yes
export SEARCHBENCH_QUERIES=queries.logql
env | sort > "$OUT/env.txt"
: > "$SB_HARNESS_LOG"
"${SEARCHBENCH_PYTHON:-python3}" "$HERE/run_until.py" "$DEADLINE" "$LOKI/benchmark.sh" --version "$SEARCHBENCH_VERSION" \
  >> "$SB_HARNESS_LOG" 2>&1
rc=$?
echo "$rc" > "$OUT/harness-rc.txt"
restore
trap - EXIT
(cd "$LOKI" && ./stop) > "$OUT/final-stop.log" 2>&1 || true
slug=$(printf '%s' "$SEARCHBENCH_VERSION" | tr -c 'A-Za-z0-9._-' '_')
res="$LOKI/results/loki_${slug}_${SEARCHBENCH_DATASET}"
if [[ -f "$res.json" ]]; then cp "$res.json" "$res.results.log" "$OUT/"; json="$res.json"
elif [[ -f "$res.partial.json" ]]; then cp "$res.partial.json" "$res.results.log" "$OUT/"; json="$res.partial.json"
else echo "no results JSON" >&2; exit "${rc:-1}"; fi
"${SEARCHBENCH_PYTHON:-python3}" "$HERE/collate_phase2.py" "$json" "$SB_HARNESS_LOG" "$SB_GETS_DIR" "$SB_ANSWERS_DIR" \
  "$HERE/references.json" "$OUT/collated" | tee "$OUT/collate.stdout"
crc=${PIPESTATUS[0]}
echo "$crc" > "$OUT/collate-rc.txt"
(( rc != 0 && crc == 0 )) && exit "$rc"
exit "$crc"
