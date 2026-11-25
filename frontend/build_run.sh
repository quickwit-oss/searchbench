#!/usr/bin/env bash
# Render one run directory (runs/<dir>/) as a self-contained SearchBench page.
#
#   frontend/build_run.sh runs/<dir> <out.html>
#   frontend/build_run.sh runs/<dir> <out.json> --rows-only
#
# A run directory mirrors the repository layout: <engine>/results/*.json (+ results/load/)
# and the <engine>/queries.* files the results were produced with (see runs/README.md).
# Steps:
#   1. frontend/build_results on a copy of the run directory -> results.json rows
#   2. <run>/phases.json -> load_* fields (load-time breakdown rows)
#   3. <run>/s3_requests.json -> S3 GETs / bytes per query and summary sums (s3_requests.py)
#   4. <run>/details.json -> per-column setup line (header tooltip, query panel)
#   5. <run>/names.json  -> display names ("<dataset>/<system>" wins over "<system>")
#   6. frontend/inject.mjs swaps them into the committed frontend/index.html
#
# Needs: bash 4+, jq, node, python3.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
RUN="${1:?usage: frontend/build_run.sh runs/<dir> <out.html>}"; RUN="${RUN%/}"
OUT="${2:?usage: frontend/build_run.sh runs/<dir> <out.html>}"
[[ -d "$RUN" ]] || { echo "no run dir: $RUN" >&2; exit 1; }
MODE="${3:-html}"
[[ "$MODE" == html || "$MODE" == --rows-only ]] || { echo "unknown output mode: $MODE" >&2; exit 1; }

work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
cp -R "$RUN"/. "$work"/
rm -rf "$work/extra"     # not columns: superseded, A/B and debug runs
mkdir -p "$work/frontend"
cp "$HERE/build_results" "$work/frontend/build_results"
(cd "$work" && bash ./frontend/build_results)
rows="$work/frontend/results.json"

if [[ -f "$RUN/phases.json" ]]; then
    jq --slurpfile p "$RUN/phases.json" 'map(($p[0][.dataset + "/" + .system] // {}) as $x
        | . + {load_ingest: $x.ingest, load_merge_after: $x.merge_after,
               load_merge_busy: $x.merge_busy, load_check: $x.check})' "$rows" > "$work/rows.json"
    mv "$work/rows.json" "$rows"
fi
# Timeline milestones from load start, derived from the phases above (null when unknown):
#   load_searchable    = ingest            (every row/doc queryable)
#   load_merged        = ingest + merges after ingest (last merge finished; settle check excluded)
#   load_merge_overlap = merge busy - merges after   (time merges ran while still ingesting)
jq 'def num: type == "number";
    def r1: (. * 10 | round) / 10;
    map(. as $r | . + {
      load_searchable: (if ($r.load_ingest | num) then $r.load_ingest else null end),
      load_merged: (if ($r.load_ingest | num) and ($r.load_merge_after | num)
                    then ($r.load_ingest + $r.load_merge_after | r1) else null end),
      load_merge_overlap: (if ($r.load_ingest | num) and ($r.load_merge_after | num) and ($r.load_merge_busy | num)
                           then ([([$r.load_merge_busy - $r.load_merge_after, 0] | max), $r.load_ingest] | min | r1)
                           else null end)})' "$rows" > "$work/rows.json"
mv "$work/rows.json" "$rows"
# Load time excludes the harness's settle check (a fixed quiet window that only confirms merges
# stopped; the engine does no work then). The measured wall time stays in load_time_raw.
jq 'map(if (.load_time|type) == "number" and (.load_check|type) == "number" and .load_check > 0
         then . + {load_time_raw: .load_time, load_time: ((([.load_time - .load_check, 0] | max) * 1000 | round) / 1000)}
         else . end)' "$rows" > "$work/rows.json"
mv "$work/rows.json" "$rows"
# S3 GETs / downloaded bytes per query (summary rows + query panel), keyed like phases.json.
if [[ -f "$RUN/s3_requests.json" ]]; then
    python3 "$HERE/s3_requests.py" "$rows" "$RUN/s3_requests.json"
fi
# Load CPU from <run>/load_cpu.json when a column's load record predates SEARCHBENCH_CPU (keyed like phases.json).
if [[ -f "$RUN/load_cpu.json" ]]; then
    jq --slurpfile c "$RUN/load_cpu.json" 'map(if .load_cpu == null then (.load_cpu = ($c[0][.dataset + "/" + .system] // null)) else . end)' "$rows" > "$work/rows.json"
    mv "$work/rows.json" "$rows"
fi
# Sparse columns (fewer than half of their supported queries finished): left out of the common
# query sets of the CPU sums and the geomean (sparse_columns.py, SPARSE_FINISHED_FRACTION).
python3 "$HERE/sparse_columns.py" "$rows"
# CPU seconds per try (results' `cpu` arrays, SEARCHBENCH_CPU=on): summary rows + query panel.
python3 "$HERE/cpu_time.py" "$rows"
# Per-round timings (query panel: range across rounds), keyed like phases.json.
if [[ -f "$RUN/rounds.json" ]]; then
    python3 "$HERE/rounds.py" "$rows" "$RUN/rounds.json"
fi
# Run details (setup line per column, shown in the header tooltip and the query panel).
if [[ -f "$RUN/details.json" ]]; then
    jq --slurpfile d "$RUN/details.json" 'map(.details = ($d[0][.dataset + "/" + .system] // $d[0][.system] // null))' "$rows" > "$work/rows.json"
    mv "$work/rows.json" "$rows"
fi
if [[ -f "$RUN/names.json" ]]; then
    jq --slurpfile n "$RUN/names.json" \
        'map(.system = ($n[0][.dataset + "/" + .system] // $n[0][.system] // .system))' "$rows" > "$work/rows.json"
    mv "$work/rows.json" "$rows"
fi

# Shareable links (?s=) store hidden/included engines as one comma-separated list of names,
# so a name with a comma is split on restore and that engine reappears. Refuse such names.
if bad=$(jq -r '[.[] | select(.system | test(",")) | .dataset + "/" + .system] | unique | .[]' "$rows") && [[ -n "$bad" ]]; then
    echo "engine names must not contain commas (they break shared links):" >&2; printf '  %s\n' "$bad" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT")"
if [[ "$MODE" == --rows-only ]]; then
    cp "$rows" "$OUT.tmp"
else
    node "$HERE/inject.mjs" "$HERE/index.html" "$rows" "$OUT.tmp"
fi
mv "$OUT.tmp" "$OUT"
echo "wrote $OUT from $RUN: $(jq -r 'group_by(.dataset) | map(.[0].dataset + " " + (length | tostring)) | join(", ")' "$rows") columns"
