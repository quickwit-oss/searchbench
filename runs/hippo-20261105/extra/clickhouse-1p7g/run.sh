#!/usr/bin/env bash
# Phase 4 on the box of the Quickwit ingest-API runs, after 20261105T-qw-ingestapi-1b-stl20: ClickHouse 26.10.2.103
# on S3 with a 1.7 GiB part cap (fresh load, parts as they land; aiming at 60-70 parts), same table and settings as
# the other ClickHouse S3 columns (#17 queries, header cache on, text-index data caches + statistics off,
# max_threads=32). Local part metadata saved to S3 right after the load. Then 3 rounds alternated with the 64-split
# Quickwit ingest-API index (shard_throughput_limit 20mb, min_shards 48), order rotated.
set -Eeuo pipefail
ROOT=/opt/sb-ch17
TAG=20261105T-ch-cap-1p7g-s3
P1=/opt/sb-qwapi3
export ROOT AWS_REGION=us-east-1
mkdir -p "$ROOT"; exec >> "$ROOT/series.log" 2>&1
stage() { printf '%s %s\n' "$(date -Is)" "$*" | tee "$ROOT/status"; }
RAW=s3://<bucket>/searchbench/runs-raw/$TAG
push() { aws s3 sync "$ROOT" "$RAW/" --exclude '*/source/*' --only-show-errors || true; }
stopall() { docker ps -q | xargs -r docker stop -t 60 >/dev/null 2>&1 || true; }
finish() {
    rc=$?; trap - EXIT; set +e
    stopall; printf '%s\n' "$rc" > "$ROOT/exit-code"
    if (( rc == 0 )); then stage completed; else stage "failed rc=$rc"; fi
    sync; shutdown -c || true; push
    shutdown -h +60 'ch17 runs over'
    exit "$rc"
}
trap finish EXIT
stage waiting-for-ingestapi-run
until [[ -f $P1/exit-code ]]; do sleep 30; done
while pgrep -f 'sb-qwapi3/run.sh' >/dev/null; do sleep 10; done
shutdown -c || true
shutdown -h +240 'SearchBench ch17 safety limit'
docker ps -q | xargs -r docker stop -t 60 >/dev/null 2>&1 || true
CH_IMAGE=clickhouse/clickhouse-server@sha256:ec7bc702d862b9f7af6c604f0fa8f9688d62d10db44f0cd340bcdceaa25db2a8
CORPUS=/mnt/local/corpus/otel_logs_1b/part_000.parquet
CH_S3_PREFIX=searchbench/m8gd4x-1b-clickhouse-s3-$TAG
QW_SHA=71c9181d4a9c9517f40107b097b031fbdddf18fc
QW_IMAGE=searchbench-quickwit:parquet-bulk-71c9181
[[ -z "$(aws s3 ls "s3://<bucket>/$CH_S3_PREFIX-ch17/" 2>/dev/null)" && -z "$(aws s3 ls "$RAW/" 2>/dev/null)" ]]
docker pull -q "$CH_IMAGE" > "$ROOT/ch-image-pull.log" 2>&1
[[ "$(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$QW_IMAGE")" == "$QW_SHA" ]]
[[ -f $CORPUS ]]
cp "$P1/benchmark-source.tar.gz" "$P1/source-files.json" "$ROOT/"
cp -r "$P1/loads" "$ROOT/loads"
cp "$P1"/quickwit-api-ingest-load/*.json "$ROOT/loads/"
df -h /mnt/local > "$ROOT/disk-before.txt"
prepare() {   # $1 trial
    RUN="$ROOT/$1"; mkdir -p "$RUN"; export RUN
    stage "running-$1"
    REPO="$RUN/source"; rm -rf "$REPO"; mkdir -p "$REPO"; tar -xzf "$ROOT/benchmark-source.tar.gz" -C "$REPO" 2>/dev/null
    python3 - <<'PY'
import hashlib,json,os,pathlib
root=pathlib.Path(os.environ['RUN'])/'source'
for n,d in json.loads((pathlib.Path(os.environ['ROOT'])/'source-files.json').read_text()).items():
    assert hashlib.sha256((root/n).read_bytes()).hexdigest()==d,n
PY
    ! grep -q use_text_index_header_cache "$REPO/clickhouse/query"   # header cache stays on (#43)
    export SEARCHBENCH_PYTHON=/opt/searchbench-venv/bin/python3 SEARCHBENCH_DATASET=otel_logs_1b SEARCHBENCH_DATA_DIR=/mnt/local/corpus
    export SEARCHBENCH_CPU=on SEARCHBENCH_CPU_SLICE=searchbench.slice SEARCHBENCH_SKIP_DOWNLOAD=yes EXPECTED_ROWS=1000000000
}
finish_trial() {   # $1 container, $2 rc
    docker logs --timestamps "$1" > "$RUN/server.log" 2>&1 || true
    docker inspect "$1" > "$RUN/container-exit.json" 2>&1 || true
    mkdir -p "$RUN/results"; cp -a results/. "$RUN/results/" 2>/dev/null || true
    printf '%s\n' "$2" > "$RUN/exit-code"
}
ingest() {   # $1 trial, $2 engine dir, $3 container
    stopall; prepare "$1"; cd "$REPO/$2"
    : > "$RUN/ingest-only.queries"; export SEARCHBENCH_QUERIES="$RUN/ingest-only.queries"
    env | grep -E '^(SEARCHBENCH_|QW_|CH_|SERENED_|AWS_REGION)' | sort > "$RUN/environment.txt"
    sync; echo 3 > /proc/sys/vm/drop_caches
    ( while sleep 30; do echo "$(date -Is) $(df -BG --output=used,avail /mnt/local | tail -1) $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"; done ) > "$RUN/resources.log" 2>&1 & local mon=$!
    set +e; bash ./benchmark.sh --index --version "$SEARCHBENCH_VERSION" > "$RUN/benchmark.log" 2>&1; local brc=$?; set -e
    kill "$mon" 2>/dev/null || true
    docker stop -t 60 "$3" >/dev/null 2>&1 || true
    finish_trial "$3" "$brc"; (( brc == 0 )) || { echo "ingest $1 failed (rc=$brc)" >&2; return 1; }
    mkdir -p "$ROOT/$1-load"; cp "$RUN/results/load/"*.json "$ROOT/$1-load/"; push
}
queries() {   # $1 trial, $2 engine dir, $3 container, $4 queries file, $5 version, $6 load build, $7 load dir
    stopall; prepare "$1"; cd "$REPO/$2"
    mkdir -p results/load; cp "$7"/*.json results/load/
    export SEARCHBENCH_QUERIES="$4" SEARCHBENCH_BUILD="$6"
    export SEARCHBENCH_TRIES=3 SEARCHBENCH_QUERY_TIMEOUT=60 SEARCHBENCH_COLD_TIMEOUT=60 SEARCHBENCH_RESTART=yes
    if [[ $2 == clickhouse ]]; then
        export CLICKHOUSE_BIN="$PWD/ch-exec"
        ./start >/dev/null 2>&1; for i in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
        echo 'SELECT count() FROM otel_logs' | ./query > "$RUN/preflight.txt" 2>&1; grep -qx 1000000000 "$RUN/preflight.txt"
    fi
    env | grep -E '^(SEARCHBENCH_|QW_|CH_|CLICKHOUSE_|SERENED_|AWS_REGION)' | sort > "$RUN/environment.txt"
    sha256sum "$4" > "$RUN/queries-sha256.txt"
    date -u '+%Y-%m-%d %H:%M:%S' > "$RUN/query-window-start.txt"
    set +e; bash ./benchmark.sh --version "$5" > "$RUN/benchmark.log" 2>&1; local brc=$?; set -e
    date -u '+%Y-%m-%d %H:%M:%S' > "$RUN/query-window-end.txt"
    docker stop -t 60 "$3" >/dev/null 2>&1 || true
    finish_trial "$3" "$brc"; (( brc == 0 )) || { echo "trial $1 failed (rc=$brc)" >&2; return 1; }
    push
}
ch_env() {   # $1 ch17 (S3)
    export CH_BACKEND=docker CH_IMAGE CH_CONTAINER=searchbench-ch-1b-$1 CH_DATA_DIR=/mnt/local/clickhouse-1b-$1 CH_STORAGE=s3
    export CH_ORDER_BY="(ServiceName, Timestamp)" CH_MAX_PART_BYTES=1825361101 CH_PHRASE_INDEX=on   # 1.7 GiB
    export CH_S3_ENDPOINT=https://<bucket>.s3.us-east-1.amazonaws.com/$CH_S3_PREFIX-$1/
    return 0
}

# 1) load
c=ch17; CB=26.10.2.103-m8gd.4xlarge-s3-sorted-$c-1b-$TAG
( ch_env $c; export SEARCHBENCH_VERSION=$CB; [[ ! -e "$CH_DATA_DIR" ]]
  ingest clickhouse-$c-ingest clickhouse searchbench-ch-1b-$c ) || exit 1
( ch_env $c; cd "$ROOT/clickhouse-$c-ingest/source/clickhouse"; export CLICKHOUSE_BIN="$PWD/ch-exec"; stopall
  ./start >/dev/null 2>&1; for i in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
  docker exec -i "$CH_CONTAINER" clickhouse client -q "SELECT count(), sum(rows), sum(bytes_on_disk), max(bytes_on_disk), min(bytes_on_disk) FROM system.parts WHERE active AND table='otel_logs'" > "$ROOT/clickhouse-$c-parts.txt"
  docker exec -i "$CH_CONTAINER" clickhouse client -q "SELECT name, rows, bytes_on_disk, level FROM system.parts WHERE active AND table='otel_logs' ORDER BY name FORMAT TSV" > "$ROOT/clickhouse-$c-parts-list.tsv"
  docker stop -t 30 "$CH_CONTAINER" >/dev/null ) || exit 1
push
# keep the table reusable: its local part metadata, with ClickHouse stopped
stage save-ch17-localstate
tar -C /mnt/local -czf /mnt/local/clickhouse-1b-ch17-localstate.tar.gz clickhouse-1b-ch17
aws s3 cp --only-show-errors /mnt/local/clickhouse-1b-ch17-localstate.tar.gz s3://<bucket>/$CH_S3_PREFIX-ch17-localstate/clickhouse-1b-ch17-localstate.tar.gz
rm /mnt/local/clickhouse-1b-ch17-localstate.tar.gz

# 2) three rounds, ch17 alternated with qw64 (ingest-API index), order rotated
declare -A ITAG=([qw64]=20261105T-qw-ingestapi-1b-stl20)
declare -A IQB=([qw64]=ingestapi-71c9181-rust16-shards48-stl20mb-commit30-split10m-stablelog-m8gd4x-64g-s3-1b-20261105T-qw-ingestapi-1b-stl20)
declare -A ISPL=([qw64]=64)
col() {   # $1 column, $2 round
    local c=$1 r=$2
    case $c in
      ch17) local CB=26.10.2.103-m8gd.4xlarge-s3-sorted-$c-1b-$TAG
        ( ch_env $c; export CH_QUERY_SETTINGS="--max_threads=32"
          queries r$r-$c clickhouse searchbench-ch-1b-$c queries-pr17.sql "s3-$c-threads32-r$r" "$CB" "$ROOT/clickhouse-$c-ingest-load" ) ;;
      qw64) local QB=${IQB[$c]}
        mkdir -p "$ROOT/load-$c"; cp "$ROOT/loads/otel_logs_1b.$QB.json" "$ROOT/load-$c/"
        ( export QW_IMAGE QW_PULL_IMAGE=no QW_STORAGE=s3 QW_CONTAINER=searchbench-qw-1b-s3-$c QW_DATA_DIR=/mnt/local/quickwit-1b-s3-$c
          export QW_ENABLE_IN_MEMORY_INDEXING=true QW_S3_ROOT=s3://<bucket>/searchbench/m8gd4x-1b-quickwit-s3-${ITAG[$c]}
          mkdir -p "$QW_DATA_DIR"; touch "$QW_DATA_DIR/.searchbench-loaded"   # query-only: searcher + metastore
          export SEARCHBENCH_QW_METRICS_LOG="$ROOT/r$r-$c-metrics.jsonl"
          queries r$r-$c quickwit "$QW_CONTAINER" queries.dsl "s3-$c-r$r" "$QB" "$ROOT/load-$c"
          grep -q '^  fast_field_cache_capacity: 0$' "$RUN/source/quickwit/config/quickwit_s3.yaml"
          python3 - "$RUN" "${ISPL[$c]}" <<'PY'
import json,glob,sys
d=json.load(open([f for f in glob.glob(sys.argv[1]+'/results/*_otel_logs_1b.json')][0]))
assert d['storage_units']=={'count':int(sys.argv[2]),'unit':'splits'},d['storage_units']
PY
        ) ;;
    esac
}
ORDER=(ch17 qw64)
for r in 1 2 3; do
    if (( r == 2 )); then seq=(qw64 ch17); else seq=(ch17 qw64); fi
    for c in "${seq[@]}"; do
        col "$c" "$r" || { echo "column $c round $r failed" >&2; echo "r$r $c" >> "$ROOT/failed-trials.txt"; }
    done
done

# 3) ClickHouse per-try ProfileEvents (S3 GETs/bytes)
c=ch17; ( ch_env $c; cd "$ROOT/clickhouse-$c-ingest/source/clickhouse"; export CLICKHOUSE_BIN="$PWD/ch-exec"; stopall
    ./start >/dev/null 2>&1; for i in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
    docker exec -i "$CH_CONTAINER" clickhouse client --query "SYSTEM FLUSH LOGS"
    docker exec -i "$CH_CONTAINER" clickhouse client --query "
        SELECT event_time_microseconds t, query_duration_ms/1000 s, type, read_rows, read_bytes, peak_threads_usage,
               ProfileEvents['S3GetObject'] gets, ProfileEvents['ReadBufferFromS3Bytes'] bytes,
               Settings['max_threads'] max_threads, Settings['use_statistics'] use_statistics,
               toJSONString(ProfileEvents) events, replaceAll(query, '\n', ' ') q
        FROM system.query_log WHERE type IN ('QueryFinish','ExceptionWhileProcessing') AND is_initial_query
          AND event_time >= toDateTime('$(cat "$ROOT/r1-$c/query-window-start.txt")')
        ORDER BY event_time_microseconds FORMAT TSVWithNames" | gzip > "$ROOT/clickhouse-$c-querylog.tsv.gz"
    docker stop -t 30 "$CH_CONTAINER" >/dev/null ) || true
push
