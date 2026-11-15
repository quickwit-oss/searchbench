#!/usr/bin/env bash
# HOST. Fresh S3 1B load + standard query phase + 600 s diagnostic. No retries of anything.
#  1. dry check (source rows, load-only limits, S3 prefix empty)
#  2. measured load: load_overlay.py (load-only limits 1024/2048/1024MB/2048MB, windowed
#     exact count gate) with an S3 SDK-call logger (aborts only if no PutObject succeeds)
#  3. load record results/load/otel_logs_1b.<build>.json (load_time = overlay wall,
#     data_size = ./data-size = all S3 objects, storage_units = ./storage-units)
#  4. independent windowed count via the read target; verify-layout
#  5. phase 2: phase2.sh (lib/benchmark.sh query-only, full queries.logql, 3 tries,
#     60/60 s, restart + drop_caches, CPU, GetObject per try, answer check)
#  6. phase 3: diag600.py (Q01, Q25, Q61; cold + 2 hot; 600 s) until the deadline
set -euo pipefail
cd /srv/lp
source ./run.env
source ./remote.env
E=/srv/lp/evidence
P=$E/progress.log
note() { echo "$(date -u +%FT%TZ) $*" | tee -a "$P"; }
[[ -f $E/setup-passed && ! -f $E/production-attempted ]]
date -u > $E/production-attempted
cat /proc/$$/cgroup > $E/client-parent-cgroup.txt
systemctl show searchbench.slice -p ActiveState -p CPUAccounting -p ControlGroup > $E/engine-slice.txt
DEADLINE=$(( WORK_CUTOFF_EPOCH - DRIVER_DEADLINE_MARGIN ))
FILE=$SEARCHBENCH_DATA_DIR/part_000.parquet
[[ ! -e $LOKI_DATA_DIR ]]
"$SEARCHBENCH_PYTHON" load_overlay.py --dry-check $E/load-dry-check | tee $E/load-dry-check.json
jq -e --arg d "$SEARCHBENCH_DATA_DIR" --arg f "$FILE" --argjson n "$EXPECTED_ROWS" --arg p "$DATA_PREFIX" \
  '.source_dir==$d and (.source_files|keys)==[$f] and .source_files[$f]==$n and .target=="all"
   and .effective_limits=={"ingestion_rate_mb":1024,"ingestion_burst_size_mb":2048,"per_stream_rate_limit":"1024MB","per_stream_rate_limit_burst":"2048MB"}
   and .query_timeout=="600s" and .storage=="s3" and .bucket=="<bucket>" and .prefix==$p and .chunk_encoding=="snappy"
   and .storage_config=={"s3":{"bucketnames":"<bucket>","region":"us-east-1"}} and .object_prefix==$p' $E/load-dry-check.json
note "dry check passed: 1 file, $EXPECTED_ROWS rows, S3 s3://<bucket>/$DATA_PREFIX empty, load-only limits 1024/2048/1024MB/2048MB"
mkdir -p $E/load
env | sort > $E/load/env.txt
sync; echo 3 > /proc/sys/vm/drop_caches
pidstat -r -u -d -h -G 'loki' 5 > $E/load/pidstat.log & PS=$!
vmstat -w 5 > $E/load/vmstat.log & VM=$!
trap 'kill "$PS" "$VM" 2>/dev/null || true' EXIT
note "load start (load_overlay.py, fresh $LOKI_DATA_DIR + s3://<bucket>/$DATA_PREFIX)"
"$SEARCHBENCH_PYTHON" load_overlay.py $E/load > $E/load/driver.log 2>&1 & LOAD=$!
"$SEARCHBENCH_PYTHON" s3ops_logger.py "http://127.0.0.1:$LOKI_PORT/metrics" $E/load/s3ops.jsonl $E/load/ABORTED "$LOAD" \
  > $E/load/s3ops-logger.log 2>&1 & S3L=$!
lrc=0; wait "$LOAD" || lrc=$?
kill "$S3L" 2>/dev/null || true
kill "$PS" "$VM" 2>/dev/null || true
trap - EXIT
tail -n 5 $E/load/driver.log
if [[ -f $E/load/ABORTED ]]; then note "LOAD ABORTED by S3 guard: $(cat $E/load/ABORTED)"; exit 3; fi
(( lrc == 0 )) || { note "load failed rc=$lrc"; exit "$lrc"; }
note "load done: $(jq -c '{status,wall_seconds,load_cpu_seconds,data_size_bytes,storage_units}' $E/load/load-record.json)"
for f in manifest.json load-started.json destination.json config.json runtime.json ingest-metrics.txt; do
  [[ -f $LOKI_DATA_DIR/$f ]] && cp "$LOKI_DATA_DIR/$f" $E/load/
done
jq -e --argjson n "$EXPECTED_ROWS" '.rows==$n and .query_store_only==true and (.persisted_inventory|type)=="string"
       and .storage=="s3" and .verification_stats.verification_method=="partitioned-native-count"
       and .verification_stats.ingester.totalReached==0' $LOKI_DATA_DIR/manifest.json
# Standard load record (lib/benchmark.sh write_load_record schema) for the query-only phase 2.
L=source/loki
(cd $L && ./start)
(cd $L && ./version) > $E/load/version.txt
grep -q "^$LOKI_EXPECTED_VERSION " $E/load/version.txt
DS=$(cd $L && ./data-size); SU=$(cd $L && ./storage-units)
mkdir -p $L/results/load
jq -n --arg build "$SEARCHBENCH_BUILD" --arg ev "$(cat $E/load/version.txt)" \
  --arg os "$(awk -F= '$1=="PRETTY_NAME"{gsub(/"/,"",$2);print $2}' /etc/os-release)" --arg date "$(date +%F)" \
  --argjson ds "$DS" --argjson su "$SU" --slurpfile r $E/load/load-record.json --slurpfile m $LOKI_DATA_DIR/manifest.json \
  '{engine:"Loki", dataset:"otel_logs_1b", build:$build, engine_version:$ev,
    load_time:($r[0].wall_seconds*1000|round/1000), data_size:$ds, storage_units:$su, os:$os, date:$date,
    loaded_by:"queries.logql",
    phases:{ingest:$m[0].ingest_seconds, adapter_load:$m[0].load_seconds},
    load_cpu:($r[0].load_cpu_seconds*1000|round/1000),
    load_cpu_containers:($r[0].load_cpu_containers_seconds*1000|round/1000),
    load_cpu_procs:($r[0].load_cpu_procs_seconds*1000|round/1000)}' > $L/results/load/otel_logs_1b.$SEARCHBENCH_BUILD.json
cp $L/results/load/otel_logs_1b.$SEARCHBENCH_BUILD.json $E/load/
jq -e --slurpfile r $E/load/load-record.json '.data_size==$r[0].data_size_bytes and .storage_units==$r[0].storage_units' \
  $L/results/load/otel_logs_1b.$SEARCHBENCH_BUILD.json
note "load record: $(jq -c '{load_time,data_size,storage_units,load_cpu}' $L/results/load/otel_logs_1b.$SEARCHBENCH_BUILD.json)"
./checkpoint.sh load
# Independent windowed exact count through the read target (store only).
"$SEARCHBENCH_PYTHON" count_windows.py independent $E/load/independent-count.json | tee $E/load/independent-count.stdout
(cd $L && ./stop)
(cd $L && "$SEARCHBENCH_PYTHON" ./adapter.py verify-layout) | tee $E/load/verify-layout.txt
du -sb "$LOKI_DATA_DIR" > $E/load/du.txt
note "row count verified: $EXPECTED_ROWS (load gate + independent read-target windows); layout unchanged"
# Phase 2: standard query phase.
pidstat -r -u -d -h -G 'loki' 5 > $E/pidstat-phase2.log & PS=$!
vmstat -w 5 > $E/vmstat-phase2.log & VM=$!
note "phase2 start (lib/benchmark.sh query-only, 92 slots); deadline $(date -u -d @"$DEADLINE" +%FT%TZ)"
p2=0
SEARCHBENCH_DATA_DIR=$SEARCHBENCH_DATA_ROOT ./phase2.sh $L $E/phase2 "$DEADLINE" > $E/phase2.log 2>&1 || p2=$?
kill "$PS" "$VM" 2>/dev/null || true
echo "$p2" > $E/phase2-rc.txt
note "phase2 rc=$p2 harness=$(cat $E/phase2/harness-rc.txt 2>/dev/null) $(tail -n 1 $E/phase2/collate.stdout 2>/dev/null)"
(( p2 == 2 )) && note "P0: phase-2 answer MISMATCH: $(jq -c '[.mismatches[]|{query,try}]' $E/phase2/collated/answers.json)"
(cd $L && "$SEARCHBENCH_PYTHON" ./adapter.py verify-layout) > $E/phase2/verify-layout.txt 2>&1 || note "phase2 verify-layout FAILED"
./checkpoint.sh phase2 || true
# Phase 3: 600 s diagnostic until the deadline.
if (( DEADLINE - $(date +%s) >= 900 )); then
  note "phase3 start (diag600 $PHASE3_QUERIES); deadline $(date -u -d @"$DEADLINE" +%FT%TZ)"
  pidstat -r -u -d -h -G 'loki' 5 > $E/pidstat-phase3.log & PS=$!
  p3=0
  "$SEARCHBENCH_PYTHON" ./run_until.py "$DEADLINE" "$SEARCHBENCH_PYTHON" diag600.py --out $E/phase3 --engine-dir $L \
    --deadline-epoch "$DEADLINE" --references references.json --queries "$PHASE3_QUERIES" > $E/phase3.log 2>&1 || p3=$?
  kill "$PS" 2>/dev/null || true
  echo "$p3" > $E/phase3-rc.txt
  note "phase3 rc=$p3 $(tail -n 1 $E/phase3.log)"
  ./checkpoint.sh phase3 || true
else
  note "phase3 skipped: under 15 min to the driver deadline"
fi
(cd $L && ./stop) || true
[[ $(stat -c %s "$FILE") == "$CORPUS_BYTES" ]] && stat -c "%s %Y %n" "$FILE" > $E/corpus-final-stat.txt
date -u > $E/production-completed
note "production complete phase2=$(<$E/phase2-rc.txt) phase3=$(cat $E/phase3-rc.txt 2>/dev/null || echo skipped)"
