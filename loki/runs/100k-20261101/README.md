# Loki 3.7.9, 2026-11-01: 100K on local disk and S3

Two 100K columns, each with its own load, on dedicated AWS `m8gd.4xlarge`
instances in us-east-1:

| Column | Storage | Results file |
|---|---|---|
| `Loki (3.7.9-m8gd4-100k-read-snappy)` | local NVMe instance store | [`loki_3.7.9-m8gd4-100k-read-snappy_otel_logs_100k.json`](../../results/loki_3.7.9-m8gd4-100k-read-snappy_otel_logs_100k.json) |
| `Loki (3.7.9-m8gd4-100k-s3-gets)` | S3, same region | [`loki_3.7.9-m8gd4-100k-s3-gets_otel_logs_100k.json`](../../results/loki_3.7.9-m8gd4-100k-s3-gets_otel_logs_100k.json) |

All 56 supported queries finish on both columns. The other 36 of the 92 query
slots are `UNSUPPORTED` in LogQL and are `null` in the results, not zero.

| | Local | S3 |
|---|--:|--:|
| Load wall | 158.9 s | 159.8 s |
| Stored size | 6,216,983 bytes | 6,219,045 bytes, 59 objects |
| Cold sum (56 queries) | 27.38 s | 32.20 s |
| Hot sum (best of tries 2 and 3) | 25.90 s | 28.81 s |
| Cold / hot geomean | 403 / 369 ms | 513 / 442 ms |
| GetObject calls, all 56 queries, per try | n/a | 2,879 |

About 157 s of each load is the adapter's completion protocol: flush, shutdown,
compaction, a 30 s stable-inventory interval and an exact persisted row count.
Sending the 100,000 rows takes about 1.3 s.

## Build and adapter

- Image `grafana/loki:3.7.9`, pulled as
  `grafana/loki@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28`
  (`./version`: `3.7.9 d3b4a2f7`). Stock Loki; no engine change.
- Both columns ran an **earlier revision of this adapter**: local from commit
  `688075b6d1a83bcab47b6f3f718c962157c80081`, S3 from
  `248c6b71c8931597865fd457a3d976e409b36254` of the private SearchBench fork
  this adapter comes from. The two have the same load and query code
  (`248c6b7` only adds `./storage-units`). That revision differs from the
  adapter in this PR in three ways:
  - **Log line encoding.** The line is the raw `Body` and the source row id is
    structured metadata. The adapter in this PR writes a versioned JSON frame
    (`encoding.py`) that keeps duplicate rows and restores the original row.
  - **No literal prefilter.** Queries use `| json` and the Body filter only.
  - **Querier concurrency 8** (now 16).
- Settings from `config.py`: single binary, TSDB v13, Snappy chunks, 1.5 MB
  chunk target, 600 MB shard target, 15 min query split. Result, instant-result
  and chunk caches are off. Persisted TSDB index metadata survives restarts.
- Queries run on a read-only `read,query-scheduler` process (no ingester, no
  WAL replay); the persisted layout is checked unchanged after the query phase.

## Hardware

AWS `m8gd.4xlarge`: 16 Graviton4 vCPUs (no SMT), 64 GiB, local NVMe instance
store, Ubuntu 24.04.4 (`local/host.txt`).

**Not the repository's reference machine.** The other published engines ran on
GCP `n2-standard-32`: 16 cores × 2 hyperthreads, 128 GB and a 3 TB `pd-ssd`.
This run has the same number of physical cores and half the memory. Local
NVMe is faster than `pd-ssd`. The S3 column reads from S3 in the same region.

## Local disk (`local/`)

A standard `lib/benchmark.sh --index` run: load, then for every query an engine
restart, `drop_caches`, and cold / hot / hot tries with a 60 s cap. Timing is
the client round trip that `./query` reports.

The same script then loaded LZ4 and Zstd variants for comparison. They are not
in this PR. Snappy is the adapter's default and the codec of the S3 column.

- `snappy-benchmark.log.gz`: the `lib/benchmark.sh` log.
- `loki_…_otel_logs_100k.results.log.gz`: the full results log (answers of every
  try).
- `correctness-vs-clickhouse.txt`: `lib/check_results.py otel_logs_100k --ref
  ClickHouse` against a ClickHouse 100K reference. All codecs agree.
- `snappy/`: Loki config, container command, runtime, load manifest, object
  inventory and the layout checks before and after the query phase.
- `run-read-codecs.sh`: the script that ran on the host. Its later steps
  (LZ4/Zstd loads, warm screens with a tuning script) are not part of this
  column.
- `host.txt`, `loki-image-digest.json`: host and image.

## S3 (`s3/`)

**Load** (`s3/load/`): a standard `lib/benchmark.sh --index` run into an S3
prefix with `LOKI_STORAGE=s3`. Its results file is kept as
`loki_3.7.9-m8gd4-100k-s3-validation_otel_logs_100k.json` with its results log
and the `check_results.py` comparison with ClickHouse. Its load record is
[`otel_logs_100k.3.7.9-m8gd4-100k-s3-validation.json`](../../results/load/otel_logs_100k.3.7.9-m8gd4-100k-s3-validation.json);
the published S3 column takes its load time and size from it.

**Query timings and GetObject counts** (`s3/queries/`): a second, query-only
run over the same prefix, by `s3/queries/queries.py`, not `lib/benchmark.sh`.
It produced the published S3 results file and pairs each try's wall time, CPU
and GetObject count. The method is the same as the standard driver's:

- one engine restart and `drop_caches` per query, then cold / hot / hot,
  60 s cap; no try reached it;
- wall time is the HTTP query inside `adapter.query_response`; CPU is the Loki
  container's cgroup CPU over the same window;
- one unmeasured Q01 runs first, to load the persisted TSDB index metadata; its
  calls are not counted;
- every returned answer (168 tries) matches the load run's answers
  (`checks.json`), and the object inventory is unchanged after the run.

The GetObject count is Loki's
`loki_s3_request_duration_seconds_count{operation="S3.GetObject"}`, differenced
around each try. It counts completed SDK calls, not HTTP requests: retries
inside the SDK are hidden. The labels `200`/`500` mean success or error returned
to the instrumentation wrapper, not HTTP status codes. Downloaded bytes were not
collected and are `null`.

- `s3_requests.json`: GETs per query and try, as
  `{"<dataset>/<system>": {source, partial, queries}}`.
- `measurements.jsonl.gz`: per try, wall, CPU, GETs and the answer.
- `method.json`, `checks.json`, `status`, `run.log.gz`: method, completion
  checks and run log.
- `environment.sh`, `config.json`, `runtime.json`, `manifest.json`,
  `destination.json`, `preflight.json`: settings, Loki config and destination.
  `original-load.json` is the load record from the load run.
- `run.sh`, `queries.py`: the scripts. They read the earlier run's files on the
  host and need its S3 data, so they don't run as is.

Prometheus scrapes (562 of them, 9.6 MB) are left out; `measurements.jsonl.gz`
holds the per-try differences computed from them.

Bucket, account, role, instance and host names are replaced by `<bucket>`,
`<account>`, `<role-arn>`, `<instance-id>`, `<host>` and `<home>`.

## Caveats

- 100,000 rows span only 181 ms of event time, so every time-window query
  covers the whole corpus. This shows little about larger scales.
- The S3 query phase ran about 3.5 hours after the load, as a separate run with
  the instance store reformatted; only the S3 data and the load manifest were
  carried over.
- One round per column.
