# Loki 3.7.9, 2026-11-15: 1B on local disk and S3

Three 1B columns from two loads, each on its own dedicated AWS `m8gd.4xlarge`
in us-east-1:

| Column | Storage | Literal prefilter | Querier concurrency | Results file |
|---|---|---|--:|---|
| `Loki (3.7.9-m8gd4-1b-local-frame-prefilter-off-c8-20261115)` | local NVMe | off | 8 | [`…prefilter-off-c8-20261115_otel_logs_1b.json`](../../results/loki_3.7.9-m8gd4-1b-local-frame-prefilter-off-c8-20261115_otel_logs_1b.json) |
| `Loki (3.7.9-m8gd4-1b-local-frame-prefilter-on-c8-20261115)` | local NVMe | on | 8 | [`…prefilter-on-c8-20261115_otel_logs_1b.json`](../../results/loki_3.7.9-m8gd4-1b-local-frame-prefilter-on-c8-20261115_otel_logs_1b.json) |
| `Loki (3.7.9-m8gd4-1b-s3-gets)` | S3, same region | on | 16 | [`loki_3.7.9-m8gd4-1b-s3-gets_otel_logs_1b.json`](../../results/loki_3.7.9-m8gd4-1b-s3-gets_otel_logs_1b.json) |

The two local columns query the same load.

| | Local | S3 |
|---|--:|--:|
| Load wall | 5,205 s | 5,046 s |
| Load CPU | 42,839 s | 42,056 s |
| Stored size | 74.86 GB, 109,849 chunk files | 74.93 GB, 109,851 objects |
| Rows (exact count after load) | 1,000,000,000 | 1,000,000,000 |

## Results: 6 of 56 queries finish

**50 of the 56 supported queries time out at 60 s on all three columns.** The
other 36 of the 92 slots are `UNSUPPORTED` in LogQL. A timed-out query shows the
60 s cap on all three tries, as `lib/benchmark.sh` records it. So **no 1B
geomean is given.**

Six queries finish on every column. They are 100-line log queries on one
service in a 30 minute window: four with a Body text filter, Q73 and Q81
without one.

| Query | Local, prefilter off, c8 | Local, prefilter on, c8 | S3, prefilter on, c16 |
|---|--:|--:|--:|
| Q68 | 57.70 / 57.88 s | 7.42 / 7.25 s | 9.19 / 7.96 s |
| Q72 | 18.55 / 18.51 s | 4.09 / 3.80 s | 4.31 / 4.29 s |
| Q73 | 1.29 / 0.85 s | 1.17 / 0.91 s | 1.36 / 1.33 s |
| Q76 | 58.14 / 58.05 s | 7.23 / 7.32 s | 8.85 / 7.97 s |
| Q80 | 18.56 / 18.58 s | 3.87 / 3.86 s | 4.15 / 4.30 s |
| Q81 | 1.32 / 0.83 s | 1.20 / 0.84 s | 1.80 / 1.47 s |

Cold / best hot. Every returned answer matches the references (see Correctness).

- **The prefilter cuts CPU, not I/O.** Lines and bytes processed are the same
  with it off and on. It drops lines that cannot match before `| json` and
  `lower` run. On the four filtered queries, cold time falls about 6× (geomean
  32.8 s → 5.4 s). Q73 and Q81 have no Body filter, so they don't change.
- **S3 vs local.** Cold is 1.05–1.50× slower on S3. The S3 column also uses
  concurrency 16 against 8, on a different instance and load, so this is not a
  storage-only comparison.
- **Hot ≈ cold.** With the chunk cache off, hot tries read the chunks again.

## Build and adapter

- Image `grafana/loki:3.7.9`, pinned as
  `grafana/loki@sha256:1a779c5f714e795ac698b125fc265ae97292f792a91535fa29726073d03d5580`
  (arm64; `./version`: `3.7.9 d3b4a2f7`). Stock Loki.
- Framed log line encoding (`encoding.py`), Snappy chunks, TSDB v13, Rust load
  client. Result, instant-result and chunk caches off. Server `query_timeout`
  600 s.
- Source commits, in the private SearchBench fork this adapter comes from:
  - **Local:** `ec8c70abdac60c35e5858c95cddde3bdb9c3d61d`. Same load and query
    code as this PR, with two older defaults (prefilter off, concurrency 8) and
    without the `literal_prefilter` field in `runtime.json`. The pair driver
    set the prefilter per column; the query server ran at concurrency 8
    (`local/raw/load/query-runtime.json`).
  - **S3:** `5e9c64e6ffe900325f70e0cbad722a8e89b48297`. Its `loki/` code is
    the adapter in this PR. Only the README, two file names (`queries.py` is
    `logql.py` here, plus one test fixture), the import and test-name edits
    that follow from those renames, and three dropped experiment files
    (`experiments/query_ab.py`, `experiments/tune_100k.py`,
    `tests/test_query_ab.py`) differ.

## Hardware

AWS `m8gd.4xlarge`: 16 Graviton4 vCPUs (no SMT), 64 GiB, local NVMe instance
store, Ubuntu 24.04.5.

**Not the repository's reference machine.** The other published engines ran on
GCP `n2-standard-32`: 16 cores × 2 hyperthreads, 128 GB and a 3 TB `pd-ssd`.
This run has the same number of physical cores and half the memory. Local
NVMe is faster than `pd-ssd`. The S3 column reads from S3 in the same region.

## Load (both)

`load_overlay.py` (`s3/scripts/`) calls `adapter.load()` with four limits
raised for the load process only: `ingestion_rate_mb` 1024,
`ingestion_burst_size_mb` 2048, `per_stream_rate_limit` 1024MB and its burst
2048MB (`load-overrides.json`). At the default 128 MB/s, an earlier 10M load
got HTTP 429. Queries use the stock limits.

An unfiltered 1B count can exceed the 600 s server timeout, so the row-count
gate counts in time windows (`count_windows.py`). An independent windowed count
through the read target also gives 1,000,000,000 rows
(`independent-count.stdout`).

The load time includes ingestion, flush, stop, finalization, compaction settle,
the count and the frozen-inventory check. Corpus download, client build and
image pull are excluded.

## Local disk (`local/`)

The two columns come from
[`experiments/query_pair.py`](../../experiments/query_pair.py), not from
`lib/benchmark.sh`. The driver runs both variants of each query back to back
on one load. Before each variant's cold try it uses the `lib/benchmark.sh` cold
cycle: stop, wait, `sync` + `drop_caches`, start. Then cold / hot / hot with
the same 60 s cap. The variant that goes first alternates by query.

Each column's results file maps the pair driver's trials to the
`lib/benchmark.sh` format:

- a finished try keeps the timing line that `./query` printed;
- a timed-out try is the cap, and its remaining tries are padded with the cap;
- CPU is the engine's cgroup CPU during the try;
- `load_time`, `data_size` and `storage_units` come from the shared load
  ([`otel_logs_1b.3.7.9-m8gd.4xlarge-nvme-frame-1b-20261115.json`](../../results/load/otel_logs_1b.3.7.9-m8gd.4xlarge-nvme-frame-1b-20261115.json)).

The converter that produced these files was tied to the original repository
layout, so it is not included. The files are the originals, byte for byte.

- `raw/phase-a/`: the pair driver's plan (its `queries_sha256` hashes the
  translator, `queries.py` at `ec8c70a`, now `logql.py`), per-try trials
  (`trials.jsonl.gz`), exact LogQL per variant (`requests.json`), equality
  check and summary. `answers/` holds the finished answers.
- `raw/load/`: load receipt, overlay, row counts, layout check, Loki version,
  config and runtime of the load and query servers, and `du` of the data dir.
- `raw/provenance/`: corpus receipt and hash, source archive hash, client hash,
  image inspect, Loki binary version, `uname`, the load phase's log.
- `check_answers.py`: compares the answers with the DuckDB 1B reference
  (`raw/reference-answers-duckdb-1b.json`). Run `python3 check_answers.py` from
  `local/`.

## S3 (`s3/`)

A standard `lib/benchmark.sh` query-only run over the loaded prefix:
`SEARCHBENCH_RESTART=yes` (restart + `drop_caches` before every query), cold /
hot / hot, 60 s cap, `SEARCHBENCH_CPU=on`. To count GetObject calls per try,
`./query` and `./cpu-usage` were wrapped for this phase only
(`scripts/sbwrap-*`; hashes in `queries/wrappers.sha256`). The wrappers save
each try's stdout and scrape the GetObject counter outside the CPU window.

GetObject calls per try on the six finished queries: 2,065–8,464. The count
is Loki's `loki_s3_request_duration_seconds_count{operation="S3.GetObject"}`.
It counts SDK calls, not HTTP requests. 2.8% of them carry the error label. For
timed-out queries the count is `null`. Bytes were not collected.

- `queries/`: results log, harness log, collated GETs
  (`collated/s3_requests.json`), per-try records (`collated/tries.jsonl`),
  answer verdicts (`collated/answers.json`: 18 of 18 match) and checks.
- `load/`: load receipt, overlay, counts, layout check, config, runtime, gzipped
  manifest, driver log.
- `provenance/`: logs, image, Loki version, `uname`, host state at the end,
  CPU-accounting slice, corpus and source hashes.
- `scripts/`: host scripts that produced the load and the query phase:
  `setup.sh`, `production.sh`, `phase2.sh`, `load_overlay.py`,
  `count_windows.py`, wrappers, GET scraper, S3 call logger, collation, answer
  check, and the references (`references.json.gz`). A later 600 s diagnostic
  that `production.sh` also starts is not part of the column and is left out,
  as are the instance launch and upload scripts. The kept scripts reference
  those omitted helpers and host env files (`run.env`, `remote.env`), so they
  are a record of what ran, not runnable as is.

## Correctness

- Local: `python3 local/check_answers.py` (in `local/`) compares the 12 stored
  answers (6 queries, off and on) with the DuckDB 1B reference. Logs are
  compared in order at microsecond precision. The off and on answers are also
  equal (`raw/phase-a/equality.json`).
- S3: the 18 returned tries match the DuckDB 1B rows in order, the ClickHouse 1B
  value and the local Loki answers (`queries/collated/answers.json`).

## Caveats

- Only 6 of 56 queries finish at 60 s. These six are similar queries and say
  nothing about the other 50.
- One load and one round per column.
- The local columns come from the pair driver. The S3 column is a
  `lib/benchmark.sh` run.
- Timed-out tries keep the CPU used until the kill (510–900 CPU-s).
  That is not the CPU the query needs.

Bucket and host names are replaced by `<bucket>` and `<host>`.
