# hippo-20261105 · m8gd.4xlarge · S3 · 1B · public bench

> **USED IN THE PUBLIC BENCH (2026-11-05, columns updated 2026-11-06; OpenSearch added 2026-11-19, its index sort changed to Timestamp desc 2026-11-20).** This run set holds the S3 numbers of the
> public benchmark, listed in [`PUBLIC_BENCH.md`](../../PUBLIC_BENCH.md). It is the site's default page.

| column | session | layout | median cold / hot | cold S3 GETs | load | load CPU |
|---|---|--:|--:|--:|--:|--:|
| Datadog BYOC 0.1.36 · S3 · ingest API · 64 splits | 2026-11-04 | 64 splits, 42.1 GiB | **515 / 338 ms** | 26.4k | 2,364 s | 26,241 s |
| ClickHouse · S3 · 1.7 GiB cap · 61 parts | 2026-11-03 | 61 parts, 58.1 GiB | **1,343 / 894 ms** | 64.1k | 1,288 s | 9,344 s |
| OpenSearch 3.8 · S3 · searchable snapshot · 16 shards | 2026-11-19 | 16 shards x 1 segment, 80.0 GiB | 6,089 / 266 ms\* | 10.6k | 8,847 s | 50,340 s |
| DuckDB 2.0 · S3 · sorted copy | 2026-11-01 | no index, 24.7 GiB copy | 7,769 / 6,683 ms | 401.6k | 1,453 s | 10,057 s |
| Loki 3.7.9 · S3 · prefilter on · c16 · 600 s cap | 2026-11-19 | 109,811 objects, 69.7 GiB | 54/56 ok (600 s cap)† | —† | 5,256 s | 31,690 s |

\* OpenSearch's hot tries read its local file cache (0 S3 GETs), not S3: only its cold try is comparable with
the other columns. See [OpenSearch](#opensearch-38--s3--searchable-snapshot--16-shards).
† Loki ran with a **600 s** timeout cap (10× the 60 s cap of the other columns), so 54 of its 56 supported
queries finish (Q14 and Q29 time out at 600 s; Q09 OR-minmatch is unsupported). It is left out of the 57-query
geomean, CPU and GET common sets. See [Loki](#loki-379--s3--prefilter-on--c16--600-s-cap).

Geomean over the **57 queries all four answer**. Datadog BYOC, ClickHouse and OpenSearch: per query, the median across 3 rounds
(hot = min of tries 2–3 in each round), then the geomean; the page shows the per-try medians of the rounds, so its
hot geomean can differ by ~2–3%. DuckDB: one run.

**Load CPU is the engine's own CPU** (`load_cpu` in the load records = the engine containers, ingest + merges
until settled). Datadog BYOC's figure excludes the Rust ingest client (2,313 s converting the Parquet to JSON and
sending it); `load_cpu_total` in the load record keeps it (28,554 s). ClickHouse's excludes `./load` (7 s). OpenSearch's excludes its bulk-ingest client (13,968 s). Loki's excludes its Rust ingest client (12,086 s).
DuckDB's is its worker process (the sort into the S3 copy), unchanged.

**Not the same box.** Datadog BYOC and ClickHouse ran a day apart on two m8gd.4xlarge. Each box also ran a Quickwit
index on the same settings, which ties them together: on the ClickHouse box, Quickwit 71c9181 through the ingest API
(48 shards, 20 MB/s, 30 s commit, 64 splits) measured 500 / 346 ms, the same as Datadog BYOC's 515 / 338 ms within 3%,
and ClickHouse vs that index on the same box was 2.7x / 2.6x. ClickHouse vs Datadog BYOC here: 2.6x / 2.6x.

## Datadog BYOC 0.1.36 · S3 · ingest API · 64 splits

Datadog BYOC 0.1.36 (`datadog/cloudprem:v0.1.36`, commit `f7a90e9`, built 2026-11-03), a Quickwit fork, run
with the repository's Quickwit-based adapter (`datadog-byoc/`). It refuses to start without a Datadog API key unless
reverse connection is off, so the adapter passes `CP_ENABLE_REVERSE_CONNECTION=false` (`QW_EXTRA_ENV`, the
adapter's default).

- **Ingest:** the HTTP ingest API, from the Rust client in `datadog-byoc/client/` (16 threads, 2,000 docs/request),
  reading `part_000.parquet`. 472k docs/s; exact 1B count checked.
- **Index settings:** `shard_throughput_limit` 20mb and `min_shards` 48, so the load stays at 48 shards (the
  default 5 MiB/s limit made Quickwit scale to ~250 shards) and 16 indexing pipelines (3 shards per pipeline).
  Commit 30 s, 10M-doc splits, `stable_log` merges, validation off, ingest queue 32 GiB / 80 GiB, in-memory
  indexing, document clustering as in the bench node config. 64 splits after merges (55 × ~17.7M docs).
- **Load:** 2,364 s to merges settled (the 180 s settle check excluded on the page); Datadog BYOC peak memory 43.3 GiB.
- **Queries:** S3, fast-field, partial-request and predicate caches off, split footer cache on; restart +
  page-cache drop before every query, 3 tries, 60 s cap (none hit). 3 rounds 2026-11-04 12:08–12:47 UTC, alternated
  with the 82-split Quickwit bulk-load index (queried with Datadog BYOC: 392 / 294 ms). Evidence: `extra/datadog-byoc-c30/`.
- **Commit 45 s** was run the same way on a second box: 101 splits (first splits ~1.04M docs, so one 10-way merge
  already passes 10M docs), 525 / 379 ms. Not a column.

## ClickHouse · S3 · 1.7 GiB cap · 61 parts

ClickHouse 26.10.2.103 (`clickhouse/clickhouse-server@sha256:ec7bc702…`), `ORDER BY (ServiceName, Timestamp)`,
`DateTime64(9)`, text index with phrase positions, **1.7 GiB part cap** (1,825,361,101 B), `s3` disk with no
filesystem cache, [serenedb/searchbench#17](https://github.com/serenedb/searchbench/pull/17) queries,
**`max_threads=32`**. Off: query cache, condition cache, text-index data caches (tokens, negative tokens, pattern
bypass, postings), statistics. On: mark, primary-index and text-index header caches.

- **Load:** 1,288 s (server-side Parquet INSERT, merges settled), 61 parts, 58.1 GiB. Its local part metadata is
  saved on S3, so the table can be reattached without a reload.
- **Queries:** restart + page-cache drop before every query, 3 tries, 60 s cap (none hit). 3 rounds 2026-11-03
  21:32–22:30 UTC, alternated with the Quickwit 71c9181 ingest-API index (64 splits, 500 / 346 ms). Evidence,
  including `system.query_log`: `extra/clickhouse-1p7g/`.

## OpenSearch 3.8 · S3 · searchable snapshot · 16 shards

OpenSearch 3.8.0 with the `repository-s3` plugin, **lean mapping** (`OS_MAPPING=lean`,
[`opensearch/config/index_mapping_lean.json`](../../opensearch/config/index_mapping_lean.json)): it indexes only
what the queries use, like the Datadog BYOC column's index config (Body text with positions; ServiceName, SeverityText
keywords; SeverityNumber and Timestamp with points and doc values; ScopeName doc values; the other fields stored
only), with derived source (no `_source`), zstd level 6, ResourceAttributes / ScopeAttributes as doc-values
keywords, and the index sort `ServiceName asc, Timestamp desc` (newest first within each service, as
Elasticsearch's logsdb does). `./load` builds the index on local disk (16 shards, each force-merged to 1 segment),
snapshots it to S3 and mounts it as a searchable snapshot (`remote_snapshot`); the node fetches 8 MiB blocks into
a local file cache.

- **Load:** 8,847 s (ingest 4,362 s; most of the rest is the force merge, one shard at a time), 16 segments,
  80.0 GiB on S3. Load CPU 50,340 s is the OpenSearch container; the ingest client's 13,968 s is excluded, as for
  Datadog BYOC (`load_cpu_total` in the load record keeps both: 64,308 s).
- **Queries:** restart + file-cache wipe before every query, so the cold try reads from S3; 3 tries, 60 s cap
  (none hit), all 83 supported queries answered (the 9 joins are unsupported). 3 rounds, 2026-11-19, on its
  own m8gd.4xlarge (not interleaved with the other columns). 56 of the 57 common queries return ClickHouse's
  answers in every round (Q67's two-key group-by reports only the outer buckets).
- **Hot tries read the local file cache.** No result caching: the request and query caches are off and count 0
  hits. The file cache cannot be disabled for a searchable snapshot, and capping it does not make hot tries read
  S3. So OpenSearch's hot tries make 0 S3 GETs, unlike the other columns, which re-read S3 on every try: **only its
  cold try is comparable**. On the page, the hot ratio to the fastest engine often takes OpenSearch as the
  baseline, which changes the other columns' hot ratios and "fastest" marks; their absolute times and the cold
  view are unchanged.
- **S3 requests:** 10.6k GETs and 82.8 GiB cold over the 57 queries, 0 hot (repository GetObject counts; bytes =
  file-cache growth, whole 8 MiB blocks).
- **Index sort `Timestamp desc` (2026-11-20).** It replaces the same index with `Timestamp asc` (cold / hot
  6,446 / 378 ms, 80.2 GiB): the 16 recent queries (sort by Timestamp desc, 100 hits) gain (cold 7,497 vs
  9,194 ms, hot 89 vs 312 ms), the other 41 common queries are unchanged (cold 5,615 vs 5,612 ms). Same-box A/B:
  [`opensearch/README.md`](../../opensearch/README.md#index-sort-servicename-asc-timestamp-desc). Rounds, metrics
  and the cache check: [`extra/opensearch-s3-1b/`](extra/opensearch-s3-1b/README.md).

## DuckDB 2.0 · S3 · sorted copy

DuckDB 2.0.0, duckdb-parquet with the 2026-11-01 scan-time token regexes (`duckdb-parquet/macros.sql`), no index
and no stored tokens. 16 threads, memory limit 48 GB; Parquet footer and HTTP metadata cached, **no data cache**
(`enable_external_file_cache` off), so every try reads its data from S3. `./load` writes a copy sorted by
`ServiceName, Timestamp` (ZSTD, 122,880-row row groups) to S3: 1,453 s, 40.99 → 26.56 GB (24.7 GiB). Same method as the
other columns (worker restart + page-cache drop per query, cold/hot/hot, 60 s cap, none hit). It was measured in
**a separate session on another m8gd.4xlarge, not interleaved with the pair, one run.** How the configuration was
chosen: [`duckdb-parquet/TUNING.md`](../../duckdb-parquet/TUNING.md). Evidence (environment, HTTP stats, result
log): `extra/duckdb/`.

## Loki 3.7.9 · S3 · prefilter on · c16 · 600 s cap

Grafana Loki 3.7.9 (`grafana/loki:3.7.9`, commit `d3b4a2f7`), TSDB schema v13, JSON frame encoding, Snappy chunks
on S3 (`object_store: s3`), query target `read,query-scheduler`, querier concurrency 16, `LOKI_LITERAL_PREFILTER=on`,
result and chunk caches off (persisted TSDB index metadata cached).

- **Load:** 5,256 s wall, 69.7 GiB (74.8 GB) in 109,811 objects (109,810 chunk objects + 1 compacted index object),
  exact 1,000,000,000 rows verified. Load CPU 31,690 s is the Loki containers only; the Rust ingest client's
  12,086 s is excluded, as for Datadog BYOC and OpenSearch (`load_cpu_total` keeps both: 43,776 s).
- **Queries (600 s cap):** one round, 2026-11-19, on its own m8gd.4xlarge. Stop + page-cache drop + start before
  every query, 1 cold + 2 hot tries, 600 s cap (client and server `query_timeout`). 56 supported queries (Q09
  OR-minmatch and BM25/fuzzy/proximity/joins are unsupported): **54 finish all three tries** (1.3–593 s; best hot ÷
  cold median 0.998), **Q14 and Q29 time out cold at 600 s**.
- **Excluded from the common sets:** because of its 600 s cap (10× the 60 s cap of the other four columns), the
  page marks timeouts at 600 s for this column and leaves it out of the 57-query geomean, CPU and S3 GET summaries,
  so the other four columns are unchanged. Per-query native S3 `GetObject` counts are in `s3_requests.json`
  (`partial: true`; bytes not collected). Evidence and conversion script:
  [`extra/loki-s3-600s-hot-1b-20261119/`](extra/loki-s3-600s-hot-1b-20261119/README.md).

## About this copy

- **Names.** The Datadog BYOC column ran the public image `datadog/cloudprem:v0.1.36` through this repository's
  Quickwit-based adapter, now [`datadog-byoc/`](../../datadog-byoc/README.md). The page-facing files (results,
  load record, `names.json`, `details.json`, `s3_requests.json`, `rounds.json`) name it Datadog BYOC. The raw round
  outputs under `extra/` keep the names recorded at run time (engine `Quickwit`, folder and build labels with
  `cloudprem`).
- **Redactions.** S3 bucket names, instance IDs and the IAM role name in scripts and environment dumps are replaced
  with `<bucket>`, `<instance>` and `<benchmark-role>`. Raw uploads in that bucket are not public; everything the
  page and this README use is in this directory.
