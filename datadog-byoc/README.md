# Datadog BYOC adapter

Datadog BYOC ("bring your own cloud") log management runs a fork of [Quickwit](https://quickwit.io).
This adapter runs its public image by default,
`datadog/cloudprem:v0.1.36@sha256:85c0bd13a088878db1a78b48563709901ff4737543c77d297b6d260a1deeb81f`
(version 0.1.36, commit `f7a90e9`), with `QW_EXTRA_ENV=CP_ENABLE_REVERSE_CONNECTION=false` because the benchmark
runs without a Datadog API key. Any Quickwit image also works: set `QW_IMAGE` (and `QW_EXTRA_ENV=`). The
environment variables keep their `QW_` prefix. The engine is queried through its
**Elasticsearch-compatible API** at `/api/v1/_elastic`. Ingest goes through the
native `/api/v1/<index>/ingest` endpoint (lossless; see below).

```
./benchmark.sh --index      # load + run
./benchmark.sh              # query-only
QW_STORAGE=s3 QW_S3_ROOT=s3://<bucket>/searchbench/<host> ./benchmark.sh --index   # index + metastore on S3
```

## 100M dataset configuration

For **`SEARCHBENCH_DATASET=otel_logs_100m`**, the adapter applies this preset to
new loads. `config/dataset-defaults.sh` is sourced by both `./start` and `./load`
so the server and index agree on document validation.

| Setting | 100M value | Override |
|---|---|---|
| Split document target | **1,000,000 documents** | `QW_SPLIT_NUM_DOCS` |
| Minimum ingest shards | **64** | `QW_MIN_SHARDS` |
| Ingest-time document validation | **Disabled** | `QW_VALIDATE_DOCS=off` |
| Commit timeout | **30 seconds** | `QW_COMMIT_TIMEOUT_SECS` |
| Merge policy | **`stable_log`**, merges enabled | `QW_MERGE_POLICY` |

The 100M `stable_log` policy explicitly uses `min_level_num_docs=100000`,
`merge_factor=10`, `max_merge_factor=12`, and `maturation_period="2days"`.
The document target is not a promise of exactly 100 final splits: shard layout,
commit timing and merge eligibility can produce a different settled split count.

Validation is disabled both in the index (`ingest_settings.validate_docs=false`)
and on the server (`QW_DISABLE_DOCUMENT_VALIDATION=true`). Documents are still
parsed by the indexing pipeline. `min_shards=64` is a minimum, not a cap, and is
independent of ingestion-client concurrency. The planned Rust HTTP rerun uses
`SEARCHBENCH_LOAD_CLIENT=rust` and `SEARCHBENCH_LOAD_PROCESSES=4`; the shard setting
does not change that to 64 clients. Parquet bulk ingestion does not use ingest-API
shards.

Explicit, non-empty `QW_*` environment overrides take precedence, including
`QW_VALIDATE_DOCS=on` and `QW_MERGE_POLICY=no_merge`. Other dataset defaults and
all existing storage, cache, clustering and ingestion options are unchanged.
These settings take effect on a **new index build** (`--index`), not retroactively
on existing data. Select a pinned Quickwit main image with `QW_IMAGE` for the rerun
and use a new build label (`--version`) so previous measurements are not overwritten.

An earlier 100M run (2026-10-30, c8gd.4xlarge, not published here) recorded `min_shards=1`; it did
not use this 64-shard / no-validation preset.

## In-memory indexing

`QW_ENABLE_IN_MEMORY_INDEXING=true` enables Quickwit's RAM-backed temporary
Tantivy indexing directory. It is **on by default** (`true`) for every dataset;
set `QW_ENABLE_IN_MEMORY_INDEXING=false` to use the disk-backed path. Builds
without this feature (e.g. the 0.9.0 release image) ignore the variable. The adapter passes it explicitly to the server
container and to the Parquet bulk-ingest CLI. Only `true` and `false` are accepted.

In the pinned main build `afb2036b75b7e6d84cfd4bdb914cbed25792cede`,
`quickwit-indexing/src/models/indexed_split.rs` selects `RamDirectory` when enabled.
At split finalization it persists the files to disk and returns a disk-backed
index for packaging. RAM-directory bytes participate in the builder's memory
accounting. This does **not** keep the entire published index in RAM, disable the
WAL, skip merges, or remove final storage writes.

The flag is cached on first use in the Quickwit process. Set it **before startup**;
changing a host-shell variable cannot change a running server. `./start` includes
the value in the container identity and recreates a container when it changes.
Check the saved container/process environment, not only the index configuration,
to verify activation. The upstream integration test
`quickwit-integration-tests/src/tests/in_memory_indexing_tests.rs` exercises ingest
and search of a published split with the flag enabled; this is not a substitute
for validating the full benchmark on the intended hardware.

Measured at 100M on m8gd.4xlarge (16 vCPU, 64 GiB), 16 client threads, 2,000
docs/request: client 371k -> 450k docs/s (+21%), searchable ingest 299.7 -> 257.6 s,
similar CPU, but peak engine anonymous memory 19.5 -> 32.9 GiB. RAM-backed scratch
is non-reclaimable; combined with larger requests it can exhaust memory (16,000
docs/request OOM-killed Quickwit on 64 GiB; 8,000 fit with 12.7 GiB host headroom).
Results recorded before this default changed ran with the flag off.

## Methodology (current)

These apply to every run of this adapter; the sections further down are the
investigation notes that led to them.

- **Result caches off.** `searcher.partial_request_cache_capacity: 0` and
  `searcher.predicate_cache_capacity: 0` (the latter added in 0.9.0: per-split
  filter hit sets for count / group_by / recent). The analogue of ES's query
  cache, which the elastic/opensearch adapters disable. Block caches (fast
  field, split footer, OS page cache) stay on, as for every engine.
- **Fast field cache off** (`fast_field_cache_capacity: 0`, from 2026-10-31): it keeps
  decoded fast-field (columnar) data in memory, so hot tries would skip storage reads.
  ClickHouse's counterparts, its text-index data caches (tokens, negative tokens,
  pattern bypass, postings), are off too. The split footer cache stays at 500 MiB
  (split metadata: all footers + hotcache of the 1b index total 33 MiB), like
  ClickHouse's mark, primary-index and text-index header caches. `QW_FAST_FIELD_CACHE`
  overrides the capacity (`./start`); `16GiB` is the setting of the earlier runs,
  sized to hold the 1b index's fast fields (~6 GiB with ns timestamps).
- **Storage requests per query** (opt-in): `SEARCHBENCH_QW_METRICS_LOG=<file>`
  makes `./query` append one JSON line per try with the object-storage GETs and
  downloaded bytes it caused: deltas of `quickwit_storage_object_storage_gets_total`
  and `..._download_num_bytes` from `/metrics` around the search (the timed request
  is unchanged; lines carry a short sha1 of the query body).
- **Millisecond timestamps.** `ingest.py` sends epoch ms and the index stores
  `fast_precision: milliseconds` -- enough for log search; documents sharing a
  millisecond keep index order. The ground-truth check (`lib/check_recent.py`)
  compares each engine at the precision it returns. (A nanosecond variant was
  measured: 1b hot 7.40 s vs 7.42 s, index 61.5 vs 55.8 GB.)
- **Only what the queries use is indexed or fast** (`config/index_config.json`).
  Body (positions), ServiceName, SeverityText and SeverityNumber are indexed and
  fast. Timestamp is fast only: time ranges, sorts and histograms run on the fast
  field, and split pruning uses each split's time range from the metastore
  (checked on a small index with the PR #6843 image). ScopeName is fast only too (the terms
  aggregations of Q57-Q60 and Q67; nothing filters on it). TraceId, SpanId and the dynamic fields (attribute
  maps, TraceFlags, ScopeVersion, schema URLs) are stored only, so documents come
  back whole. Quickwit rejects `tokenizer` on a field with `indexed: false`; the
  fast field keeps the raw normalizer, so the group-by keys are unchanged.
- **Merges settle before queries.** `./load` waits until no merge is running
  (`ongoing_merge_operations`) *and* the published split set is unchanged for
  `QW_MERGE_QUIET_SECS` (180s; 45s on smoke scales), then logs the split layout.
  The wait is part of `load_time`.
- **Size = published split bytes** (`./data-size`), not `du` of the data dir,
  which also counts the WAL, ingest queues, the indexer split cache and splits
  the janitor has not deleted yet (2-3x the index). `QW_DATA_SIZE=du` for the
  old measure.
- **Document clustering** (Quickwit main, node config `docs_clustering`):
  structure -> ServiceName -> SeverityText -> tokenized Body, the Datadog logs
  policy mapped onto OTel. -33% split bytes at 10m, identical results.
  `QW_CLUSTERING=off` for the baseline.
- **Queries run on a searcher + metastore node.** After `./load` (marker
  `.searchbench-loaded` in the data dir), `./start` sets
  `QW_ENABLED_SERVICES=searcher,metastore`. An all-in-one node's ingester waits
  a hard-coded 15s on shutdown before decommissioning; the driver restarts
  before every query, so that was 77 x 15s per run. Results are identical
  (92/92 at 100k); a Quickwit query-only run went from 23.6 to 4.2 min.
- **Server timeout 300s** so the driver's cap, not Quickwit's default 30s,
  decides when a cold try is too slow.

| var | default | meaning |
|---|---|---|
| `QW_IMAGE` | `quickwit/quickwit:0.9.0` | image; a local build (e.g. of main) is used without pulling |
| `QW_STORAGE` | `local` | `s3`: `config/quickwit_s3.yaml`, file-backed metastore + splits under `QW_S3_ROOT` |
| `QW_S3_ROOT` | -- | required for s3; `./install` wipes it and refuses anything not matching `s3://*/searchbench/*` |
| `QW_CLUSTERING` | `on` | `off` sets `QW_DISABLE_DOCS_CLUSTERING=true` |
| `QW_ENABLE_IN_MEMORY_INDEXING` | `true` | `false`: disk-backed temporary indexing files. `true`: RAM-backed temporary indexing files, persisted on split finalization; requires a supporting Quickwit build |
| `QW_SPLIT_NUM_DOCS` | 1M for 100M; otherwise server default (10M) | `split_num_docs_target`: target documents per split |
| `QW_MIN_SHARDS` | 64 for 100M; otherwise server default | minimum ingest-API shards, not a cap |
| `QW_VALIDATE_DOCS` | `off` for 100M; otherwise `on` | ingest-time document validation; indexing still parses documents |
| `QW_COMMIT_TIMEOUT_SECS` | 30 | split commit timeout, in seconds |
| `QW_MERGE_POLICY` | explicit `stable_log` for 100M; otherwise server default | `no_merge` remains available as an override |
| `QW_WAIT_MERGES` / `QW_MERGE_QUIET_SECS` | `yes` / 180 | merge-settle wait |
| `SEARCHBENCH_LOAD_PROCESSES` | 16 | ingest processes (use 4 on small boxes: 16 + a 32 GiB queue exhausted 30 GiB) |

### Sort by timestamp desc drops results (Quickwit bug, fixed on our build)

`CanSplitDoBetter::record_new_worst_hit` rounded the worst top-K timestamp UP
to the next second before comparing it to splits' whole-second
`timestamp_end`, so every split ending in the same second as the worst hit was
skipped. `ORDER BY Timestamp DESC LIMIT 100` then returned real but not-newest
documents. It is timing-dependent (the threshold is set as splits complete),
so it shows up mostly on S3 -- but at 1b it also hit local disk: 3 of the 4
checkable recent queries (Q70, Q72, Q73) were wrong on unpatched main. The fix
rounds down; with it every recent query matches the parquet ground truth
(`lib/check_recent.py`), local and S3, at 100k, 10m, 100m and 1b.

## ⚠ Ingest v2 silently drops documents under concurrency

**This is the most serious defect found in any engine in this batch, and it is
invisible to the client.** Every dropped batch returns `HTTP 200` with
`"errors": false` and no per-item error. The only signal is a line in the
*server* log, and even that does not appear in every case.

Measured on `otel_logs_1m`, identical data, identical index config, varying only
the number of concurrent bulk requests:

| concurrent requests | indexed | lost | notes |
|---:|---:|---:|---|
| 1 | 1,000,000 | — | lossless, 14,923 docs/s |
| 4 | 858,000 | **14.2%** | permanent; count unchanged after 160s |
| 4 (32GiB ingest queue) | 830,000 | **17.0%** | and **zero** "failed to fetch" errors |
| 4 (sub-1MB batches) | 864,200 | **13.6%** | |
| 16 | 896,000 | **10.4%** | 7 × "failed to fetch records from ingester" |

The loss is **permanent**, not a commit lag: the count was still 858,000 after
160 seconds of polling, and the settle curve for a healthy load is visible and
short (975,311 → 998,400 → 1,000,000 over 45s).

### What causes it

The server prints the real constraint at startup:

```
INFO quickwit_config::node_config: ingestion shard throughput limit: 5.2 MB
```

Ingest v2 distributes documents into WAL shards, each rate-limited. Exceed the
limit and Quickwit **discards the excess instead of applying backpressure or
returning an error**.

> **Correction (2026-09-12, 0.9.0).** This section previously said no
> shard-throughput setting is exposed. That is wrong: `ingest_api.shard_throughput_limit`
> exists. Default 5.2 MB, maximum 20mb — the validator rejects anything higher
> with `must be within 1mb and 20mb`. Raising it to 20mb is a genuine 3.7–6.7x
> on ingest throughput. It does **not** fix the loss; see below.

### What does not fix it

- **A bigger ingest queue.** Raising `ingest_api.max_queue_memory_usage` from
  2GiB to 32GiB made it *worse* (830,000) and eliminated the
  "failed to fetch records" errors entirely — proving the queue was never the
  bottleneck. Those settings are still in `config/quickwit.yaml`, which `./start`
  mounts, because they are what the published numbers were measured under — not
  because they help. They are irrelevant to the serial ingest that is actually
  used.
- **Sub-1MB batches.** The community workaround for ingest-v2 stalls
  ([Journey to v9](https://github.com/quickwit-oss/quickwit/discussions/5961)
  reports that cutting messages below 1MB resolved indexing stalls) does not
  apply here: 700-document batches at ~0.9MB still lost 13.6%.
- **`?refresh=wait_for` on the bulk endpoint.** Accepted (HTTP 200) but has no
  effect on the loss.

### Raising the shard limit: faster, still lossy

Measured on `otel_logs_100m`, same corpus and index config, varying the ceiling
and the number of ingest processes:

| processes | shard limit | rate | ≈ MB/s | indexed | lost |
|---:|---:|---:|---:|---:|---:|
| 1 | 5.2 MB (default) | 16,528/s | ~5 | 99,988,000 | 12,000 (0.012%) |
| 4 | 20mb | 61,660/s | ~18.5 | 99,718,000 | 282,000 (0.28%) |
| 8 | 20mb | 110,526/s | ~33 | 98,682,000 | 1,318,000 (1.32%) |

Two things this shows that the concurrency table above does not:

1. **Exceeding the configured limit is not the mechanism.** The 4-process run
   sat at ~18.5 MB/s, *under* its 20mb ceiling, and still lost 0.28%. Loss
   grows superlinearly with throughput regardless of headroom — 3.7x the rate
   cost 23x the documents; 6.7x the rate cost 110x.
2. **Serial ingest is not lossless either.** The row above contradicts the
   "1 concurrent request → lossless" line in the earlier table: at 100m scale,
   one process lost 12,000 documents. And it is not deterministic — the 1b load
   at that same rate returned exactly 1,000,000,000.

So there is no configuration verified to load this corpus completely through
the ES-compatible bulk API. `shard_throughput_limit` is left commented out in
`config/quickwit.yaml`: it trades correctness for speed, and no clean load was
ever produced with it.

### What partially fixes it

**Serial ingest — one connection.** `ingest.py` therefore defaults to
`--processes 1 --bulk-workers 1`. That still does ~15k docs/s, which is faster
than Meilisearch and far faster than Typesense, so the cost is modest.

The native `/api/v1/<index>/ingest?commit=wait_for` endpoint *does* apply real
backpressure and is lossless, but collapses throughput to roughly 16,000
documents in five minutes (~50× slower) — unusable at this scale.

### Why `./load` counts afterwards

Because the response cannot be trusted, `./load` compares the final indexed
count against the parquet row count and **fails loudly** on a mismatch rather
than benchmarking a partial corpus. Any adapter for this engine needs that
check; without it a 14% shortfall looks exactly like a successful load.

This is consistent with publicly reported ingest-v2 problems — dropped
documents, indexing stalls, and throughput regressions against v1, with users
reporting they *"could not reach even half the throughput they had with
ingest_v1"* — see
[ingest-v2 internals](https://github.com/quickwit-oss/quickwit/blob/main/docs/internals/ingest-v2.md).

## Query coverage — 77 of 92, the best in this batch

Quickwit speaks the ES query DSL, so `queries.dsl` is
`../elastic/queries.dsl` with **three constructs rewritten**, not a translation
into another language. 45 of the 92 run byte-identical to the ES file.

| rewritten | why |
|---|---|
| `match` object form (`operator`, `minimum_should_match`) | HTTP 400 → expanded to a `bool` over single-term `match` clauses |
| `date_histogram` `calendar_interval` | HTTP 500 → `fixed_interval` (minute → 60s). The corpus has no DST or leap seconds, so the buckets coincide |

| unsupported | why |
|---|---|
| 9 (Q84–Q92) | no join |
| 6 (Q22–Q24, Q48, Q49, Q59) | **fuzzy** — verified absent on *both* APIs: `{"fuzzy":…}` → 400, `match`+`fuzziness` → 400, `query_string "term~1"` → 0 hits, native `Body:term~1` → 0 hits. `term~` returns the exact-match count only, so `~` is parsed and ignored |

Compare: Typesense 74, RavenDB 71, Meilisearch 69, VictoriaLogs 57.

### Q67 is not equivalent across engines

Q67 is a **nested** aggregation here (top-100 severities, then each one's top-20
scopes), but a **flat** `GROUP BY (SeverityText, ScopeName) ... LIMIT 20` in the
SQL adapters, and single-key `GROUP BY SeverityText` in cratedb, victorialogs,
ravendb, surrealdb, meilisearch and typesense — which drop the ScopeName
dimension entirely. These compute different things: the flat form can be
swallowed by the largest severity while rare severities vanish, whereas the
nested form guarantees each severity its own top-20.

They coincide on this corpus only because cardinality is tiny (5 severities, 7
distinct pairs), so all three shapes return the same 7 pairs. That is luck, not
equivalence, and it would break on a corpus with more scopes. Q60 and Q66 differ
more mildly: `terms size:100` caps and orders by count, while the SQL forms have
no `LIMIT` (and Q60 no `ORDER BY`).

Quickwit needs **no workarounds** for `regexp`, `wildcard` (infix *and* suffix),
`prefix` with many expansions, `minimum_should_match`, or `must_not` — every
other engine in this batch needed a rewrite, an approximation, or an exclusion
for at least one of those. It is also the only one that can express
`date_histogram`.

## Results at 100m

| | |
|---|---|
| load | 6747s (1.9h), 14,731 docs/s — no degradation from 1M's 14,923 |
| on disk | 22.7 GB |
| queries | 77/92 ran, **3.63s total hot**, 0 timeouts |
| by task | count 1.61s/29 · group_by 0.39s/13 · top_k 1.22s/19 · recent 0.40s/16 |
| memory | flat ~1.5 GiB throughout |

## ⚠ The result cache must be disabled or the numbers are meaningless

`searcher.partial_request_cache_capacity` (default **64M**) caches whole
aggregation and search *results*, keyed on the exact query body. The driver runs
each query three times and reports `min(try2, try3)`, so with the cache on
**every reported number is a cache lookup, not a query**.

Measured on Q67 (nested terms aggregation over 11.7M matching docs):

| | |
|---|---|
| first execution | 115ms |
| every execution after | **2ms** |
| same query, `should` clauses reordered | 29ms, then 2ms |
| same query, `size` 100 → 99 | 42ms, then 2ms |
| cache disabled | 26–47ms, stable |

2ms for aggregating 11.7M documents is 5.8 billion docs/sec — the giveaway that
no work was being done.

Suite-wide the cache inflated Quickwit by **10.2×**: 0.40s reported vs 3.63s
real, with a cold/hot ratio of 10.2x against 1.2x for an engine with no such
cache. `count` queries were the worst hit at 26x (1.84s cold → 0.07s hot).
With the cache off the ratio is 1.1x, as expected.

`config/quickwit.yaml` therefore sets `partial_request_cache_capacity: 0`. Any
comparison against engines that do not cache results is invalid without it.

**Quickwit is the only engine in this suite found to have such a cache.** An
earlier version of this note claimed Elasticsearch (1.7x), OpenSearch (3.0x) and
Elasticsearch-columnar ES|QL (3.3x) shared the signature and needed
`?request_cache=false`. That was wrong: all three already set
`index.requests.cache.enable=false` and `index.queries.cache.enabled=false` in
`config/index_mapping.json`, and re-running OpenSearch at 1b with the parameter
changed no timing beyond run-to-run noise. Their residual cold/hot ratios are OS
page cache and JVM warm-up -- the query executes every time -- which is a
different thing from a cache that returns a precomputed result without doing the
work.

An earlier recording measured 0.52s; it both used the cache and overlapped a
concurrent Typesense load. Discarded.

## Verified against ground truth

**All 77 runnable queries are verified, not a sample.** Row counts agreeing is
not evidence — a `size: 100` cap makes every hit-returning query return exactly
100 rows whether the underlying match set is right or wrong. Each query shape is
therefore checked differently:

| shape | n | method | result |
|---|---:|---|---|
| count | 29 | exact integer vs Elasticsearch | all identical |
| group_by | 13 | every bucket key **and** doc_count vs ES | all identical |
| top_k / recent | 35 | full match total (`size:0`, `track_total_hits`) — 18 share a predicate with an already-verified count query, 17 checked against parquet ground truth | all identical |
| returned docs | 3,500 | every returned document re-evaluated against its own predicate | 0 false positives |
| sorted queries | 8 | top-100 timestamp window vs ES | all identical |

**Counts are exact, not estimated.** `hits.total.relation` is `eq` (not `gte`),
and the terms aggregation reports `doc_count_error_upper_bound: 0` with
`sum_other_doc_count: 0`; its buckets sum to exactly 100,000,000. This matters
because Tantivy's terms aggregation is approximate in the general case — it does
not bite here only because the corpus has 17 services, well inside the exact
regime. A high-cardinality grouping field would need `shard_size` scrutiny.

The 19 `_score`-sorted top_k queries are compared by match total only: Tantivy
and Lucene compute BM25 differently, so which 100 documents tie into the page is
a legitimate engine difference, not an error.

The 10 predicates without a verified twin were recomputed from the 100m parquet
in a single vectorised pass, and every one matched Quickwit exactly:

| predicate | Quickwit | truth |
|---|---:|---:|
| `charge` | 7,301,803 | 7,301,803 |
| `connection` | 1,340,416 | 1,340,416 |
| charge + payment + 6h | 1,831,881 | 1,831,881 |
| failed + order + checkout + 30m | 123,480 | 123,480 |
| (error\|failed\|charge) + sev≥13 + 30m | 7,039,396 | 7,039,396 |
| phrase "failed to place order" + 6h | 2,664,716 | 2,664,716 |
| charge + payment + 30m | 1,831,881 | 1,831,881 |
| cart + 30m | 44,161,325 | 44,161,325 |
| `regexp charg.*` + 6h | 7,301,803 | 7,301,803 |
| (connection\|request\|conversion) + 30m | 5,066,633 | 5,066,633 |

`regexp charg.*` returning exactly the `charge` count looks like a bug and is
not: the parquet confirms no other `charg`-prefixed token occurs in the corpus.

Additionally, 15 probes at 1m scale, computed from the parquet with the
reference analyzer (lowercase, split on non-alphanumerics):

| case | Quickwit | truth |
|---|---:|---:|
| term `error` | 101,481 | 101,481 |
| term `cache` | 57,206 | 57,206 |
| underscore: `order` | 65,611 | 65,611 |
| AND `failed`+`order` | 62,029 | 62,029 |
| OR `error`\|`failed` | 137,116 | 137,116 |
| ≥2 of 4 (`minimum_should_match`) | 93,367 | 93,367 |
| phrase | 31,323 | 31,323 |
| prefix `ord*` | 68,706 | 68,706 |
| prefix `conn*` | 30,448 | 30,448 |
| regexp `c.che` | 57,206 | 57,206 |
| wildcard `*tion` | 113,086 | 113,086 |
| wildcard `*nnec*` | 30,127 | 30,127 |
| `must_not`: error !cache | 50,683 | 50,683 |
| `term` ServiceName=cart | 416,919 | 416,919 |
| range severity>=13 | 83,382 | 83,382 |

`prefix conn*` is worth singling out: Typesense returned 20 of 6,493 for the
equivalent query because its prefix expansion is capped at 4 candidates by
default. Quickwit needs no such tuning.

## Other operational notes

- **No parquet ingest.** The CLI reads *"NDJSON documents from a file or
  streamed from stdin"*, and the source config supports only `json`,
  `otlp_logs_*`, `otlp_traces_*` and `plain_text`. The parquet → NDJSON
  conversion in `ingest.py` is unavoidable.
- **Bulk bodies are capped at 10MB** (`ingest_api.content_length_limit`);
  20,000-document batches returned HTTP 413. Default batch size is 2,000.
- **Commits are on a timer** (`commit_timeout_secs`, set to 30 in
  `config/index_config.json`), so documents are not searchable the instant the
  bulk call returns. `./load` waits for the count to be stable across six
  consecutive polls — two equal readings are not enough, because a commit
  landing between polls produces a false plateau that reported 858,000 of
  1,000,000 as "settled".
- **`--ulimit nofile=65535`** is set from the outset, after the typesense
  adapter lost 7.5 hours to descriptor exhaustion presenting as a hang.

## Environment (base)

| var | default | meaning |
|---|---|---|
| `QW_PORT` | `7280` | HTTP port |
| `QW_IMAGE` | `quickwit/quickwit:0.9.0` | pinned image |
| `QW_CONTAINER` | `searchbench-quickwit` | container name |
| `QW_DATA_DIR` | `./qw-data` | data path (gitignored) |
| `QW_INDEX` | `otel_logs` | index id |

## Rust client safeguards

`SEARCHBENCH_LOAD_CLIENT=rust` keeps the existing HTTP-ingest path and all
`QW_*` storage, clustering, validation, shard, cache, merge and split-limit
overrides. The `QW_INGEST=parquet` path is unchanged. `./install` now builds the
client with `--locked` before load/CPU timing when Rust HTTP ingestion is selected,
or validates a supplied `QW_CLIENT_BIN`. `QW_CLIENT_BUILD=auto|native|docker` chooses
the builder; `QW_RUST_IMAGE` selects its image. Compilation defaults to 8 Cargo jobs
(`CARGO_BUILD_JOBS`), independently of ingestion concurrency.

Client retries are bounded by `SEARCHBENCH_LOAD_MAX_RETRIES` (8) and
`SEARCHBENCH_LOAD_REQUEST_TIMEOUT` (60 seconds). A fatal worker error cancels the
other workers instead of leaving an unlimited retry loop. The default ingestion
concurrency remains main's 16; set `SEARCHBENCH_LOAD_PROCESSES=4` explicitly for
the lower-memory retry configuration. The 100M dataset preset above now selects
30s commits and a 1M split target with 64 minimum shards and validation off;
other datasets retain their existing defaults.

`QW_PULL_IMAGE=auto` retains main's local-image-first behavior; `yes` forces a pull,
and `no` requires an already-built local image. `storage-units` reports published
splits independently of the storage backend, for the run page's neutral count row.
