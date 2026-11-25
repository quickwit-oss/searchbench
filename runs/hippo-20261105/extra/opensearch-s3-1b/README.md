# OpenSearch 3.8.0 · S3 · 1B: the 3 rounds behind the hippo-20261105 column

The column **OpenSearch 3.8 · S3 · searchable snapshot · 16 shards · public bench**
(`opensearch/results/opensearch_s3-16x1-leandesc-median3_otel_logs_1b.json`) is the per-query, per-try median of
the 3 rounds in [`rounds/`](rounds/) (lean mapping, index sort `ServiceName asc, Timestamp desc`, 2026-11-19).
The load record sets `load_cpu` to the OpenSearch container only (50,340 s, as for the other hippo-20261105 columns)
and keeps `load_cpu_total` (64,308 s) and `load_cpu_client` (13,968 s).

- **Rounds** (geomean over the 57 common queries, cold / hot): 6,375 / 266, 6,147 / 265, 5,776 / 268 ms; median
  file 6,089 / 266 ms.
- **S3 GETs and bytes:** per-try median of the rounds' `SEARCHBENCH_OS_METRICS_LOG` lines, in
  [`metrics/`](metrics/) (one line per try, in query order; the counts are the same in every round). Over the 57
  common queries: 10.6k GETs and 82.8 GiB cold, 0 hot.
- **Cache check** ([`cache-check/`](cache-check/), on the `Timestamp asc` index): no result caching (request/query
  caches 0 hits); hot tries make 0 S3 GETs and run as fast after dropping the OS page cache; a 4 GB or 1 GB file
  cache does not make hot tries read S3.
- **Previous rounds:** [`rounds-asc/`](rounds-asc/), the 2026-11-18/19 rounds with `Timestamp asc` in the index
  sort (cold / hot 6,446 / 378 ms, 80.2 GiB).
- **Setup and A/B comparisons:** [`opensearch/README.md`](../../../../opensearch/README.md#index-sort-servicename-asc-timestamp-desc).
