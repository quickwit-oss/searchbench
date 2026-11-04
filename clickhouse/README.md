# SearchBench / ClickHouse engine

Reproduces [TextBench](https://github.com/ClickHouse/TextBench)'s
ClickHouse setup (same schema including the `ORDER BY (ServiceName, Timestamp)`
sorting key, same `text` index plus positions for phrase search, same Q1–Q9)
wired to SearchBench's shared driver. The unsorted `ORDER BY tuple()` variant
was removed: the sorted table is TextBench's schema and was faster at every
published scale (1B hot median 349 vs 489 ms, 10B 10 vs 22 timeouts) and 12%
smaller on disk. Numbers line up with both TextBench's
leaderboard and SereneDB's `results/`.

Needs: `jq`, `wget`/`curl`, and `ss`/`fuser`/`lsof`. `./install` fetches
the ClickHouse binary automatically if `$CLICKHOUSE_BIN` isn't set and
`./clickhouse` doesn't exist (runs `curl https://clickhouse.com/ | sh`).

## Configuration

| | |
|---|---|
| ClickHouse | `clickhouse/clickhouse-server:26.9.10.4` (latest stable release; set `CH_IMAGE` to override). It is the first 26.9 patch with the text index posting-list cache on by default ([ClickHouse#120677](https://github.com/ClickHouse/ClickHouse/pull/120677), backported in [ClickHouse#123759](https://github.com/ClickHouse/ClickHouse/pull/123759)) |
| Merges | `./load` issues `SYSTEM STOP MERGES otel_logs` after the insert and `./start` re-issues it after every restart (the driver restarts the server before each 1B query and the setting does not persist). Without it the load leaves ~1000 parts that merge for the whole query phase, and every per-query restart aborts and restarts those merges, so the query phase measures the merge backlog |
| 10B cold ceiling | run with `SEARCHBENCH_COLD_TIMEOUT=180`: on dropped caches every scan query's first try takes over 60 s at 10B, and with the default 60 s ceiling a capped cold try ends the query and pads the hot tries with 60 s. The published 10B ClickHouse row was produced the same way (its file has cold values of 99 s and 180 s) |

## Run

```bash
# Smoke (streams first 100k rows over HTTPS)
SEARCHBENCH_DATASET=otel_logs_100k ./benchmark.sh

# Full benchmark (~100 GB free disk + sudo for drop_caches)
./benchmark.sh
# -> results/clickhouse_otel_logs_1b.json
```

## Env

| Var | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_BIN` | `./clickhouse` | binary |
| `CH_HOST` / `CH_TCP_PORT` / `CH_HTTP_PORT` | `127.0.0.1` / `9000` / `8123` | server endpoint |
| `CH_DATA_DIR` | `./clickhouse_data` | datadir |
| `CH_ORDER_BY` | `(ServiceName, Timestamp)` | MergeTree sorting key; `tuple()` = unsorted |
| `CH_MAX_PART_BYTES` | `1073741824` at 100M, else ClickHouse's 150 GiB | merge part-size cap; set empty to use ClickHouse's default |
| `CH_LOAD_WORKERS` | `min(nproc,4)` | parallel parquet INSERTs |
| `CH_INDEX_SETTING` | `enable_full_text_index=1` | text-index session flag |

`CH_INDEX_SETTING` is the only version-sensitive knob — override if a
non-26.x build names the flag differently.

## Methodology changes (quickwit-oss fork)

- **Nanosecond timestamps**: `Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1))`
  instead of TextBench's whole-second `DateTime`, so time-sorted queries rank
  the same rows as the other engines.
- **Sorting key** `ORDER BY (ServiceName, Timestamp)`, as upstream and TextBench
  (`CH_ORDER_BY`). Runs recorded between 2026-09-27 and 2026-10-31 used
  `ORDER BY tuple()` (unsorted); e.g. the 2026-10-31 m8gd.4xlarge ingest column.
- **Merges settle before queries**: `./load` waits until `system.merges` is
  empty and the active part set is unchanged for `CH_MERGE_QUIET_SECS` (180s;
  20s on smoke scales). At 1b the INSERT returns with ~740 parts and ~28
  merges running; querying then measured a fragmented table (token queries 2x
  slower). The wait counts in `load_time`.
- **Version 26.9.9.28** (`CH_IMAGE`), as in serenedb/searchbench#9.
- **Phrase index on by default** (`CH_PHRASE_INDEX=on`, upstream's schema):
  `support_phrase_search = 1`. At 1b on 26.9.9.28 (parts settled, <= 1.75 GiB)
  it costs +16% size (62.4 vs 53.9 GB) and +155s load, same query speed. On
  26.8.2.7 it made the text index 126 GiB and `matchPhrase` >60s once parts
  were fully merged. `off` keeps `matchPhrase` on the index for candidates and
  re-checks adjacency on the column; results are identical.
- **Query condition cache off** (`--use_query_condition_cache=0` on every
  query). It is on by default in 26.x and replays per-granule filter results
  on hot tries -- the counterpart of Quickwit's predicate cache, which is off.
- **Data caches and statistics off on every query** (`clickhouse/query`, from
  2026-10-31). Off:
  - The four text-index caches that hold data or lookup results:
    `use_text_index_{tokens,negative_tokens,pattern_bypass,postings}_cache=0`. In the
    26.10 source (`TextIndexCache.h`):
    - **tokens:** caches each token's posting info: its cardinality (which answers a
      count with no read), its block offsets, and posting lists of up to 6 rows inline;
    - **negative tokens:** caches that a token is absent from a part;
    - **pattern bypass:** caches the outcome of a pattern dictionary scan;
    - **postings:** caches decoded posting-list blocks and phrase-search results.

    With them on, hot count and phrase queries on S3 make 0 GETs.
  - Column statistics (`use_statistics=0`, `use_statistics_for_part_pruning=0`,
    `use_statistics_for_min_max_aggregation=0`). 26.x reads every part's
    `statistics.packed` one GET at a time before pruning, and they prune nothing here.

  On, as metadata (like Quickwit's split footer cache, footer + hotcache):
  - the mark and primary-index caches;
  - since 2026-11-01, the **text-index header cache**. It holds, per part, the format
    version, the codecs and the sparse index of the token dictionary (the first token and
    offset of each dictionary block). Run sets up to 2026-11-01 had it off too.

  The uncompressed and columns caches are off by default. `CH_DATA_CACHES=on` restores
  the engine defaults; `CH_QUERY_SETTINGS` still overrides any setting.
- **S3 request counts** come from `system.query_log` ProfileEvents: GETs =
  `ProfileEvents['S3GetObject']`, bytes = `ProfileEvents['ReadBufferFromS3Bytes']`.
  `S3GetObject` counts every GET; `DiskS3GetObject` is the subset made by a disk, also
  incremented for the same request (`ReadBufferFromS3::sendRequest`), so do not add them
  (here every GET is a disk GET and both are equal).
- **`CH_STORAGE=s3`**: the table uses an `s3` disk (`config.d/storage_s3.xml`,
  policy `s3_main`, endpoint `CH_S3_ENDPOINT`, instance-profile credentials).
  Columns and the text index live in S3, only part metadata stays local. No
  filesystem cache, like Quickwit on S3 without its split cache.
- **`CH_RESOLV_CONF`** bind-mounts a resolv.conf into the container (unset: the
  host's). On S3, 26.9 resolves the endpoint for every new connection and opens
  hundreds per query; on a stock Ubuntu EC2 host the systemd-resolved stub
  (127.0.0.53) drops part of those bursts and each dropped lookup waits the 5 s
  resolver retry, so hot tries take ~5.2 s instead of ~0.2 s (100m and 1b; 26.8
  is not affected). Pointing the container at the VPC resolver
  (`nameserver 172.31.0.2`) removes it.
- **Part count** is tunable with `CH_MAX_PART_BYTES`
  (`max_bytes_to_merge_at_max_space_in_pool`). The default merges a 1b table
  down to a single part.

## Q10–Q14

ClickHouse's `text` index does boolean token matching only — no BM25
ranking. Those slots are `UNSUPPORTED:` sentinel lines that `./query`
short-circuits to `null`, keeping the result array 14 rows long.

Schema in [`create.sql`](create.sql); workload in
[`queries.sql`](queries.sql). `./data-size` returns
`sum(bytes_on_disk)` over active parts.

## Phrase search (`matchPhrase`)

ClickHouse 26.7 has `matchPhrase`, and it is served by the text index:

```
EXPLAIN indexes = 1 SELECT count() FROM otel_logs
WHERE matchPhrase(Body, 'failed to place order');
  Prewhere filter: hasPhrase(lower(Body), '…', 'splitByNonAlpha')
                   AND __text_index_text_idx_hasPhrase_…
  Indexes: Skip / Name: text_idx
```

That plan is the **non-positional** build: the index narrows to candidate
granules by token and `hasPhrase` re-checks the raw column for adjacency.

The text index *can* store positions — `positions = 1`, gated behind the
MergeTree setting `allow_experimental_text_index_positions = 1`. With it the
`hasPhrase` prewhere disappears entirely and the index resolves the phrase
itself. `create.sql` enables it. Measured at 100M:

| | positions off | positions = 1 |
|---|---:|---:|
| total hot (52 queries) | 8.85s | **5.46s** |
| load | 221s | 277s |
| active parts on disk | 7.9 GB | 20.1 GB |

Phrase queries are 50–90% faster (Q10 -90%, Q12 -87%, Q15 -76%, Q11 -71%,
Q86 -53%), paid for with **2.5x the index size and 25% more ingest time**. The
two exceptions are Q71 (+89%) and Q79 (+347%): time-windowed `LIMIT 100`
lookups already down at 30–80ms, where reading the larger index costs more than
phrase resolution saves.

Semantics are exact adjacency either way: it matches `failed to place order` but
not `order failed to place` nor `failed to place the order`.

This moved 9 queries off the `UNSUPPORTED` list — Q10, Q11, Q12, Q14, Q15, Q63,
Q71, Q79, Q86 — taking coverage from **43/92 to 52/92**. All five count-shaped
ones match Elasticsearch exactly (Q10/Q11 2,664,716; Q12 123,480; Q14 9,185,085;
Q15 781,434), and 19 of 19 count-shaped queries overall are identical to ES.

**Q13 stays unsupported**: it is `match_phrase` with `slop: 2`, and
`matchPhrase` is adjacency-only with no proximity form.

Still unsupported (40): 21 BM25 top_k (the text index does boolean matching, no
ranking), 8 regexp, 4 fuzzy, 3 prefix, 3 like, 1 proximity.

### Loading natively

`./load` ingests with `INSERT … FROM file('parquet/part_*.parquet')`, which
resolves relative to `user_files_path`. `./start` bind-mounts the corpus there
only on its **docker** path; run against a local binary there is no mount, so
the INSERT matched nothing, inserted **0 rows and reported success** — the whole
suite then "passed" in 20s against an empty table. `./load` now creates that
symlink itself.

Note that `du` on the data directory overshoots badly mid-load (22 GB while
ingesting 100M) because each INSERT block lands as its own part with its own
index; background merges collapse them. `./data-size` reports active parts,
which settle at 7.9 GB.
