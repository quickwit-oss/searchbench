# DuckDB: how we chose the benchmark configuration

This page explains how the DuckDB configuration used in the published results was chosen: what we
measured, what we kept, what we dropped and why, and the trade-offs that remain. It is for readers
who want to judge whether DuckDB was given a fair setup, or who want to reproduce it.

- The adapter itself is described in [`README.md`](README.md).
- The published DuckDB column is the S3 sorted copy in the
  [`hippo-20261105`](../runs/hippo-20261105/README.md) run set (1B on S3).
- Numbers come from files under [`tuning/`](tuning/) (the 1B local and S3 columns are in
  [`tuning/results-1b/`](tuning/results-1b/)), except a few probe outputs marked as not stored.

## What "DuckDB" means in this benchmark

DuckDB is the **no-index baseline**: it queries the Parquet file of the corpus directly.
- No ingestion into a DuckDB database, no stored tokens, and no full-text index.
- Every text predicate is evaluated at query time over the `Body` column.
- The [full-text search extension](https://duckdb.org/docs/stable/core_extensions/full_text_search)
  was deliberately not used: it builds an inverted index (that is what the other engines are for).
  Its BM25 ranking is the only part of the query set it would add.

Two variants are published, both with the same query helpers and settings:

| Variant | What DuckDB reads | Load step |
|---|---|---|
| **Parquet as-is** | the corpus file, unchanged | none (view setup only) |
| **Sorted copy** | a copy written by `./load`, sorted by `(ServiceName, Timestamp)`, 122,880-row row groups | the sort, counted as load time |

## The configuration we kept

| Setting | Value | Why |
|---|---|---|
| DuckDB | 2.0.0 (`requirements.txt`) | Latest release when the runs were made (November 2026). |
| `threads` | 16 = the instance's vCPUs | 32 threads was not faster (below). |
| `memory_limit` | 48 GB on a 64 GiB host (adapter default 4 GB, set by the runs) | Room for the 1B sort; no effect on query times at 100M (16 GB measured the same). |
| Parquet footer + HTTP metadata caches | on (`parquet_metadata_cache`, `enable_http_metadata_cache`) | The other engines keep their metadata in memory too. |
| Data cache | off (`enable_external_file_cache = false`) | Every S3 try reads its data from S3, as the other engines' S3 runs do with their data caches off. |
| Query helpers | [`macros.sql`](macros.sql), [`queries.sql`](queries.sql) | Regexes on `Body` instead of token lists: cold sum over all 70 queries at 100M 449.5 → 92.7 s, with the original's three 60 s-capped queries counted at 60 s (below). |
| Sorted copy layout | `ORDER BY ServiceName, Timestamp`, ZSTD, `ROW_GROUP_SIZE 122880` | Best of the layouts and row-group sizes we measured (below). |

Everything else is at DuckDB's defaults.

## How we measured

- **Instance:** AWS m8gd.4xlarge (16 vCPU Graviton4, 64 GiB, local NVMe), us-east-1. This is the
  instance type of every published column. Tuning used `otel_logs_100m` (the first 100M rows of the
  corpus); the result was then validated at 1B.
- **Method:** the same as for the ClickHouse and Quickwit columns. The worker restarts and the OS page cache is dropped
  before each query, then the query runs 3 times (cold, hot, hot), with a 60 s cap. CPU is the
  worker process's CPU per try.
- **Repetitions:** one run per configuration. We did not measure DuckDB's run-to-run variance, so
  differences of a few percent are not significant.
- **Correctness:** every change had to return **the same rows as before**. `tuning/checks/diffcheck*.py`
  runs all 70 supported queries on both versions and compares full results. The top-100 queries can
  differ only in the order of rows tied on `Timestamp`, and the unordered `LIMIT 100` queries can
  return another set of 100 matching rows. Where that happens, `tuning/checks/limitcheck2.py` evaluates the
  original predicate on every returned row.

Sums and geomeans below are over the **57 count, group-by and recent-rows queries** unless noted.
That is the set every engine of the published comparison answers; it excludes joins, fuzzy and
top-k queries. "All 70" adds DuckDB's fuzzy and join queries.

## Step 1: query helpers (the big win)

The original helpers tokenized every row: `lower(Body)`, split into a list of tokens, then a list
test per term. Two costs dominated:
- **Repeated `lower`:** DuckDB does not share common subexpressions across lambda bodies, so
  `lower(Body)` ran once per term.
- **Token lists:** a list of tokens was built for every row.

Each rewrite below kept exactly the same results. Times are at 100M, cold sums:

| Version | What changed | All 70 | 57 queries | Slowest |
|---|---|---:|---:|---:|
| original | token lists; three fuzzy queries at the 60 s cap | 449.5 s | 248.3 s | 60 s (Q22, Q23, Q59) |
| v1 | one `lower(Body)` + one regex per predicate: *t* is a token iff it occurs with a non-alphanumeric character or the string edge on each side | 208.1 s | 110.6 s | 35.9 s |
| v4 | the regex runs on `Body` itself with case classes (`[eE][rR]…`), so no lowercased copy of each row; a case-class literal search runs first | 170.8 s | 75.4 s | 35.9 s |
| v5 | fuzzy queries: one regex for all tokens within 1–2 edits (below) | 99.2 s | 75.7 s | 4.2 s |
| **v6 (kept)** | `sb_any` as separate term searches up to 3 terms; literal prefilter for Q19 | **92.7 s** | **69.5 s** | **4.2 s** |

Per query, original → v6, cold:

| Query | Original | v6 |
|---|---:|---:|
| Q01 (term) | 4.40 s | 1.09 s |
| Q05 (8-term AND) | 8.58 s | 1.13 s |
| Q09 (2 of 4 terms) | 15.69 s | 3.89 s |
| Q22 (fuzzy) | 60 s cap | 3.76 s |

Two details make the regex form exact:

- **Case classes are exact for DuckDB's `lower()`.** `lower()` turns exactly three kinds of characters
  into ASCII letters: A–Z, `İ` (U+0130 → `i`) and the Kelvin sign `K` (U+212A → `k`). No character
  changes length. This was checked over every Unicode code point, and the check is a unit test.
  So `i` matches `[iIİ]`, `k` matches `[kKK]`, and a separator is anything else. A
  case-insensitive regex flag (RE2's `(?i)`, see [RE2 syntax](https://github.com/google/re2/wiki/Syntax))
  would not be exact: it folds characters that `lower()` leaves alone. For example, `ſ` (U+017F):
  `regexp_matches('ſend', '(?i)send')` is true, while `lower('ſend')` keeps `ſ`, so `send` is not a
  token there.
- **Fuzzy matching is a finite set of strings.** DuckDB's
  [`damerau_levenshtein`](https://duckdb.org/docs/stable/sql/functions/text) is the unrestricted
  distance (`damerau_levenshtein('ca', 'abc') = 2`). So the tokens within distance *d* of a word are
  exactly those reachable by *d* single edits: insertions, deletions, substitutions and adjacent
  transpositions. `sb_ball` generates them, with "any token character" for inserted or substituted
  letters (39 patterns at distance 1 and 691 at distance 2 for `connection`). `sb_ball_re` joins them
  into one regex that selects the rows, and tokens are compared only on those rows. A unit test
  checks the regex against `damerau_levenshtein` on random edits.

`test_adapter.py` checks every helper against its token-list definition. At 100M, all 70 queries
return the same rows as the original helpers (`tuning/checks/diff-v6-100m.log`). The three top-100
queries differ only in tie order, and the original version does not repeat that order between runs
either.

### What the remaining time is made of

At 100M, `tuning/probes/floor.py` measures the cost of the basic operations (16 threads, best of 2):

| Operation | Time |
|---|---:|
| `count(*)` | 0.00 s |
| read `Body` (`sum(strlen(Body))`) | 0.18 s |
| one literal or case-class search over `Body` | 0.84 s |
| `sb_term(Body, 'error')` | 0.97 s |
| any of 2 terms (`sb_any`, before v6) | 2.23 s |
| fuzzy, distance 1 | 3.61 s |
| group-by on `SeverityText` or `ScopeName` alone | 0.04–0.05 s |

So a single-term query is close to the cost of one pass over the text, and grouping is negligible.
The expensive patterns are **alternations**: RE2 searches a literal very fast, but a pattern with
alternatives runs at plain automaton speed, about 2.2 s per 100M rows whatever the terms. Hence:

- **v6 `sb_any`:** separate term searches combined with `OR` are faster up to 3 terms (2 terms:
  1.50 s instead of 2.22 s), and one alternation is faster beyond (8 terms: 2.53 s alternation vs
  4.77 s as `OR`s; `tuning/probes/any.py`, `opt2.py`, `opt3.py`).
- **Q19 (`c?che`):** checking the literal `che` first halves it (2.41 → 1.17 s).

Tried and dropped, with the same results but slower or no faster:
- **Q09 (2 of 4 terms):** counting matching terms (10.3 s), or an alternation prefilter plus counting
  (3.86 vs 3.77 s): `q09.py`.
- **Phrase prefilters:** checking every word first was 50–100× slower ('place order' 1.03 → 63 s,
  'failed to place order' 1.27 → 127 s), because `lower` ran per word inside a lambda; the rarest word
  first was no faster (1.00–1.06 s). This probe, `opt3.py`, was stopped by its time limit after these
  lines, so its output is quoted here, not stored.
- **Fuzzy prefilters:** substrings that a near match must contain appear in 17% (distance 1) and 49%
  (distance 2) of rows, so they cost more than they save.

**Pitfall worth knowing:** DuckDB only compiles a regex pattern once per query when the pattern
expression can be folded to a constant. Building the fuzzy pattern through a `CASE … ELSE error(…)`
prevented that, so the pattern was rebuilt for every row: 4.5 s instead of 9 ms on a 20k-row sample
(measured on a laptop, not stored). The distance check now sits outside the pattern expression (see
`sb_fuzzy` in `macros.sql`).

## Step 2: session settings (no gain)

Each setting was changed alone, on the v5 helpers, at 100M:

| Setting | All 70, cold sum | 57 q. cold geomean | Effect |
|---|---:|---:|---|
| v5 defaults (16 threads, 48 GB) | 99.2 s | 1,022 ms | — |
| `preserve_insertion_order = false` | 99.1 s | 1,021 ms | none |
| 32 threads | 99.2 s | 1,088 ms | no gain overall (hot sum 87.5 s); short queries slower; the instance has 16 vCPUs |
| `memory_limit = 16GB` | 99.2 s | 1,020 ms | none |

All queries are CPU-bound: 15–16 of the 16 cores are busy, and hot tries are within about 10% of
cold. Of the knobs in the
[workload tuning guide](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads),
none we tried helped beyond matching threads to cores. `DUCKDB_SETTINGS` (in the adapter) is what made these
runs possible without code changes.

## Step 3: file layout

DuckDB documents what matters for Parquet scans: row-group size, file size, sorting for min/max
pruning, Hive partitioning and Bloom filters
([file formats guide](https://duckdb.org/docs/2.0/guides/performance/file_formats),
[Parquet tips](https://duckdb.org/docs/2.0/data/parquet/tips),
[sorting for selective queries](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries)).
The corpus file is written by another Parquet writer:
- **Row groups:** about 613k rows each (163 at 100M, 1,624 at 1B).
- **`Body`:** `PLAIN`-encoded, 87 GB uncompressed and 9 GB with ZSTD at 1B.
- **Rows:** in time order.
- **Statistics:** no min/max statistics on `ServiceName` (checked at 1B), so service filters cannot
  skip row groups.

A rewritten copy is a load step, so it must be counted like the other engines' ingest. That is why
the as-is variant is published too.

**Layouts at 100M**, all ZSTD with 122,880-row groups (`tuning/results/rw-*`):

| File | Size | Load | 57 q. cold sum | Cold geomean | Hot sum |
|---|---:|---:|---:|---:|---:|
| original | 7.87 GB | 0.1 s | 69.5 s | 971 ms | 62.0 s |
| `ORDER BY Timestamp` (same order, DuckDB's writer) | 3.60 GB | 105 s | 68.3 s | 881 ms | 63.6 s |
| **`ORDER BY ServiceName, Timestamp`** | **2.67 GB** | **103 s** | **58.6 s** | **788 ms** | **54.1 s** |

- **DuckDB's writer:** rewriting alone halves the file but barely changes query time. Decompression
  is not the bottleneck on local NVMe.
- **Sorting by service:** a further 26% smaller, because similar log lines end up together. It lets
  service-filtered recent-rows queries skip row groups (Q75 1.95 → 0.22 s).
- **Cost:** unordered `LIMIT 100` queries now find their first matches later in the file (Q79
  0.18 → 0.58 s).

**Row-group size, (ServiceName, Timestamp)**: `ROW_GROUP_SIZE` is respected, and the counts were read
back with `parquet_metadata`.

| Rows per group | Groups | Load | Size | 57 q. cold sum | Cold geomean |
|---:|---:|---:|---:|---:|---:|
| 30,720 | 3,256 | 174 s | 2.76 GB | 63.1 s | 895 ms |
| **122,880** (DuckDB default) | 814 | 103 s | 2.67 GB | **58.6 s** | **788 ms** |
| 491,520 | 204 | 226 s | 2.63 GB | 62.3 s | 876 ms |
| 1,048,576 | 96 | 323 s | 2.65 GB | 70.1 s | 1,018 ms |

- **Smaller groups:** they add per-group overhead without enough extra skipping.
- **Larger groups:** each group spans more services and more time, so less is skipped. At 1M rows the
  benefit of sorting is gone.
- **The docs' range:** this matches DuckDB's guide, which recommends 100K–1M rows and puts sizes
  above 100K within about 10% of each other on full scans.

**Not adopted:**

| Lever | Why not |
|---|---|
| Sort by `Body` | Best compression, but it breaks the time order that the recent-rows queries rely on. |
| Hive partitioning by service | Prunes whole files on the few service-filtered queries, which the sorted copy already prunes; the text predicate still scans every partition. |
| [Bloom filters](https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb) | Only used for `=` / `IN` on dictionary-encoded columns, not for substring or token tests. |
| Dictionary-encoded `Body` | Since [duckdb#16136](https://github.com/duckdb/duckdb/pull/16136), a filter pushed into the Parquet scan can run once per dictionary value. Measured on a synthetic file with 100 distinct bodies, `contains`, `LIKE`, `ILIKE`, `lower(…) LIKE` and literal regexes became 20–50× faster, but case-class regexes, full matches and token lists still ran per row. On the real logs about 25% of the bodies in each row group are distinct (messages carry IDs), and a dictionary-encoded `Body` with a `contains(lower(Body), …)` prefilter was not faster than the current helpers. |
| Late materialization ([duckdb#15692](https://github.com/duckdb/duckdb/pull/15692), [#17325](https://github.com/duckdb/duckdb/pull/17325)) | Already on by default: `LIMIT` and top-N queries fetch the other columns only for the rows they keep. |

## Step 4: validation at 1B

The 1B corpus has 1B rows and 40.99 GB. The sorted copy has 26.56 GB and 8,139 row groups; the sort
takes about 24 minutes (1,440 s locally, 1,453 s when written to S3) and spills a few hundred GB to
local disk. No query reaches the 60 s cap.

| 1B, 57 queries | Cold sum | Cold geomean | Hot sum | S3 cold GETs / bytes |
|---|---:|---:|---:|---:|
| local, as-is | 642.8 s | 6,511 ms | 614.7 s | — |
| local, sorted | 542.0 s | 6,394 ms | 521.0 s | — |
| S3, as-is | 749.6 s | 9,246 ms | 710.7 s | 318,205 / 574.4 GB |
| S3, sorted | 615.3 s | 7,769 ms | 583.8 s | 401,572 / 357.4 GB |

- **Scaling:** times are close to 10× the 100M ones. S3 is only 1.1–1.2× slower than local
  NVMe, because DuckDB is CPU-bound.
- **The sorted copy:**
  - Counts and group-bys take 16–18% less time, and S3 reads 38% fewer bytes (with more, smaller GETs).
  - Service-filtered top-100 queries are much faster: Q75 5.7 → 0.5 s.
  - Unordered `LIMIT 100` queries are slower: Q79 0.17 → 5.0 s.
  - Q71 (top-100 without a service filter) is slower: 5.5 → 11.1 s.
  - So the local geomean barely moves.
- **Results:** the sorted copy matches the original file on 63 of 70 queries. The other 7 are tie
  order or another valid 100 rows; every returned row satisfies the original predicate.
- **Against the earlier published DuckDB 1B run** (old helpers; a 32-vCPU m8g.8xlarge reading the file
  from EBS gp3, so its cold tries also include disk reads): cold sum on the 56 queries both finished,
  1,187 s there vs 605 s here with half the cores. Different hardware, so this is indicative only
  (that run is not published in this repository).

## Pros and cons of the retained configuration

**Pros**
- **Nothing to build** for the as-is variant: it queries the published Parquet file directly, on
  local disk or S3. Its load time is the view setup (0.1–0.3 s).
- **Results:** the same rows as a straightforward token-list implementation, checked on every query,
  with the edge cases covered by tests.
- **Coverage:** 70 of 92 queries, including fuzzy and joins. Only proximity (Q13) and BM25 ranking
  (Q33–Q53) are unsupported.
- **The sorted copy:** 35% smaller at 1B (66% at 100M), 16% less total time locally and 18% less on
  S3 at 1B, and much faster on service-filtered top-100 queries.

**Cons**
- **No index:** every query scans and regex-matches the text column. Cost grows linearly with data,
  and every core is busy for seconds per query at 1B.
  - Cold geomean at 1B: about 6.4 s locally vs 97 ms for Quickwit and 159 ms for ClickHouse.
  - On S3: 7.8 s vs 385 ms and 2.2 s.
- **S3 bytes:** the median cold query reads 9.1 GB from S3 at 1B (6.1 GB with the sorted copy).
- **The sorted copy:**
  - It costs a 24-minute sort with hundreds of GB of temporary disk at 1B.
  - It is no longer "the file as published".
  - It makes unordered `LIMIT 100` queries 2–29× slower (worst: Q79 0.17 → 5.0 s).
- **Hand-written helpers:** the speed comes from SQL helpers that encode the benchmark's token
  definition (ASCII alphanumeric tokens, lowercased). They are specific to this workload, not a
  general DuckDB full-text feature, and make no claim of analyzer equivalence with the other engines
  for non-ASCII text.
- **Statistics:** one run per configuration; small differences are not significant.

## Not explored

- A different instance size or more vCPUs.
- Splitting the corpus into many files.
- Disabling Parquet prefetching on S3. [duckdb#21474](https://github.com/duckdb/duckdb/issues/21474)
  reports that it helps when most row groups are pruned, which is rarely the case here.
- Columns precomputed at load time, such as a lowercased body or tokens. They would be a form of
  index, outside the purpose of this baseline.

## Files

| Path | Content |
|---|---|
| [`tuning/results/`](tuning/results/) | Driver results and load records of every 100M configuration above (`base`, `v1`–`v6`, `s-*` settings, `rw-*` layouts). |
| [`tuning/variants/`](tuning/variants/) | `macros.sql` / `queries.sql` of each helper version, and the original ones. |
| [`tuning/checks/`](tuning/checks/) | Result comparisons (`diff-*.log`), the scripts that produced them, and the row-level check of the sorted copy. |
| [`tuning/probes/`](tuning/probes/) | The 100M probes quoted above: operation costs (`floor.py`), `sb_any` variants (`any.py`, `opt3.py`), Q09 (`q09.py`), fuzzy prefilters and Q19 (`opt2.py`), phrase prefilters (`opt3.py`). Their outputs are quoted in this page; the dictionary-encoding test ran on a 300k-row sample and is not stored. |
| [`tuning/results-1b/`](tuning/results-1b/) | The 1B results and load records (local and S3, as-is and sorted copy) and the S3 HTTP logs. The S3 sorted copy is the column published in [`hippo-20261105`](../runs/hippo-20261105/README.md), with its evidence in `extra/duckdb/` there. |

The tuning ran on 2026-11-01, on an m8gd.4xlarge instance, with duckdb-parquet at commit
6b87f9d for the final columns.
