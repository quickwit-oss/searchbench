# SearchBench frontend

The React UI exports `ProductApp` through `@searchbench/frontend`. It has two
entry points and one set of components:

- `src/standalone.tsx` supplies `results.json`, mounts React and owns the theme.
- `src/index.ts` exports the product for a host such as SereneDB Playground.
  It does not import the standalone JSON or mount a second React root.

All npm configuration, dependencies and commands live in `frontend/`. The
design-kit submodule is pinned at `frontend/deps/serene-design`.

```bash
git submodule update --init --recursive
cd frontend
npm ci
npm run dev
npm run build
```

`dev.html` is Vite's development entry. The dev server also serves it at `/`
and `/index.html`, so development never displays an old committed build.

`npm run build` creates **one self-contained `frontend/index.html`**, including
JS, CSS, fonts and the current `frontend/results.json`. Commit the HTML. It can
be opened with a double click, copied by itself, or hosted as a static page.
No CDN, module fetch or JSON fetch is needed to render it. URL filters and the
theme toggle work in both modes.

## Preview this repository's runs locally

For the **full UI** (CPU/S3 metric switches, load breakdowns, variants,
rounds and run details), use the same unified-page pipeline as GitHub Pages:

```bash
cd frontend
npm run preview:runs                 # http://127.0.0.1:5173/
# npm run preview:runs -- 5174       # optional port
```

The page opens directly to the benchmark interface. **Run / environment**, above
Dataset, chooses a run set or the original upstream reference. Only that set's
rows enter the app's dataset/engine lists, rankings, charts and CPU/S3 summaries.
The newest run is selected by default; a missing/unknown run ID falls back to it.

The choice is pinned in `?runSet=<directory-name>` alongside the existing packed
`?s=` state and metric hash. Switching sets reloads the **same page**, preserving
view, cache state, units, categories, theme and metric. It keeps the dataset when
available, otherwise selects the new set's first dataset. Engine allowlists,
exclusions and ordering reset because they refer to the previous run's columns.
The legacy `run=hot|cold` parameter still means cache state, not a run set.

Build the complete self-contained site without starting a server:

```bash
node frontend/build_site.mjs /tmp/searchbench-site  # from repository root
```

Old `/runs/<dir>/` and `/upstream/` links redirect to this interface with their run,
query parameters and metric hash intact. No result data is fetched at runtime;
the unified `index.html` also works over `file://` offline.

`npm run dev` remains the unaugmented base React component; it is not equivalent
to the generated run-selector page. Do not replace the pinned `index.html` by
rebuilding it without also updating/testing every guarded patch in `inject.mjs`.

## Storage-unit counts

Generated run pages show **Splits / parts / files** directly below load time,
and beneath load-time values in Charts. These are neutral metadata, not rankings.
They remain counts when switching Wall/CPU/S3 metrics or seconds/milliseconds.
Unknown measurements display `—`; a measured zero stays `0 files` (or its unit).

Results/load records can supply `"storage_units":{"count":100,"unit":"splits"}`.
Allowed units are `splits`, `parts`, `segments`, `files`, `objects`; counts must be non-negative
safe integers. An authoritative load record overrides the field only when it
contains it, so old records do not erase newer result metadata. New result rows
record their explicit `build` identity for correct overlays.

The optional `storage-units` adapter hook measures after load/settling, outside
the load timer. Query-only runs inherit the same build's recorded count. Hooks:
Quickwit counts published splits from metadata (local or object storage);
ClickHouse counts active table parts; OpenSearch counts the searchable Lucene
segments of the primaries (local index or mounted searchable snapshot); Loki counts persisted chunk/index files
or S3 objects, excluding transient caches/WALs and the reservation marker.

The new c8gd run's 100 Quickwit splits and 76 ClickHouse parts are backed by saved
measurement evidence. Other historic runs remain unknown; no counts are inferred.
This addition preserves all existing CPU, S3, phase, rounds and details patches.

## Stable embed links

Use `?dataset=otel_logs_100m&include=SereneDB,ArangoDB&theme=dark` to pin the
engines in an embed. `include` matches exact `system` names, takes precedence
over legacy `hidden` exclusions, and stays an allowlist when the URL is
canonicalized to `?s=` (packed key `i`). New engines are excluded automatically.
An empty or unknown-only list shows no engines. Legacy `hidden` / packed `h`
links still work. Pin `theme=light` or `theme=dark` explicitly in each themed
iframe; an omitted theme uses the visitor's saved preference.

## Refresh results

With Bash and jq installed, run `./frontend/build_results` from the repository
root (or `./build_results` from this directory). It reads `../<engine>/results/`
and writes `results.json` atomically, using the same query selection as the
original assembler on main. Then run `npm run build` in
`frontend/` and commit the JSON and HTML together. The script does not run benchmarks.

## Playground integration

The host provides results before dynamically importing the product:

```ts
const { provideResults } = await import('@searchbench/frontend/results');
provideResults(rows);
const { ProductApp } = await import('@searchbench/frontend');
```

The host supplies `ThemeProvider` and its React Router context. Inside that
context, header links use the host router; standalone links use ordinary anchors
and absolute service URLs. There is no dependency on `@playground/web-kit`.

After this PR is approved, `products/searchbench` can become a submodule. The
playground's data-copy plugin and Docker/ignore rules must then read
`products/searchbench/frontend/results.json` instead of `data/results.json`.
Its `web` build continues importing the frontend's source export. Keep any
private benchmark variants in the host, separate from this public snapshot.

## Checks

From `frontend/`:

```bash
npm run typecheck
npx playwright install chromium   # once, for browser checks
npm test
```

Tests exercise result assembly, failure handling and the committed HTML opened
over `file://` with the network disabled. Rebuild before running browser tests
after changing source or results.

## Run data and the unified page (this repository)

`build_run.sh runs/<dir> <out.html>` renders a run directory (see `../runs/README.md`)
with the committed `index.html`: `inject.mjs` replaces the embedded results literal and
applies small UI patches (load-time breakdown rows, S3 request and CPU-time rows, the engine
version under each column name, run details in the header tooltip and query panel), each required to match exactly once.
The version line is the result file's `version` (what the engine's `./version` recorded),
without the parenthesised run settings DuckDB appends; the tooltip keeps the full string.
`build_run.sh ... --rows-only` exports the same enriched rows without HTML.
`build_site.mjs` runs that pipeline independently for each set, adds a `run_id`
boundary to each row, and generates the single selector page published by
`.github/workflows/pages.yml`. The directory name is a run set's stable URL ID. Optional
`runs/<dir>/run.json` fields:
- `label` and `description`.
- `links`: shown under the description, after an automatic README link. They point to the repository
  at the deployed commit (`GITHUB_SHA`; locally `SEARCHBENCH_REPO_URL` / `SEARCHBENCH_REPO_REF`, default
  quickwit-oss/searchbench at `main`).
- `compose`: a list of other run sets whose already-enriched columns this set shows side by side. An entry can
  be `{"run": "<run set>", "columns": ["<column name>", …]}` to take only some columns, by page name.

The original run files and metric values remain unchanged.

**Engine names must not contain commas.** Shared links (`?s=`) store hidden or included engines as one
comma-separated list of names, so a name with a comma is split when the link is opened and that engine
reappears. `build_run.sh` refuses such names; use ` · ` or ` + ` instead.

**CPU time.** A run with `SEARCHBENCH_CPU=on` records the engine's CPU seconds per try (`cpu`,
parallel to `result`), from the adapter's `./cpu-usage` (see `../lib/cpu-usage.sh`). `cpu_time.py`
turns it into the "CPU time · cold / hot" summary rows (summed over the queries every column with
CPU data has) and the query panel's `CPU s [cold, hot, hot]`.

**Metric switch.** The config panel's "metric" row picks what the per-query cells, the sort and the
geomean measure: Wall (as published), CPU (`cpu_by_q` from `cpu_time.py`), S3 GETs or S3 bytes
(`s3_requests` from `s3_requests.py`). The choice is in the URL hash (`#m=cpu|gets|bytes`), so shared
links keep it; picking one reloads the page. Columns without that data show "—"; the 60 s timeout mark
only applies to wall time; S3 ratios use a floor of 1 GET / 1 MB so a query that reads nothing is 1.00×.

**Geomean row.** It is computed over the queries **every displayed engine** answered without a
timeout (after the participants and category filters), as the geometric mean of each engine's
ratio to the fastest engine on each query (`(10 ms + t) / (10 ms + fastest)`). The label shows the
size of that set, e.g. `Geomean · 57 common q.`; with no common query the row shows `—`. Before
2026-11-01 each engine was averaged over the queries *it* answered, so an engine that alone runs
some queries (x1 there) looked better than on the shared set. `common_geomean.mjs` holds the logic
and the bundle patch.

**Load CPU.** With `SEARCHBENCH_CPU=on` the driver also records the load's CPU (`load_cpu` in the load
record): every container under the `searchbench.slice` cgroup during `./load` (merges and their settle wait
included; the slice keeps the CPU of containers that exited, such as Quickwit's `tool local-ingest`), plus
`./load`'s own processes (DuckDB's sort). It needs the Docker daemon's `"cgroup-parent": "searchbench.slice"`.
The page shows it as "CPU time · load". `<run>/load_cpu.json` (keyed like `phases.json`) fills it for a
column whose load record predates it.
