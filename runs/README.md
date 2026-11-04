# Runs by machine and storage

Our SearchBench results are grouped by machine type, storage and configuration, kept apart
from the engines' own `results/` (upstream's published runs). A machine group can include
independent instances, sessions and layouts; column details preserve that provenance.
Dataset sizes stay separate in the UI. Directory names are stable historical IDs, not
restrictions on which dataset sizes a group can contain. One directory per run set:

```
runs/<date>-<machine>/
  <engine>/results/*.json          driver output, one file per --version (as in <engine>/results/)
  <engine>/results/load/*.json     load records (load_time, data_size, phases, storage_units)
  <engine>/queries.*               the queries the results were produced with
  run.json                         optional selector label, description, links and compose list (directory name = stable ID)
  names.json                       "<dataset>/<system as recorded>" -> column name on the page
  phases.json                      load-time breakdown rows (ingest, merges after, merge busy, check)
  README.md                        what each column is: build, settings, layout, caveats
  extra/                           runs that are not columns (superseded, A/B, debug), scripts,
                                   gzipped per-query results logs
```

## Run sets

| run set | machine | datasets | engines |
|---|---|---|---|
| [hippo-20261105](hippo-20261105/README.md) | AWS m8gd.4xlarge (16 vCPU ARM64, 64 GiB), S3 us-east-1 | otel_logs_1b | The public bench: Datadog BYOC 0.1.36 (ingest API, 64 splits) and ClickHouse 26.10.2.103 (1.7 GiB part cap, 61 parts), median of 3 rounds each; DuckDB 2.0 over a sorted Parquet copy, one run. Listed in [PUBLIC_BENCH.md](../PUBLIC_BENCH.md) |

## Pages

GitHub Pages, built by `.github/workflows/pages.yml`
on every push to `main` that touches `runs/` or `frontend/`:

| path | content |
|---|---|
| `/` | one interface with a Run / environment selector; newest run selected by default |
| `/?runSet=<dir>` | the same interface with that run selected |
| `/?runSet=upstream` | the same interface with upstream's original published results |
| `/runs/<dir>/`, `/upstream/` | compatibility redirects, preserving old link state |

`frontend/build_site.mjs` enriches each run separately through `build_run.sh`,
then embeds all sets in one self-contained page using the pinned UI bundle.
Selection happens **before** deriving datasets, engines or rankings; results from
separate run sets are never silently combined. CPU/S3 summary query counts are
scoped to the selected run and dataset. Some historical sets themselves include
other-machine references: their descriptions and column details identify these.

The selector is stored in `runSet`; `run=hot|cold` keeps its old cache-state meaning.
Run directories remain separate for provenance, not as separate browsing pages.
Optional `run.json` fields:
- `label`, `description`: shown in the selector. A long description shows its first segments on one line
  (machine, RAM, storage); the full text and the links are under a collapsed "details" toggle.
- `links`: `[{"label":"…","href":"…"}]`, shown under the description. Every run set also gets a
  link to its `README.md`, first. `href` is an `https://` URL or a path relative to the run directory
  (it must exist under `runs/`). Paths resolve to the repository at the deployed commit.
- `compose`: `["<run set>", …]`. It makes a run set from other run sets' columns, without copying
  data. Each source is built unchanged, and column names must be unique across them. A composed
  directory holds only `README.md` and `run.json`.
  - A source can also be `{"run":"<run set>","columns":["<column name>", …]}`. Only those columns
    are taken, matched by their name on the page (after `names.json`). A name the source doesn't have
    fails the build.
- `redirect`: `{"run":"<canonical run set>","dataset":"<default dataset>"}` retires a
  selector entry while preserving its old direct and `/runs/…/` links. The target must be a
  visible run containing that dataset (no redirect chains). Packed/legacy engine and view
  state is preserved; the default dataset is used only when the old link omits one.

Locally:

```bash
(cd frontend && npm run preview:runs)             # unified interface at localhost:5173
node frontend/build_site.mjs /tmp/searchbench-site  # build the same published site
# Optional single-run export for diagnostics:
frontend/build_run.sh runs/<dir> /tmp/page.html
```
