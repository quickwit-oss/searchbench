#!/usr/bin/env python3
# Ground truth for the time-sorted top-100 "recent" queries (Q68-Q75), any
# engine, any scale.
#
#   lib/check_recent.py <dataset>/part_000.parquet <engine>/results/<run>.results.log ... Truth comes from the parquet via row-group statistics:
# scan row groups by descending max(Timestamp) inside the query window and stop
# once the top-100 can no longer change.
#
# Engine outputs are parsed to integer nanoseconds from all three formats:
#   Quickwit   JSON  {"Timestamp": "2025-09-23T00:29:24.039010157Z", ...}
#   ClickHouse TSV   2025-09-23 00:29:24.039010157<TAB>...
#   SereneDB   psql  2025-09-23 00:29:24.039010157+00|...
# A run is OK at the precision it stores: exact ns if it reports ns, else the
# multiset of truth timestamps truncated to its precision (ms / s).
import json, re, sys, pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa
from calendar import timegm

path, logs = sys.argv[1], sys.argv[2:]
pf = pq.ParquetFile(path); md = pf.metadata
ti = pf.schema_arrow.get_field_index("Timestamp")
rg = [(i, int(md.row_group(i).column(ti).statistics.min_raw), int(md.row_group(i).column(ti).statistics.max_raw))
      for i in range(md.num_row_groups)]

TS = re.compile(r"(\d{4})-(\d\d)-(\d\d)[T ](\d\d):(\d\d):(\d\d)(?:\.(\d{1,9}))?")
def to_ns(s):
    m = TS.search(s)
    y, mo, d, h, mi, se, frac = m.groups()
    frac = (frac or "")
    return timegm((int(y), int(mo), int(d), int(h), int(mi), int(se))) * 10**9 + int(frac.ljust(9, "0") or 0), len(frac)
def row_ts(line):
    line = line.strip()
    if line.startswith("{"):
        return to_ns(json.loads(line)["Timestamp"])
    return to_ns(re.split(r"[\t|]", line, 1)[0])
def ns(s):  # query-literal 'YYYY-MM-DDTHH:MM:SS'
    return to_ns(s)[0]

def parse(f):
    out = {}
    for blk in open(f).read().split("===== Q")[1:]:
        out[blk.split(" ", 1)[0].lstrip("0")] = blk.split("----- result -----\n", 1)[1].strip()
    return out
runs = {f.rsplit("/", 1)[-1].replace(".results.log", ""): parse(f) for f in logs}

# Predicates are taken from datadog-byoc/queries.dsl (the ES DSL is easy to parse);
# every engine's Q68-Q75 expresses the same predicate.
import os
dsl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "datadog-byoc", "queries.dsl")
dsl = [l for l in open(dsl_path) if l.strip() and not l.startswith("--")]
for qn in range(68, 76):
    b = json.loads(dsl[qn - 1])["query"]["bool"]
    terms, ok = [], True
    def walk(n):
        global ok
        if "match" in n and isinstance(n["match"]["Body"], str) and " " not in n["match"]["Body"]:
            terms.append(n["match"]["Body"].lower())
        elif "bool" in n and list(n["bool"]) == ["must"]:
            for x in n["bool"]["must"]: walk(x)
        else: ok = False
    for x in b.get("must", []): walk(x)
    service = lo = hi = None
    for f in b.get("filter", []):
        if "term" in f: service = f["term"]["ServiceName"]
        elif "range" in f and "Timestamp" in f["range"]:
            lo, hi = ns(f["range"]["Timestamp"]["gte"]), ns(f["range"]["Timestamp"]["lte"])
        else: ok = False
    if not ok:
        print(qn, "skip (not a plain term query)", flush=True); continue
    top = []
    for i, mn, mx in sorted([r for r in rg if r[2] >= lo and r[1] <= hi], key=lambda r: -r[2]):
        if len(top) >= 100 and top[99] > mx: break
        t = pf.read_row_group(i, columns=["Timestamp", "Body", "ServiceName"])
        tsv = t.column("Timestamp").cast(pa.int64())
        m = pc.and_(pc.greater_equal(tsv, lo), pc.less_equal(tsv, hi))
        if service: m = pc.and_(m, pc.equal(t.column("ServiceName"), service))
        low = pc.utf8_lower(t.column("Body"))
        for w in terms: m = pc.and_(m, pc.match_substring_regex(low, "(^|[^a-z0-9])" + w + "([^a-z0-9]|$)"))
        top = sorted(top + pc.filter(tsv, m).to_pylist(), reverse=True)[:100]
    parts = []
    for name, r in runs.items():
        if str(qn) not in r: parts.append(f"{name}:missing"); continue
        got = [row_ts(l) for l in r[str(qn)].splitlines() if TS.search(l)]   # skips header lines
        digits = max(d for _, d in got)
        unit = 10 ** (9 - digits)        # precision the engine reported
        g = sorted((v // unit for v, _ in got), reverse=True)
        want = sorted((v // unit for v in top), reverse=True)
        parts.append(f"{name}:{'OK' if g == want else 'WRONG'}@{['s','','','ms','','','us','','','ns'][digits] or digits}")
    print(qn, terms, service or "", *parts, flush=True)
