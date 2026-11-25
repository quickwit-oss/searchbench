#!/usr/bin/env python3
"""Answer verdict for one Loki stdout against references.json (built locally).

  oracle   DuckDB 1B rows (logs at microsecond precision) -- primary.
  clickhouse  ClickHouse 1B normalized value (count / group_by count multiset / rows).
  loki_multiset_sha256  prior local Loki 1B answers, where any try finished.

Verdicts: "match" (ordered rows equal the oracle at us precision, ClickHouse
agrees, and the prior Loki multiset agrees where one exists);
"match-tie-order" (logs only: same multiset at us precision, order differs
inside equal timestamps); "MISMATCH" otherwise (P0).
"""
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.environ.get("SB_SOURCE_LIB", str(Path(__file__).resolve().parent / "source/lib")))


def multiset_sha(stdout):
    return hashlib.sha256("\n".join(sorted(stdout.splitlines())).encode()).hexdigest()


def _us(row):
    return {**row, "Timestamp": row["Timestamp"][:26]} if "Timestamp" in row else row


def _key(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def verdict(ref, stdout):
    import check_results as cr
    lines = [line for line in stdout.splitlines() if line.strip()]
    rows = [json.loads(line) for line in lines]
    mine, oracle = [_us(r) for r in rows], [_us(r) for r in ref["oracle_rows"]]
    sha = multiset_sha(stdout)
    checks = {
        "multiset_sha256": sha, "rows": len(rows),
        "oracle_ordered": mine == oracle,
        "oracle_multiset": sorted(map(_key, mine)) == sorted(map(_key, oracle)),
        "clickhouse": cr.values_match(ref["task"], cr.normalize(ref["task"], lines), ref["clickhouse"]),
        "loki_multiset": None if ref.get("loki_multiset_sha256") is None else sha == ref["loki_multiset_sha256"],
    }
    if checks["clickhouse"] and checks["loki_multiset"] is not False:
        if checks["oracle_ordered"]:
            checks["verdict"] = "match"
        elif checks["oracle_multiset"] and "Timestamp" in (rows[0] if rows else {}):
            checks["verdict"] = "match-tie-order"
    checks.setdefault("verdict", "MISMATCH")
    return checks
