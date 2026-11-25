#!/usr/bin/env python3
"""Compare the stored Loki answers with the DuckDB 1B oracle subset.

Logs are compared in order at the oracle's microsecond precision; counts and
histograms exactly. Run from this directory: python3 check_answers.py
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
reference = json.loads((HERE / "raw/reference-answers-duckdb-1b.json").read_text())["queries"]


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def comparable(row):
    if "Timestamp" in row:
        return {**row, "Timestamp": row["Timestamp"][:26]}  # ns -> us
    return row


failures = 0
for path in sorted((HERE / "raw").glob("phase-*/answers/*.stdout")):
    query = path.name.split("-")[0]
    ok = [comparable(r) for r in rows(path)] == [comparable(r) for r in reference[query]["rows"]]
    failures += not ok
    print(f"{'equal' if ok else 'DIFFERENT':9} {path.relative_to(HERE)}")
raise SystemExit(1 if failures else 0)
