#!/usr/bin/env python3
"""Native Loki S3 GetObject SDK-call counter scrape (method of loki-s3-100k queries.py).

Metric: loki_s3_request_duration_seconds_count{operation="S3.GetObject"}, summed
per (synthetic) status_code label; process_start_time_seconds identifies the
process. Standard library only (fast enough to run around every try).

  parse(text)                     -> {"getobject": {status: count}, "process_start": float|None}
  delta(before, after)            -> {status: int}; raises on reset/non-integer
  getscrape.py URL [RAW_GZ_PATH]  -> one JSON line: scrape result (+ wall seconds), never raises
"""
import gzip
import json
import math
import re
import sys
import time
import urllib.request

METRIC = "loki_s3_request_duration_seconds_count"
OPERATION = "S3.GetObject"
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse(text):
    counts, start = {}, None
    for line in text.splitlines():
        if line.startswith(METRIC + "{"):
            labels_text, _, value = line[len(METRIC) + 1:].rpartition("}")
            labels = dict(LABEL.findall(labels_text))
            if labels.get("operation") == OPERATION:
                code = labels.get("status_code", "")
                counts[code] = counts.get(code, 0.0) + float(value.split()[0])
        elif line.startswith("process_start_time_seconds "):
            start = float(line.split()[1])
    return {"getobject": counts, "process_start": start}


def delta(before, after):
    result = {k: after.get(k, 0) - before.get(k, 0) for k in set(before) | set(after)}
    if not all(math.isfinite(v) and v >= 0 and v == int(v) for v in result.values()):
        raise ValueError(f"invalid/reset GetObject counter: {before} -> {after}")
    return {k: int(v) for k, v in result.items()}


def scrape(url, raw_path=None, timeout=10):
    t0 = time.time()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            text = response.read().decode()
        if raw_path:
            with open(raw_path, "wb") as f:
                f.write(gzip.compress(text.encode(), mtime=0))
        out = parse(text)
        out["ok"] = True
    except Exception as exc:  # recorded, never fatal for the measured try
        out = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "getobject": None, "process_start": None}
    out.update(scrape_unix=t0, scrape_seconds=time.time() - t0)
    return out


if __name__ == "__main__":
    print(json.dumps(scrape(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None), separators=(",", ":")))
