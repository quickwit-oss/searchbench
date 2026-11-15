#!/usr/bin/env python3
"""HOST, background during the load. Every 60 s: Loki's S3 SDK-call counters by
operation/status (loki_s3_request_duration_seconds_count) -> JSONL; unreachable
scrapes (restarts) are recorded and skipped. Observation only, except one guard:
if after >= 15 min not a single S3.PutObject has succeeded while >= 20 have
failed (e.g. no container credentials), write ABORT_FILE and SIGTERM PID
(load_overlay.py) so the paid run does not ingest for an hour into nothing.

  s3ops_logger.py URL OUT.jsonl ABORT_FILE PID
"""
import json
import os
import re
import signal
import sys
import time
import urllib.request

LINE = re.compile(r'^loki_s3_request_duration_seconds_count\{([^}]*)\} (\S+)$', re.M)
LABEL = re.compile(r'([a-zA-Z_]+)="([^"]*)"')


def counters(text):
    out = {}
    for labels, value in LINE.findall(text):
        d = dict(LABEL.findall(labels))
        key = f"{d.get('operation')}|{d.get('status_code')}"
        out[key] = out.get(key, 0) + float(value)
    return out


def main(url, out_path, abort_file, pid):
    started = time.time()
    with open(out_path, "a", buffering=1) as out:
        while True:
            try:
                os.kill(pid, 0)
            except OSError:
                return 0
            rec = {"unix": time.time()}
            try:
                with urllib.request.urlopen(url, timeout=10) as r:
                    rec["s3"] = counters(r.read().decode())
            except Exception as exc:
                rec["error"] = f"{type(exc).__name__}: {exc}"
            out.write(json.dumps(rec, separators=(",", ":")) + "\n")
            s3 = rec.get("s3") or {}
            ok = sum(v for k, v in s3.items() if k.startswith("S3.PutObject|") and k.endswith("|200"))
            bad = sum(v for k, v in s3.items() if k.startswith("S3.PutObject|") and not k.endswith("|200"))
            if s3 and time.time() - started >= 900 and ok == 0 and bad >= 20:
                with open(abort_file, "w") as f:
                    f.write(json.dumps({"reason": "no successful S3.PutObject after 15 min", "s3": s3}) + "\n")
                os.kill(pid, signal.SIGTERM)
                return 3
            time.sleep(60)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])))
