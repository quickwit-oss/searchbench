#!/usr/bin/env python3
"""Test-only native-binary stand-in for the adapter's Docker start/stop/check.

Used by test_query_pair.py as an --engine-dir, so the pair driver exercises the
same script lifecycle as lib/benchmark.sh against the real Loki binary. Uses
the production make_config/settings; never ingests (the read target rejects
pushes). LOKI_TEST_BINARY and LOKI_TEST_GRPC_PORT are test-only inputs.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests  # noqa: E402

from config import make_config, settings  # noqa: E402
from logql import literal_prefilter_enabled  # noqa: E402

OWNED = {}  # pid -> Popen when start/stop run in one (test) process
TARGETS = {"all": "all", "read": "read,query-scheduler", "read,compactor": "read,query-scheduler,compactor"}


def url():
    return f"http://127.0.0.1:{settings()['port']}"


def ready():
    try:
        return requests.get(url() + "/ready", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def start(target_override=None):
    s = settings()
    literal_prefilter = literal_prefilter_enabled()
    root = Path(s["data_dir"])
    root.mkdir(parents=True, exist_ok=True)
    pidfile = root / "native.pid"
    if pidfile.exists():
        raise RuntimeError("native Loki already running")
    config = make_config(str(root), s["port"], int(os.environ["LOKI_TEST_GRPC_PORT"]), "127.0.0.1")
    (root / "config.json").write_text(json.dumps(config, sort_keys=True, indent=2) + "\n")
    target = TARGETS[target_override or s["target"]]
    with (root / "native-server.log").open("a") as log:
        process = subprocess.Popen([os.environ["LOKI_TEST_BINARY"], "-config.file=" + str(root / "config.json"),
                                    "-target=" + target], stdout=log, stderr=log, start_new_session=True)
    pidfile.write_text(str(process.pid))
    OWNED[process.pid] = process
    with (root / "native-starts.jsonl").open("a") as starts:
        starts.write(json.dumps({"pid": process.pid, "target": target, "time": time.time()}) + "\n")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pidfile.unlink()
            raise RuntimeError("native Loki exited during startup")
        if ready():
            (root / "runtime.json").write_text(json.dumps({
                "target": target, "image": "native:" + os.environ["LOKI_TEST_BINARY"], "container": "native",
                "query_concurrency": config["querier"]["max_concurrent"],
                "literal_prefilter": literal_prefilter}, indent=2) + "\n")
            return
        time.sleep(.2)
    raise RuntimeError("native Loki readiness timeout")


def stop():
    pidfile = Path(settings()["data_dir"]) / "native.pid"
    if not pidfile.exists():
        return
    pid = int(pidfile.read_text())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pidfile.unlink()
        return
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if pid in OWNED:
            if OWNED[pid].poll() is not None:
                del OWNED[pid]
                break
        else:
            try:
                os.kill(pid, 0)  # started by another shim process; init reaps it
            except ProcessLookupError:
                break
        time.sleep(.1)
    else:
        os.kill(pid, signal.SIGKILL)
        raise RuntimeError("native Loki needed SIGKILL")
    pidfile.unlink()


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "start":
        start()
    elif command == "stop":
        stop()
    elif command == "check":
        sys.exit(0 if ready() else 1)
    else:
        raise SystemExit("usage: native_engine.py start|stop|check")
