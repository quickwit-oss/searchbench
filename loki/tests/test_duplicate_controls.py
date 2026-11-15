"""Distinguish distinct source identities from legitimate replicated copies."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import requests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import make_config
from ingest import entry
from logql import build_request, normalize
from test_adapter import CONTRACT, completed_manifest, legacy_request

T = 1758585600000000000


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY"), "set LOKI_TEST_BINARY")
class DuplicateControls(unittest.TestCase):
    def test_raw_cross_chunk_failure_and_same_identity_replication(self):
        def port():
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                return sock.getsockname()[1]
        with tempfile.TemporaryDirectory(prefix="loki-duplicate-controls-") as tmp:
            root = Path(tmp)
            receipts, processes = [], []
            for name, encoded, batches, expected, duplicates in [
                    ("raw-one-chunk", False, [list(range(9))], 9, 0),
                    ("raw-two-chunks", False, [list(range(8)), [8]], 8, 1),
                    ("frame-two-chunks", True, [list(range(8)), [8]], 9, 0),
                    # Replay IDs 0 and 8, NOT new source rows. Two entries make
                    # the replica chunk physically different: replaying the
                    # identical singleton chunk can reuse its content key.
                    ("frame-replica", True, [list(range(8)), [8], [0, 8]], 9, 2)]:
                directory = root / name
                directory.mkdir()
                http, grpc = port(), port()
                url = f"http://127.0.0.1:{http}"
                with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem"}):
                    config = make_config(str(directory / "data"), http, grpc, "127.0.0.1")
                path = directory / "config.json"
                path.write_text(json.dumps(config))
                process = None
                with (directory / "server.log").open("w") as log:
                    def stop():
                        nonlocal process
                        if process is not None and process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=60)
                            except subprocess.TimeoutExpired:
                                process.kill(); process.wait(timeout=10)
                                raise
                        if process is not None:
                            processes[-1].update(stopped=True, exit_code=process.returncode)
                        process = None
                    def start(target):
                        nonlocal process
                        process = subprocess.Popen([os.environ["LOKI_TEST_BINARY"], "-config.file=" + str(path),
                                                    "-target=" + target], stdout=log, stderr=log)
                        processes.append({"pid": process.pid, "target": target, "stopped": False})
                        for _ in range(160):
                            if process.poll() is not None:
                                raise AssertionError((directory / "server.log").read_text()[-3000:])
                            try:
                                if requests.get(url + "/ready", timeout=1).status_code == 200:
                                    return
                            except requests.RequestException:
                                pass
                            time.sleep(.2)
                        raise AssertionError("native readiness timeout")
                    try:
                        start("all")
                        for batch in batches:
                            before = sum(p.is_file() for p in (directory / "data/chunks/fake").rglob("*"))
                            values = []
                            for i in batch:
                                labels, value = entry({"Body": "duplicate body", "ServiceName": "fixture", "SeverityText": "INFO"},
                                                      T + min(i, 7), f"part_0.parquet:{i}")
                                if not encoded:
                                    value[1] = "duplicate body"  # explicit negative control, never production fallback
                                values.append(value)
                            response = requests.post(url + "/loki/api/v1/push", json={"streams": [{"stream": labels, "values": values}]}, timeout=30)
                            self.assertEqual(response.status_code, 204, response.text)
                            requests.post(url + "/flush", timeout=30).raise_for_status()
                            for _ in range(150):
                                if sum(p.is_file() for p in (directory / "data/chunks/fake").rglob("*")) > before:
                                    break
                                time.sleep(.1)
                            else:
                                self.fail(name + ": new durable chunk was not observed")
                        stop(); start("read,query-scheduler")
                        response = requests.get(url + "/loki/api/v1/query", params={
                            "query": 'sum(count_over_time({dataset="otel_logs"} | keep dataset [1ms]))',
                            "time": str(T + 7)}, timeout=30)
                        response.raise_for_status()
                        data = response.json()["data"]
                        self.assertEqual(int(data["result"][0]["value"][1]), expected)
                        stats = data["stats"]
                        self.assertEqual(stats["ingester"]["totalReached"], 0)
                        self.assertEqual(stats["querier"]["store"]["chunk"]["totalDuplicates"], duplicates)
                        logs = requests.get(url + "/loki/api/v1/query_range", params={
                            "query": '{dataset="otel_logs"}', "start": str(T), "end": str(T + 8), "limit": 100}, timeout=30)
                        logs.raise_for_status()
                        self.assertEqual(sum(len(s["values"]) for s in logs.json()["data"]["result"]), 9)
                        receipt = {"name": name, "acknowledged": sum(map(len, batches)),
                                   "distinct_source_ids": 9, "count": expected, "duplicates": duplicates,
                                   "count_response": response.json(), "logs_response": logs.json()}
                        if encoded:
                            manifest = completed_manifest({**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T + 7})
                            receipt["optimized_equivalence"] = []
                            for kind in ("count", "logs"):
                                for limit in (1, 7, 9, 100):
                                    spec = {"kind": kind, "filters": ['|~ "duplicate"'], "limit": limit}
                                    answers = []
                                    for builder in (legacy_request, build_request):
                                        query_path, params = builder(spec, manifest)
                                        if kind == "logs":
                                            params["query"] += " | keep dataset,service_name,severity_text"
                                        reply = requests.get(url + query_path, params=params, timeout=30)
                                        reply.raise_for_status()
                                        answer = normalize(spec, reply.json(), manifest)
                                        answers.append(answer)
                                        if kind == "count":
                                            self.assertEqual(answer, [{"count": 9}])
                                        else:
                                            self.assertEqual(len(answer), min(limit, 9))
                                        receipt["optimized_equivalence"].append({"builder": builder.__name__, "spec": spec,
                                                                                 "params": params, "response": reply.json()})
                                    self.assertCountEqual(*answers)
                        receipts.append(receipt)
                    finally:
                        stop()
            (root / "results.json").write_text(json.dumps(receipts, indent=2))
            (root / "processes.json").write_text(json.dumps(processes, indent=2))
            if os.environ.get("LOKI_TEST_ARTIFACTS"):
                shutil.copytree(root, Path(os.environ["LOKI_TEST_ARTIFACTS"]) / root.name)
