import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ingest import ingest_file, ingest_directory, source_inventory
from encoding import ENCODING, ENCODING_FLAG, decode_frame
from test_adapter import CONTRACT

CLIENT = os.environ.get("LOKI_TEST_CLIENT", "")
COMMAND = [CLIENT, "--line-encoding", ENCODING_FLAG, "--destination-id", CONTRACT["destination_id"]]
HAS_DEPS = all(importlib.util.find_spec(name) for name in ("pyarrow", "snappy", "google.protobuf")) if importlib.util.find_spec("google") else False


@unittest.skipUnless(CLIENT and HAS_DEPS, "set LOKI_TEST_CLIENT and install requirements-test.txt")
class RustClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="loki-rust-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bodies, self.headers = [], []
        self.response_code = 204
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                owner.bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
                owner.headers.append(dict(self.headers))
                if owner.response_code in (-1, -2):
                    if owner.response_code == -2:
                        threading.Event().wait(2)  # inject a timeout AFTER the full request was received
                    self.connection.close()
                    return
                self.send_response(owner.response_code)
                self.end_headers()
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.thread.join, 3)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def fixture(self, name="part_000.parquet", rows=31, statistics=True, unit="ns"):
        import pyarrow as pa
        import pyarrow.parquet as pq
        base = 1758585600000000000
        # Original file order is NOT timestamp order. IDs must be assigned
        # before per-stream wire sorting, across both row groups and batches.
        values = [base + (rows - i) * 1000 + 1 for i in range(rows)]
        values[1] = values[0]  # two distinct source rows with identical data
        scale = {"ns": 1, "us": 1000, "ms": 1000000}[unit]
        timestamps = pa.array([x // scale for x in values], type=pa.timestamp(unit, tz="UTC"))
        table = pa.table({
            "Timestamp": timestamps,
            "Body": [["ERROR é\nline\twith \"quotes\" and \\slash", "", "\n", "雪😀ſ\u2028\u2029",
                      "".join(chr(c) for c in range(32)), '{"v":1,"id":"raw","body":"error"}']
                     [(0 if i == 1 else i) % 6] for i in range(rows)],
            "ServiceName": ['svc"\\\n' if i % 2 and i != 1 else "cart" for i in range(rows)],
            "SeverityText": ["ERROR"] * rows,
            "SeverityNumber": pa.array([17] * rows, type=pa.int8()),
            "TraceId": ["trace"] * rows,
            "ResourceAttributes": pa.array([[('key', 'value'), ('unicode', 'é')]] * rows, type=pa.map_(pa.string(), pa.string())),
            "LogAttributes": pa.array([{"arr": [1, 2], "flag": True, "maybe": None}] * rows),
            # serde_json's own exact-roundtrip regression value: its default
            # parser can otherwise change this finite f64 by one ULP.
            "Float64": pa.array([51.248178375505404] * rows, type=pa.float64()),
            "Float32": pa.array([0.1] * rows, type=pa.float32()),
            "FloatStruct": pa.array([{"f": 0.1}] * rows, type=pa.struct([("f", pa.float32())])),
            "FloatList": pa.array([[0.1]] * rows, type=pa.list_(pa.float32())),
            "FloatMap": pa.array([[('f', 0.1)]] * rows, type=pa.map_(pa.string(), pa.float32())),
            "Optional": pa.array([None] * rows, type=pa.string()),
            "ScopeName": [""] * rows,
        })
        path = self.root / name
        pq.write_table(table, path, row_group_size=3, write_statistics=statistics)
        return path

    def invoke(self, files, *extra):
        args = [*COMMAND, "--url", self.url, "--threads", "4", "--http-workers", "3", "--batch-size", "2", "--queue-depth", "2"]
        for file in files:
            args += ["--file", str(file)]
        return subprocess.run(args + list(extra), capture_output=True, text=True, timeout=30)

    def decode(self):
        import snappy
        from push_protocol import PushRequest
        rows = []
        for body in self.bodies:
            request = PushRequest.FromString(snappy.decompress(body))
            for stream in request.streams:
                labels = {key: json.loads(value) for key, value in re.findall(r'([A-Za-z_]\w*)=("(?:\\.|[^"\\])*")', stream.labels)}
                times = []
                for entry in stream.entries:
                    ts = entry.timestamp.seconds * 10**9 + entry.timestamp.nanos
                    times.append(ts)
                    rows.append((labels, [str(ts), entry.line, {p.name: p.value for p in entry.structured_metadata}]))
                self.assertEqual(times, sorted(times))
        return rows

    @staticmethod
    def canonical(rows):
        result = {}
        for labels, (ts, line, metadata) in rows:
            metadata = dict(metadata)
            for field in ("ResourceAttributes", "LogAttributes", "FloatStruct", "FloatList", "FloatMap"):
                if field in metadata:
                    metadata[field] = json.loads(metadata[field])
            identity = metadata["sb_row_id"]
            if identity in result:
                raise AssertionError("duplicate source row identity")
            result[identity] = (labels, ts, line, metadata)
        return result

    def test_wire_equals_python_across_row_groups_and_files(self):
        files = [self.fixture(), self.fixture('part_é_"\\.parquet')]
        expected = []
        with patch("ingest.push", side_effect=lambda session, url, rows: expected.extend(rows)):
            for file in files:
                ingest_file((str(file), self.url, 7))
        result = self.invoke(files)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["rows"], 62)
        self.assertEqual(summary["readers"], 4)
        self.assertEqual(summary["row_groups"], 22)
        self.assertEqual(summary["wire_format"], "snappy-protobuf")
        self.assertEqual(summary["encoding"], ENCODING)
        self.assertEqual(summary["destination_id"], CONTRACT["destination_id"])
        self.assertEqual(set(self.canonical(self.decode())), {f"{f.name}:{i}" for f in files for i in range(31)})
        for _, (_, line, metadata) in self.decode():
            self.assertEqual(decode_frame(line)["id"], metadata["sb_row_id"])
        self.assertTrue(all(h["Content-Type"] == "application/x-protobuf" for h in self.headers))
        self.assertEqual(self.canonical(self.decode()), self.canonical(expected))
        self.assertEqual(summary["requests"], len(self.bodies))
        self.assertEqual(summary["compressed_bytes"], sum(map(len, self.bodies)))

    def test_required_encoding_flags_fail_before_push(self):
        file = self.fixture()
        for extra in [[], ["--line-encoding", "unknown", "--destination-id", "fixture"],
                      ["--line-encoding", ENCODING_FLAG, "--destination-id", ""]]:
            result = subprocess.run([CLIENT, "--url", self.url, "--file", str(file), *extra],
                                    capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertFalse(self.bodies)

    def test_physical_line_preflight_scans_all_files_before_push(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        files = [self.fixture(), self.fixture("part_999.parquet")]
        table = pq.read_table(files[1])
        for body in ["x" * (1024 * 1024), "\0" * 180000]:
            table = table.set_column(table.schema.get_field_index("Body"), "Body",
                                     pa.array(["valid"] * 30 + [body]))
            pq.write_table(table, files[1], row_group_size=3)
            result = self.invoke(files)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("physical frame", result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertFalse(self.bodies)

    def test_near_line_cap_exact_fallback_and_protocol(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from encoding import MAX_LINE_BYTES, frame_overhead
        file = self.fixture(rows=2)
        table = pq.read_table(file)
        body = "x" * (MAX_LINE_BYTES - frame_overhead(file.name + ":0"))
        table = table.set_column(table.schema.get_field_index("Body"), "Body", pa.array([body, body]))
        pq.write_table(table, file, row_group_size=1)
        result = self.invoke([file])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.decode()), 2)
        self.assertTrue(all(len(value[1].encode()) == MAX_LINE_BYTES for _, value in self.decode()))

    def test_time_units_and_missing_parquet_statistics(self):
        for unit in ("ns", "us", "ms"):
            with self.subTest(unit=unit):
                self.bodies.clear(); self.headers.clear()
                file = self.fixture(statistics=False, unit=unit)
                result = self.invoke([file])
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = []
                with patch("ingest.push", side_effect=lambda session, url, rows: expected.extend(rows)):
                    ingest_file((str(file), self.url, 5))
                self.assertEqual(self.canonical(self.decode()), self.canonical(expected))

    def test_python_directory_json_matches_rust_protobuf(self):
        files = [self.fixture(), self.fixture("part_001.parquet")]
        with patch.dict(os.environ, {"LOKI_LOAD_CLIENT": "python", "LOKI_LOAD_WORKERS": "2",
                                     "LOKI_BATCH_ROWS": "5", "LOKI_MAX_REQUEST_BYTES": "2048"}):
            summary = ingest_directory(self.root, self.url, {**CONTRACT, "source_files": source_inventory(files)})
        self.assertEqual(summary["encoding"], ENCODING)
        self.assertEqual(summary["destination_id"], CONTRACT["destination_id"])
        self.assertEqual(summary["rows"], 62)
        self.assertTrue(all(len(body) <= 2048 for body in self.bodies))
        expected = [(stream["stream"], value) for body in self.bodies
                    for stream in json.loads(body)["streams"] for value in stream["values"]]
        self.assertTrue(all(h["Content-Type"] == "application/json" for h in self.headers))
        self.bodies.clear(); self.headers.clear()
        result = self.invoke(files, "--max-request-bytes", "2048")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.canonical(self.decode()), self.canonical(expected))

    def test_python_dispatch_uses_rust_and_validates_manifest(self):
        self.fixture()
        with patch.dict(os.environ, {"LOKI_LOAD_CLIENT": "rust", "LOKI_CLIENT_BIN": CLIENT, "LOKI_LOAD_WORKERS": "4", "LOKI_HTTP_WORKERS": "2"}):
            summary = ingest_directory(self.root, self.url, {**CONTRACT, "source_files": source_inventory(sorted(self.root.glob("part_*.parquet")))})
        self.assertEqual(summary["rows"], 31)
        self.assertEqual(summary["readers"], 4)  # ONE file still uses several readers
        self.assertEqual(summary["client"], "rust")

    def test_default_request_size_stays_below_loki_grpc_limit(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import snappy
        file = self.fixture(rows=10000)
        table = pq.read_table(file)
        table = table.set_column(table.schema.get_field_index("Body"), "Body", pa.array(["X" * 1024] * len(table)))
        pq.write_table(table, file, row_group_size=10000)
        result = subprocess.run([*COMMAND, "--url", self.url, "--file", str(file), "--batch-size", "5000"],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["rows"], 10000)
        self.assertGreater(len(self.bodies), 2)
        self.assertTrue(all(len(snappy.decompress(body)) <= 3 * 1024 * 1024 for body in self.bodies))

    def test_request_size_bound(self):
        import snappy
        file = self.fixture(rows=31)
        # Bigger Arrow batches than request limit: split without losing IDs.
        args = [*COMMAND, "--url", self.url, "--file", str(file), "--batch-size", "50", "--max-request-bytes", "1024"]
        result = subprocess.run(args, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(len(snappy.decompress(b)) <= 1024 for b in self.bodies))
        self.assertEqual(len(self.canonical(self.decode())), 31)

    def test_http_error_is_not_retried_and_no_success_manifest(self):
        file = self.fixture()
        args = [*COMMAND, "--url", self.url, "--file", str(file), "--threads", "1", "--http-workers", "1", "--batch-size", "1", "--queue-depth", "1"]
        for code in (429, 503, 260):
            with self.subTest(status=code):
                self.response_code = code
                self.bodies.clear(); self.headers.clear()
                result = subprocess.run(args, capture_output=True, text=True, timeout=15)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertEqual(len(self.bodies), 1)
                self.assertIn(f"HTTP {code}", result.stderr)

    def test_ambiguous_disconnect_and_timeout_are_not_retried(self):
        file = self.fixture()
        for code in (-1, -2):
            with self.subTest(mode=code):
                self.response_code = code
                self.bodies.clear(); self.headers.clear()
                result = subprocess.run([*COMMAND, "--url", self.url, "--file", str(file), "--threads", "1",
                                         "--http-workers", "1", "--batch-size", "1", "--queue-depth", "1", "--timeout-secs", "1"],
                                        capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertEqual(len(self.bodies), 1)
                self.assertIn("ambiguous acceptance", result.stderr)

    def test_invalid_worker_count_fails_without_push(self):
        file = self.fixture()
        result = subprocess.run([*COMMAND, "--url", self.url, "--file", str(file), "--threads", "0"], capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.bodies)

    def test_preflight_invalid_timestamp_and_wide_span(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        file = self.fixture()
        for values, typ in [([None] * 31, pa.int64()), ([1758585600000000000 + i * 3600 * 10**9 for i in range(31)], pa.int64()), (["invalid"] * 31, pa.string())]:
            with self.subTest(type=str(typ)):
                table = pq.read_table(file).set_column(0, "Timestamp", pa.array(values, type=typ))
                pq.write_table(table, file)
                result = self.invoke([file])
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.bodies)
                self.assertEqual(result.stdout, "")

    def test_duplicate_struct_fields_and_nonfinite_metadata_fail(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate):
                file = self.fixture()
                table = pq.read_table(file)
                if duplicate:
                    values = pa.StructArray.from_arrays([pa.array([1] * len(table)), pa.array([2] * len(table))], names=["same", "same"])
                    error = "duplicate metadata struct field"
                else:
                    values = pa.array([float('nan')] * len(table), type=pa.float64())
                    error = "non-finite metadata float"
                table = table.append_column("BadMetadata", values)
                pq.write_table(table, file)
                result = self.invoke([file])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(error, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertFalse(self.bodies)

    def test_oversized_row_is_not_truncated(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        file = self.fixture()
        table = pq.read_table(file)
        table = table.set_column(table.schema.get_field_index("Body"), "Body", pa.array(["X" * 2048] * len(table)))
        pq.write_table(table, file)
        result = self.invoke([file], "--max-request-bytes", "1024")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("one row exceeds", result.stderr)
        self.assertFalse(self.bodies)
        self.assertEqual(result.stdout, "")

    def test_duplicate_source_names_and_metadata_keys_fail(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        file = self.fixture()
        result = self.invoke([file, file])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.bodies)
        table = pq.read_table(file)
        index = table.schema.get_field_index("ResourceAttributes")
        table = table.set_column(index, "ResourceAttributes", pa.array([[('same', 'a'), ('same', 'b')]] * len(table), type=pa.map_(pa.string(), pa.string())))
        pq.write_table(table, file)
        result = self.invoke([file])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate metadata map key", result.stderr)
        self.assertEqual(result.stdout, "")
