"""Same query/correctness suite after native Rust protobuf ingestion."""
import json
import os
import subprocess
import unittest

import test_adapter


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY") and os.environ.get("LOKI_TEST_CLIENT"),
                     "set LOKI_TEST_BINARY and LOKI_TEST_CLIENT for real protobuf integration")
class RustLokiIntegrationTests(test_adapter.IntegrationTests):
    @classmethod
    def ingest_fixture(cls):
        import pyarrow as pa
        import pyarrow.parquet as pq
        # The parallel loader deliberately supports <1h corpora. The original
        # JSON suite separately tests the six-hour endpoints with sorted pushes.
        cls.docs = [d for d in cls.docs if d["Timestamp"] < cls.t + 3600 * 10**9]
        from encoding import ENCODING, ENCODING_FLAG
        for number, batch in enumerate(cls.fixture_batches()):
            docs = list(reversed([d for _, d in batch]))  # unordered row groups
            table = pa.Table.from_pylist(docs)
            index = table.schema.get_field_index("Timestamp")
            table = table.set_column(index, "Timestamp", pa.array([d["Timestamp"] for d in docs], type=pa.timestamp("ns", tz="UTC")))
            path = cls.root / f"part_{number:03}.parquet"
            pq.write_table(table, path, row_group_size=4)
            previous = sum(p.is_file() for p in (cls.root / "data/chunks/fake").rglob("*"))
            result = subprocess.run([os.environ["LOKI_TEST_CLIENT"], "--file", str(path), "--url", cls.url,
                                     "--line-encoding", ENCODING_FLAG, "--destination-id", test_adapter.CONTRACT["destination_id"],
                                     "--threads", "4", "--http-workers", "3", "--batch-size", "2", "--queue-depth", "2"],
                                    capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise AssertionError(result.stderr)
            cls.load_summary = json.loads(result.stdout)
            if cls.load_summary["rows"] != len(docs) or cls.load_summary["encoding"] != ENCODING:
                raise AssertionError(cls.load_summary)
            cls.flush_fixture(previous)
