import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import adapter


class StorageUnitTests(unittest.TestCase):
    def test_files_exclude_wal_and_caches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ['chunks/a', 'chunks/index/b', 'wal/c', 'tsdb-cache/d', 'load-started.json']:
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('data')
            output = io.StringIO()
            with patch.object(adapter, 'settings', return_value={'storage': 'filesystem', 'data_dir': tmp}), redirect_stdout(output):
                adapter.storage_units()
            self.assertEqual(json.loads(output.getvalue()), {'count': 2, 'unit': 'files'})

    def test_missing_store_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(adapter, 'settings', return_value={'storage': 'filesystem', 'data_dir': tmp}):
            with self.assertRaisesRegex(ValueError, 'unknown'):
                adapter.storage_units()

    def test_s3_counts_data_objects_not_reservation(self):
        prefix = 'searchbench/loki/test/'
        objects = [{'Key': prefix + name} for name in ['chunk/a', 'index/b', '_searchbench_load_lock.json']]
        output = io.StringIO()
        with patch.object(adapter, 'settings', return_value={'storage': 's3', 'prefix': prefix}), \
             patch.object(adapter, 's3_objects', return_value=iter(objects)), redirect_stdout(output):
            adapter.storage_units()
        self.assertEqual(json.loads(output.getvalue()), {'count': 2, 'unit': 'objects'})


if __name__ == '__main__':
    unittest.main()
