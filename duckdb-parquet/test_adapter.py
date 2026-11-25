"""Offline synthetic fixtures only: no corpus downloads, S3 access, or cache drops."""
import http.server
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from urllib.parse import urlparse

import duckdb

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('adapter', HERE / 'adapter.py')
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='sb-duckdb-', dir='/tmp')
        self.root = Path(self.tmp.name)
        self.parquet = self.root / "data's" / 'part_000.parquet'
        self.parquet.parent.mkdir()
        self.con = duckdb.connect()
        self.con.execute('CREATE TABLE fixture (Timestamp BIGINT, Body VARCHAR, ServiceName VARCHAR, SeverityText VARCHAR, SeverityNumber INTEGER, ScopeName VARCHAR, TraceId VARCHAR)')
        self.base = 1758585600000000000  # 2025-09-23 00:00:00, epoch ns
        rows = [
            (self.base + 1, 'ERROR failed to place order charge cache', 'frontend', 'ERROR', 17, 'a', 't1'),
            (self.base + 2, 'Payment connection request', 'payment', 'INFO', 9, 'b', 't1'),
            (self.base + 3, 'failed-to/place,order', 'checkout', 'WARN', 13, 'a', 't2'),
            (self.base + 4, 'cart order', 'cart', 'INFO', 9, 'c', 't2'),
            (self.base + 5, 'terror errors failed to place orders', 'frontend', 'INFO', 9, 'a', 't3'),
            (self.base + 6, 'connetcion conversion post', 'frontend', 'INFO', 9, 'a', 't4'),
            (self.base + 7, 'post to email service expected 200 got 500', 'frontend', 'ERROR', 17, 'a', ''),
            (self.base + 8, None, 'cart', 'INFO', 9, 'c', ''),
        ]
        self.con.executemany('INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?)', rows)
        self.con.execute('COPY fixture TO ? (FORMAT PARQUET)', [str(self.parquet)])
        self.config = {'source': str(self.parquet), 'threads': 2, 'memory_limit': '256MB',
                       's3_auth': 'unsigned', 's3_region': 'us-east-1'}
        self.con.execute((HERE / 'macros.sql').read_text())
        adapter.prepare(self.con, self.config)
        base_env = {k: v for k, v in os.environ.items()
                    if not k.startswith(('SEARCHBENCH_', 'DUCKDB_'))}
        self.env = dict(base_env, DUCKDB_PARQUET_PATH=str(self.parquet),
                        DUCKDB_STATE_DIR=str(self.root / 'worker'),
                        DUCKDB_PYTHON=sys.executable, DUCKDB_THREADS='2',
                        DUCKDB_MEMORY_LIMIT='256MB', DUCKDB_S3_AUTH='unsigned')

    def tearDown(self):
        subprocess.run([sys.executable, str(HERE / 'adapter.py'), 'stop'], env=self.env,
                       capture_output=True, timeout=20)
        self.con.close()
        self.tmp.cleanup()

    def call(self, command, sql=None, check=True, timeout=10):
        return subprocess.run([sys.executable, str(HERE / 'adapter.py'), command],
                              env=self.env, input=sql, text=True, capture_output=True,
                              check=check, timeout=timeout)

    def queries(self):
        return [line for line in (HERE / 'queries.sql').read_text().splitlines()
                if line and not line.startswith('--')]

    def test_all_workload_queries_bind_and_execute(self):
        queries = self.queries()
        self.assertEqual(len(queries), 92)
        self.assertEqual(sum(q.startswith('UNSUPPORTED:') for q in queries), 22)
        for number, sql in enumerate(queries, 1):
            if not sql.startswith('UNSUPPORTED:'):
                with self.subTest(query=number):
                    self.con.execute(sql).fetchall()
        self.assertEqual(self.con.execute(queries[0]).fetchone(), (1,))
        self.assertEqual(self.con.execute(queries[9]).fetchone(), (2,))
        self.assertEqual(self.con.execute(queries[83]).fetchone(), (1,))

    def test_query_tags_match_reference(self):
        reference = HERE.parent / 'clickhouse' / 'queries.sql'
        tags = lambda p: re.findall(r'^-- Q\d+ task=.*$', p.read_text(), re.M)
        self.assertEqual(tags(HERE / 'queries.sql'), tags(reference))

    def test_token_and_phrase_semantics(self):
        cases = [
            ("sb_term('ERROR-error_123', 'error')", True),
            ("sb_term('terror errors', 'error')", False),
            ("sb_term(NULL, 'error')", False),
            ("sb_all('failed FAILED order', ['failed','order'])", True),
            ("sb_any('failed order', ['charge','cache'])", False),
            ("sb_phrase('FAILED-to/place,order', 'failed to place order')", True),
            ("sb_phrase('failed to place orders', 'failed to place order')", False),
            ("sb_phrase('failed to quickly place order', 'failed to place order')", False),
            ("sb_phrase(NULL, 'place order')", False),
            ("damerau_levenshtein('connetcion', 'connection')", 1),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(self.con.execute('SELECT ' + sql).fetchone()[0], expected)

    def test_regex_helpers_equal_token_list_definitions(self):
        # Each helper must select exactly the rows of the token-list test it replaces.
        bodies = ['', None, 'error', 'Error.', 'terror', 'errors', 'x-error_1', 'ERROR\nfailed', 'a\terror\n',
                  'failed to place order', 'FAILED--to  place;order!', 'failed to place orders', 'failed\nto place order',
                  'connection', 'Connections', 'conncetion', 'konnetcion', 'disconnect', 'reconnection', 'connectionid',
                  'charge charged', 'recharge', 'cache CACHE', 'ca-che', 'caxche', 'c\u00e9che', 'nation', 'station.',
                  'ﬁle errör éerror errorà', 'İerror Kconn', 'KIND error', 'kİnd-ERROR', 'xkİnd', 'connectİon', 'CONNECTİONS',
                  'post to email service expected 200 got 500',
                  'Error: failed to place order: post email service']
        self.con.execute('CREATE OR REPLACE TEMP TABLE b AS SELECT * FROM (SELECT unnest(?) AS Body)', [bodies])
        pairs = [
            ("sb_term(Body, 'error')", "list_contains(sb_tokens(Body), 'error')"),
            ("sb_any(Body, ['error', 'cache', 'order'])", "list_has_any(sb_tokens(Body), ['error', 'cache', 'order'])"),
            ("sb_any(Body, ['kind', 'failed'])", "list_has_any(sb_tokens(Body), ['kind', 'failed'])"),
            ("sb_any(Body, ['kind', 'cache', 'post', 'connection'])", "list_has_any(sb_tokens(Body), ['kind', 'cache', 'post', 'connection'])"),
            ("sb_all(Body, ['failed', 'order'])", "list_has_all(sb_tokens(Body), ['failed', 'order'])"),
            ("sb_all(Body, ['failed', 'to', 'place', 'order', 'post', 'email', 'service', 'error'])",
             "list_has_all(sb_tokens(Body), ['failed', 'to', 'place', 'order', 'post', 'email', 'service', 'error'])"),
            ("sb_phrase(Body, 'failed to place order')",
             "contains(' ' || array_to_string(sb_tokens(Body), ' ') || ' ', ' failed to place order ')"),
            ("sb_prefix(Body, 'conn')", "list_count(list_filter(sb_tokens(Body), lambda x: starts_with(x, 'conn'))) > 0"),
            ("sb_prefix(Body, 'charg')", "list_count(list_filter(sb_tokens(Body), lambda x: regexp_full_match(x, 'charg.*'))) > 0"),
            ("sb_suffix(Body, 'tion')", "list_count(list_filter(sb_tokens(Body), lambda x: ends_with(x, 'tion'))) > 0"),
            ("sb_infix(Body, 'nnec')", "list_count(list_filter(sb_tokens(Body), lambda x: strpos(x, 'nnec') > 0)) > 0"),
            ("sb_token_re(Body, '[cC]' || sb_alnum() || '[cC][hH][eE]')", "list_count(list_filter(sb_tokens(Body), lambda x: regexp_full_match(x, 'c.che'))) > 0"),
            ("sb_term(Body, 'kind')", "list_contains(sb_tokens(Body), 'kind')"),
            ("sb_phrase(Body, 'kind error')", "contains(' ' || array_to_string(sb_tokens(Body), ' ') || ' ', ' kind error ')"),
            ("sb_fuzzy(Body, 'connection', 1)", "list_count(list_filter(sb_tokens(Body), lambda x: damerau_levenshtein(x, 'connection') <= 1)) > 0"),
            ("sb_fuzzy(Body, 'connection', 2)", "list_count(list_filter(sb_tokens(Body), lambda x: damerau_levenshtein(x, 'connection') <= 2)) > 0"),
        ]
        for helper, reference in pairs:
            with self.subTest(helper=helper):
                got = self.con.execute(f'SELECT list(coalesce({helper}, false)) FROM b').fetchone()[0]
                want = self.con.execute(f'SELECT list(coalesce({reference}, false)) FROM b').fetchone()[0]
                self.assertEqual(got, want)
                self.assertTrue(any(want), 'case set must exercise a match')
        with self.assertRaises(duckdb.Error):
            self.con.execute("SELECT sb_all('a', ['a','b','c','d','e','f','g','h','i'])").fetchall()

    def test_lower_maps_only_these_characters_to_token_characters(self):
        # The helpers match case classes on Body instead of lower(Body). That is exact only if
        # lower() turns no other code point into [a-z0-9] and never changes a character count.
        rows = self.con.execute("""
            SELECT list(i ORDER BY i) FILTER (WHERE regexp_matches(lower(chr(i::INT)), '[a-z0-9]')
                                              AND NOT regexp_matches(chr(i::INT), '^[A-Za-z0-9]$')),
                   count(*) FILTER (WHERE length(lower(chr(i::INT))) != 1)
            FROM (SELECT range AS i FROM range(1, 1114112) WHERE NOT (range BETWEEN 55296 AND 57343))""").fetchone()
        self.assertEqual(rows, ([0x130, 0x212A], 0))

    def test_fuzzy_ball_regex_is_the_distance(self):
        # Edits of 'connection' (and of other words): the regex accepts exactly the tokens
        # within the distance, also upper-cased inside other text.
        import random
        rnd = random.Random(7); alphabet = 'connectiabz09'; tokens = set()
        for _ in range(4000):
            s = list(rnd.choice(['connection', 'conn', 'x']))
            for _ in range(rnd.randint(0, 4)):
                op, i = rnd.randint(0, 3), rnd.randrange(len(s) + 1)
                if op == 0 and i < len(s): del s[i]
                elif op == 1 and i < len(s): s[i] = rnd.choice(alphabet)
                elif op == 2: s.insert(i, rnd.choice(alphabet))
                elif op == 3 and i + 1 < len(s): s[i], s[i + 1] = s[i + 1], s[i]
            tokens.add(''.join(s))
        self.con.execute('CREATE OR REPLACE TEMP TABLE tk AS SELECT unnest(?) AS x', [sorted(t for t in tokens if t)])
        self.assertEqual(self.con.execute("SELECT damerau_levenshtein('ca', 'abc')").fetchone()[0], 2)
        for d in (1, 2):
            with self.subTest(distance=d):
                within, wrong, wrong_upper = self.con.execute(f"""
                    SELECT count(*) FILTER (WHERE damerau_levenshtein(x, 'connection') <= {d}),
                           count(*) FILTER (WHERE (damerau_levenshtein(x, 'connection') <= {d}) != sb_re(x, sb_ball_re('connection', {d}))),
                           count(*) FILTER (WHERE (damerau_levenshtein(x, 'connection') <= {d}) != sb_re('Foo-' || upper(x) || '.bar', sb_ball_re('connection', {d})))
                    FROM tk""").fetchone()
                self.assertGreater(within, 50)
                self.assertEqual((wrong, wrong_upper), (0, 0))
        with self.assertRaises(duckdb.Error):
            self.con.execute("SELECT count(*) FROM tk WHERE sb_fuzzy(x, 'connection', 3)").fetchall()

    def test_fuzzy_transpositions_crossing_prefilter_boundaries(self):
        # Chunk-substring prefilters are NOT sound for Damerau distance: a
        # transposition can damage two adjacent chunks in a single edit.
        self.con.execute("CREATE OR REPLACE VIEW otel_logs AS SELECT * FROM (VALUES ('conncetion', 'a'), ('konnetcion', 'b')) t(Body, ScopeName)")
        queries = self.queries()
        self.assertEqual(self.con.execute(queries[21]).fetchone(), (1,))  # Q22
        self.assertEqual(self.con.execute(queries[22]).fetchone(), (2,))  # Q23
        self.assertEqual(self.con.execute(queries[58]).fetchall(), [('a', 1)])  # Q59

    def test_timestamp_nanoseconds_and_typed_parquet(self):
        self.assertEqual(self.con.execute('SELECT min(epoch_ns(Timestamp)) FROM otel_logs').fetchone()[0], self.base + 1)
        typed = self.root / 'typed.parquet'
        self.con.execute('COPY (SELECT * FROM otel_logs) TO ? (FORMAT PARQUET)', [str(typed)])
        adapter.prepare(self.con, dict(self.config, source=str(typed)))
        self.assertEqual(self.con.execute('SELECT min(epoch_ns(Timestamp)) FROM otel_logs').fetchone()[0], self.base + 1)

    def test_timezone_aware_parquet_uses_utc_and_fetches_timestamps(self):
        # The real corpus is timestamp[ns, tz=UTC], not the numeric fixture.
        # Exercise Python datetime conversion (requires pytz), window literal
        # interpretation, and minute grouping through the actual IPC worker.
        typed = self.root / 'zoned.parquet'
        self.con.execute("COPY (SELECT * REPLACE (TIMESTAMPTZ '2025-09-23 00:15:00+00' AS Timestamp) FROM fixture) TO ? (FORMAT PARQUET)", [str(typed)])
        self.env['DUCKDB_PARQUET_PATH'] = str(typed)
        self.env['TZ'] = 'America/Los_Angeles'
        self.call('start')
        self.call('load')
        self.assertEqual(self.call('query', "SELECT current_setting('TimeZone')").stdout.strip(), 'UTC')
        self.assertEqual(self.call('query', "SELECT count(*) FROM otel_logs WHERE Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00'").stdout.strip(), '8')
        for index in (60, 69):  # Q61 minute aggregation, Q70 timestamp projection.
            result = self.call('query', self.queries()[index])
            self.assertIn('2025-09-23 00:15:00+00:00', result.stdout)
            self.assertIn('SEARCHBENCH_ROWS=1', result.stderr)

    def test_no_materialization_and_multiple_files_size(self):
        copy = self.parquet.with_name('part_001.parquet')
        shutil.copyfile(self.parquet, copy)
        self.env['DUCKDB_PARQUET_PATH'] = str(self.parquet.parent / 'part_*.parquet')
        self.call('start')
        self.call('load')
        self.assertEqual(int(self.call('data-size').stdout), self.parquet.stat().st_size * 2)
        self.assertEqual(self.call('query', 'SELECT count(*) FROM otel_logs').stdout.strip(), '16')
        copy.unlink()
        self.assertEqual(self.call('query', 'SELECT count(*) FROM otel_logs').stdout.strip(), '8')

    def test_worker_lifecycle_timing_and_failures(self):
        self.assertNotEqual(self.call('check', check=False).returncode, 0)
        self.call('start')
        self.call('start')
        self.call('check')
        self.call('load')
        for _ in range(3):
            result = self.call('query', "SELECT Body FROM otel_logs WHERE sb_term(Body, 'error')")
            self.assertIn('ERROR failed', result.stdout)
            self.assertEqual(result.stderr.splitlines()[-2], 'SEARCHBENCH_ROWS=1')
            self.assertGreater(float(result.stderr.splitlines()[-1]), 0)
        failed = self.call('query', 'SELECT nonexistent FROM otel_logs', check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertNotRegex(failed.stderr.splitlines()[-1], r'^\d+\.\d+$')
        self.assertEqual(self.call('query', 'SELECT count(*) FROM otel_logs').stdout.strip(), '8')
        self.env['DUCKDB_THREADS'] = '1'
        self.assertNotEqual(self.call('check', check=False).returncode, 0)
        self.assertNotEqual(self.call('start', check=False).returncode, 0)
        self.call('stop')  # Stop works even after configuration changes.
        self.call('stop')
        self.assertNotEqual(self.call('check', check=False).returncode, 0)

    def test_extra_settings(self):
        self.assertEqual(adapter.extra_settings(' preserve_insertion_order=false; ;threads = 4'),
                         [['preserve_insertion_order', 'false'], ['threads', '4']])
        for bad in ['nosign', 'x=1; DROP TABLE t', "x='a';y", 'Bad-Name=1']:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                adapter.extra_settings(bad)

    def test_unsupported_without_worker(self):
        result = self.call('query', 'UNSUPPORTED: BM25')
        self.assertEqual(result.stdout, '')
        self.assertEqual(result.stderr.strip(), 'UNSUPPORTED: BM25')

    def test_timeout_disconnect_cancels_query(self):
        self.call('start')
        self.call('load')
        child = subprocess.Popen([sys.executable, str(HERE / 'adapter.py'), 'query'],
                                 env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            child.stdin.write('SELECT sum(sqrt(i)) FROM range(1000000000000) t(i)')
            child.stdin.close()
            time.sleep(0.3)
            self.assertIsNone(child.poll())
            child.terminate()  # Exactly what the shared runner's timeout does.
            child.wait(timeout=3)
            result = self.call('query', 'SELECT 42', timeout=5)
            self.assertEqual(result.stdout.strip(), '42')
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()
            child.stderr.close()

    def test_stop_rejects_queued_queries_and_interrupts_active_work(self):
        self.call('start')
        self.call('load')
        clients = []
        try:
            for _ in range(2):
                client = subprocess.Popen([sys.executable, str(HERE / 'adapter.py'), 'query'],
                                          env=self.env, stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                clients.append(client)
                client.stdin.write('SELECT sum(sqrt(i)) FROM range(1000000000000) t(i)')
                client.stdin.close()
                time.sleep(0.3)
                self.assertIsNone(client.poll())
            self.call('stop', timeout=5)
            for client in clients:
                self.assertNotEqual(client.wait(timeout=3), 0)
            self.assertNotEqual(self.call('check', check=False).returncode, 0)
            self.assertFalse((self.root / 'worker/worker.sock').exists())
        finally:
            for client in clients:
                if client.poll() is None:
                    client.kill()
                    client.wait()
                client.stdout.close()
                client.stderr.close()

    def test_missing_dataset_fails_load_not_start(self):
        self.env['DUCKDB_PARQUET_PATH'] = str(self.root / 'absent.parquet')
        self.call('start')  # Driver starts before its download step.
        self.call('check')
        self.assertNotEqual(self.call('load', check=False).returncode, 0)

    def test_s3_range_reads_auth_and_object_size(self):
        installed = self.con.execute("SELECT count(*) FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'aws') AND installed").fetchone()[0]
        if installed != 2:
            self.skipTest('install extensions first with: python adapter.py extensions')
        payload = self.parquet.read_bytes()
        requests = []

        class S3Fixture(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_HEAD(self):
                requests.append(('HEAD', self.path, None, self.headers.get('Authorization')))
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.send_header('Accept-Ranges', 'bytes')
                self.end_headers()

            def do_GET(self):
                range_header = self.headers.get('Range')
                requests.append(('GET', self.path, range_header, self.headers.get('Authorization')))
                if 'list-type=' in urlparse(self.path).query:
                    listing = ('<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                               '<Name>fixture</Name><IsTruncated>false</IsTruncated><Contents>'
                               '<Key>data/part_000.parquet</Key><Size>' + str(len(payload)) + '</Size>'
                               '</Contents></ListBucketResult>').encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(listing)))
                    self.end_headers()
                    self.wfile.write(listing)
                    return
                if urlparse(self.path).path != '/fixture/data/part_000.parquet':
                    self.send_error(404)
                    return
                start, end = 0, len(payload) - 1
                if range_header:
                    start, end = map(int, range_header.removeprefix('bytes=').split('-'))
                    end = min(end, len(payload) - 1)
                self.send_response(206 if range_header else 200)
                self.send_header('Content-Length', str(end - start + 1))
                self.send_header('Content-Range', f'bytes {start}-{end}/{len(payload)}')
                self.end_headers()
                self.wfile.write(payload[start:end + 1])

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), S3Fixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        config = dict(self.config, source='s3://fixture/data/part_*.parquet',
                      s3_endpoint='127.0.0.1:' + str(server.server_port),
                      s3_url_style='path', s3_use_ssl='false')
        con = None
        try:
            for auth in ('unsigned', 'credential_chain'):
                with self.subTest(auth=auth):
                    requests.clear()
                    # Test signing without contacting AWS or using real credentials.
                    env = dict(self.env, AWS_ACCESS_KEY_ID='fixture-key',
                               AWS_SECRET_ACCESS_KEY='fixture-secret', AWS_SESSION_TOKEN='',
                               AWS_EC2_METADATA_DISABLED='true')
                    with mock.patch.dict(os.environ, env):
                        con = adapter.connect_database(dict(config, s3_auth=auth))
                    adapter.prepare(con, config)
                    self.assertEqual(con.execute("SELECT count(*) FROM otel_logs WHERE sb_term(Body, 'error')").fetchone(), (1,))
                    size = con.execute('SELECT sum(file_size_bytes) FROM parquet_file_metadata(?)', [config['source']]).fetchone()[0]
                    self.assertEqual(size, len(payload))
                    self.assertTrue(any(r[2] for r in requests), 'expected ranged object reads')
                    self.assertTrue(any('list-type=' in r[1] for r in requests), 'expected S3 glob listing')
                    self.assertEqual(any(r[3] for r in requests), auth == 'credential_chain')
                    con.close()
                    con = None
        finally:
            if con:
                con.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_s3_entrypoint_skips_download_and_sets_distinct_label(self):
        checkout = self.root / 'entrypoint'
        engine = checkout / 'duckdb-parquet'
        lib = checkout / 'lib'
        engine.mkdir(parents=True)
        lib.mkdir()
        shutil.copyfile(HERE / 'benchmark.sh', engine / 'benchmark.sh')
        stub = lib / 'benchmark.sh'
        stub.write_text('#!/usr/bin/env bash\nexec "$DUCKDB_PYTHON" -c '
                        "'import os,json; print(json.dumps(dict(os.environ)))'\n")
        stub.chmod(0o755)
        env = dict(self.env, DUCKDB_PARQUET_PATH='s3://fixture/data/part_*.parquet')
        result = subprocess.run(['bash', './benchmark.sh'], cwd=engine, env=env,
                                check=True, capture_output=True, text=True)
        forwarded = json.loads(result.stdout)
        self.assertEqual(forwarded['SEARCHBENCH_SKIP_DOWNLOAD'], 'yes')
        self.assertEqual(forwarded['SEARCHBENCH_VERSION'], 's3-no-index')
        self.assertEqual(forwarded['ENGINE_NAME'], 'DuckDB-Parquet')
        self.assertEqual(forwarded['DUCKDB_PARQUET_PATH'], env['DUCKDB_PARQUET_PATH'])
        self.assertTrue(forwarded['SEARCHBENCH_DATA_DIR'])

    @unittest.skipUnless(shutil.which('jq'), 'shared runner requires jq')
    def test_shared_runner_results_in_isolated_checkout(self):
        checkout = self.root / 'checkout'
        engine = checkout / 'duckdb-parquet'
        shutil.copytree(HERE, engine, ignore=shutil.ignore_patterns('.venv', '__pycache__', 'results', '.tmp'))
        # SEARCHBENCH_RESTART=no still flushes caches in the shared runner!
        # Never execute that host-wide operation in tests, even as root. Copy
        # the driver and replace ONLY its cache hook; also avoid global sync.
        lib = checkout / 'lib'
        lib.mkdir()
        driver = (HERE.parent / 'lib/benchmark.sh').read_text()
        driver, replacements = re.subn(r'flush_caches\(\) \{.*?\n\}',
                                       'flush_caches() { :; }\nsync() { :; }',
                                       driver, flags=re.S)
        self.assertEqual(replacements, 1, 'runner cache hook changed; refusing unsafe test')
        (lib / 'benchmark.sh').write_text(driver)
        (lib / 'benchmark.sh').chmod(0o755)
        # Three representative rows, including the unsupported/null contract.
        queries = self.queries()
        small = engine / 'sample.sql'
        small.write_text('-- Q01 task=count filter=term freq=hi\n' + queries[0] + '\n'
                         '-- Q33 task=top_k filter=term freq=hi\n' + queries[32] + '\n'
                         '-- Q84 task=join filter=term freq=hi\n' + queries[83] + '\n')
        env = dict(self.env, SEARCHBENCH_QUERIES='sample.sql', SEARCHBENCH_DATASET='otel_logs_100k',
                   SEARCHBENCH_TRIES='3', SEARCHBENCH_RESTART='no', SEARCHBENCH_QUERY_TIMEOUT='0')
        result = subprocess.run(['bash', './benchmark.sh'], cwd=engine, env=env,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(next((engine / 'results').glob('*.json')).read_text())
        self.assertEqual(output['system'], 'DuckDB-Parquet (local-no-index)')
        self.assertEqual(output['result'][1], [None, None, None])
        self.assertTrue(all(isinstance(x, (int, float)) for x in output['result'][0]))
        self.assertEqual([q['id'] for q in output['query_tags']], ['Q01', 'Q33', 'Q84'])
        self.assertIn('threads=2, memory_limit=256MB', output['version'])

        # Exercise --index too when GNU date is available (the shared runner
        # needs %N). All writes stay in the isolated checkout; no cache drops.
        date = shutil.which('gdate') or shutil.which('date')
        stamp = subprocess.check_output([date, '+%s.%N'], text=True).strip()
        if re.fullmatch(r'\d+\.\d+', stamp):
            tools = self.root / 'tools'
            tools.mkdir()
            (tools / 'date').symlink_to(date)
            env['PATH'] = str(tools) + os.pathsep + env['PATH']
            result = subprocess.run(['bash', './benchmark.sh', '--index'], cwd=engine, env=env,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            load_path = engine / 'results/load/otel_logs_100k.local-no-index.json'
            record = json.loads(load_path.read_text())
            self.assertEqual(record['data_size'], self.parquet.stat().st_size)
            self.assertGreaterEqual(record['load_time'], 0)
            # A subsequent query-only run must inherit that variant's record.
            result = subprocess.run(['bash', './benchmark.sh'], cwd=engine, env=env,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            output = json.loads(next((engine / 'results').glob('*.json')).read_text())
            self.assertEqual(output['data_size'], record['data_size'])
            self.assertEqual(output['load_time'], record['load_time'])


if __name__ == '__main__':
    unittest.main()
