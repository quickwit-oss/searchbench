#!/usr/bin/env python3
"""Small local IPC worker: one in-memory DuckDB connection, direct Parquet scans."""
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import select
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time

HERE = Path(__file__).resolve().parent


def configuration():
    source = os.environ.get('DUCKDB_PARQUET_PATH')
    if not source:
        root = os.environ.get('SEARCHBENCH_DATA_DIR')
        if not root:
            raise ValueError('set SEARCHBENCH_DATA_DIR or DUCKDB_PARQUET_PATH')
        source = str(Path(root).resolve() / 'part_*.parquet')
    elif not source.startswith('s3://'):
        source = str(Path(source).resolve())
    return {
        'source': source,
        'threads': int(os.environ.get('DUCKDB_THREADS', os.cpu_count() or 1)),
        'memory_limit': os.environ.get('DUCKDB_MEMORY_LIMIT', '4GB'),
        's3_auth': os.environ.get('DUCKDB_S3_AUTH', 'credential_chain'),
        's3_region': os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')),
        's3_endpoint': os.environ.get('DUCKDB_S3_ENDPOINT'),
        's3_url_style': os.environ.get('DUCKDB_S3_URL_STYLE', 'vhost'),
        's3_use_ssl': os.environ.get('DUCKDB_S3_USE_SSL', 'true'),
        'external_file_cache': os.environ.get('DUCKDB_EXTERNAL_FILE_CACHE', 'false').lower(),
        'http_stats': bool(os.environ.get('DUCKDB_HTTP_STATS_LOG')),
        'metadata_cache': os.environ.get('DUCKDB_METADATA_CACHE', 'true').lower(),
        # DUCKDB_SORT_FROM: ./load writes the queried file (DUCKDB_PARQUET_PATH) as a copy
        # of these files sorted by DUCKDB_SORT_BY (see materialize()).
        'sort_from': sort_source(os.environ.get('DUCKDB_SORT_FROM')),
        'sort_by': os.environ.get('DUCKDB_SORT_BY', 'ServiceName, Timestamp'),
        'sort_row_group_size': int(os.environ.get('DUCKDB_SORT_ROW_GROUP_SIZE', '122880')),
        'settings': extra_settings(os.environ.get('DUCKDB_SETTINGS', '')),
    }


def extra_settings(text):
    """DUCKDB_SETTINGS='name=value;name=value': more DuckDB settings for the worker session,
    e.g. preserve_insertion_order=false. Names must be DuckDB settings; values are literals."""
    out = []
    for item in filter(None, (x.strip() for x in text.split(';'))):
        name, sep, value = item.partition('=')
        name, value = name.strip(), value.strip()
        if not sep or not re.fullmatch(r'[a-z_][a-z0-9_]*', name) or not re.fullmatch(r"[A-Za-z0-9_.' -]+", value):
            raise ValueError(f'DUCKDB_SETTINGS item must be name=value: {item!r}')
        out.append([name, value])
    return out


def sort_source(path):
    if not path or path.startswith('s3://'):
        return path or None
    return str(Path(path).resolve())


def state_dir():
    # Short path: AF_UNIX has a 104-byte limit on macOS. Separate checkout state.
    key = hashlib.sha256(str(HERE).encode()).hexdigest()[:12]
    path = Path(os.environ.get('DUCKDB_STATE_DIR', f'/tmp/searchbench-duckdb-{os.getuid()}-{key}'))
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink() or path.stat().st_uid != os.getuid():
        raise ValueError(f'unsafe worker state directory: {path}')
    path.chmod(0o700)
    return path.resolve()


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect_database(config):
    import duckdb
    if config['threads'] < 1:
        raise ValueError('DUCKDB_THREADS must be positive')
    cache = config.get('external_file_cache', 'false')
    if cache not in {'true', 'false'}:
        raise ValueError('DUCKDB_EXTERNAL_FILE_CACHE must be true or false')
    con = duckdb.connect(config={
        'threads': config['threads'], 'memory_limit': config['memory_limit'],
        'temp_directory': str(state_dir() / 'spill'),
    })
    # Corpus timestamps are UTC; unqualified window literals and date_trunc
    # must not use the host's timezone. Also makes returned datetimes portable.
    con.execute("SET TimeZone = 'UTC'")
    # DuckDB caches data read from remote files in memory (on by default since 1.3).
    # Off by default here: on S3 every try reads from S3, like the other engines'
    # S3 runs (no ClickHouse filesystem cache, no Quickwit split cache, SereneDB's
    # external file cache off). DUCKDB_EXTERNAL_FILE_CACHE=true turns it back on.
    con.execute(f'SET enable_external_file_cache = {cache}')
    # Metadata caches (off by default in DuckDB): Parquet footers and HTTP object metadata
    # stay in memory between queries, as every other engine keeps its metadata (Quickwit's
    # split footer cache, ClickHouse's marks and index headers). Data is not cached.
    meta = config.get('metadata_cache', 'true')
    if meta not in {'true', 'false'}:
        raise ValueError('DUCKDB_METADATA_CACHE must be true or false')
    con.execute(f'SET parquet_metadata_cache = {meta}')
    con.execute(f'SET enable_http_metadata_cache = {meta}')
    if config['source'].startswith('s3://'):
        con.execute('LOAD httpfs')
        options = ['TYPE s3', 'REGION ' + sql_literal(config['s3_region'])]
        if config['s3_auth'] == 'credential_chain':
            con.execute('LOAD aws')
            options.append('PROVIDER credential_chain')
        elif config['s3_auth'] == 'unsigned':
            # httpfs also reads legacy AWS env credentials. Explicit empty keys
            # prevent accidental signing even when those variables are present.
            options += ['PROVIDER config', "KEY_ID ''", "SECRET ''", "SESSION_TOKEN ''"]
        else:
            raise ValueError('DUCKDB_S3_AUTH must be credential_chain or unsigned')
        if config.get('s3_endpoint'):
            options.append('ENDPOINT ' + sql_literal(config['s3_endpoint']))
        style = config.get('s3_url_style', 'vhost')
        ssl = config.get('s3_use_ssl', 'true').lower()
        if style not in {'vhost', 'path'} or ssl not in {'true', 'false'}:
            raise ValueError('S3 URL style must be vhost/path and USE_SSL must be true/false')
        options += ['URL_STYLE ' + sql_literal(style), 'USE_SSL ' + ssl]
        con.execute('CREATE SECRET (' + ', '.join(options) + ')')
    if config.get('http_stats'):
        # DUCKDB_HTTP_STATS_LOG: log HTTP requests in memory to count GETs and bytes per
        # query (see http_stats()). Entries carry signed request headers; only counts
        # leave the worker.
        con.execute("CALL enable_logging('HTTP')")
    for name, value in config.get('settings', []):
        con.execute(f'SET {name} = {value}')
    con.execute((HERE / 'macros.sql').read_text())
    return con


SORT_KEY = re.compile(r'^\s*[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?(\s*,\s*[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?)*\s*$', re.I)


def materialize(con, config):
    """Write the queried file as a sorted copy of DUCKDB_SORT_FROM: one Parquet file, ZSTD,
    DUCKDB_SORT_ROW_GROUP_SIZE rows per row group (DuckDB's default 122880), with DuckDB's
    row-group statistics. Run by ./load, so the sort counts in load_time, like the
    other engines' sorted loads (ClickHouse and DEN ORDER BY (ServiceName, Timestamp))."""
    target = config['source']
    if any(c in target for c in '*?['):
        raise ValueError('with DUCKDB_SORT_FROM, DUCKDB_PARQUET_PATH must be one file, not a glob')
    if not SORT_KEY.match(config['sort_by']):
        raise ValueError('DUCKDB_SORT_BY must be a list of column names (optionally ASC/DESC)')
    if not target.startswith('s3://'):
        Path(target).parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM read_parquet({sql_literal(config['sort_from'])}) ORDER BY {config['sort_by']}) "
                f"TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {int(config['sort_row_group_size'])})")


def prepare(con, config):
    """Bind only metadata; never COPY/INSERT or scan/tokenize the corpus here."""
    source = sql_literal(config['source'])
    columns = con.execute(f'DESCRIBE SELECT * FROM read_parquet({source})').fetchall()
    types = {name.lower(): dtype for name, dtype, *_ in columns}
    required = {'timestamp', 'body', 'servicename', 'severitytext', 'severitynumber', 'scopename', 'traceid'}
    missing = required - types.keys()
    if missing:
        raise ValueError('missing corpus columns: ' + ', '.join(sorted(missing)))
    timestamp_type = types['timestamp']
    if timestamp_type in {'BIGINT', 'UBIGINT', 'HUGEINT'}:
        # Also accept epoch-nanosecond datasets without reducing precision.
        projection = '* REPLACE (make_timestamp_ns(CAST(Timestamp AS BIGINT)) AS Timestamp)'
    elif timestamp_type.startswith('TIMESTAMP'):
        projection = '*'
    else:
        raise ValueError(f'unsupported Timestamp type {timestamp_type}; expected epoch ns or timestamp')
    con.execute(f'CREATE OR REPLACE VIEW otel_logs AS SELECT {projection} FROM read_parquet({source})')


def http_stats(con):
    """GET requests and response bytes since the last call (then clears the log): the
    query, plus the view setup (metadata) when it was the first after a restart."""
    gets, nbytes, total = con.execute("""
        SELECT count(*) FILTER (WHERE request.type = 'GET'),
               coalesce(sum(TRY_CAST(response.headers['Content-Length'] AS BIGINT))
                        FILTER (WHERE request.type = 'GET'), 0),
               count(*)
        FROM duckdb_logs_parsed('HTTP')""").fetchone()
    con.execute('CALL truncate_duckdb_logs()')
    return {'http_gets': gets, 'http_bytes': nbytes, 'http_requests': total}


def send(sock, message):
    sock.sendall((json.dumps(message, default=str) + '\n').encode())


class Worker(socketserver.ThreadingUnixStreamServer):
    daemon_threads = False

    def __init__(self, path, config):
        self.config = config
        self.con = connect_database(config)
        self.ready = False
        self.query_lock = threading.Lock()
        self.stopping = threading.Event()
        super().__init__(str(path), Handler)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        worker = self.server
        try:
            # A connected but stalled peer must not hold shutdown indefinitely.
            # This bounds socket I/O, not the duration of DuckDB execution.
            self.request.settimeout(5)
            request = json.loads(self.rfile.readline())
            command = request['command']
            if command == 'check':
                send(self.request, {'config': worker.config})
                return
            if command == 'stop':
                worker.stopping.set()
                worker.con.interrupt()
                send(self.request, {'ok': True})
                threading.Thread(target=worker.shutdown, daemon=True).start()
                return
            if request.get('config') != worker.config:
                raise ValueError('worker configuration changed; run ./stop, then ./start')
            if command not in {'load', 'data-size', 'query'}:
                raise ValueError(f'unknown command: {command}')
            with worker.query_lock:
                if worker.stopping.is_set():
                    raise RuntimeError('worker is stopping')
                done = threading.Event()
                cancelled = threading.Event()

                def cancel_on_disconnect():
                    while not done.wait(0.02):
                        if worker.stopping.is_set():
                            cancelled.set()
                            worker.con.interrupt()
                            continue
                        readable, _, _ = select.select([self.request], [], [], 0)
                        if readable and not self.request.recv(1, socket.MSG_PEEK):
                            cancelled.set()
                            # Repeat until the handler exits: avoids an interrupt
                            # just before execute() missing the following query.
                            worker.con.interrupt()

                watcher = threading.Thread(target=cancel_on_disconnect, daemon=True)
                watcher.start()
                try:
                    # The first query after a (re)start binds the view, i.e. reads the
                    # Parquet metadata. That is part of a cold query (the other engines'
                    # cold tries open their metadata too), so it is timed with it.
                    start = time.perf_counter()
                    if command == 'load' and worker.config.get('sort_from'):
                        materialize(worker.con, worker.config)
                    if command == 'load' or not worker.ready:
                        prepare(worker.con, worker.config)
                        worker.ready = True
                    if cancelled.is_set():
                        return
                    if command == 'load':
                        send(self.request, {'ok': True})
                    elif command == 'data-size':
                        # Includes full Parquet object sizes, NOT index bytes or
                        # bytes transferred. Works for both local files and S3.
                        size = worker.con.execute(
                            'SELECT sum(file_size_bytes) FROM parquet_file_metadata(?)',
                            [worker.config['source']],
                        ).fetchone()[0]
                        if not size:
                            raise ValueError('empty Parquet dataset')
                        send(self.request, {'size': size})
                    else:
                        # Excludes process startup and IPC connect; includes view setup
                        # (metadata) on the first query after a restart, execution,
                        # fetching, and row serialization.
                        cursor = worker.con.execute(request['sql'])
                        count = 0
                        while rows := cursor.fetchmany(1024):
                            count += len(rows)
                            send(self.request, {'rows': rows})
                        elapsed = time.perf_counter() - start
                        message = {'elapsed': elapsed, 'row_count': count}
                        if worker.config.get('http_stats'):
                            message.update(http_stats(worker.con))
                        send(self.request, message)
                finally:
                    done.set()
                    watcher.join()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            try:
                send(self.request, {'error': str(exc)})
            except OSError:
                pass


def rpc(command, config=None, sql=None, timeout=None):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(state_dir() / 'worker.sock'))
        send(sock, {'command': command, 'config': config, 'sql': sql})
        with sock.makefile('r') as stream:
            for line in stream:
                message = json.loads(line)
                if 'error' in message:
                    raise RuntimeError(message['error'])
                yield message


def ping():
    return next(rpc('check', timeout=1))


def serve():
    state = state_dir()
    # The lock prevents concurrent starts from unlinking a live worker socket.
    with (state / 'worker.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # ./stop reads the PID to wait for (or kill) this process.
        lock.write(str(os.getpid())); lock.flush()
        path = state / 'worker.sock'
        path.unlink(missing_ok=True)
        worker = Worker(path, configuration())
        try:
            worker.serve_forever(poll_interval=0.05)
        finally:
            worker.server_close()
            worker.con.close()
            path.unlink(missing_ok=True)


def worker_running():
    """PID of the worker holding the lock, or None once it is released (process exited)."""
    path = state_dir() / 'worker.lock'
    if not path.exists():
        return None
    with path.open('r+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pid = lock.read().strip()
            return int(pid) if pid.isdigit() else -1
        fcntl.flock(lock, fcntl.LOCK_UN)
        return None


def stop():
    """Stop the worker and return only once its process is gone: a query interrupted at the
    runner's cap can take a while to unwind, and a new worker cannot start (lock held) until
    the old process exits. Graceful stop first; SIGKILL after DUCKDB_STOP_TIMEOUT (30 s)."""
    if worker_running() is None:
        (state_dir() / 'worker.sock').unlink(missing_ok=True)
        return
    try:
        list(rpc('stop', timeout=5))
    except (OSError, StopIteration, RuntimeError):
        pass
    deadline = time.monotonic() + float(os.environ.get('DUCKDB_STOP_TIMEOUT', '30'))
    while (pid := worker_running()) is not None:
        if time.monotonic() >= deadline:
            if pid > 0:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            kill_deadline = time.monotonic() + 15
            while worker_running() is not None:
                if time.monotonic() >= kill_deadline:
                    raise RuntimeError(f'worker {pid} did not exit after SIGKILL')
                time.sleep(0.05)
            break
        time.sleep(0.05)
    (state_dir() / 'worker.sock').unlink(missing_ok=True)


def start():
    config = configuration()
    try:
        existing = ping()
    except (OSError, StopIteration):
        existing = None
    if existing:
        if existing['config'] != config:
            raise ValueError('worker configuration changed; run ./stop, then ./start')
        return
    state = state_dir()
    with (state / 'worker.log').open('a') as log:
        child = subprocess.Popen([sys.executable, str(HERE / 'adapter.py'), 'serve'],
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                 start_new_session=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if ping()['config'] == config:
                return
        except (OSError, StopIteration):
            pass
        if child.poll() is not None:
            raise RuntimeError(f'worker failed to start; see {state / "worker.log"}')
        time.sleep(0.05)
    child.terminate()
    raise RuntimeError(f'worker startup timed out; see {state / "worker.log"}')


def main():
    command = sys.argv[1]
    if command == 'serve':
        serve()
    elif command == 'start':
        start()
    elif command == 'stop':
        stop()
    elif command == 'check':
        if ping()['config'] != configuration():
            raise ValueError('worker configuration changed; run ./stop, then ./start')
    elif command == 'version':
        import duckdb
        threads = os.environ.get('DUCKDB_THREADS', str(os.cpu_count() or 1))
        memory = os.environ.get('DUCKDB_MEMORY_LIMIT', '4GB')
        cache = os.environ.get('DUCKDB_EXTERNAL_FILE_CACHE', 'false').lower()
        meta = os.environ.get('DUCKDB_METADATA_CACHE', 'true').lower()
        sort = f", sorted_by={os.environ.get('DUCKDB_SORT_BY', 'ServiceName, Timestamp')}" if os.environ.get('DUCKDB_SORT_FROM') else ''
        extra = ''.join(f', {n}={v}' for n, v in extra_settings(os.environ.get('DUCKDB_SETTINGS', '')))
        print(f'{duckdb.__version__} (threads={threads}, memory_limit={memory}, external_file_cache={cache}, metadata_cache={meta}{sort}{extra})')
    elif command == 'extensions':
        import duckdb
        with duckdb.connect() as con:
            con.execute('INSTALL httpfs')
            con.execute('INSTALL aws')
    elif command in {'load', 'data-size', 'query'}:
        sql = sys.stdin.read() if command == 'query' else None
        if sql and sql.lstrip().startswith('UNSUPPORTED:'):
            print(sql.strip(), file=sys.stderr)
            return
        finished = False
        for message in rpc(command, configuration(), sql):
            if 'rows' in message:
                for row in message['rows']:
                    # Escape embedded newlines/tabs so one result row = one line.
                    print('\t'.join('NULL' if v is None else str(v).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r') for v in row))
            else:
                finished = True
                if 'size' in message:
                    print(message['size'])
                if 'elapsed' in message:
                    print(f'SEARCHBENCH_ROWS={message["row_count"]}', file=sys.stderr)
                    print(f'{message["elapsed"]:.9f}', file=sys.stderr)
                    if 'http_gets' in message and os.environ.get('DUCKDB_HTTP_STATS_LOG'):
                        with open(os.environ['DUCKDB_HTTP_STATS_LOG'], 'a') as log:
                            log.write(json.dumps({'query_sha1': hashlib.sha1(sql.encode()).hexdigest()[:12],
                                                  'time': message['elapsed'], 'gets': message['http_gets'],
                                                  'bytes': message['http_bytes'], 'requests': message['http_requests']}) + '\n')
        if not finished:
            raise RuntimeError('worker disconnected before completing the request')
    else:
        raise ValueError(f'unknown command: {command}')


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        print(f'duckdb-parquet: {exc}', file=sys.stderr)
        sys.exit(1)
