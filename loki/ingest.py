"""Bounded Parquet -> Loki push ingestion. No timestamp shifting or truncation.

Every source row gets a unique physical JSON frame as well as an unindexed
metadata identity. Metadata identity alone does NOT prevent cross-chunk sample
deduplication in stock Loki. The identity
is stable across attempts; ambiguous/partial HTTP failures abort, never replay
an entire file. A store-only count gate must pass before a load is publishable.
"""
import json
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import requests
from encoding import (ENCODING, ENCODING_FLAG, MAX_LINE_BYTES, MAX_REQUEST_BYTES,
                      RESERVED, encode_frame, frame_overhead, validate_contract)


def metadata_value(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def entry(row, timestamp_ns, identity):
    if type(timestamp_ns) is not int or not -(2**63) <= timestamp_ns < 2**63 or not isinstance(row.get("Body"), str):
        raise ValueError("Timestamp and string Body are required; refusing to drop a document")
    service, severity = row.get("ServiceName"), row.get("SeverityText")
    if not isinstance(service, str) or not isinstance(severity, str):
        raise ValueError("ServiceName and SeverityText must be strings")
    labels = {"dataset": "otel_logs", "service_name": service, "severity_text": severity}
    metadata = {k: metadata_value(v) for k, v in row.items()
                if k not in ("Timestamp", "TimestampTime", "Body", "ServiceName", "SeverityText") and v is not None}
    # TimestampTime is a redundant corpus helper, not one of the 15 log fields.
    if RESERVED.intersection(row):
        raise ValueError("source field collides with reserved adapter metadata")
    metadata["sb_row_id"] = identity
    return labels, [str(timestamp_ns), encode_frame(row["Body"], identity), metadata]


def request_budget():
    limit = int(os.environ.get("LOKI_MAX_REQUEST_BYTES", MAX_REQUEST_BYTES))
    if not 1024 <= limit <= MAX_REQUEST_BYTES:
        raise ValueError("LOKI_MAX_REQUEST_BYTES must be between 1024 and 3145728")
    return limit


def internal_row_bound(labels, value):
    # Upper bound for protobuf Entry + repeated Stream framing, including
    # timestamp, length varints and repeated label overhead per row. The 32
    # bytes per metadata pair and 128 per row exceed all tag/varint overhead
    # for requests <=3 MiB. This deliberately overcounts shared stream labels.
    label_text = "{" + ",".join(k + "=" + json.dumps(v, ensure_ascii=False)
                              for k, v in sorted(labels.items())) + "}"
    return (len(value[1].encode("utf-8")) + len(label_text.encode("utf-8")) + 128
            + sum(len(k.encode("utf-8")) + len(v.encode("utf-8")) + 32
                  for k, v in value[2].items()))


def prepare_pushes(session, url, rows, limit):
    """Plan the entire batch before sending; exact requests JSON wire budget.

    Splitting prepared requests also accounts for requests' actual whitespace
    and ASCII escaping (not a guessed compact UTF-8 JSON representation).
    """
    if not rows:
        return []
    streams, internal = {}, 0
    for labels, value in rows:
        if len(value[1].encode("utf-8")) > MAX_LINE_BYTES:
            raise ValueError("physical frame exceeds the 1 MiB Loki line cap")
        internal += internal_row_bound(labels, value)
        streams.setdefault(tuple(sorted(labels.items())), []).append(value)
    payload = {"streams": [{"stream": dict(key), "values": sorted(values, key=lambda v: int(v[0]))}
                           for key, values in streams.items()]}
    prepared = session.prepare_request(requests.Request("POST", url + "/loki/api/v1/push", json=payload))
    if len(prepared.body) <= limit and internal <= limit:
        return [prepared]
    if len(rows) == 1:
        raise ValueError("one row exceeds the JSON/internal request byte budget; refusing truncation")
    middle = len(rows) // 2
    return (prepare_pushes(session, url, rows[:middle], limit)
            + prepare_pushes(session, url, rows[middle:], limit))


def push(session, url, rows):
    for prepared in prepare_pushes(session, url, rows, request_budget()):
        # No retries, including ambiguous transport/partial HTTP failures.
        response = session.send(prepared, timeout=(10, 120), allow_redirects=False)
        if response.status_code != 204:
            raise RuntimeError(f"push failed ({response.status_code}); partial load must not be reused: {response.text[:1000]}")


def ingest_file(args):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    path, url, batch_size = args
    parquet = pq.ParquetFile(path)
    total = 0
    minimum = maximum = None
    with requests.Session() as session:
        for batch in parquet.iter_batches(batch_size=batch_size):
            column = batch.column("Timestamp")  # fail on missing/ambiguous name, never index -1
            if pa.types.is_timestamp(column.type):
                times = pc.cast(pc.cast(column, pa.timestamp("ns", tz=column.type.tz)), pa.int64()).to_pylist()
            elif pa.types.is_int64(column.type):
                times = column.to_pylist()
            else:
                raise ValueError(f"unsupported Timestamp type: {column.type}; expected ns timestamp or int64")
            # Remove timestamp columns before Python conversion: datetime
            # cannot represent ns, and Arrow otherwise requires pandas even
            # though these redundant values are never used by entry().
            rows = batch.select([k for k in batch.schema.names if k not in ("Timestamp", "TimestampTime")]).to_pylist(maps_as_pydicts="strict")
            values = []
            for i, (row, ts) in enumerate(zip(rows, times)):
                values.append(entry(row, ts, f"{Path(path).name}:{total + i}"))
                minimum = ts if minimum is None else min(minimum, ts)
                maximum = ts if maximum is None else max(maximum, ts)
            push(session, url, values)
            total += len(values)
    return {"rows": total, "min_timestamp_ns": minimum, "max_timestamp_ns": maximum}


def preflight_frames(path, parquet):
    """Vectorized safe fast path; serialize only possibly oversized Bodies.

    Each input UTF-8 byte costs at most six JSON bytes (ASCII controls are
    worst). Fixed frame overhead includes JSON-escaped filename and the longest
    row offset. Thus 6*binary_length(Body)+overhead <= cap proves safety without
    creating a billion Python strings/frames during corpus preflight.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    schema = parquet.schema_arrow
    if len(set(schema.names)) != len(schema.names) or RESERVED.intersection(schema.names):
        raise ValueError(f"{path}: duplicate/reserved source field")
    for name in ("Body", "ServiceName", "SeverityText"):
        if name not in schema.names:
            raise ValueError(f"{path}: missing {name}")
        kind = schema.field(name).type
        if pa.types.is_dictionary(kind):
            kind = kind.value_type
        if not (pa.types.is_string(kind) or pa.types.is_large_string(kind) or pa.types.is_string_view(kind)):
            raise ValueError(f"{path}: {name} must be a string column")
    overhead = frame_overhead(f"{Path(path).name}:{max(parquet.metadata.num_rows - 1, 0)}")
    threshold = (MAX_LINE_BYTES - overhead) // 6
    offset = 0
    for batch in parquet.iter_batches(batch_size=65536, columns=["Body"]):
        body = pc.cast(batch.column(0), pa.large_string())
        if body.null_count:
            raise ValueError(f"{path}: null Body")
        near = pc.indices_nonzero(pc.greater(pc.binary_length(body), threshold))
        for index, value in zip(near.to_pylist(), pc.take(body, near).to_pylist()):
            encode_frame(value, f"{Path(path).name}:{offset + index}")
        offset += batch.num_rows


def source_inventory(files):
    return [{"name": Path(path).name, "bytes": Path(path).stat().st_size,
             "mtime_ns": Path(path).stat().st_mtime_ns} for path in files]


def preflight(files):
    """Reject inputs unsafe for unordered parallel ingestion before any pushes.

    The published corpus spans 29.4 minutes. max_chunk_age=2h admits one hour
    of out-of-order data; a corpus narrower than that is safe under ANY worker
    ordering. Wider corpora need a time-ordered loader, not silent rejection.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    minima, maxima, rows = [], [], 0
    names = [Path(path).name for path in files]
    if len(set(names)) != len(names):
        raise ValueError("duplicate source basename would collide in row identity")
    for path in files:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        if schema.names.count("Timestamp") != 1:
            raise ValueError(f"{path}: exactly one Timestamp field is required")
        kind = schema.field("Timestamp").type
        if not (pa.types.is_timestamp(kind) or pa.types.is_int64(kind)):
            raise ValueError(f"{path}: unsupported Timestamp type {kind}")
        scale = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}[kind.unit] if pa.types.is_timestamp(kind) else 1
        leaf = parquet.schema.names.index("Timestamp")
        for i in range(parquet.num_row_groups):
            group = parquet.metadata.row_group(i)
            rows += group.num_rows
            stat = group.column(leaf).statistics
            if stat and stat.has_min_max and stat.null_count == 0:
                lo, hi = int(stat.min_raw) * scale, int(stat.max_raw) * scale
            else:
                column = parquet.read_row_group(i, columns=["Timestamp"]).column("Timestamp")
                if column.null_count:
                    raise ValueError(f"{path}: null Timestamp")
                if not len(column):
                    continue
                mm = pc.min_max(pc.cast(column, pa.int64())).as_py()
                lo, hi = mm["min"] * scale, mm["max"] * scale
            minima.append(lo); maxima.append(hi)
    if not rows or not minima:
        raise ValueError("empty corpus")
    if min(minima) < 1577836800 * 10**9:
        raise ValueError("timestamps predate the configured 2020 schema")
    if max(maxima) - min(minima) >= 3600 * 10**9:
        raise ValueError("corpus spans >=1h; parallel loader requires a time-ordered implementation for this input")
    for path in files:
        preflight_frames(path, pq.ParquetFile(path))
    return {"rows": rows, "min_timestamp_ns": min(minima), "max_timestamp_ns": max(maxima)}


def ingest_python_directory(directory, url):
    files = sorted(Path(directory).glob("part_*.parquet"))
    if not files:
        raise ValueError(f"no part_*.parquet in {directory}")
    request_budget()  # validate before workers or HTTP
    inventory = source_inventory(files)
    expected = preflight(files)
    workers = int(os.environ.get("LOKI_LOAD_WORKERS", "8"))
    batch_size = int(os.environ.get("LOKI_BATCH_ROWS", "5000"))
    if workers < 1 or batch_size < 1:
        raise ValueError("worker and batch sizes must be positive")
    results = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(ingest_file, [(str(f), url, batch_size) for f in files]):
            if result["rows"]:
                results.append(result)
            print(f"[load] completed files={len(results)}/{len(files)} rows={sum(r['rows'] for r in results)}", flush=True)
    if not results:
        raise ValueError("empty corpus")
    actual = {"rows": sum(r["rows"] for r in results),
              "min_timestamp_ns": min(r["min_timestamp_ns"] for r in results),
              "max_timestamp_ns": max(r["max_timestamp_ns"] for r in results)}
    if actual != expected or source_inventory(files) != inventory:
        raise ValueError("ingested rows/time bounds or source inventory changed after preflight")
    return {**actual, "source_files": inventory, "files": len(files), "workers": workers,
            "batch_rows": batch_size, "max_request_bytes": request_budget(),
            "client": "python", "wire_format": "json", "encoding": dict(ENCODING)}


def client_kind():
    kind = os.environ.get("LOKI_LOAD_CLIENT", "rust")
    if kind not in ("rust", "python"):
        raise ValueError("LOKI_LOAD_CLIENT must be rust or python")
    return kind


def rust_binary():
    return Path(os.environ.get("LOKI_CLIENT_BIN", Path(__file__).parent / "client/target/release/loki-parquet-ingest")).resolve()


def ingest_directory(directory, url, receipt):
    validate_contract(receipt)
    files = sorted(Path(directory).glob("part_*.parquet"))
    if not files:
        raise ValueError(f"no part_*.parquet in {directory}")
    inventory = source_inventory(files)
    if receipt.get("source_files") != inventory:
        raise ValueError("source inventory differs from fresh-load receipt; refusing to push")
    if client_kind() == "python":
        summary = ingest_python_directory(directory, url)
        if summary["source_files"] != inventory:
            raise ValueError("source inventory changed after receipt validation")
        return {**summary, "destination_id": receipt["destination_id"], "destination": receipt["destination"]}
    budget = request_budget()
    expected = preflight(files)  # independent check of Rust's source/ack manifest
    binary = rust_binary()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"Rust client missing: {binary}; run ./client/build.sh BEFORE the measured load")
    # Required flags are rejected by old/raw clients before they can push.
    command = [str(binary), "--url", url, "--line-encoding", ENCODING_FLAG,
               "--destination-id", receipt["destination_id"],
               "--threads", os.environ.get("LOKI_LOAD_WORKERS", "8"),
               "--http-workers", os.environ.get("LOKI_HTTP_WORKERS", "8"),
               "--batch-size", os.environ.get("LOKI_BATCH_ROWS", "5000"),
               "--queue-depth", os.environ.get("LOKI_QUEUE_DEPTH", "8"),
               "--max-request-bytes", str(budget)]
    for path in files:
        command.extend(["--file", str(path.resolve())])
    # Child stderr is progress/errors; stdout is exactly one successful summary.
    # Compilation is intentionally NOT performed here or counted as load CPU.
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    summary = json.loads(result.stdout)
    if (summary.get("encoding") != ENCODING or type(summary.get("encoding", {}).get("version")) is not int
            or summary.get("destination_id") != receipt["destination_id"]
            or summary.get("max_request_bytes") != budget
            or summary.get("client") != "rust" or summary.get("wire_format") != "snappy-protobuf"):
        raise ValueError("Rust client encoding/destination/budget handshake mismatch; load is not publishable")
    if {key: summary[key] for key in expected} != expected or source_inventory(files) != inventory:
        raise ValueError("Rust acknowledged rows/time bounds or source inventory disagree with preflight")
    return {**summary, "destination": receipt["destination"], "source_files": inventory}
