#!/usr/bin/env python3
"""SearchBench lifecycle and query adapter for Loki 3.7.9 (Docker backend)."""
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import requests

from config import make_config, settings
from logql import build_request, literal_prefilter_enabled, normalize
from encoding import ENCODING, validate_contract


HERE = Path(__file__).resolve().parent


def run(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True)


def url():
    return f"http://127.0.0.1:{settings()['port']}"


def api(path, params=None, method="GET", timeout=610):
    response = requests.request(method, url() + path, params=params, timeout=(10, timeout))
    if response.status_code >= 300:
        raise RuntimeError(f"Loki {path}: HTTP {response.status_code}: {response.text[:1500]}")
    return response


def s3_objects():
    import boto3
    s = settings()
    client = boto3.client("s3", region_name=s["region"])
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=s["bucket"], Prefix=s["prefix"]):
        yield from page.get("Contents", [])


def prepare():
    s = settings()
    root = Path(s["data_dir"])
    identity = {k: s[k] for k in ("storage", "bucket", "prefix")}
    marker = root / "destination.json"
    if marker.exists():
        if json.loads(marker.read_text()) != identity:
            raise ValueError("data directory belongs to a different storage destination; use a fresh LOKI_DATA_DIR")
    else:
        if root.exists() and any(root.iterdir()):
            raise ValueError("LOKI_DATA_DIR is not empty or adapter-owned; refusing to reuse it")
        if s["storage"] == "s3" and next(s3_objects(), None) is not None:
            raise ValueError("S3 prefix is not empty; refusing to mix a load with existing objects")
        root.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps(identity, indent=2) + "\n")
    if (root / "manifest.json").exists():
        read_manifest(root)
    return root


def destination_identity(root=None):
    s = settings()
    identity = {key: s[key] for key in ("storage", "bucket", "prefix")}
    if s["storage"] == "filesystem":
        identity["data_dir"] = str(Path(root).resolve()) if root is not None else s["data_dir"]
    return identity


def load_receipt(root=None):
    root = Path(settings()["data_dir"]) if root is None else root
    try:
        receipt = json.loads((root / "load-started.json").read_text())
    except FileNotFoundError as exc:
        raise ValueError("missing fresh-load encoding receipt; legacy storage cannot be reused") from exc
    validate_contract(receipt)
    if receipt["destination"] != destination_identity():
        raise ValueError("load receipt does not match configured physical destination")
    return receipt


def read_manifest(root=None):
    root = Path(settings()["data_dir"]) if root is None else root
    manifest = json.loads((root / "manifest.json").read_text())
    receipt = load_receipt(root)
    validate_contract(manifest, receipt)
    if manifest.get("source_files") != receipt.get("source_files"):
        raise ValueError("manifest/receipt source inventory mismatch")
    return manifest


def start(target_override=None):
    s = settings()
    if target_override is not None:
        if target_override != "read,compactor":
            raise ValueError("invalid internal finalization target")
        s["target"] = target_override
    # The read group discovers a scheduler but does not activate one itself
    # in this release. Keep the scheduler local in our single-node deployment.
    s["target"] = {"all": "all", "read": "read,query-scheduler",
                   "read,compactor": "read,query-scheduler,compactor"}[s["target"]]
    # Client-side query setting, validated before any container change and
    # recorded in runtime.json: lib/benchmark.sh keeps only timing and row
    # count from ./query's stderr, so LOKI_QUERY_SETTINGS is not in its results.
    literal_prefilter = literal_prefilter_enabled()
    root = prepare()
    config = make_config(port=s["port"], listen="127.0.0.1")
    config_text = json.dumps(config, sort_keys=True, indent=2) + "\n"
    # Mount the directory, not the config inode, so replacements are visible.
    (root / "config.json").write_text(config_text)
    signature = hashlib.sha256((config_text + json.dumps(s, sort_keys=True)).encode()).hexdigest()
    inspection = run("docker", "inspect", s["container"], check=False)
    exists = inspection.returncode == 0
    if exists:
        info = json.loads(inspection.stdout)[0]
        assert_owned_container(info)
        if (info["Config"].get("Labels") or {}).get("searchbench.loki") != signature:
            stop()
            run("docker", "rm", s["container"])
            exists = False
    if not exists:
        args = ["docker", "run", "-d", "--name", s["container"], "--user", f"{os.getuid()}:{os.getgid()}",
                "--ulimit", "nofile=65535:65535", "--network", "host",
                "--label", "searchbench.engine=loki", "--label", f"searchbench.loki={signature}",
                "-v", f"{root}:/data", "-e", "TZ=UTC"]
        for key in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            if key in os.environ:
                args.extend(["-e", key])
        args.extend([s["image"], "-config.file=/data/config.json", "-target=" + s["target"]])
        run(*args)
    else:
        run("docker", "start", s["container"])
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            check()
            (root / "runtime.json").write_text(json.dumps({
                "target": s["target"], "image": s["image"], "container": s["container"],
                "query_concurrency": config["querier"]["max_concurrent"],
                "literal_prefilter": literal_prefilter}, indent=2) + "\n")
            return
        except (requests.RequestException, RuntimeError):
            time.sleep(1)
    logs = run("docker", "logs", "--tail", "30", s["container"], check=False)
    raise RuntimeError("Loki did not become ready: " + logs.stdout + logs.stderr)


def assert_owned_container(info):
    if (info["Config"].get("Labels") or {}).get("searchbench.engine") != "loki":
        raise ValueError("container name is already in use by an unrelated container; refusing to stop or remove it")


def stop():
    s = settings()
    info = run("docker", "inspect", s["container"], check=False)
    if info.returncode:
        return
    container = json.loads(info.stdout)[0]
    assert_owned_container(container)
    if container["State"]["Running"]:
        run("docker", "stop", "--time", "180", s["container"])
        info = json.loads(run("docker", "inspect", s["container"]).stdout)[0]
        if info["State"]["ExitCode"] == 137:
            raise RuntimeError("Loki needed SIGKILL; shutdown/flush was not clean")


def check():
    api("/ready", timeout=2)


def install():
    import pyarrow  # noqa: F401 -- fail early, before starting a load
    if settings()["storage"] == "s3":
        import boto3  # noqa: F401
    run("docker", "info")
    run("docker", "pull", "--quiet", settings()["image"])
    prepare()  # does not wipe any existing state
    from ingest import client_kind, rust_binary
    if client_kind() == "rust":
        if "LOKI_CLIENT_BIN" not in os.environ:
            subprocess.run([str(HERE / "client/build.sh")], check=True)
        if not os.access(rust_binary(), os.X_OK):
            raise ValueError(f"Rust client is not executable: {rust_binary()}")
    print(f"Loki Docker adapter ready; {client_kind()} load client; no data deleted")


def query_response(spec, manifest, timeout=610, diagnostics=None):
    """Keep the 3-tuple and primary HTTP elapsed unchanged.

    Optional caller-owned dict receives separate client timings, including on
    failure. Never put these fields in Loki's native stats or turn a timeout
    into a successful query. Full wall/CPU include binding checks and building.
    """
    wall, cpu = time.perf_counter(), time.process_time()
    decode_wall = decode_cpu = None
    success = False
    error = None
    elapsed = None
    try:
        receipt = load_receipt()
        validate_contract(manifest, receipt)
        if manifest.get("source_files") != receipt.get("source_files"):
            raise ValueError("manifest/receipt source inventory mismatch")
        path, params = build_request(spec, manifest)
        before = time.perf_counter()
        try:
            response = api(path, params, timeout=timeout)
        finally:
            elapsed = time.perf_counter() - before
        decode_wall, decode_cpu = time.perf_counter(), time.process_time()
        data = response.json()
        rows = normalize(spec, data, manifest)
        success = True
        return rows, elapsed, data["data"].get("stats", {})
    except BaseException as exc:
        # External benchmark deadlines may deliberately inherit BaseException.
        # Record partial work, but preserve the exact exception/control flow.
        success = False
        error = type(exc).__name__
        raise
    finally:
        end_wall, end_cpu = time.perf_counter(), time.process_time()
        if diagnostics is not None:
            diagnostics.update(success=success, error_type=error,
                http_elapsed_seconds=elapsed, full_normalized_wall_seconds=end_wall - wall,
                total_client_cpu_seconds=end_cpu - cpu,
                normalization_wall_seconds=None if decode_wall is None else end_wall - decode_wall,
                normalization_cpu_seconds=None if decode_cpu is None else end_cpu - decode_cpu)


def query_settings():
    """Client query mode plus the running server's querier concurrency."""
    try:
        runtime = json.loads((Path(settings()["data_dir"]) / "runtime.json").read_text())
    except FileNotFoundError:
        runtime = {}
    return {"line_encoding": ENCODING["name"] + "-v" + str(ENCODING["version"]),
            "literal_prefilter": literal_prefilter_enabled(),
            "query_concurrency": runtime.get("query_concurrency")}


def query():
    raw = sys.stdin.read().strip()
    if raw.startswith("UNSUPPORTED:"):
        print(raw, file=sys.stderr)
        return
    spec = json.loads(raw)
    manifest = read_manifest()
    rows, elapsed, stats = query_response(spec, manifest)
    for row in rows:
        print(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
    print("LOKI_QUERY_SETTINGS=" + json.dumps(query_settings(), separators=(",", ":")), file=sys.stderr)
    print("LOKI_QUERY_STATS=" + json.dumps(stats, separators=(",", ":")), file=sys.stderr)
    print(f"SEARCHBENCH_ROWS={len(rows)}", file=sys.stderr)
    print(f"{elapsed:.6f}", file=sys.stderr)


def metric_sum(text, prefix, required=None):
    total = 0.0
    for line in text.splitlines():
        if line.startswith(prefix + "{") or line.startswith(prefix + " "):
            if required is None or required in line:
                total += float(line.split()[-1])
    return total


def claim_load(root, source, source_files):
    # prepare() can have run hours earlier. Recheck at load time and reserve
    # the prefix atomically, so two installations cannot mix their corpora.
    s = settings()
    if (root / "load-started.json").exists():
        raise FileExistsError("destination already claimed; choose a fresh destination")
    if any(p.is_file() for p in (root / "chunks").rglob("*")):
        raise ValueError("local destination already contains persisted objects")
    receipt = {"source": str(source), "source_files": source_files,
               "data_dir": str(root), "time": time.time(),
               "encoding": dict(ENCODING), "destination_id": str(uuid.uuid4()),
               "destination": destination_identity(root)}
    claim = json.dumps(receipt)
    if s["storage"] == "s3":
        import boto3
        if next(s3_objects(), None) is not None:
            raise ValueError("S3 destination is no longer empty; use a fresh prefix")
        boto3.client("s3", region_name=s["region"]).put_object(
            Bucket=s["bucket"], Key=s["prefix"] + "_searchbench_load_lock.json",
            Body=claim.encode(), ContentType="application/json", IfNoneMatch="*")
    with (root / "load-started.json").open("x") as file:
        file.write(claim)
    return receipt


def load():
    from ingest import ingest_directory, source_inventory
    root = prepare()
    if os.environ.get("SEARCHBENCH_SKIP_LOAD") == "yes":
        if not (root / "manifest.json").exists():
            raise ValueError("no completed load to skip")
        read_manifest(root)
        return
    if (root / "load-started.json").exists():
        raise ValueError("destination already used for a load; choose fresh local/S3 paths (no implicit deletion)")
    if settings()["target"] != "all":
        raise ValueError("ingestion requires LOKI_TARGET=all or auto on a fresh destination")
    source = os.environ["SEARCHBENCH_DATA_DIR"]  # driver already appends dataset
    files = sorted(Path(source).glob("part_*.parquet"))
    if not files:
        raise ValueError("no part_*.parquet source files")
    receipt = claim_load(root, source, source_inventory(files))
    start()
    before = time.monotonic()
    manifest = ingest_directory(source, url(), receipt)
    validate_contract(manifest, receipt)
    if manifest.get("source_files") != receipt["source_files"]:
        raise ValueError("source inventory changed since fresh destination claim")
    manifest["ingest_seconds"] = time.monotonic() - before
    metrics = api("/metrics").text
    (root / "ingest-metrics.txt").write_text(metrics)
    if metric_sum(metrics, "loki_discarded_samples_total"):
        raise RuntimeError("Loki rejected samples; inspect ingest-metrics.txt")
    # Stop is deliberately graceful: WAL flush plus TSDB head/index shipping.
    # Query-store-only is set even during ingestion, so readiness cannot mask
    # unpersisted data by serving ingester memory or replayed WAL entries.
    api("/flush", method="POST")
    stop()
    # Finalize/verify without bringing the ingester back: replaying its WAL
    # would regenerate and upload index files on every cold-query restart.
    start(target_override="read,compactor")
    deadline = time.monotonic() + int(os.environ.get("LOKI_SETTLE_TIMEOUT", "1800"))
    verified = False
    last_error = ""
    previous_inventory, stable_since = None, time.monotonic()
    quiet = int(os.environ.get("LOKI_SETTLE_QUIET_SECS", "30"))
    count_spec = {"kind": "count", "filters": [], "metadata_filters": [], "labels": {}}
    while time.monotonic() < deadline:
        metrics = api("/metrics").text
        # Two completed passes after restart plus an unchanged object store:
        # do not call a mere flush acknowledgement a settled load.
        compacted = metric_sum(metrics, "loki_boltdb_shipper_compact_tables_operation_total", 'status="success"') >= 2
        if compacted:
            current_inventory = object_inventory()
            if current_inventory != previous_inventory:
                previous_inventory, stable_since = current_inventory, time.monotonic()
            if time.monotonic() - stable_since < quiet:
                time.sleep(5)
                continue
            try:
                rows, _, stats = query_response(count_spec, manifest)
                actual = rows[0]["count"]
                if actual > manifest["rows"]:
                    raise ValueError(f"duplicate rows: expected {manifest['rows']}, got {actual}")
                if actual == manifest["rows"]:
                    verified = True
                    manifest["verification_stats"] = stats
                    break
                last_error = f"persisted rows {actual}/{manifest['rows']}"
            except (requests.RequestException, RuntimeError) as exc:
                last_error = str(exc)
        time.sleep(5)
    if not verified:
        raise RuntimeError("storage-only row-count/compaction gate did not pass: " + last_error)
    manifest["persisted_inventory"] = object_inventory()
    manifest.update(storage=settings()["storage"], image=settings()["image"],
                    load_seconds=time.monotonic() - before, query_store_only=True,
                    verification_target="read,query-scheduler,compactor",
                    query_target="all" if os.environ.get("LOKI_TARGET") == "all" else "read,query-scheduler")
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"[load] verified {manifest['rows']} persisted rows; {manifest['load_seconds']:.3f}s including flush, compaction and verification")


def object_inventory():
    """Fingerprint persisted objects; ignore local WAL/cache copies."""
    s = settings()
    if s["storage"] == "s3":
        entries = [(o["Key"], o["Size"], o.get("ETag", "")) for o in s3_objects()]
    else:
        base = Path(s["data_dir"]) / "chunks"
        entries = []
        for p in base.rglob("*"):
            if p.is_file():
                stat = p.stat()
                entries.append((str(p.relative_to(base)), stat.st_size, stat.st_mtime_ns))
    return hashlib.sha256(json.dumps(sorted(entries)).encode()).hexdigest()


def verify_layout():
    manifest = read_manifest()
    if "persisted_inventory" not in manifest:
        raise ValueError("legacy load has no persisted inventory fingerprint; use a fresh load for layout validation")
    if object_inventory() != manifest["persisted_inventory"]:
        raise ValueError("persisted objects changed during query runs; inspect WAL replay/compaction before publishing")
    print("Persisted object inventory unchanged")


def data_size():
    s = settings()
    if s["storage"] == "s3":
        print(sum(obj["Size"] for obj in s3_objects()))
    else:
        # Persistent object-store contents include chunks AND shipped indexes.
        # Do not count transient WALs, duplicate active index files or caches.
        print(sum(p.stat().st_size for p in (Path(s["data_dir"]) / "chunks").rglob("*") if p.is_file()))


def storage_units():
    """Persisted chunk/index files or objects, not WAL/cache copies or load locks."""
    s = settings()
    if s["storage"] == "s3":
        count = sum(1 for obj in s3_objects()
                    if obj["Key"] != s["prefix"] + "_searchbench_load_lock.json")
        unit = "objects"
    else:
        chunks = Path(s["data_dir"]) / "chunks"
        if not chunks.is_dir():
            raise ValueError("persisted chunk/index directory is missing; count is unknown")
        count = sum(1 for p in chunks.rglob("*") if p.is_file())
        unit = "files"
    print(json.dumps({"count": count, "unit": unit}))


def version():
    build = api("/loki/api/v1/status/buildinfo").json()
    print(build["version"] + " " + build.get("revision", ""))


if __name__ == "__main__":
    commands = {"install": install, "start": start, "stop": stop, "check": check,
                "load": load, "query": query, "data-size": data_size,
                "storage-units": storage_units, "version": version, "verify-layout": verify_layout}
    try:
        if len(sys.argv) != 2 or sys.argv[1] not in commands:
            raise ValueError("usage: adapter.py " + "|".join(commands))
        commands[sys.argv[1]]()
    except (ValueError, KeyError, OSError, RuntimeError, requests.RequestException, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError):
            print(exc.stderr, file=sys.stderr)
        sys.exit(1)
