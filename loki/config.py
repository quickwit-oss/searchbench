"""Version-pinned single-binary config. JSON is valid YAML for Loki."""
import os
import re
from pathlib import Path


def positive(name, default):
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def duration(name, default):
    value = os.environ.get(name, default)
    if not re.fullmatch(r"[0-9]+[smh]", value):
        raise ValueError(f"{name} must be a non-negative duration such as 0s, 15m or 1h")
    return value


def byte_size(name, default):
    value = os.environ.get(name, default)
    if not re.fullmatch(r"[1-9][0-9]*(?:KB|MB|GB)", value):
        raise ValueError(f"{name} must be a positive byte size such as 16MB or 600MB")
    return value


def settings():
    storage = os.environ.get("LOKI_STORAGE", "filesystem")
    if storage not in ("filesystem", "s3"):
        raise ValueError("LOKI_STORAGE must be filesystem or s3")
    bucket = os.environ.get("LOKI_S3_BUCKET", "")
    prefix = os.environ.get("LOKI_S3_PREFIX", "")
    if storage == "s3" and (not bucket or not re.fullmatch(r"searchbench/loki/[A-Za-z0-9_-]+/", prefix)):
        raise ValueError("S3 needs LOKI_S3_BUCKET and a dedicated LOKI_S3_PREFIX=searchbench/loki/<run>/")
    data_dir = Path(os.environ.get("LOKI_DATA_DIR", Path(__file__).parent / "loki-data")).resolve()
    target = os.environ.get("LOKI_TARGET", "auto")
    if target not in ("auto", "all", "read"):
        raise ValueError("LOKI_TARGET must be auto, all or read")
    if target == "auto":
        target = "read" if (data_dir / "manifest.json").is_file() else "all"
    return {
        "storage": storage, "bucket": bucket, "prefix": prefix, "target": target,
        "region": os.environ.get("AWS_REGION", "us-east-1"),
        "data_dir": str(data_dir),
        "container": os.environ.get("LOKI_CONTAINER", "searchbench-loki"),
        "image": os.environ.get("LOKI_IMAGE", "grafana/loki:3.7.9"),
        "port": positive("LOKI_PORT", 3100),
    }


def make_config(root="/data", port=3100, grpc_port=9096, listen="0.0.0.0"):
    s = settings()
    storage = {"filesystem": {"chunks_directory": root + "/chunks", "rules_directory": root + "/rules"}}
    if s["storage"] == "s3":
        storage = {"s3": {"bucketnames": s["bucket"], "region": s["region"]}}
    return {
        "auth_enabled": False,
        "server": {"http_listen_address": listen, "http_listen_port": port,
                   "grpc_listen_address": "127.0.0.1", "grpc_listen_port": grpc_port,
                   "http_server_read_timeout": "600s", "http_server_write_timeout": "600s"},
        "common": {"instance_addr": "127.0.0.1", "path_prefix": root, "replication_factor": 1,
                   "compactor_grpc_address": f"127.0.0.1:{grpc_port}",
                   "ring": {"kvstore": {"store": "inmemory"}}, "storage": storage},
        "schema_config": {"configs": [{"from": "2020-01-01", "store": "tsdb", "object_store": s["storage"],
                                      "schema": "v13", "index": {"prefix": "index_", "period": "24h"}}]},
        "storage_config": {
            "object_prefix": s["prefix"],
            "tsdb_shipper": {"active_index_directory": root + "/tsdb-index", "cache_location": root + "/tsdb-cache"},
        },
        "ingester": {
            "wal": {"enabled": True, "dir": root + "/wal", "flush_on_shutdown": True},
            "lifecycler": {"min_ready_duration": "0s"},
            "chunk_encoding": os.environ.get("LOKI_CHUNK_ENCODING", "snappy"),
            "chunk_target_size": positive("LOKI_CHUNK_TARGET_SIZE", 1572864),
            "chunk_idle_period": "30s", "max_chunk_age": "2h", "chunk_retain_period": "0s",
        },
        "compactor": {"working_directory": root + "/compactor", "compaction_interval": "1m",
                      "retention_enabled": False},
        "querier": {"query_store_only": True, "max_concurrent": positive("LOKI_QUERY_CONCURRENCY", 16)},
        # Single-node benchmark: explicit loopback addresses avoid scheduler
        # ring discovery/polling (~10s of otherwise idle cold-start delay).
        "frontend": {"scheduler_address": f"127.0.0.1:{grpc_port}"},
        "frontend_worker": {"scheduler_address": f"127.0.0.1:{grpc_port}"},
        "query_scheduler": {"use_scheduler_ring": False},
        "query_range": {"cache_results": False, "cache_instant_metric_results": False},
        "chunk_store_config": {"chunk_cache_config": {"embedded_cache": {"enabled": False}},
                               "chunk_cache_config_l2": {"embedded_cache": {"enabled": False}}},
        "limits_config": {
            # Static benchmark corpus: no deletion requests. A read-only query
            # process must not depend on an active compactor for tombstones.
            "deletion_mode": "disabled",
            "reject_old_samples": False, "allow_structured_metadata": True,
            "discover_service_name": [], "discover_log_levels": False,
            "unordered_writes": True, "increment_duplicate_timestamp": False,
            "max_global_streams_per_user": 0, "max_streams_per_user": 0,
            "ingestion_rate_mb": positive("LOKI_INGESTION_RATE_MB", 128), "ingestion_burst_size_mb": 256,
            "per_stream_rate_limit": "32MB", "per_stream_rate_limit_burst": "64MB",
            "max_line_size": "1MB", "max_line_size_truncate": False,
            "query_timeout": "600s", "max_query_series": 100000,
            "split_queries_by_interval": duration("LOKI_SPLIT_QUERIES_INTERVAL", "15m"),
            "split_instant_metric_queries_by_interval": duration("LOKI_SPLIT_METRIC_QUERIES_INTERVAL", "1h"),
            "tsdb_max_query_parallelism": positive("LOKI_TSDB_MAX_QUERY_PARALLELISM", 32),
            "tsdb_max_bytes_per_shard": byte_size("LOKI_TSDB_MAX_BYTES_PER_SHARD", "600MB"),
        },
        "analytics": {"reporting_enabled": False},
    }
