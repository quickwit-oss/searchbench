#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mode=local
if [[ "${DUCKDB_PARQUET_PATH:-}" == s3://* ]]; then
    mode=s3
fi
# An explicit path is already materialized (or remote); never download a
# different corpus beside it. The driver still requires a local data root.
if [[ -n "${DUCKDB_PARQUET_PATH:-}" ]]; then
    export SEARCHBENCH_SKIP_DOWNLOAD=yes
    export SEARCHBENCH_DATA_DIR="${SEARCHBENCH_DATA_DIR:-$PWD/.tmp/data}"
fi
export ENGINE_NAME="DuckDB-Parquet"
export ENGINE_TAGS='["C++","SQL","Parquet","No index"]'
export SEARCHBENCH_VERSION="${SEARCHBENCH_VERSION:-${mode}-no-index}"
export SEARCHBENCH_QUERIES="${SEARCHBENCH_QUERIES:-queries.sql}"
# The shared runner uses the version label for both results and load records.
# Default labels keep local and S3 runs separate; use unique --version labels
# when comparing thread counts, memory limits, regions, or datasets in place.
exec ../lib/benchmark.sh "$@"
