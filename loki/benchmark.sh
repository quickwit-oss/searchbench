#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export ENGINE_NAME="Loki"
export ENGINE_TAGS='["Go","Loki"]'
export SEARCHBENCH_QUERIES="${SEARCHBENCH_QUERIES:-queries.logql}"
exec ../lib/benchmark.sh "$@"
