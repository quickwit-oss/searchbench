#!/usr/bin/env bash
# Build before load CPU measurement; never compile concurrently with a benchmark.
set -euo pipefail
cd "$(dirname "$0")"
mode="${LOKI_CLIENT_BUILD:-auto}"
if [[ "$mode" == auto ]]; then
    if command -v cargo >/dev/null; then mode=native; else mode=docker; fi
fi
case "$mode" in
    native) cargo build --release --locked ;;
    docker)
        cache="${CARGO_HOME_CACHE:-$HOME/.cache/loki-client-cargo}"
        mkdir -p "$cache"
        docker run --rm -v "$PWD:/src" -v "$cache:/usr/local/cargo/registry" -w /src \
            "${LOKI_RUST_IMAGE:-rust:bookworm}" \
            bash -c 'cargo build --release --locked && chown -R "$1:$2" /src/target' \
            _ "$(id -u)" "$(id -g)"
        ;;
    *) echo "LOKI_CLIENT_BUILD must be auto, native or docker" >&2; exit 1 ;;
esac
