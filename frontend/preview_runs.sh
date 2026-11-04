#!/usr/bin/env bash
# Serve the same unified run-selector page that GitHub Pages publishes.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PORT=${1:-5173}
[[ "$PORT" =~ ^[1-9][0-9]{0,4}$ ]] && (( PORT <= 65535 )) || { echo 'invalid port' >&2; exit 1; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
node "$HERE/build_site.mjs" "$work"
printf '\nSearchBench: http://127.0.0.1:%s/\n' "$PORT"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$work"
