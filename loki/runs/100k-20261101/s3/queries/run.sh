#!/usr/bin/env bash
set -euo pipefail
RUN_ID=${1:?run ID required}
[[ "$RUN_ID" =~ ^gets-100k-[0-9TZ]+$ ]] || exit 2
BASE=<home>/loki-s3-gets
OUT=$BASE/$RUN_ID
OLD=<home>/loki-s3-validation/s3-100k-20261101T175953Z
ROOT=/mnt/local-ssd/searchbench
mkdir -p "$OUT"
exec >> "$OUT/run.log" 2>&1
status() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/status"; }
finish() {
 rc=$?
 trap - EXIT
 if [[ -f "$OUT/environment.sh" && -f "$ROOT/loki/adapter.py" ]]; then
  source "$OUT/environment.sh"
  (cd "$ROOT/loki"; ./stop) || true
  for f in manifest.json destination.json runtime.json config.json; do
   [[ ! -f "$LOKI_DATA_DIR/$f" ]] || cp "$LOKI_DATA_DIR/$f" "$OUT/"
  done
 fi
 status "query-only exit rc=$rc"
 tar -czf "$BASE/$RUN_ID.tar.gz" -C "$BASE" "$RUN_ID"
 sha256sum "$BASE/$RUN_ID.tar.gz" > "$BASE/$RUN_ID.sha256"
 sudo shutdown -h +10 'Loki 100K query-only run finished; artifacts saved on root EBS' || true
 exit "$rc"
}
trap finish EXIT
trap 'exit 143' TERM
status '100K existing-data query-only GET collection; no ingestion'
test -f /var/lib/searchbench-bootstrap-ready
test -f "$OLD/manifest.json"
if [[ -n "$(docker ps -q)" ]]; then echo 'Other containers running; refusing'; exit 1; fi
sudo systemctl stop docker.service docker.socket
if mountpoint -q /mnt/local-ssd; then echo 'Unexpected mounted disk; inspect rather than overwrite'; exit 1; fi
DEV=$(python3 - <<'PY'
from pathlib import Path
matches=[('/dev/'+p.name) for p in Path('/sys/block').glob('nvme*n1') if (p/'device/model').read_text().strip()=='Amazon EC2 NVMe Instance Storage']
assert len(matches)==1,matches
print(matches[0])
PY
)
if sudo blkid "$DEV" >/dev/null 2>&1; then echo 'Refusing to format a filesystem'; exit 1; fi
sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "$DEV"
sudo mount -o noatime "$DEV" /mnt/local-ssd
sudo chown ubuntu:ubuntu /mnt/local-ssd
sudo systemctl start docker
mkdir -p "$ROOT" /mnt/local-ssd/loki-s3-100k-20261101T175953Z
tar -xf <home>/loki-s3-validation/source.tar -C "$ROOT"
cp "$OLD/SOURCE_COMMIT" "$ROOT/SOURCE_COMMIT"
cp "$OLD"/{manifest.json,destination.json,load-started.json} /mnt/local-ssd/loki-s3-100k-20261101T175953Z/
python3 -m venv /mnt/local-ssd/venv
/mnt/local-ssd/venv/bin/pip -q install requests boto3 prometheus-client
cp "$OLD/environment.sh" "$OUT/environment.sh"
printf '\nexport LOKI_TARGET=read\n' >> "$OUT/environment.sh"
source "$OUT/environment.sh"
cp "$BASE"/{queries.py,run.sh} "$OUT/"
cp "$ROOT/SOURCE_COMMIT" "$OUT/SOURCE_COMMIT"
cp "$OLD/loki-results/load/otel_logs_100k.3.7.9-m8gd4-100k-s3-validation.json" "$OUT/original-load.json"
python - <<'PY' | tee "$OUT/preflight.json"
import boto3,contextlib,json,os,sys
from pathlib import Path
sys.path.insert(0,'/mnt/local-ssd/searchbench/loki')
import adapter
m=json.loads((Path(os.environ['LOKI_DATA_DIR'])/'manifest.json').read_text())
assert m['rows']==100000 and m['storage']=='s3'
who=boto3.client('sts').get_caller_identity()
assert '/searchbench-loki-gets-100k-' in who['Arn']
with contextlib.redirect_stdout(sys.stderr):adapter.verify_layout()
print(json.dumps({'rows':m['rows'],'query_only':True,'role_arn':who['Arn'],'credential_method':boto3.Session().get_credentials().method,
 'bucket':os.environ['LOKI_S3_BUCKET'],'prefix':os.environ['LOKI_S3_PREFIX']},indent=2))
PY
python -m pip freeze > "$OUT/python-packages.txt"
findmnt /mnt/local-ssd > "$OUT/mount.txt"
docker pull --quiet "$LOKI_IMAGE"
docker image inspect "$LOKI_IMAGE" --format '{{json .RepoDigests}}' > "$OUT/image-digest.json"
status 'stock image and original 100K S3 layout verified; start 56-query suite'
python "$BASE/queries.py" --out "$OUT/queries"
status 'PASS: all 56 queries with paired native GETs/wall/CPU; original S3 objects unchanged'
