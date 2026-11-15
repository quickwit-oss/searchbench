#!/usr/bin/env bash
# HOST. Dependency setup + corpus download/verification (outside measured load);
# staged hashed wheels only. Corpus: size+ETag before and after download, full SHA256.
set -euo pipefail
cd /srv/lp
source ./run.env
source ./remote.env
E=/srv/lp/evidence
systemctl is-active lp-deadline.timer
systemctl list-timers --all > $E/pre-setup-timers.txt
(( $(date +%s) < WORK_CUTOFF_EPOCH ))
# Do not kill installers. Wait until idle; then pause only the timers and recheck.
while pgrep -x apt >/dev/null || pgrep -x apt-get >/dev/null || pgrep -x dpkg >/dev/null || systemctl is-active --quiet apt-daily.service || systemctl is-active --quiet apt-daily-upgrade.service; do sleep 5; done
[[ -z $(dpkg --audit) ]]
systemctl stop apt-daily.timer apt-daily-upgrade.timer
while pgrep -x apt >/dev/null || pgrep -x apt-get >/dev/null || pgrep -x dpkg >/dev/null; do sleep 5; done
[[ -z $(dpkg --audit) ]]
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io jq python3-venv build-essential pkg-config sysstat nvme-cli curl
[[ -z $(dpkg --audit) ]]
dpkg-query -W docker.io sysstat jq python3-venv > $E/packages.txt
# No containers yet: persistent CPU-accounting parent for engine containers.
systemctl stop docker.service docker.socket
cat > /etc/systemd/system/searchbench.slice <<'EOF'
[Unit]
Description=Searchbench engine containers CPU accounting
[Slice]
CPUAccounting=yes
[Install]
WantedBy=multi-user.target
EOF
mkdir -p /etc/docker
printf '{"cgroup-parent":"searchbench.slice"}\n' > /etc/docker/daemon.json
systemctl daemon-reload
systemctl enable --now searchbench.slice
systemctl start docker
lsblk -J -o NAME,PATH,MODEL,SIZE,MOUNTPOINTS > $E/disks-before.json
mapfile -t disks < <(lsblk -dn -o PATH,MODEL | awk '$0 ~ /Amazon EC2 NVMe Instance Storage/ {print $1}')
[[ ${#disks[@]} == 1 ]]
[[ -z $(lsblk -n -o MOUNTPOINTS "${disks[0]}" | tr -d '[:space:]') ]]
[[ $(lsblk -nr -o NAME "${disks[0]}" | wc -l) == 1 ]]
mkfs.ext4 -q -F "${disks[0]}"
mkdir -p /mnt/lp
mount "${disks[0]}" /mnt/lp
findmnt -J /mnt/lp > $E/nvme-mount.json
sha256sum source.tar.gz | tee $E/source-archive.sha256
[[ $(sha256sum source.tar.gz | awk '{print $1}') == $(awk '{print $1}' source.sha256) ]]
mkdir -p source
tar xzf source.tar.gz -C source
mkdir -p source/loki/results
curl --proto '=https' --tlsv1.2 -fsS https://sh.rustup.rs -o rustup-init.sh
sha256sum rustup-init.sh > $E/rustup-bootstrap.sha256
sh rustup-init.sh -y --profile minimal --default-toolchain "$RUST_TOOLCHAIN"
source /root/.cargo/env
rustc -Vv | tee $E/rustc-version.txt
cargo -V | tee $E/cargo-version.txt
python3 -m venv venv
venv/bin/pip install --no-index --find-links=/srv/lp/wheels --require-hashes -r /srv/lp/requirements.lock
[[ ! -e /usr/local/bin/aws ]]
ln -s /srv/lp/venv/bin/aws /usr/local/bin/aws
aws --version 2>&1 | tee $E/awscli-version.txt
venv/bin/pip check
# Instance role (IMDSv2, hop limit 2 so the Loki container can also reach IMDS).
aws sts get-caller-identity > $E/sts-host.json
jq -e --arg r "assumed-role/$N-host/" '.Arn|contains($r)' $E/sts-host.json
aws s3api list-objects-v2 --bucket <bucket> --prefix "$DATA_PREFIX" --no-paginate > $E/data-prefix-before.json
jq -e '(.Contents // []) | length == 0' $E/data-prefix-before.json
venv/bin/pip freeze > $E/python-freeze.txt
# Build + Rust tests before any measured load; LOKI_CLIENT_BIN makes install skip rebuilding.
(cd source/loki/client && LOKI_CLIENT_BUILD=native ./build.sh && cargo test --release --locked) 2>&1 | tee $E/client-build-and-tests.log
sha256sum "$LOKI_CLIENT_BIN" > $E/client.sha256
docker pull "$LOKI_IMAGE"
docker image inspect "$LOKI_IMAGE" > $E/image-inspect.json
jq -e '.[0].Architecture=="arm64" and .[0].Os=="linux"' $E/image-inspect.json
docker run --rm "$LOKI_IMAGE" -version > $E/loki-binary-version.txt 2>&1
grep -q "version $LOKI_EXPECTED_VERSION " $E/loki-binary-version.txt
# Driver unit/fake-engine tests on Linux (real cache drop as root, process-group kill).
(cd source/loki/tests && /srv/lp/venv/bin/python -m unittest -v test_query_pair.PlanAndSummaryTests test_query_pair.FakeEngineTests) 2>&1 | tee $E/driver-linux-tests.log
# Phase-2/3 helpers: syntax + stdlib unit checks (wrappers are installed only by phase2.sh).
bash -n phase2.sh sbwrap-cpu-usage sbwrap-query
/srv/lp/venv/bin/python -m py_compile getscrape.py answers.py collate_phase2.py diag600.py s3ops_logger.py run_until.py load_overlay.py count_windows.py
SB_SOURCE_LIB=/srv/lp/source/lib /srv/lp/venv/bin/python -c '
import json, answers
refs = json.load(open("references.json"))["queries"]
assert len(refs) == 56, len(refs)
print("references", len(refs), "queries")' | tee $E/helpers-check.txt
# Corpus to instance-store NVMe (after the driver tests: no dirty-page sync stalls in their cold cycles).
aws s3api head-object --bucket <bucket> --key "$CORPUS_KEY" > $E/corpus-head-before.json
jq -e --arg etag "$CORPUS_ETAG" --argjson n "$CORPUS_BYTES" '.ETag==$etag and .ContentLength==$n' $E/corpus-head-before.json
mkdir -p "$SEARCHBENCH_DATA_DIR"
[[ -z $(ls -A "$SEARCHBENCH_DATA_DIR") ]]
aws configure set default.s3.max_concurrent_requests 32
aws configure set default.s3.multipart_chunksize 64MB
t0=$(date +%s)
aws s3 cp --only-show-errors "s3://<bucket>/$CORPUS_KEY" "$SEARCHBENCH_DATA_DIR/part_000.parquet"
t1=$(date +%s)
aws s3api head-object --bucket <bucket> --key "$CORPUS_KEY" > $E/corpus-head-after.json
jq -e --arg etag "$CORPUS_ETAG" --argjson n "$CORPUS_BYTES" '.ETag==$etag and .ContentLength==$n' $E/corpus-head-after.json
[[ $(stat -c %s "$SEARCHBENCH_DATA_DIR/part_000.parquet") == "$CORPUS_BYTES" ]]
sha256sum "$SEARCHBENCH_DATA_DIR/part_000.parquet" | tee $E/corpus.sha256
t2=$(date +%s)
[[ $(awk '{print $1}' $E/corpus.sha256) == "$CORPUS_SHA256" ]]
jq -n --argjson dl $((t1 - t0)) --argjson sha $((t2 - t1)) --argjson n "$CORPUS_BYTES" --arg sha256 "$CORPUS_SHA256" \
  '{download_seconds:$dl, sha256_seconds:$sha, bytes:$n, sha256:$sha256, size_etag_sha256_verified:true}' > $E/corpus.json
date -u > $E/setup-passed
