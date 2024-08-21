#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

check_cluster_version 4 0 0 'local backend' || exit 0

# the default mode (aes-128-ecb) can be easily compressed, switch to cbc to reduce the compression effect.
run_sql 'DROP DATABASE IF EXISTS disk_quota;'
run_sql "SELECT @@block_encryption_mode"
OLD_ENCRYPTION_MODE=$(read_result)
run_sql "SET GLOBAL block_encryption_mode = 'aes-256-cbc';"

DISK_QUOTA_DIR="$TEST_DIR/with-disk-quota"
FINISHED_FILE="$TEST_DIR/sorted-with-disk-quota.finished"

mkdir -p "$DISK_QUOTA_DIR"
rm -f "$FINISHED_FILE"
cleanup() {
    touch "$FINISHED_FILE"
    run_sql "SET GLOBAL block_encryption_mode = '$OLD_ENCRYPTION_MODE';"
}
trap cleanup EXIT

# There is normally a 2 second delay between these SET GLOBAL statements returns
# and the changes are actually effective. So we have this check-and-retry loop
# below to ensure Lightning gets our desired global vars.
for i in $(seq 3); do
    sleep 1
    run_sql "SELECT @@block_encryption_mode"
    if [ "$(read_result)" = 'aes-256-cbc' ]; then
        break
    fi
done

while [ ! -e "$FINISHED_FILE" ] && [ -e "$DISK_QUOTA_DIR" ]; do
    # du may fail because the directory is removed by lightning
    DISK_USAGE=$(du -s -B1 "$DISK_QUOTA_DIR" | cut -f 1)
    # the disk quota of 75 MiB is a just soft limit.
    # the reserved size we have is (512 MiB + 4 files × 1000ms × 1 KiB/ms) = 516 MiB,
    # which sums up to 591 MiB as the hard limit.
    if [ "0$DISK_USAGE" -gt 619610112 ]; then
        echo "hard disk quota exceeded, actual size = $DISK_USAGE" > "$FINISHED_FILE"
        break
    else
        sleep 1
    fi
done &

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/SlowDownWriteRows=sleep(50)"
run_lightning --sorted-kv-dir "$DISK_QUOTA_DIR/sorted" --log-file "$TEST_DIR/lightning-disk-quota.log"
touch "$FINISHED_FILE"
# if $FINISHED_FILE has content, it is only because the hard disk quota is exceeded.
[ -s "$FINISHED_FILE" ] && cat "$FINISHED_FILE" && exit 1

# check that disk quota is indeed triggered.
grep -q 'disk quota exceeded' "$TEST_DIR/lightning-disk-quota.log"

# check that the columns are correct.
run_sql "select cast(trim(trailing 'a' from aes_decrypt(sa, 'xxx', 'iviviviviviviviv')) as char) a from disk_quota.t where id = 1357"
check_contains 'a: 1357'
run_sql "select cast(trim(trailing 'e' from aes_decrypt(se, 'xxx', 'iviviviviviviviv')) as char) e from disk_quota.t where id = 246"
check_contains 'e: 246'
