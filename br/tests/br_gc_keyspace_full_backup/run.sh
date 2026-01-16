#!/bin/bash
#
# Copyright 2025 PingCAP, Inc.
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
. run_services

wait_file_exists() {
  local f="$1"
  local msg="${2:-}"
  for _ in $(seq 1 100); do
    if [ -f "$f" ]; then
      return 0
    fi
    sleep 0.1
  done
  echo "timeout waiting file: $f $msg"
  exit 1
}

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

DB="${TEST_NAME}"
GLOBAL_STORAGE="local://$TEST_DIR/gc_global_backup"
KEYSPACE_STORAGE="local://$TEST_DIR/gc_keyspace_backup"

SIG_GLOBAL_SET="$TEST_DIR/gc_global_set.${TEST_NAME}"
SIG_GLOBAL_DEL="$TEST_DIR/gc_global_del.${TEST_NAME}"
SIG_KS_SET="$TEST_DIR/gc_keyspace_set.${TEST_NAME}"
SIG_KS_DEL="$TEST_DIR/gc_keyspace_del.${TEST_NAME}"

rm -f "$SIG_GLOBAL_SET" "$SIG_GLOBAL_DEL" "$SIG_KS_SET" "$SIG_KS_DEL"
rm -rf "$TEST_DIR/gc_global_backup" "$TEST_DIR/gc_keyspace_backup" 2>/dev/null || true

echo "restart services"
restart_services

run_sql "drop database if exists $DB;"
run_sql "create database $DB;"
run_sql "create table $DB.t(id int primary key, v int);"
run_sql "insert into $DB.t values (1, 10), (2, 20), (3, 30);"

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/gc/hint-gc-global-set-safepoint=1*return(\"$SIG_GLOBAL_SET\");\
github.com/pingcap/tidb/br/pkg/gc/hint-gc-global-delete-safepoint=1*return(\"$SIG_GLOBAL_DEL\");\
github.com/pingcap/tidb/br/pkg/gc/hint-gc-keyspace-set-barrier=1*return(\"$SIG_KS_SET\");\
github.com/pingcap/tidb/br/pkg/gc/hint-gc-keyspace-delete-barrier=1*return(\"$SIG_KS_DEL\")"

echo "=== Global mode: backup full ==="
run_br --pd $PD_ADDR backup full -s "$GLOBAL_STORAGE" &
global_pid=$!

wait_file_exists "$SIG_GLOBAL_SET" "(waiting global set)"
# Verify global backup did not trigger keyspace failpoint
test ! -f "$SIG_KS_SET"

global_sp_id=$(cat "$SIG_GLOBAL_SET" | head -n 1 | tr -d '\n')
safe_point=$(run_pd_ctl -u https://$PD_ADDR service-gc-safepoint)
echo "$safe_point" | grep -q "\"service_id\": \"$global_sp_id\""

wait "$global_pid" || { echo "Global backup failed"; exit 1; }
wait_file_exists "$SIG_GLOBAL_DEL" "(waiting global delete)"

safe_point=$(run_pd_ctl -u https://$PD_ADDR service-gc-safepoint)
if echo "$safe_point" | grep -q "\"service_id\": \"$global_sp_id\""; then
  echo "global safepoint should be removed after backup: $global_sp_id"
  exit 1
fi

echo "=== Keyspace mode: backup full (keyspace1) ==="
rm -f "$SIG_GLOBAL_SET" "$SIG_GLOBAL_DEL" "$SIG_KS_SET" "$SIG_KS_DEL"
# Restart to reset failpoints for isolation verification
restart_services

run_br --pd $PD_ADDR backup full --keyspace-name keyspace1 -s "$KEYSPACE_STORAGE" &
ks_pid=$!

wait_file_exists "$SIG_KS_SET" "(waiting keyspace set)"
# Verify keyspace backup did not trigger global failpoint
test ! -f "$SIG_GLOBAL_SET"
# Verify signal file contains keyspace info
grep -q "keyspace=" "$SIG_KS_SET"

wait "$ks_pid" || { echo "Keyspace backup failed"; exit 1; }
wait_file_exists "$SIG_KS_DEL" "(waiting keyspace delete)"
# Verify delete also did not trigger global failpoint
test ! -f "$SIG_GLOBAL_DEL"
# Verify delete signal file contains keyspace info
grep -q "keyspace=" "$SIG_KS_DEL"

export GO_FAILPOINTS=""

run_sql "drop database if exists $DB;"
