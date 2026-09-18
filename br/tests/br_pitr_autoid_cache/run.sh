#!/bin/bash
#
# Copyright 2026 PingCAP, Inc.
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

# Regression test for https://github.com/pingcap/tidb/issues/69485: after a PiTR
# restore, a table with AUTO_ID_CACHE=1 must not hand out auto-increment IDs that
# collide with already-restored rows. Before the fix the autoid service kept a
# stale in-memory base (from the snapshot restore) while the persisted counter had
# been advanced by log replay, so the first insert reused an existing ID and
# failed with a duplicate-key error.

set -eu
. run_services
CUR=$(cd "$(dirname "$0")"; pwd)

# NOTICE: don't start the prefix with 'br' because `restart services` removes br* files.
PREFIX="pitr_autoid_cache_backup"
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
TASK_NAME="br_pitr_autoid_cache"
DB="autoid_pitr"

# This test only exercises auto-increment allocation, so TiFlash is not needed.
restart_services_no_tiflash() {
	stop_services
	start_services --no-tiflash
}

# Start a clean cluster.
restart_services_no_tiflash

# Prepare data: an AUTO_ID_CACHE=1 table with a single row, matching the issue repro.
run_sql "create database if not exists $DB;"
run_sql "create table $DB.t (id bigint primary key auto_increment, data bigint not null) auto_id_cache=1;"
run_sql "insert into $DB.t (data) values (0);"

# Start the log backup task, then take a full (snapshot) backup.
echo "start log backup task"
run_br --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "local://$TEST_DIR/$PREFIX/log"

echo "run snapshot backup"
run_br --pd "$PD_ADDR" backup full -s "local://$TEST_DIR/$PREFIX/full"

# Insert 5000 more rows after the snapshot so the restored data occupies IDs well
# beyond the snapshot's cached allocator range (this is what the stale base later
# collides with).
run_sql "set @@cte_max_recursion_depth=5000; insert into $DB.t (data) with recursive cte as (select 1 as n union all select n+1 from cte where n < 5000) select n from cte;"

# Make sure the log backup captured the inserts.
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

max_id_before=$(run_sql "select max(id) as m from $DB.t;" | tail -n 1 | awk '{print $2}')
row_count_before=$(run_sql "select count(*) as c from $DB.t;" | tail -n 1 | awk '{print $2}')
echo "before restore: rows=$row_count_before max_id=$max_id_before"

# Restore into a fresh cluster (full snapshot + all log up to the checkpoint).
restart_services_no_tiflash
echo "run PiTR restore"
# --skip-goleak: syncing AUTO_ID_CACHE=1 tables opens a grpc client to the autoid
# service that BR keeps until process exit (harmless for the CLI). The coverage
# test binary's goleak check would otherwise flag those background grpc goroutines.
run_br --skip-goleak --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" > "$res_file" 2>&1 || ( cat "$res_file" && exit 1 )

# The core assertion: inserting after restore must succeed and must not reuse an
# existing ID. With the bug this insert fails with "Duplicate entry" (set -e then
# aborts the test); the explicit comparison also guards against a non-colliding
# but still-stale allocation.
run_sql "insert into $DB.t (data) values (5001);"
new_id=$(run_sql "select id from $DB.t where data = 5001;" | tail -n 1 | awk '{print $2}')
echo "after restore: newly inserted id=$new_id (must be > $max_id_before)"

if [ "$new_id" -le "$max_id_before" ]; then
	echo "TEST FAILED: post-restore auto-increment id $new_id collides with restored data (max was $max_id_before)"
	exit 1
fi

# The restored rows themselves must be intact.
row_count_after=$(run_sql "select count(*) as c from $DB.t;" | tail -n 1 | awk '{print $2}')
if [ "$row_count_after" -ne "$((row_count_before + 1))" ]; then
	echo "TEST FAILED: expected $((row_count_before + 1)) rows after restore+insert, got $row_count_after"
	exit 1
fi

echo "PASS: post-restore insert allocated id=$new_id above restored max=$max_id_before"
