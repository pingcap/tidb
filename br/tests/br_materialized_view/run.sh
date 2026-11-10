#!/usr/bin/env bash
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

# Run through the BR harness, which starts TiFlash:
# TEST_NAME=br_materialized_view br/tests/run.sh
# Only full snapshots and same-task PITR checkpoint retries are covered.
# Chained log-only restore and automatic grouping-index rename/drop are excluded.
set -euo pipefail
. run_services
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR/../br_test_utils.sh"

DB="$TEST_NAME"
# restart_services removes paths starting with br/tidb/tiflash/tikv/pd.
PREFIX="materialized_view_backup"
TASK_NAME="materialized_view_backup"
BASE_MLOG='$mlog$base'
MV_MLOG='$mlog$mv'
CHECKPOINT_DB="__TiDB_BR_Temporary_Log_Restore_Checkpoint"

query_result() {
    run_sql "$1" | sed -n 's/^result: //p'
}

assert_result() {
    local actual
    actual=$(query_result "$1")
    if [[ "$actual" != "$2" ]]; then
        echo "Unexpected result for $1: expected '$2', got '$actual'"
        exit 1
    fi
}

check_rows() {
    assert_result "SELECT GROUP_CONCAT(CONCAT(a, ':', s, ':', cnt) ORDER BY a SEPARATOR ',') AS result FROM $DB.$1" "$2"
}

check_graph() {
    local extra_mv=$1
    local expected_rows=$2
    local replicas=$3
    for view in mv mv_child "$extra_mv"; do
        check_rows "$view" "$expected_rows"
    done
    assert_result "SELECT COUNT(*) AS result FROM information_schema.tables WHERE table_schema='$DB'" "6"
    assert_result "SELECT COUNT(*) AS result FROM information_schema.statistics
        WHERE table_schema='$DB' AND table_name='mv' AND index_name='idx_s'
        AND is_visible='NO' AND index_comment='restored secondary index'" "1"
    # Maintenance rows must refer to the rebuilt target IDs and have fresh progress.
    assert_result "SELECT COUNT(*) AS result FROM mysql.tidb_mview_refresh_info r
        JOIN information_schema.tables t ON r.MVIEW_ID=t.TIDB_TABLE_ID
        WHERE t.TABLE_SCHEMA='$DB' AND r.LAST_SUCCESS_READ_TSO>0
        AND r.NEXT_REFRESH_UNIX_SECONDS IS NOT NULL" "3"
    assert_result "SELECT COUNT(*) AS result FROM mysql.tidb_mlog_purge_info r
        JOIN information_schema.tables t ON r.MLOG_ID=t.TIDB_TABLE_ID
        WHERE t.TABLE_SCHEMA='$DB' AND r.NEXT_PURGE_UNIX_SECONDS IS NOT NULL" "2"
    assert_result "SELECT COUNT(*) AS result FROM mysql.tidb_mview_fast_refresh_source_info r
        JOIN information_schema.tables v ON r.MVIEW_ID=v.TIDB_TABLE_ID
        JOIN information_schema.tables l ON r.MLOG_ID=l.TIDB_TABLE_ID
        WHERE v.TABLE_SCHEMA='$DB' AND l.TABLE_SCHEMA='$DB'" "3"
    # Assert immediately after BR returns, without an additional TiFlash wait.
    assert_result "SELECT COUNT(*) AS result FROM information_schema.tiflash_replica
        WHERE table_schema='$DB' AND replica_count=1 AND available=1" "$replicas"
}

check_fast_refresh() {
    local extra_mv=$1
    run_sql "UPDATE $DB.base SET b=17 WHERE a=1;
        DELETE FROM $DB.base WHERE a<>1;
        INSERT INTO $DB.base VALUES (5,50);
        REFRESH MATERIALIZED VIEW $DB.mv FAST;
        REFRESH MATERIALIZED VIEW $DB.mv_child FAST;
        REFRESH MATERIALIZED VIEW $DB.$extra_mv FAST;"
    for view in mv mv_child "$extra_mv"; do
        check_rows "$view" "1:17:1,5:50:1"
    done
}

run_pitr() {
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$PREFIX/log" \
        --full-backup-storage "local://$TEST_DIR/$PREFIX/full" \
        --restored-ts "$restore_ts" --use-checkpoint=true --wait-tiflash-ready=true
}

expect_pitr_failure() {
    local failpoint=$1
    local message=$2
    local output="$TEST_DIR/$3.log"
    if GO_FAILPOINTS="$failpoint=return(true)" run_pitr >"$output" 2>&1; then
        echo "PITR unexpectedly succeeded with $failpoint"
        exit 1
    fi
    # An unrelated error must not make checkpoint coverage pass accidentally.
    if ! grep -Fq "$message" "$output"; then
        cat "$output"
        exit 1
    fi
}

restart_services
run_sql "CREATE DATABASE $DB;
    CREATE TABLE $DB.base (a INT PRIMARY KEY, b INT NOT NULL);
    INSERT INTO $DB.base VALUES (1,10),(2,20);
    CREATE MATERIALIZED VIEW LOG ON $DB.base (a,b) PURGE NEXT NOW()+INTERVAL 1 HOUR;
    CREATE MATERIALIZED VIEW $DB.mv (a,s,cnt) REFRESH FAST NEXT NOW()+INTERVAL 1 HOUR
        AS SELECT a,SUM(b),COUNT(*) FROM $DB.base GROUP BY a;
    ALTER TABLE $DB.mv ADD INDEX idx_s(s) COMMENT 'restored secondary index' INVISIBLE;
    CREATE MATERIALIZED VIEW LOG ON $DB.mv (a,s,cnt) PURGE NEXT NOW()+INTERVAL 1 HOUR;
    CREATE MATERIALIZED VIEW $DB.mv_child (a,s,cs,cnt) REFRESH FAST NEXT NOW()+INTERVAL 1 HOUR
        AS SELECT a,SUM(s),COUNT(s),COUNT(*) FROM $DB.mv GROUP BY a;
    CREATE MATERIALIZED VIEW $DB.mv_dropped (a,s,cnt) REFRESH FAST NEXT NOW()+INTERVAL 1 HOUR
        AS SELECT a,SUM(b),COUNT(*) FROM $DB.base GROUP BY a;"

for table in base "$BASE_MLOG" mv "$MV_MLOG" mv_child; do
    run_sql "ALTER TABLE $DB.\`$table\` SET TIFLASH REPLICA 1"
done
for attempt in $(seq 1 120); do
    ready=$(query_result "SELECT COUNT(*) AS result FROM information_schema.tiflash_replica
        WHERE table_schema='$DB' AND replica_count=1 AND available=1")
    if [[ "$ready" == "5" ]]; then
        break
    fi
    if [[ "$attempt" == "120" ]]; then
        echo "Source TiFlash replicas did not become available"
        exit 1
    fi
    sleep 3
done

# Keep the source MVs stale so restoring their old SSTs cannot pass the checks.
run_sql "UPDATE $DB.base SET b=11 WHERE a=1;
    DELETE FROM $DB.base WHERE a=2;
    INSERT INTO $DB.base VALUES (3,30);"
check_rows mv "1:10:1,2:20:1"
check_rows mv_child "1:10:1,2:20:1"
run_br --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "local://$TEST_DIR/$PREFIX/log"
run_br --pd "$PD_ADDR" backup db --db "$DB" -s "local://$TEST_DIR/$PREFIX/full"

# These definitions must be reconstructed from log metadata, not just the snapshot.
run_sql "UPDATE $DB.base SET b=15 WHERE a=1;
    DELETE FROM $DB.base WHERE a=3;
    INSERT INTO $DB.base VALUES (4,40);
    DROP MATERIALIZED VIEW $DB.mv_dropped;
    CREATE MATERIALIZED VIEW $DB.mv_new (a,s,cnt) REFRESH FAST NEXT NOW()+INTERVAL 1 HOUR
        AS SELECT a,SUM(b),COUNT(*) FROM $DB.base GROUP BY a;
    ALTER TABLE $DB.mv_new SET TIFLASH REPLICA 1;
    ALTER TABLE $DB.mv_child ADD INDEX idx_cnt(cnt);
    ALTER MATERIALIZED VIEW $DB.mv REFRESH NEXT NOW()+INTERVAL 2 HOUR;"
# Read a base row to activate the transaction before recording its real PD TSO.
restore_ts=$(query_result "BEGIN; SELECT a FROM $DB.base LIMIT 1; SELECT @@tidb_current_ts AS result; COMMIT;")
if [[ ! "$restore_ts" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid restore TSO: $restore_ts"
    exit 1
fi
wait_log_checkpoint_advance "$TASK_NAME"
run_br --pd "$PD_ADDR" log stop --task-name "$TASK_NAME"

# Snapshot-only restore must rebuild against the snapshot's base data.
restart_services
# Force initial builds to use the restored base/parent TiFlash replicas.
run_sql "SET GLOBAL tidb_mv_maintain_isolation_read_engines='tiflash'"
run_br --pd "$PD_ADDR" restore db --db "$DB" -s "local://$TEST_DIR/$PREFIX/full" --wait-tiflash-ready=true
check_graph mv_dropped "1:11:1,3:30:1" "5"
assert_result "SELECT COUNT(*) AS result FROM information_schema.tables
    WHERE table_schema='$DB' AND table_name='mv_new'" "0"
assert_result "SELECT COUNT(*) AS result FROM information_schema.statistics
    WHERE table_schema='$DB' AND table_name='mv_child' AND index_name='idx_cnt'" "0"
check_fast_refresh mv_dropped

# Each PITR starts from a fresh cluster; retries below belong to this one task.
restart_services
run_sql "SET GLOBAL tidb_mv_maintain_isolation_read_engines='tiflash'"
expect_pitr_failure "github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved" \
    "failpoint: failed after id maps saved" "materialized_view_id_map"
assert_result "SELECT COUNT(*)>0 AS result FROM $CHECKPOINT_DB.cpt_mview" "1"

expect_pitr_failure "github.com/pingcap/tidb/br/pkg/task/failed-before-materialized-view-repair" \
    "failpoint: before materialized view repair" "materialized_view_repair"
assert_result "SELECT JSON_EXTRACT(CAST(data AS CHAR), '$.progress') AS result
    FROM $CHECKPOINT_DB.cpt_progress WHERE segment_id=0" "2"
assert_result "SELECT COUNT(*) AS result FROM information_schema.tables WHERE table_schema='$DB'" "1"
base_id=$(query_result "SELECT TIDB_TABLE_ID AS result FROM information_schema.tables
    WHERE table_schema='$DB' AND table_name='base'")

run_pitr
assert_result "SELECT TIDB_TABLE_ID AS result FROM information_schema.tables
    WHERE table_schema='$DB' AND table_name='base'" "$base_id"
check_graph mv_new "1:15:1,4:40:1" "6"
assert_result "SELECT COUNT(*) AS result FROM information_schema.tables
    WHERE table_schema='$DB' AND table_name='mv_dropped'" "0"
assert_result "SELECT COUNT(*) AS result FROM information_schema.statistics
    WHERE table_schema='$DB' AND table_name='mv_child' AND index_name='idx_cnt'" "1"
check_fast_refresh mv_new
run_sql "DROP DATABASE $DB"
