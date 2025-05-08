#!/bin/bash
#
# Copyright 2024 PingCAP, Inc.
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

set -eu
. run_services
CUR=$(cd `dirname $0`; pwd)

# const value
PREFIX="checkpoint" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
DB=$TEST_NAME
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
TASK_NAME="br_restore_checkpoint"

# start a new cluster
restart_services

# prepare snapshot data
echo "prepare the data"
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
run_sql "CREATE TABLE IF NOT EXISTS $DB.tbl1 (id int, val varchar(20));"
run_sql "CREATE TABLE IF NOT EXISTS $DB.tbl2 (id int, val varchar(20));"
run_sql "INSERT INTO $DB.tbl1 values (1, 'a');"
run_sql "INSERT INTO $DB.tbl2 values (2, 'b');"

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup db --db $DB -s "local://$TEST_DIR/$PREFIX/full"

# prepare incremental data
echo "prepare the incremental data"
run_sql "RENAME TABLE $DB.tbl2 TO $DB.tbl4;"
run_sql "CREATE TABLE IF NOT EXISTS $DB.tbl3 (id int, val varchar(20));"
run_sql "INSERT INTO $DB.tbl1 values (11, 'aa');"
run_sql "INSERT INTO $DB.tbl4 values (22, 'bb');"
run_sql "INSERT INTO $DB.tbl3 values (33, 'cc');"

# wait checkpoint advance
echo "wait checkpoint advance"
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# start a new cluster
restart_services

# PITR but failed in the snapshot restore stage
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return(\"corrupt-last-table-files\")"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success'
    exit 1
fi

# PITR with checkpoint but failed in the log restore metakv stage
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return(\"only-last-table-files\")"
export GO_FAILPOINTS=$GO_FAILPOINTS";github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success, but should fail'
    exit 1
fi

# check the snapshot restore has checkpoint data
run_sql 'select count(*) from '"__TiDB_BR_Temporary_Snapshot_Restore_Checkpoint"'.`cpt_data`;'
check_contains "count(*): 1"

# check the log restore save id map into the table mysql.tidb_pitr_id_map
run_sql 'select count(*) from mysql.tidb_pitr_id_map;'
check_contains "count(*): 1"

# PITR with checkpoint but failed in the log restore datakv stage
# skip the snapshot restore stage
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/corrupt-files=return(\"corrupt-last-table-files\")"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success'
    exit 1
fi

# PITR with checkpoint
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/corrupt-files=return(\"only-last-table-files\")"
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log"
export GO_FAILPOINTS=""

# check the data consistency
# $DB.tbl1 has (1, 'a'), (11, 'aa')
# $DB.tbl4 has (2, 'b'), (22, 'bb')
# $DB.tbl3 has (33, 'cc')
check_result() {
    run_sql "SELECT count(*) AS RESCNT FROM $DB.tbl1;"
    check_contains "RESCNT: 2"
    run_sql "SELECT count(*) AS RESCNT FROM $DB.tbl4;"
    check_contains "RESCNT: 2"
    run_sql "SELECT count(*) AS RESCNT FROM $DB.tbl3;"
    check_contains "RESCNT: 1"
    run_sql "SELECT id, val FROM $DB.tbl1 WHERE val = 'a';"
    check_contains "id: 1"
    run_sql "SELECT id, val FROM $DB.tbl1 WHERE val = 'aa';"
    check_contains "id: 11"
    run_sql "SELECT id, val FROM $DB.tbl4 WHERE val = 'b';"
    check_contains "id: 2"
    run_sql "SELECT id, val FROM $DB.tbl4 WHERE val = 'bb';"
    check_contains "id: 22"
    run_sql "SELECT id, val FROM $DB.tbl3 WHERE val = 'cc';"
    check_contains "id: 33"
}

check_result
# check mysql.tidb_pitr_id_map has data
count=$(run_sql 'select count(*) from mysql.tidb_pitr_id_map;' | awk '/count/{print $2}')
if [ $count -eq 0 ]; then
    echo "the number of pitr id map is $count"
    exit 1
fi

# test if the cluster does not have table mysql.tidb_pitr_id_map
restart_services
run_sql "DROP TABLE IF EXISTS mysql.tidb_pitr_id_map;"
rm -rf $TEST_DIR/$PREFIX/log/pitr_id_maps
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success'
    exit 1
fi

# check the pitr id map is saved in the log storage
count=$(ls $TEST_DIR/$PREFIX/log/pitr_id_maps | wc -l)
if [ $count -ne 1 ]; then
    echo "the number of pitr id map is $count instead of 1"
    exit 1
fi

run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log"
check_result
rm -rf $TEST_DIR/$PREFIX/log/pitr_id_maps

# test if the cluster use checkpoint storage
restart_services
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" --checkpoint-storage "local://$TEST_DIR/$PREFIX/checkpoints" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success'
    exit 1
fi

# check the pitr id map is saved in the checkpoint storage
count=$(ls $TEST_DIR/$PREFIX/log/pitr_id_maps | wc -l)
if [ $count -ne 0 ]; then
    echo "the number of pitr id map is $count instead of 0"
    exit 1
fi
run_sql 'select count(*) from mysql.tidb_pitr_id_map;'
check_contains "count(*): 0"
count=$(ls $TEST_DIR/$PREFIX/checkpoints/pitr_id_maps | wc -l)
if [ $count -ne 1 ]; then
    echo "the number of pitr id map is $count instead of 1"
    exit 1
fi

run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" --checkpoint-storage "local://$TEST_DIR/$PREFIX/checkpoints"
check_result
rm -rf $TEST_DIR/$PREFIX/checkpoints/pitr_id_maps
