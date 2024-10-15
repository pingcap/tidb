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

# start a new cluster
echo "restart a services"
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
run_br --pd $PD_ADDR log start --task-name integration_test -s "local://$TEST_DIR/$PREFIX/log"

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
sleep 10
current_ts=$(echo $(($(date +%s%3N) << 18)))
echo "current ts: $current_ts"
i=0
while true; do
    # extract the checkpoint ts of the log backup task. If there is some error, the checkpoint ts should be empty
    log_backup_status=$(unset BR_LOG_TO_TERM && run_br --skip-goleak --pd $PD_ADDR log status --task-name integration_test --json 2>br.log)
    echo "log backup status: $log_backup_status"
    checkpoint_ts=$(echo "$log_backup_status" | head -n 1 | jq 'if .[0].last_errors | length  == 0 then .[0].checkpoint else empty end')
    echo "checkpoint ts: $checkpoint_ts"

    # check whether the checkpoint ts is a number
    if [ $checkpoint_ts -gt 0 ] 2>/dev/null; then
        # check whether the checkpoint has advanced
        if [ $checkpoint_ts -gt $current_ts ]; then
            echo "the checkpoint has advanced"
            break
        fi
        # the checkpoint hasn't advanced
        echo "the checkpoint hasn't advanced"
        i=$((i+1))
        if [ "$i" -gt 50 ]; then
            echo 'the checkpoint lag is too large'
            exit 1
        fi
        sleep 10
    else
        # unknown status, maybe somewhere is wrong
        echo "TEST: [$TEST_NAME] failed to wait checkpoint advance!"
        exit 1
    fi
done

# start a new cluster
echo "restart a services"
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
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return(\"only-last-table-files\");\
github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$PREFIX/full" -s "local://$TEST_DIR/$PREFIX/log" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'PITR success'
    exit 1
fi

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
