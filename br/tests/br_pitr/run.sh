#!/bin/bash
#
# Copyright 2023 PingCAP, Inc.
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
PREFIX="pitr_backup" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
TASK_NAME="br_pitr"

# start a new cluster
echo "restart a services"
restart_services

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/delete_range.sql
# ...

# check something after prepare the data
prepare_delete_range_count=$(run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range;" | tail -n 1 | awk '{print $2}')
echo "prepare_delete_range_count: $prepare_delete_range_count"

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"

# load the incremental data
echo "load the incremental data"
run_sql_file $CUR/incremental_data/delete_range.sql
# ...

# check something after load the incremental data
incremental_delete_range_count=$(run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range;" | tail -n 1 | awk '{print $2}')
echo "incremental_delete_range_count: $incremental_delete_range_count"

# wait checkpoint advance
current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# dump some info from upstream cluster
# ...

# start a new cluster
echo "restart a services"
restart_services

# PITR restore
echo "run pitr"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" > $res_file 2>&1

# check something in downstream cluster
echo "check br log"
check_contains "restore log success summary"
check_not_contains "rewrite delete range"
echo "" > $res_file
echo "check sql result"
run_sql "select * from mysql.gc_delete_range"
run_sql "select * from mysql.gc_delete_range_done"
run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range group by ts order by DELETE_RANGE_CNT desc limit 1;"
expect_delete_range=$(($incremental_delete_range_count-$prepare_delete_range_count))
check_contains "DELETE_RANGE_CNT: $expect_delete_range"

# start a new cluster for corruption
echo "restart a services"
restart_services

file_corruption() {
    echo "corrupt the whole log files"
    for filename in $(find $TEST_DIR/$PREFIX/log -regex ".*\.log" | grep -v "schema-meta"); do
        echo "corrupt the log file $filename"
        filename_temp=$filename"_temp"
        echo "corruption" > $filename_temp
        cat $filename >> $filename_temp
        mv $filename_temp $filename
        truncate -s -11 $filename
    done
}

# file corruption
file_corruption
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-import-attempt-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'pitr success on file corruption' 
    exit 1
fi

# start a new cluster for corruption
echo "restart a services"
restart_services

file_lost() {
    echo "lost the whole log files"
    for filename in $(find $TEST_DIR/$PREFIX/log -regex ".*\.log" | grep -v "schema-meta"); do
        echo "lost the log file $filename"
        filename_temp=$filename"_temp"
        mv $filename $filename_temp
    done
}

# file lost
file_lost
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-import-attempt-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'pitr success on file lost' 
    exit 1
fi
