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

# start a new cluster
echo "restart a services"
restart_services

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/delete_range.sql
run_sql_file $CUR/prepare_data/ingest_repair.sql
# ...

# check something after prepare the data
prepare_delete_range_count=$(run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range;" | tail -n 1 | awk '{print $2}')
echo "prepare_delete_range_count: $prepare_delete_range_count"

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name integration_test -s "local://$TEST_DIR/$PREFIX/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"

# load the incremental data
echo "load the incremental data"
run_sql_file $CUR/incremental_data/delete_range.sql
run_sql_file $CUR/incremental_data/ingest_repair.sql
# ...

# check something after load the incremental data
incremental_delete_range_count=$(run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range;" | tail -n 1 | awk '{print $2}')
echo "incremental_delete_range_count: $incremental_delete_range_count"

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
## check feature history ddl delete range
check_not_contains "rewrite delete range"
echo "" > $res_file
echo "check sql result"
run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range group by ts order by DELETE_RANGE_CNT desc limit 1;"
expect_delete_range=$(($incremental_delete_range_count-$prepare_delete_range_count))
check_contains "DELETE_RANGE_CNT: $expect_delete_range"
## check feature compatibility between PITR and accelerate indexing
bash $CUR/check/check_ingest_repair.sh
