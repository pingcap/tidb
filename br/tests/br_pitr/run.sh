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
WAIT_DONE_CODE=50
WAIT_NOT_DONE_CODE=51
PREFIX="pitr_backup" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/delete_range.sql
# ...

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name integration_test -s "local://$TEST_DIR/$PREFIX/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"

# load the incremental data
echo "load the incremental data"
run_sql_file $CUR/incremental_data/delete_range.sql
# ...

# wait checkpoint advance
echo "wait checkpoint advance"
sleep 10
now_time=$(date "+%Y-%m-%d %H:%M:%S %z")
echo "get the current time: $now_time"
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/stream/only-checkpoint-ts-with-check=return(\"$now_time\")"
set +e # we need to get the exit code of br
i=0
while true; do
    # run br with failpoint to compare the current checkpoint ts with the current time
    run_br --pd $PD_ADDR log status --task-name integration_test
    exit_code=$?
    echo "exit code: $exit_code"
    
    # the checkpoint has advanced
    if [ $exit_code -eq $WAIT_DONE_CODE ]; then
        echo "the checkpoint has advanced"
        break
    fi

    # the checkpoint hasn't advanced
    if [ $exit_code -eq $WAIT_NOT_DONE_CODE ]; then
        echo "the checkpoint hasn't advanced"
        i=$((i+1))
        if [ "$i" -gt 50 ]; then
            echo 'the checkpoint lag is too large'
            exit 1
        fi
        sleep 10
        continue
    fi

    # unknown status, maybe somewhere is wrong
    echo "TEST: [$TEST_NAME] failed to wait checkpoint advance!"
    exit 1
done
set -e

# dump some info from upstream cluster
# ...

# start a new cluster
echo "restart a services"
restart_services

# PITR restore
echo "run pitr"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full"

# check something in downstream cluster
echo "check something"
run_sql "select count(*) DELETE_RANGE_CNT from mysql.gc_delete_range group by ts order by DELETE_RANGE_CNT desc limit 1;"
check_contains "DELETE_RANGE_CNT: 44"
