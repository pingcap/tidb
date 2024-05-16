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
PREFIX="pitr_backup_failpoint" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
hint_sig_file_public=$TEST_DIR/hint_sig_file_public
hint_sig_file_history=$TEST_DIR/hint_sig_file_history

# inject some failpoints for TiDB-server
export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/ddl/create-index-stuck-before-public=return(\"$hint_sig_file_public\");\
github.com/pingcap/tidb/pkg/ddl/create-index-stuck-before-ddlhistory=return(\"$hint_sig_file_history\")"

# start a new cluster
echo "restart a services"
restart_services

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/ingest_repair.sql

# prepare the intersect data
run_sql_file $CUR/intersect_data/ingest_repair1.sql &
sql_pid=$!

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name integration_test -s "local://$TEST_DIR/$PREFIX/log"

# wait until the index creation is running
retry_cnt=0
while true; do
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'running' AND SCHEMA_STATE = 'write reorganization' AND JOB_TYPE = 'add index /* ingest */';"
    if grep -Fq "1. row" $res_file; then
        break
    fi

    retry_cnt=$((retry_cnt+1))
    if [ "$retry_cnt" -gt 50 ]; then
        echo 'the wait lag is too large'
        exit 1
    fi

    sleep 1
done

# run snapshot backup 1 -- before the index becomes public
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full-1"

# advance the progress of index creation, make the index become public
touch $hint_sig_file_public

# wait until the index creation is done
retry_cnt=0
while true; do
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'done' AND SCHEMA_STATE = 'public' AND JOB_TYPE = 'add index /* ingest */';"
    if grep -Fq "1. row" $res_file; then
        break
    fi

    retry_cnt=$((retry_cnt+1))
    if [ "$retry_cnt" -gt 50 ]; then
        echo 'the wait lag is too large'
        exit 1
    fi

    sleep 1
done

# run snapshot backup 2 -- before the ddl history is generated
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full-2"

# advance the progress of index creation, generate ddl history
touch $hint_sig_file_history

# wait index creation done
wait $sql_pid

# wait until the index creation is done
retry_cnt=0
while true; do
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'synced' AND SCHEMA_STATE = 'public' AND JOB_TYPE = 'add index /* ingest */';"
    if grep -Fq "1. row" $res_file; then
        break
    fi

    retry_cnt=$((retry_cnt+1))
    if [ "$retry_cnt" -gt 50 ]; then
        echo 'the wait lag is too large'
        exit 1
    fi

    sleep 1
done

# clean the failpoints
export GO_FAILPOINTS=""

# check something in the upstream
run_sql "SHOW INDEX FROM test.pairs WHERE Key_name = 'i1';"
check_contains "Column_name: y"
check_contains "Column_name: z"

# wait checkpoint advance
echo "wait checkpoint advance"
sleep 10
current_ts=$(echo $(($(date +%s%3N) << 18)))
echo "current ts: $current_ts"
i=0
while true; do
    # extract the checkpoint ts of the log backup task. If there is some error, the checkpoint ts should be empty
    log_backup_status=$(unset BR_LOG_TO_TERM && run_br --skip-goleak --pd $PD_ADDR log status --task-name integration_test --json 2>/dev/null)
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

# PITR restore - 1
echo "run pitr 1 -- before the index becomes public"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full-1" > $res_file 2>&1

# check something in downstream cluster
echo "check br log"
check_contains "restore log success summary"
## check feature compatibility between PITR and accelerate indexing
bash $CUR/check/check_ingest_repair.sh

# Clean the data
run_sql "DROP DATABASE test; CREATE DATABASE test;"

# PITR restore - 2
echo "run pitr 2 -- before the index becomes public"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full-2" > $res_file 2>&1

# check something in downstream cluster
echo "check br log"
check_contains "restore log success summary"
## check feature compatibility between PITR and accelerate indexing
bash $CUR/check/check_ingest_repair.sh
