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
TASK_NAME="br_pitr_failpoint"

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
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

# wait until the index creation is running
retry_cnt=0
while true; do
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'running' AND SCHEMA_STATE = 'write reorganization' AND JOB_TYPE = 'add index';"
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
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'done' AND SCHEMA_STATE = 'public' AND JOB_TYPE = 'add index';"
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
    run_sql "ADMIN SHOW DDL JOBS WHERE DB_NAME = 'test' AND TABLE_NAME = 'pairs' AND STATE = 'synced' AND SCHEMA_STATE = 'public' AND JOB_TYPE = 'add index';"
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
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# start a new cluster
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
