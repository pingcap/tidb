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

set -eu
. run_services
CUR=$(cd `dirname $0`; pwd)

# const value
PREFIX="pitr_backup" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
TASK_NAME="br_pitr_log_restore_backup_compatibility"

restart_services

# prepare the data
run_sql "create database if not exists test"
run_sql "create table test.t1 (id int)"
run_sql "insert into test.t1 values (1), (10), (100)"

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"

# prepare the incremental data
run_sql "create table test.t2(id int)"
run_sql "insert into test.t1 values (11), (111)"
run_sql "insert into test.t2 values (2), (20), (200)"

# get the checkpoint ts
sleep 5
ok_restored_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
sleep 5

## prepare another log restore

# prepare the data
run_sql "create table test.t3(id int)"
run_sql "insert into test.t3 values (3), (30), (300)"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full2"

# prepare the incremental data
run_sql "insert into test.t3 values (33), (333)"

# wait checkpoint advance
sleep 5
restored_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# clean the table test.t3
run_sql "drop table test.t3"

# run PITR restore
echo "run PITR restore"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full2" --filter test.t3 --restored-ts $restored_ts

# check the blocklist file
if [ -z "$(ls -A $TEST_DIR/$PREFIX/log/v1/log_restore_tables_blocklists)" ]; then
    echo "Error: no blocklist is saved"
    exit 1
fi

sleep 5
truncate_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")

# prepare the data
run_sql "create table test.t4(id int)"
run_sql "insert into test.t4 values (4), (40), (400)"

# snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full3"

# prepare the incremental data
run_sql "insert into test.t4 values (44), (444)"

# wait checkpoint advance
sleep 5
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

## test log restore with block list

# pass because restored ts is less than BackupTS of snapshot backup 2
restart_services
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" --restored-ts $ok_restored_ts
run_sql "select sum(id) as SUM from test.t1"
check_contains "SUM: 233"

# pass because backup ts is larger than restore commit ts
restart_services
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full3"
run_sql "select sum(id) as SUM from test.t1"
check_contains "SUM: 233"
run_sql "select sum(id) as SUM from test.t2"
check_contains "SUM: 222"
run_sql "select sum(id) as SUM from test.t3"
check_contains "SUM: 699"
run_sql "select sum(id) as SUM from test.t4"
check_contains "SUM: 932"

# otherwise, failed
restart_services
success=true
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" > $res_file 2>&1 || success=false
if $success; then
    echo "Error: PITR restore must be failed"
    exit 1
fi
check_contains "cannot restore the table"

# truncate the blocklist
run_br log truncate -s "local://$TEST_DIR/$PREFIX/log" --until $ok_restored_ts -y
if [ -z "$(ls -A $TEST_DIR/$PREFIX/log/v1/log_restore_tables_blocklists)" ]; then
    echo "Error: blocklist is truncated"
    exit 1
fi

run_br log truncate -s "local://$TEST_DIR/$PREFIX/log" --until $truncate_ts -y
if [ -z "$(ls -A $TEST_DIR/$PREFIX/log/v1/log_restore_tables_blocklists)" ]; then
    echo "blocklist is truncated"
else
    echo "Error: blocklist is not truncated"
    exit 1
fi
