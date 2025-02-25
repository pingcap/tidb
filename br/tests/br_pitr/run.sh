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

restart_services_allowing_huge_index() {
    echo "restarting services with huge indices enabled..."
    stop_services
    start_services --tidb-cfg "$CUR/config/tidb-max-index-length.toml"
    echo "restart services done..."
}

# start a new cluster
echo "restart a services"
restart_services_allowing_huge_index

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
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

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
current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# dump some info from upstream cluster
# ...

# start a new cluster
echo "restart a services"
restart_services_allowing_huge_index

# PITR restore
echo "run pitr"
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" > $res_file 2>&1 || ( cat $res_file && exit 1 )

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
