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
TASK_NAME="br_pitr_gc_safepoint"

# start a new cluster
echo "restart a services"
restart_services

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$PREFIX/log"

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/delete_range.sql
run_sql_file $CUR/prepare_data/ingest_repair.sql
# ...

# check something after prepare the data
prepare_delete_range_count=$(run_sql "select count(*) DELETE_RANGE_CNT from (select * from mysql.gc_delete_range union all select * from mysql.gc_delete_range_done) del_range;" | tail -n 1 | awk '{print $2}')
echo "prepare_delete_range_count: $prepare_delete_range_count"

# wait checkpoint advance
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

run_br --pd $PD_ADDR log pause --task-name $TASK_NAME

safe_point=$(run_pd_ctl -u https://$PD_ADDR service-gc-safepoint)

# Check if "log-backup-coordinator" exists
log_backup_exists=$(echo "$safe_point" | grep -o '"service_id": "log-backup-coordinator"')
if [ -z "$log_backup_exists" ]; then
  echo '"log-backup-coordinator" does not exist.'
  exit 1
fi

# Extract min_service_gc_safe_point
min_service_gc_safe_point=$(echo "$safe_point" | grep -o '"min_service_gc_safe_point": [0-9]*' | awk '{print $2}')

# Extract safe_point for "log-backup-coordinator"
log_backup_safe_point=$(echo "$safe_point" | grep -A 2 '"service_id": "log-backup-coordinator"' | grep '"safe_point":' | awk '{print $2}' | tr -d ',')

# Compare the points
if (( min_service_gc_safe_point < log_backup_safe_point )); then
  echo '"min_service_gc_safe_point" is less than the safe_point in "log-backup-coordinator".'
else
  echo '"min_service_gc_safe_point" is not less than the safe_point in "log-backup-coordinator".'
  exit 1
fi
