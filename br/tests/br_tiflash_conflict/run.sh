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

# we need to keep backup data after restart service
source $UTILS_DIR/run_services
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
backup_dir=$TEST_DIR/keep/${TEST_NAME}
pitr_dir=${backup_dir}_pitr
br_log_file=$TEST_DIR/br.log
TASK_NAME="br_tiflash_conflict"

# start a new cluster
echo "restart a services"
restart_services

# prepare the data
echo "prepare the data"
run_sql_file $CUR/prepare_data/prepare_data.sql

#run pitr backup
echo "run pitr backup"
run_br log start --task-name $TASK_NAME -s "local://$pitr_dir"

# run snapshot backup
echo "run snapshot backup"
run_br backup full --log-file $br_log_file -s "local://$backup_dir"

start_ts=$(echo $(($(date +%s%3N) << 18)))
echo "start ts: $start_ts"

# load the incremental data
echo "load the incremental data"
run_sql_file $CUR/prepare_data/insert_data.sql

# wait checkpoint advance
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

# start a new cluster
echo "restart a services"
restart_services

# pitr restore to tso0
echo "pitr restore to tso0"
run_br restore point -s "local://$pitr_dir" --full-backup-storage "local://$backup_dir" --restored-ts $start_ts

# pitr restore to tso1
echo "pitr restore to tso1"
RESTORE_LOG=$TEST_DIR/restore.log
if run_br restore point -s "local://$pitr_dir" --start-ts $start_ts 2> $RESTORE_LOG; then
    echo "TEST: [$TEST_NAME] restore success, but it should fail"
    exit 1
fi

run_sql "DROP DATABASE IF EXISTS test;"

# check if contain the error message
echo "check br log"
if ! grep -q "have tiflash replica, please remove it before restore" "$RESTORE_LOG"; then
    echo "The log doesn't contains the message: 'have tiflash replica, please remove it before restore'"
    exit 1
fi

#check status of Tiflash
if ! run_curl "https://$TIFLASH_HTTP/tiflash/store-status" 1>/dev/null 2>&1; then
    echo "Tiflash is not running"
    exit 1
fi
