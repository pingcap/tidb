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

TASK_NAME="pitr_long_running_schema_loading"
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
DB="$TEST_NAME"

restart_services

run_sql "CREATE SCHEMA $DB;"

# start the log backup
run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

run_sql "USE $DB; CREATE TABLE t1 (id INT PRIMARY KEY, value VARCHAR(255));"
run_sql "USE $DB; INSERT INTO t1 VALUES (1, 'before-backup-1'), (2, 'before-backup-2');"


# do a full backup
run_br --pd "$PD_ADDR" backup full -s "local://$TEST_DIR/$TASK_NAME/full"

run_sql "USE $DB; INSERT INTO t1 VALUES (3, 'after-backup-1'), (4, 'after-backup-2');"
run_sql "USE $DB; DROP TABLE t1;"
run_sql "USE $DB; CREATE TABLE t2 (id INT PRIMARY KEY, data TEXT);"
run_sql "USE $DB; INSERT INTO t2 VALUES (1, 'new-table-data');"

echo "wait checkpoint advance"
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME

restart_services

export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/domain/mock-load-schema-long-time=return(true);github.com/pingcap/tidb/br/pkg/task/post-restore-kv-pending=return(true)"
run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"
export GO_FAILPOINTS=""
