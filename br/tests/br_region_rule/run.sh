#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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

DB="$TEST_NAME"
TABLES_COUNT=300
LOG_FILE="$TEST_DIR/log_file"

run_sql "create schema $DB;"

i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "create table $DB.sbtest$i(
                id int primary key,
                k int not null,
                c char(120) not null,
                pad char(60) not null
             );"
    run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
    i=$(($i+1))
done

echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/${DB}" --pd $PD_ADDR

echo "restore start..."
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status=return(\"$LOG_FILE\")"

#Test 1: restore the whole db without checkpoint
run_sql "drop schema $DB;"

run_br restore db --db "$DB" -s "local://$TEST_DIR/${DB}" --pd $PD_ADDR &
RESTORE_PID=$!

echo "Monitoring for checkpoint stage (waiting for log file creation)..."
# Wait for the checkpoint: the restore process will create $LOG_FILE when pausing.
while [ ! -f "$LOG_FILE" ]; do
    sleep 1
done

exists=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | \
         jq 'any(.[]; .labels[]? | (.key=="schedule" and .value=="deny"))')
if [ "$exists" != "true" ]; then
    echo "Error: Expected region label rule (schedule=deny) not found."
    # If the rule is missing, kill the restore process and exit with failure.
    kill $RESTORE_PID
    exit 1
fi

rm -f "$LOG_FILE"
wait $RESTORE_PID

echo "Restore finished successfully."

exists=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | \
         jq 'any(.[]; .labels[]? | (.key=="schedule" and .value=="deny"))')
if [ "$exists" != "false" ]; then
    echo "Error: Region label rule (schedule=deny) should have been removed."
    exit 1
fi

echo "Test 1: restore the whole db without checkpoint finished successfully!"

#Test 2: restore random tables without checkpoint
run_sql "drop schema $DB;"

TABLE_LIST=$(shuf -i 1-300 -n 20 | awk '{printf "-t sbtest%s ", $1}')

run_br restore table --db "$DB" -s "local://$TEST_DIR/${DB}" \
    --pd $PD_ADDR $TABLE_LIST &
RESTORE_PID=$!

echo "Monitoring for checkpoint stage (waiting for log file creation)..."
# Wait for the checkpoint: the restore process will create $LOG_FILE when pausing.
while [ ! -f "$LOG_FILE" ]; do
    sleep 1
done

exists=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | \
         jq 'any(.[]; .labels[]? | (.key=="schedule" and .value=="deny"))')
if [ "$exists" != "true" ]; then
    echo "Error: Expected region label rule (schedule=deny) not found (Test 2)."
    kill $RESTORE_PID
    exit 1
fi

rm -f "$LOG_FILE"
wait $RESTORE_PID

exists=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules" | \
         jq 'any(.[]; .labels[]? | (.key=="schedule" and .value=="deny"))')
if [ "$exists" != "false" ]; then
    echo "Error: Region label rule (schedule=deny) should have been removed (Test 2)."
    exit 1
fi

echo "Test 2: restore random tables without checkpoint finished successfully!"

export GO_FAILPOINTS=""
exit 0