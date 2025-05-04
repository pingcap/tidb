#!/bin/sh
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

DB="$TEST_NAME"
TABLES_COUNT=300
LOG_FILE="$TEST_DIR/log_file"
BACKUP_STORAGE="local://$TEST_DIR/${DB}"

#######################################
# Create schema and sample tables for testing.
#######################################
create_test_tables() {
  run_sql "create schema $DB;"
  local i=1
  while [ $i -le $TABLES_COUNT ]; do
      run_sql "create table $DB.sbtest$i (
                  id int primary key,
                  k int not null,
                  c char(120) not null,
                  pad char(60) not null
               );"
      run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
      i=$((i+1))
  done
}

#######################################
# Run backup for the entire db.
#######################################
backup_db() {
  echo "Running backup for database: $DB ..."
  run_br backup db --db "$DB" -s "$BACKUP_STORAGE" --pd "$PD_ADDR"
  echo "Backup finished."
}

#######################################
# Start a restore process in the background with custom arguments.
# Sets RESTORE_PID to the PID of the background restore.
# Arguments:
#   $@ = additional arguments to pass to run_br (e.g., table list, flags)
#######################################
run_restore_in_background() {
  run_br restore "$@" -s "$BACKUP_STORAGE" --pd "$PD_ADDR" &
  RESTORE_PID=$!
}

#######################################
# Wait for the checkpoint stage:
# - Waits for $LOG_FILE creation
# - Checks if region label rule is present (schedule=deny)
#   If not found, kills the restore process and exits with failure.
#######################################
wait_for_checkpoint_stage() {
  echo "Monitoring checkpoint stage (waiting for $LOG_FILE creation)..."
  while [ ! -f "$LOG_FILE" ]; do
      sleep 1
  done
  ensure_region_label_rule_exists || {
    echo "Error: Expected region label rule (schedule=deny) not found."
    kill $RESTORE_PID
    exit 1
  }
  rm -f "$LOG_FILE"
}

#######################################
# Check if region label rule 'schedule=deny' exists.
# Returns 0 if found, 1 if not found.
#######################################
check_region_label_rule_exists() {
  local response exists
  response=$(run_curl "https://${PD_ADDR}/pd/api/v1/config/region-label/rules")
  echo "$response" 
  exists=$(echo "$response" | jq 'any(.[]; .labels[]? | (.key=="schedule" and .value=="deny"))')
  [ "$exists" = "true" ]
}

#######################################
# Exits with 0 if 'schedule=deny' rule is found,
# otherwise returns 1 (for use in if-statements).
#######################################
ensure_region_label_rule_exists() {
  check_region_label_rule_exists
}

#######################################
# Exits with error if 'schedule=deny' rule is still present.
#######################################
ensure_region_label_rule_absent() {
  if check_region_label_rule_exists; then
    echo "Error: Region label rule (schedule=deny) should have been removed."
    exit 1
  fi
}

#######################################
# Perform restore test flow:
#   1) Drop schema $DB.
#   2) Start restore in background (with optional user-specified arguments).
#   3) Wait for checkpoint, check label rule exists, remove log, wait for restore to finish.
#   4) Check label rule is absent afterwards.
# Arguments:
#   $@ = additional arguments passed to run_restore_in_background()
#######################################
perform_restore_test() {
  run_sql "drop schema if exists $DB;"
  run_restore_in_background "$@"
  wait_for_checkpoint_stage
  wait $RESTORE_PID
  ensure_region_label_rule_absent
}

#######################################
# MAIN TEST FLOW
#######################################

# 1. Create tables and backup
create_test_tables
backup_db
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status=return(\"$LOG_FILE\")"

# Test 1: Restore the whole db without checkpoint
echo "=== Test 1: restore the whole db without checkpoint ==="
perform_restore_test db --db "$DB"
echo "Test 1 finished successfully!"

# Test 2: Restore random tables without checkpoint
echo "=== Test 2: restore random tables without checkpoint ==="
# We pick 50 random tables from 1..300
TABLE_LIST=$(shuf -i 1-$TABLES_COUNT -n 50 | awk -v db="$DB" '{printf "-f %s.sbtest%s ", db, $1}')
perform_restore_test full $TABLE_LIST
echo "Test 2 finished successfully!"

# Test 3: Attempt restore with checkpoint (inject corruption to force error)
echo "=== Test 3: restore with checkpoint (injected corruption) ==="
run_sql "drop schema if exists $DB;"

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status=return(\"$LOG_FILE\");github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return(\"corrupt-last-table-files\")"

run_restore_in_background full $TABLE_LIST
wait_for_checkpoint_stage

set +e
wait $RESTORE_PID
exit_code=$?
set -e

if [ $exit_code -eq 0 ]; then
    echo "Error: restore unexpectedly succeeded despite corruption"
    exit 1
fi
ensure_region_label_rule_absent

echo "=== Test 3: retry restore full db with checkpoint enabled ==="

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status=return(\"$LOG_FILE\")"
run_restore_in_background db --db "$DB"
wait_for_checkpoint_stage
wait $RESTORE_PID
ensure_region_label_rule_absent
echo "Test 3 finished successfully!"

# Test 4: Restore full without checkpoint (check deny rule absent)
echo "=== Test4: restore full without checkpoint (check deny rule absent) ==="
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/sleep_for_check_scheduler_status=return(\"$LOG_FILE\")"
run_sql "drop schema if exists $DB;"
run_restore_in_background full
echo "Monitoring checkpoint stage (expecting no deny rule)..."
while [ ! -f "$LOG_FILE" ]; do
    sleep 1
done
ensure_region_label_rule_absent
rm -f "$LOG_FILE"
wait $RESTORE_PID
ensure_region_label_rule_absent
echo "Test4 finished successfully!"

export GO_FAILPOINTS=""
echo "All tests finished successfully!"
exit 0