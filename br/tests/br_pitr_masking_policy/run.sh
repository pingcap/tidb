#!/bin/bash
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
. run_services
CUR=$(cd "$(dirname "$0")"; pwd)
PREFIX="pitr_masking_policy_backup"
TASK_NAME="br_pitr_masking_policy"
DB="pitr_masking_policy"

stop_services
start_services --no-tiflash
run_sql "CREATE DATABASE $DB;"
for name in added enabled changed dropped; do
    run_sql "CREATE TABLE $DB.$name(c VARCHAR(20)); INSERT INTO $DB.$name VALUES ('secret');"
done
run_sql "CREATE MASKING POLICY p ON $DB.enabled(c) AS 'enabled' DISABLE;"
run_sql "CREATE MASKING POLICY p ON $DB.changed(c) AS 'baseline';"
run_sql "CREATE MASKING POLICY p ON $DB.dropped(c) AS 'baseline';"
run_br --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "local://$TEST_DIR/$PREFIX/log"
run_br --pd "$PD_ADDR" backup full -s "local://$TEST_DIR/$PREFIX/full"

# All these changes occur after the snapshot and must come from log replay.
run_sql "CREATE MASKING POLICY p ON $DB.added(c) AS MASK_FULL(c);"
run_sql "ALTER TABLE $DB.enabled ENABLE MASKING POLICY p;"
run_sql "ALTER TABLE $DB.changed MODIFY MASKING POLICY p SET EXPRESSION='target';"
run_sql "ALTER TABLE $DB.dropped DROP MASKING POLICY p;"
. "$CUR/../br_test_utils.sh"
wait_log_checkpoint_advance "$TASK_NAME"

stop_services
start_services --no-tiflash
run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$PREFIX/log" \
    --full-backup-storage "local://$TEST_DIR/$PREFIX/full" --filter "$DB.*"

assert_value() {
    output=$(run_sql "SELECT c AS masked_value FROM $DB.$1;")
    if ! printf '%s\n' "$output" | grep -q "^masked_value: $2$"; then
        printf 'Unexpected restored value for %s:\n%s\n' "$1" "$output"
        exit 1
    fi
}
assert_value added XXXXXX
assert_value enabled enabled
assert_value changed target
assert_value dropped secret

# Restored policies must also protect data written after recovery.
run_sql "INSERT INTO $DB.added VALUES ('another secret');"
output=$(run_sql "SELECT c AS masked_value FROM $DB.added;")
# MASK_FULL returns one X per character: 6 for 'secret', 14 for 'another secret'.
masked_values=$(printf '%s\n' "$output" | sed -n 's/^masked_value: //p' | sort)
expected_values=$(printf '%s\n' XXXXXX XXXXXXXXXXXXXX)
if [ "$masked_values" != "$expected_values" ]; then
    printf 'Policy did not protect post-restore writes:\n%s\n' "$output"
    exit 1
fi
