#!/bin/bash
set -eu
. run_services

PREFIX="blocklist_backup"
TASK_NAME="br_blocklist"

# Disable encryption validation for this test
# Log backup doesn't support the same encryption parameters as snapshot backup
export ENABLE_ENCRYPTION_CHECK=false

# Source test utilities for checkpoint wait function
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR/../br_test_utils.sh"

echo "=========================================="
echo "BR Blocklist Integration Test"
echo "=========================================="
echo "Testing blocklist mechanism with RestoreStartTS"
echo ""

# Restart services
restart_services || { echo "Failed to restart services"; exit 1; }

# ==================== T1: Start log backup and prepare test data ====================
echo ""
echo ">>> T1: Starting log backup and preparing test data..."

# Start log backup FIRST
LOG_DIR="local://$TEST_DIR/$PREFIX/log"
run_br log start --task-name "$TASK_NAME" -s "$LOG_DIR" --pd "$PD_ADDR"
echo "✓ Log backup started to $LOG_DIR"
sleep 2

# Create full snapshot backup (after log backup started)
SNAPSHOT_DIR="local://$TEST_DIR/$PREFIX/full"
run_br backup full -s "$SNAPSHOT_DIR" --pd "$PD_ADDR" \
    --log-file "$TEST_DIR/backup_full.log"
echo "✓ Snapshot backup completed at $SNAPSHOT_DIR"
sleep 1

# Prepare initial data
run_sql "CREATE DATABASE initial_db;"
run_sql "CREATE TABLE initial_db.t0 (id INT PRIMARY KEY);"
run_sql "INSERT INTO initial_db.t0 VALUES (1);"

# Verify initial_db data
t0_count=$(run_sql "SELECT COUNT(*) FROM initial_db.t0;" | tail -n1 | awk '{print $NF}')
if [ "$t0_count" != "1" ]; then
    echo "ERROR: Expected 1 row in initial_db.t0, got $t0_count"
    exit 1
fi
echo "✓ initial_db prepared"

# Create test tables and insert data
run_sql "CREATE DATABASE test_db;"
run_sql "CREATE TABLE test_db.t1 (id INT PRIMARY KEY);"
run_sql "INSERT INTO test_db.t1 VALUES (1), (2), (3);"
run_sql "CREATE TABLE test_db.t2 (id INT PRIMARY KEY);"
run_sql "INSERT INTO test_db.t2 VALUES (10), (20);"

# Verify test_db data
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')
if [ "$t1_count" != "3" ]; then
    echo "ERROR: Expected 3 rows in test_db.t1, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in test_db.t2, got $t2_count"
    exit 1
fi
echo "✓ test_db prepared: t1 has 3 rows, t2 has 2 rows"

# Record T1 timestamp
echo ">>> Waiting for log backup checkpoint to advance..."
wait_log_checkpoint_advance "$TASK_NAME"
T1=$LATEST_CHECKPOINT_TS
echo "✓ T1 timestamp: $T1 (data state: t1=3 rows, t2=2 rows)"

# ==================== T2: Insert additional data ====================
echo ""
echo ">>> T2: Inserting additional data..."
run_sql "INSERT INTO test_db.t1 VALUES (4), (5);"
run_sql "INSERT INTO test_db.t2 VALUES (30), (40);"

# Verify T2 data
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')
if [ "$t1_count" != "5" ]; then
    echo "ERROR: Expected 5 rows in test_db.t1, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in test_db.t2, got $t2_count"
    exit 1
fi
echo "✓ test_db updated: t1 has 5 rows, t2 has 4 rows"

# Record T2 timestamp
echo ">>> Waiting for log backup checkpoint to advance..."
wait_log_checkpoint_advance "$TASK_NAME"
T2=$LATEST_CHECKPOINT_TS
echo "✓ T2 timestamp: $T2 (data state: t1=5 rows, t2=4 rows)"

# Drop test tables before first restore
echo ""
echo ">>> Dropping test tables before restore..."
run_sql "DROP DATABASE test_db;"
run_sql "DROP DATABASE initial_db;"
echo "✓ Databases dropped"

# ==================== T3: First PITR restore to T1 ====================
echo ""
echo ">>> T3: First PITR restore to T1..."
echo ">>> This will create blocklist with RestoreStartTs captured from PD"
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T1" \
    --log-file "$TEST_DIR/restore_t1.log"
echo "✓ First PITR completed (restored to T1)"

# Verify first PITR data correctness
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')
if [ "$t1_count" != "3" ]; then
    echo "ERROR: Expected 3 rows in test_db.t1 at T1, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in test_db.t2 at T1, got $t2_count"
    exit 1
fi
echo "✓ First PITR data verification passed (t1: 3 rows, t2: 2 rows)"

# Verify blocklist file generation
blocklist_dir="$TEST_DIR/$PREFIX/log/v1/log_restore_tables_blocklists"
blocklist_count=$(find "$blocklist_dir" -name "R*_S*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "1" ]; then
    echo "ERROR: Expected 1 blocklist file after first PITR, found $blocklist_count"
    exit 1
fi
echo "✓ Blocklist file generated (R*_S*.meta format)"

# Insert new data after first PITR to create time separation
echo ">>> Inserting new data to create time separation..."
run_sql "INSERT INTO test_db.t1 VALUES (6), (7);"
run_sql "INSERT INTO test_db.t2 VALUES (50), (60);"

# Verify data after insertion
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')
if [ "$t1_count" != "5" ]; then
    echo "ERROR: Expected 5 rows in test_db.t1 after insertion, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in test_db.t2 after insertion, got $t2_count"
    exit 1
fi
echo "✓ Data prepared: test_db.t1 has 5 rows (IDs: 1,2,3,6,7), test_db.t2 has 4 rows"

# Record T3 timestamp
echo ">>> Waiting for log backup checkpoint to advance..."
wait_log_checkpoint_advance "$TASK_NAME"
T3=$LATEST_CHECKPOINT_TS
echo "✓ T3 timestamp: $T3 (first PITR completed, new data inserted)"
echo ">>> Note: Blocklist contains RestoreStartTs ≤ T3"

# ==================== TEST 1: Restore to T2 (should succeed) ====================
echo ""
echo "=========================================="
echo "TEST 1: Restore to T2 (Different Time)"
echo "=========================================="
echo ">>> Objective: Verify blocklist doesn't block valid restore to different historical time"
echo ">>> T1 = $T1 (first PITR target: t1=3 rows, t2=2 rows)"
echo ">>> T2 = $T2 (second PITR target: t1=5 rows, t2=4 rows)"
echo ">>> Blocklist check logic:"
echo ">>>   - restoredTs=$T2 < RestoreStartTs (≤T3=$T3)"
echo ">>>   - Expected: SUCCESS (tables didn't exist at T2 in snapshot)"

# Drop all databases for test
echo ""
echo ">>> Dropping all databases for test..."
run_sql "DROP DATABASE test_db;"
run_sql "DROP DATABASE initial_db;"
echo "✓ Databases dropped"

# Restore to T2
echo ""
echo ">>> Restoring to T2..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T2" \
    --log-file "$TEST_DIR/restore_t2.log"
echo "✓ Restore to T2 completed"

# Verify data correctness: should be T2 data (5 rows), not T1 data (3 rows)
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')

if [ "$t1_count" != "5" ]; then
    echo "ERROR: Expected 5 rows in test_db.t1 at T2, got $t1_count"
    echo "ERROR: This indicates blocklist incorrectly blocked the restore"
    exit 1
fi

if [ "$t2_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in test_db.t2 at T2, got $t2_count"
    exit 1
fi

echo "✓ Data verification passed: t1 has 5 rows (not 3!), t2 has 4 rows (not 2!)"
echo "✓ Comparison:"
echo "    First PITR  (to T1): t1=3 rows, t2=2 rows"
echo "    Second PITR (to T2): t1=5 rows, t2=4 rows"
echo "✓✓✓ TEST 1 PASSED: Restored to different time with correct data ✓✓✓"

# Verify second blocklist file generation
blocklist_count=$(find "$blocklist_dir" -name "R*_S*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "2" ]; then
    echo "ERROR: Expected 2 blocklist files after second PITR, found $blocklist_count"
    exit 1
fi
echo "✓ Second blocklist file generated"

# ==================== TEST 2: Restore to T4 (should be blocked) ====================
echo ""
echo "=========================================="
echo "TEST 2: Restore to T4 (Should Be Blocked)"
echo "=========================================="
echo ">>> Objective: Verify blocklist blocks restore when tables don't exist in snapshot"
echo ">>> T3 = $T3 (first PITR completion time)"

# Drop all databases FIRST
echo ""
echo ">>> Dropping all databases..."
run_sql "DROP DATABASE test_db;"
run_sql "DROP DATABASE initial_db;"
echo "✓ Databases dropped"

# Wait for checkpoint to advance AFTER the drop operations
echo ">>> Waiting for checkpoint to advance after database drop..."
wait_log_checkpoint_advance "$TASK_NAME"

# Use the checkpoint ts as T4 (guaranteed to be within log backup range)
T4=$LATEST_CHECKPOINT_TS
echo ">>> T4 = $T4 (checkpoint ts from log backup)"

echo ">>> Blocklist check logic:"
echo ">>>   - restoredTs=$T4 > RestoreStartTs (≤T3=$T3)"
echo ">>>   - startTs=snapshot_ts < RestoreCommitTs (from blocklist)"
echo ">>>   - Snapshot doesn't contain test_db"
echo ">>>   - Log backup contains operations on test_db"
echo ">>>   - Expected: BLOCKED (conflict detected)"

# Attempt to restore to T4
echo ""
echo ">>> Attempting restore to T4 (should be blocked)..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T4" \
    --log-file "$TEST_DIR/restore_t4_blocked.log" 2>&1 | tee "$TEST_DIR/restore_t4_output.log" || true

# Check if correctly blocked
if grep -q "because it is log restored" "$TEST_DIR/restore_t4_output.log"; then
    echo "✓ Restore to T4 correctly blocked by blocklist"

    # Verify error message contains table names
    if grep -qE "test_db|t1|t2" "$TEST_DIR/restore_t4_output.log"; then
        echo "✓ Error message contains blocked table/database names"
    else
        echo "WARNING: Error message does not contain specific table names"
        cat "$TEST_DIR/restore_t4_output.log"
    fi

    echo "✓✓✓ TEST 2 PASSED: Forward restore blocked correctly ✓✓✓"
else
    # Check if restore unexpectedly succeeded
    test_db_exists=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='test_db';" 2>/dev/null | tail -n1 | awk '{print $NF}' || echo "0")

    if [ "$test_db_exists" == "0" ]; then
        echo "ERROR: Restore failed but not due to blocklist"
        echo "Expected blocklist error but got:"
        cat "$TEST_DIR/restore_t4_output.log"
        exit 1
    else
        echo "ERROR: Restore to T4 succeeded when it should have been blocked"
        echo "Database test_db exists when it shouldn't"
        exit 1
    fi
fi

# ==================== Test Summary ====================
echo ""
echo "=========================================="
echo "All Tests Passed!"
echo "=========================================="
echo "✓ TEST 1: Restore to different historical time (T2) succeeded"
echo "           - Verified data difference: T1(3 rows) vs T2(5 rows)"
echo "✓ TEST 2: Restore to future time (T4 > T3) blocked by blocklist"
echo ""

echo ">>> Blocklist files summary:"
blocklist_count=$(find "$blocklist_dir" -name "R*_S*.meta" 2>/dev/null | wc -l | tr -d ' ')
echo "Total blocklist files: $blocklist_count"
ls -lh "$blocklist_dir" || true

# ==================== Stop log backup ====================
echo ""
echo ">>> Stopping log backup..."
run_br log stop --task-name "$TASK_NAME" --pd "$PD_ADDR"
echo "✓ Log backup stopped"

# ==================== Cleanup ====================
echo ""
echo ">>> Cleaning up..."
run_sql "DROP DATABASE IF EXISTS test_db;"
run_sql "DROP DATABASE IF EXISTS initial_db;"

# Clean up backup directory
rm -rf "$TEST_DIR/$PREFIX"

echo "✓ Test completed successfully"
