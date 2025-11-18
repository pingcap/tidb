#!/bin/bash
set -eu
. run_services

PREFIX="blocklist_backup"
TASK_NAME="br_blocklist"

echo "=========================================="
echo "BR Blocklist Integration Test"
echo "=========================================="
echo "Testing blocklist mechanism with RestoreStartTS"
echo ""

# Restart services
restart_services || { echo "Failed to restart services"; exit 1; }

# ==================== T0: Prepare initial data and create snapshot backup ====================
echo ">>> T0: Preparing initial data and creating snapshot backup..."
run_sql "CREATE DATABASE initial_db;"
run_sql "CREATE TABLE initial_db.t0 (id INT PRIMARY KEY);"
run_sql "INSERT INTO initial_db.t0 VALUES (1);"

# Verify T0 data
t0_count=$(run_sql "SELECT COUNT(*) FROM initial_db.t0;" | tail -n1 | awk '{print $NF}')
if [ "$t0_count" != "1" ]; then
    echo "ERROR: Expected 1 row in initial_db.t0, got $t0_count"
    exit 1
fi
echo "✓ T0 initial data prepared"

# Create full snapshot backup
SNAPSHOT_DIR="local://$TEST_DIR/$PREFIX/full"
run_br backup full -s "$SNAPSHOT_DIR" --pd "$PD_ADDR" \
    --log-file "$TEST_DIR/backup_full.log"
echo "✓ Snapshot backup completed at $SNAPSHOT_DIR"

# Start log backup
LOG_DIR="local://$TEST_DIR/$PREFIX/log"
run_br log start --task-name "$TASK_NAME" -s "$LOG_DIR" --pd "$PD_ADDR"
echo "✓ Log backup started to $LOG_DIR"
sleep 2

# ==================== T1: Create test tables ====================
echo ""
echo ">>> T1: Creating test tables..."
run_sql "CREATE DATABASE test_db;"
run_sql "CREATE TABLE test_db.t1 (id INT PRIMARY KEY);"
run_sql "INSERT INTO test_db.t1 VALUES (1), (2), (3);"
run_sql "CREATE TABLE test_db.t2 (id INT PRIMARY KEY);"
run_sql "INSERT INTO test_db.t2 VALUES (10), (20);"

# Verify T1 data
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
echo "✓ T1 data prepared: test_db.t1 has 3 rows, test_db.t2 has 2 rows"

# ==================== T2: Record first restore target timestamp ====================
echo ""
echo ">>> T2: Recording first restore target timestamp..."
sleep 1  # Ensure log backup has recorded T1 data
T2=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T2 timestamp: $T2 (data state: t1=3 rows, t2=2 rows)"

# ==================== T3: Insert additional data ====================
echo ""
echo ">>> T3: Inserting additional data..."
run_sql "INSERT INTO test_db.t1 VALUES (4), (5);"
run_sql "INSERT INTO test_db.t2 VALUES (30), (40);"

# Verify T3 data
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
echo "✓ T3 data prepared: test_db.t1 has 5 rows, test_db.t2 has 4 rows"

# Record T3 timestamp
sleep 1  # Ensure log backup has recorded new data
T3=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T3 timestamp: $T3 (data state: t1=5 rows, t2=4 rows)"

# ==================== T4: Drop test tables ====================
echo ""
echo ">>> T4: Dropping test tables to simulate data loss..."
run_sql "DROP DATABASE test_db;"
echo "✓ test_db dropped"

# ==================== T6: First PITR restore to T2 ====================
echo ""
echo ">>> T6: First PITR restore to T2..."
echo ">>> This will create blocklist with RestoreStartTs captured from PD"
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T2" \
    --log-file "$TEST_DIR/restore_t2.log"
echo "✓ First PITR completed (restored to T2)"

# Verify first PITR data correctness
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')
if [ "$t1_count" != "3" ]; then
    echo "ERROR: Expected 3 rows in test_db.t1 at T2, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in test_db.t2 at T2, got $t2_count"
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

# ==================== T7: Record timestamp after first PITR completion ====================
echo ""
echo ">>> T7: Recording timestamp after first PITR completion..."
T7=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T7 timestamp: $T7 (first PITR completed)"
echo ">>> Note: Blocklist contains RestoreStartTs ≤ T7"

# ==================== TEST 1: Restore to T3 (should succeed) ====================
echo ""
echo "=========================================="
echo "TEST 1: Restore to T3 (Different Time)"
echo "=========================================="
echo ">>> Objective: Verify blocklist doesn't block valid restore to different historical time"
echo ">>> T2 = $T2 (first PITR target: t1=3 rows)"
echo ">>> T3 = $T3 (second PITR target: t1=5 rows)"
echo ">>> Blocklist check logic:"
echo ">>>   - restoredTs=$T3 < RestoreStartTs (≤T7=$T7)"
echo ">>>   - Expected: SUCCESS (tables didn't exist at T3)"

# T8: Drop all databases
echo ""
echo ">>> T8: Dropping all databases for test..."
run_sql "DROP DATABASE test_db;"
run_sql "DROP DATABASE initial_db;"
echo "✓ Databases dropped"

# T9: Restore to T3
echo ""
echo ">>> T9: Restoring to T3..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T3" \
    --log-file "$TEST_DIR/restore_t3.log"
echo "✓ Restore to T3 completed"

# Verify data correctness: should be T3 data (5 rows), not T2 data (3 rows)
t1_count=$(run_sql "SELECT COUNT(*) FROM test_db.t1;" | tail -n1 | awk '{print $NF}')
t2_count=$(run_sql "SELECT COUNT(*) FROM test_db.t2;" | tail -n1 | awk '{print $NF}')

if [ "$t1_count" != "5" ]; then
    echo "ERROR: Expected 5 rows in test_db.t1 at T3, got $t1_count"
    echo "ERROR: This indicates blocklist incorrectly blocked the restore"
    exit 1
fi

if [ "$t2_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in test_db.t2 at T3, got $t2_count"
    exit 1
fi

echo "✓ Data verification passed: t1 has 5 rows (not 3!), t2 has 4 rows (not 2!)"
echo "✓ Comparison:"
echo "    First PITR  (to T2): t1=3 rows, t2=2 rows"
echo "    Second PITR (to T3): t1=5 rows, t2=4 rows"
echo "✓✓✓ TEST 1 PASSED: Restored to different time with correct data ✓✓✓"

# Verify second blocklist file generation
blocklist_count=$(find "$blocklist_dir" -name "R*_S*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "2" ]; then
    echo "ERROR: Expected 2 blocklist files after second PITR, found $blocklist_count"
    exit 1
fi
echo "✓ Second blocklist file generated"

# ==================== TEST 2: Restore to T7+5s (should be blocked) ====================
echo ""
echo "=========================================="
echo "TEST 2: Restore to T7+5s (Should Be Blocked)"
echo "=========================================="
echo ">>> Objective: Verify blocklist blocks restore when tables don't exist in snapshot"
echo ">>> T7 = $T7 (first PITR completion time)"

# Calculate T7 + 5 seconds TSO
# TSO format: (physical_ms << 18) | logical
# 5 seconds = 5000 ms
DELTA_MS=5000
SHIFT=18
T10=$(( T7 + (DELTA_MS << SHIFT) ))
echo ">>> T10 = $T10 (T7 + 5 seconds)"

echo ">>> Blocklist check logic:"
echo ">>>   - restoredTs=$T10 > RestoreStartTs (≤T7=$T7)"
echo ">>>   - startTs=T0 < RestoreCommitTs (from blocklist)"
echo ">>>   - Snapshot (T0) doesn't contain test_db"
echo ">>>   - Log backup contains operations on test_db"
echo ">>>   - Expected: BLOCKED (conflict detected)"

# Drop all databases before test
echo ""
echo ">>> Dropping all databases for test..."
run_sql "DROP DATABASE test_db;"
run_sql "DROP DATABASE initial_db;"
echo "✓ Databases dropped"

# T10: Attempt to restore to T7+5s
echo ""
echo ">>> T10: Attempting restore to T7+5s (should be blocked)..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T10" \
    --log-file "$TEST_DIR/restore_t10_blocked.log" 2>&1 | tee "$TEST_DIR/restore_t10_output.log" || true

# Check if correctly blocked
if grep -q "because it is log restored" "$TEST_DIR/restore_t10_output.log"; then
    echo "✓ Restore to T10 correctly blocked by blocklist"

    # Verify error message contains table names
    if grep -qE "test_db|t1|t2" "$TEST_DIR/restore_t10_output.log"; then
        echo "✓ Error message contains blocked table/database names"
    else
        echo "WARNING: Error message does not contain specific table names"
        cat "$TEST_DIR/restore_t10_output.log"
    fi

    echo "✓✓✓ TEST 2 PASSED: Forward restore blocked correctly ✓✓✓"
else
    # Check if restore unexpectedly succeeded
    test_db_exists=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='test_db';" 2>/dev/null | tail -n1 | awk '{print $NF}' || echo "0")

    if [ "$test_db_exists" == "0" ]; then
        echo "ERROR: Restore failed but not due to blocklist"
        echo "Expected blocklist error but got:"
        cat "$TEST_DIR/restore_t10_output.log"
        exit 1
    else
        echo "ERROR: Restore to T10 succeeded when it should have been blocked"
        echo "Database test_db exists when it shouldn't"
        exit 1
    fi
fi

# ==================== Test Summary ====================
echo ""
echo "=========================================="
echo "All Tests Passed!"
echo "=========================================="
echo "✓ TEST 1: Restore to different historical time (T3) succeeded"
echo "           - Verified data difference: T2(3 rows) vs T3(5 rows)"
echo "✓ TEST 2: Restore to future time (T7+5s) blocked by blocklist"
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
