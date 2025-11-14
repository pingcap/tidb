#!/bin/bash
set -eu
. run_services

PREFIX="blocklist_backup"
TASK_NAME="br_blocklist"

echo "=========================================="
echo "BR Blocklist Integration Test"
echo "=========================================="

# Restart services
restart_services || { echo "Failed to restart services"; exit 1; }

# ==================== 阶段 0: 准备初始数据 ====================
echo ""
echo ">>> Phase 0: Preparing initial data at T0..."
run_sql "CREATE DATABASE original_db;"
run_sql "CREATE TABLE original_db.t1 (id INT PRIMARY KEY, val VARCHAR(50));"
run_sql "INSERT INTO original_db.t1 (id, val) VALUES (1, 'initial_data_row1'), (2, 'initial_data_row2'), (3, 'initial_data_row3');"

# 验证 T0 数据
t1_count=$(run_sql "SELECT COUNT(*) FROM original_db.t1;" | tail -n1)
if [ "$t1_count" != "3" ]; then
    echo "ERROR: Expected 3 rows in t1 at T0, got $t1_count"
    exit 1
fi
echo "✓ T0 data prepared: original_db.t1 has 3 rows"

# ==================== 阶段 1: 快照备份 ====================
echo ""
echo ">>> Phase 1: Creating snapshot backup..."
SNAPSHOT_DIR="local://$TEST_DIR/$PREFIX/full"
run_br backup full -s "$SNAPSHOT_DIR" --pd "$PD_ADDR" \
    --log-file "$TEST_DIR/backup_full.log"
echo "✓ Snapshot backup completed at $SNAPSHOT_DIR"

# ==================== 阶段 2: 启动日志备份 ====================
echo ""
echo ">>> Phase 2: Starting log backup..."
LOG_DIR="local://$TEST_DIR/$PREFIX/log"
run_br log start --task-name "$TASK_NAME" -s "$LOG_DIR" --pd "$PD_ADDR"
echo "✓ Log backup started to $LOG_DIR"

# 等待日志备份任务完全启动
sleep 2

# ==================== 阶段 3: 插入 T1 数据 ====================
echo ""
echo ">>> Phase 3: Inserting data at T1..."
run_sql "INSERT INTO original_db.t1 (id, val) VALUES (4, 'data_at_t1');"
run_sql "CREATE TABLE original_db.t2 (id INT PRIMARY KEY, description TEXT);"
run_sql "INSERT INTO original_db.t2 (id, description) VALUES (100, 't2_data_row1'), (101, 't2_data_row2');"

# 等待数据写入日志备份
sleep 2

# 记录 T1 时间戳
T1=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T1 timestamp: $T1"

# 验证 T1 数据
t1_count=$(run_sql "SELECT COUNT(*) FROM original_db.t1;" | tail -n1)
t2_count=$(run_sql "SELECT COUNT(*) FROM original_db.t2;" | tail -n1)
if [ "$t1_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in t1 at T1, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in t2 at T1, got $t2_count"
    exit 1
fi
echo "✓ T1 data prepared: t1 has 4 rows, t2 has 2 rows"

# ==================== 阶段 4: 插入 T2 数据 ====================
echo ""
echo ">>> Phase 4: Inserting data at T2 (creating new_db)..."
run_sql "CREATE DATABASE new_db;"
run_sql "CREATE TABLE new_db.t3 (id INT PRIMARY KEY, description TEXT);"
run_sql "INSERT INTO new_db.t3 (id, description) VALUES (200, 'new_db_data'), (201, 'created_at_t2');"

# 等待数据写入日志备份
sleep 2

# 记录 T2 时间戳
T2=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T2 timestamp: $T2"

# 验证 T2 数据
new_db_exists=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='new_db';" | tail -n1)
if [ "$new_db_exists" != "1" ]; then
    echo "ERROR: Database new_db should exist"
    exit 1
fi
echo "✓ T2 data prepared: database new_db and table t3 created"

# ==================== 阶段 6: 插入 T3 数据 ====================
echo ""
echo ">>> Phase 6: Inserting data at T3 (operations on new_db)..."
run_sql "INSERT INTO new_db.t3 (id, description) VALUES (202, 'data_at_t3_row1'), (203, 'data_at_t3_row2'), (204, 'data_at_t3_row3');"
run_sql "UPDATE original_db.t1 SET val = 'updated_at_t3' WHERE id = 4;"

# 等待数据写入日志备份
sleep 2

# 记录 T3 时间戳
T3=$(run_br validate decode \
    --field="end-version" \
    --storage="$LOG_DIR" | tail -n1)
echo "✓ T3 timestamp: $T3"

# 验证 T3 数据
t3_count=$(run_sql "SELECT COUNT(*) FROM new_db.t3;" | tail -n1)
if [ "$t3_count" != "5" ]; then
    echo "ERROR: Expected 5 rows in t3 at T3, got $t3_count"
    exit 1
fi
echo "✓ T3 data prepared: new_db.t3 has 5 rows"

echo ""
echo ">>> Data preparation completed"
echo ">>> Timestamps: T1=$T1, T2=$T2, T3=$T3"

# 定义 blocklist 文件目录
blocklist_dir="$TEST_DIR/$PREFIX/log/v1/log_restore_tables_blocklists"

# ==================== 测试 1: 成功场景 - 时间回滚 ====================
echo ""
echo "=========================================="
echo "TEST 1: Time Rollback (T2 → T1)"
echo "=========================================="

# Step 1.1: 删除所有数据
echo ">>> Step 1.1: Dropping all databases..."
run_sql "DROP DATABASE original_db;"
run_sql "DROP DATABASE IF EXISTS new_db;"
echo "✓ Databases dropped"

# Step 1.2: PITR 恢复到 T2
echo ">>> Step 1.2: Restoring to T2..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T2" \
    --log-file "$TEST_DIR/restore_t2.log"
echo "✓ Restored to T2"

# Step 1.3: 验证 T2 数据
echo ">>> Step 1.3: Verifying T2 data..."
t1_count=$(run_sql "SELECT COUNT(*) FROM original_db.t1;" | tail -n1)
t2_count=$(run_sql "SELECT COUNT(*) FROM original_db.t2;" | tail -n1)
t3_count=$(run_sql "SELECT COUNT(*) FROM new_db.t3;" | tail -n1)

if [ "$t1_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in t1 at T2, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in t2 at T2, got $t2_count"
    exit 1
fi
if [ "$t3_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in t3 at T2, got $t3_count"
    exit 1
fi
echo "✓ T2 data verification passed"

# 验证第一个 blocklist 文件生成
blocklist_count=$(find "$blocklist_dir" -name "R*_T*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "1" ]; then
    echo "ERROR: Expected 1 blocklist file after first PITR, found $blocklist_count"
    exit 1
fi
echo "✓ First blocklist file generated"

# Step 1.4: 删除所有数据，准备时间回滚
echo ">>> Step 1.4: Dropping all databases for time rollback..."
run_sql "DROP DATABASE original_db;"
run_sql "DROP DATABASE new_db;"
echo "✓ Databases dropped"

# Step 1.5: PITR 恢复到 T1 (时间回滚: T1 < T2)
echo ">>> Step 1.5: Restoring to T1 (time rollback: T1 < T2)..."
echo ">>> T1=$T1, T2=$T2 (T1 < T2)"
echo ">>> Blocklist check logic:"
echo ">>>   - restoredTs=$T1 < restoreTargetTs=$T2 (filtered)"
echo ">>>   - Expected: SUCCESS (time rollback allowed)"

run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T1" \
    --log-file "$TEST_DIR/restore_t1.log"
echo "✓ Restored to T1"

# Step 1.6: 验证 T1 数据
echo ">>> Step 1.6: Verifying T1 data..."
t1_count=$(run_sql "SELECT COUNT(*) FROM original_db.t1;" | tail -n1)
t2_count=$(run_sql "SELECT COUNT(*) FROM original_db.t2;" | tail -n1)

if [ "$t1_count" != "4" ]; then
    echo "ERROR: Expected 4 rows in t1 at T1, got $t1_count"
    exit 1
fi
if [ "$t2_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in t2 at T1, got $t2_count"
    exit 1
fi
echo "✓ T1 data verification passed (t1: 4 rows, t2: 2 rows)"

# Step 1.7: 验证 new_db 不存在（因为 T1 时还未创建）
new_db_exists=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='new_db';" | tail -n1)
if [ "$new_db_exists" != "0" ]; then
    echo "ERROR: Database new_db should not exist at T1"
    exit 1
fi
echo "✓ Verified: new_db does not exist at T1 (correct)"

# Step 1.8: 验证第二个 blocklist 文件生成
blocklist_count=$(find "$blocklist_dir" -name "R*_T*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "2" ]; then
    echo "ERROR: Expected 2 blocklist files after second PITR, found $blocklist_count"
    exit 1
fi

echo "✓ Second blocklist file generated"
echo "✓✓✓ TEST 1 PASSED: Time rollback works correctly ✓✓✓"

# ==================== 测试 2: 失败场景 - 向前恢复被拦截 ====================
echo ""
echo "=========================================="
echo "TEST 2: Forward Restore to T3 Blocked by Blocklist"
echo "=========================================="

# Step 2.1: 删除所有数据
echo ">>> Step 2.1: Dropping all databases..."
run_sql "DROP DATABASE original_db;"
run_sql "DROP DATABASE IF EXISTS new_db;"
echo "✓ Databases dropped"

# Step 2.2: PITR 恢复到 T2（创建 new_db 和 t2）
echo ">>> Step 2.2: Restoring to T2 to create new_db and t2..."
run_br restore point \
    --pd "$PD_ADDR" \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T2" \
    --log-file "$TEST_DIR/restore_t2_2nd.log"

# 验证 new_db 存在且有 2 行数据
t3_count=$(run_sql "SELECT COUNT(*) FROM new_db.t3;" | tail -n1)
if [ "$t3_count" != "2" ]; then
    echo "ERROR: Expected 2 rows in new_db.t3 at T2, got $t3_count"
    exit 1
fi
echo "✓ PITR to T2 successful: new_db.t3 has 2 rows"

# Step 2.3: 验证第三个 blocklist 文件生成
blocklist_count=$(find "$blocklist_dir" -name "R*_T*.meta" 2>/dev/null | wc -l | tr -d ' ')
if [ "$blocklist_count" != "3" ]; then
    echo "ERROR: Expected 3 blocklist files, found $blocklist_count"
    exit 1
fi
echo "✓ Third blocklist file generated (records new_db and t2 created at T2)"

# Step 2.4: 删除所有数据，准备向前恢复到 T3
echo ">>> Step 2.4: Dropping all databases to simulate data loss..."
run_sql "DROP DATABASE original_db;"
run_sql "DROP DATABASE new_db;"
echo "✓ Databases dropped (new_db and t2 are now deleted locally)"
echo ">>> Note: Log backup still contains T2~T3 operations on new_db.t3"

# Step 2.5: 尝试 PITR 恢复到 T3（应该被 blocklist 拦截）
echo ">>> Step 2.5: Attempting restore to T3 (T3 > T2, should be blocked)..."
echo ">>> T2=$T2, T3=$T3 (T3 > T2)"
echo ">>> Blocklist check logic:"
echo ">>>   - startTs < restoreCommitTs (not filtered)"
echo ">>>   - restoredTs=$T3 >= restoreTargetTs=$T2 (not filtered)"
echo ">>>   - Tracker contains new_db and t2 IDs from blocklist"
echo ">>>   - Expected: BLOCKED (cannot replay T2~T3 ops on deleted tables)"

# 使用 restore point 恢复到 T3
run_br restore point \
    -s "$LOG_DIR" \
    --full-backup-storage "$SNAPSHOT_DIR" \
    --restored-ts "$T3" \
    --pd "$PD_ADDR" \
    --log-file "$TEST_DIR/restore_t3_blocked.log" 2>&1 | tee "$TEST_DIR/restore_t3_output.log" || true

# 检查是否包含 blocklist 相关错误信息
# 根据代码，错误信息格式为：
# "cannot restore the table(Id=%d, name=%s at %d) because it is log restored(at %d) before snapshot backup(at %d)"
# 或
# "cannot restore the database(Id=%d, name %s at %d) because it is log restored(at %d) before snapshot backup(at %d)"

if grep -q "because it is log restored" "$TEST_DIR/restore_t3_output.log"; then
    echo "✓ Restore to T3 correctly blocked by blocklist"

    # 验证错误信息包含相关数据库/表名
    if grep -qE "new_db|t2|t3" "$TEST_DIR/restore_t3_output.log"; then
        echo "✓ Error message contains blocked table/database names (new_db/t2/t3)"
    else
        echo "WARNING: Error message does not contain specific table/database names"
        cat "$TEST_DIR/restore_t3_output.log"
    fi

    echo "✓✓✓ TEST 2 PASSED: Forward restore to T3 blocked correctly ✓✓✓"
else
    # 如果没有被拦截，检查恢复是否意外成功
    new_db_exists=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='new_db';" 2>/dev/null | tail -n1 || echo "0")

    if [ "$new_db_exists" == "0" ]; then
        echo "ERROR: Restore to T3 failed but not due to blocklist"
        echo "Expected blocklist error message but got:"
        cat "$TEST_DIR/restore_t3_output.log"
        exit 1
    else
        echo "ERROR: Restore to T3 succeeded when it should have been blocked by blocklist"
        echo "This indicates a bug in blocklist enforcement"
        echo "Database new_db exists when it shouldn't (trying to replay ops on deleted tables)"
        exit 1
    fi
fi

# ==================== 测试总结 ====================
echo ""
echo "=========================================="
echo "All Tests Passed!"
echo "=========================================="
echo "✓ TEST 1: Time rollback (T2 → T1) works correctly"
echo "✓ TEST 2: Forward restore to T3 blocked by blocklist"
echo ""
echo "Blocklist files generated: $blocklist_count"
ls -lh "$blocklist_dir"

# ==================== 停止日志备份 ====================
echo ""
echo ">>> Stopping log backup after all tests..."
run_br log stop --task-name "$TASK_NAME" --pd "$PD_ADDR"
echo "✓ Log backup stopped"

# ==================== 清理 ====================
echo ""
echo ">>> Cleaning up..."
run_sql "DROP DATABASE IF EXISTS original_db;"
run_sql "DROP DATABASE IF EXISTS new_db;"

# 清理备份目录
rm -rf "$TEST_DIR/$PREFIX"

echo "✓ Test completed successfully"
