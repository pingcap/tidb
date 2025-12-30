# Issue #65289: Before/After Fix Comparison

This document demonstrates the bug and the fix for global indexes on partitioned tables after EXCHANGE PARTITION.

## Test Scenario

```sql
-- Non-partitioned table with NONCLUSTERED primary key
CREATE TABLE t (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED);

-- Partitioned table with NONCLUSTERED primary key
CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)
PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (5),     -- ID: 119
    PARTITION p1 VALUES LESS THAN (11),    -- ID: 120
    PARTITION p2 VALUES LESS THAN (20)     -- ID: 116
);

-- Insert data
INSERT INTO tp (a,b) VALUES (2,2),(4,4),(6,6);    -- p0: rowid 1,2  p1: rowid 3
INSERT INTO t (a,b) VALUES (12,2),(14,4),(16,6);  -- t: rowid 1,2,3

-- EXCHANGE PARTITION (creates duplicate _tidb_rowid values!)
ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t;

-- Result: p0 has rowid 1,2   p1 has rowid 3   p2 has rowid 1,2,3
--         ^^^ DUPLICATES across partitions! ^^^

-- Create global index
CREATE INDEX idx_b ON tp(b) GLOBAL;
```

---

## BEFORE Fix (Buggy Behavior)

### Test Output

```
=== Issue #65289: Exchange Partition + Global Index Bug Demo ===

Step 1: Create tables with NONCLUSTERED primary key
  ✓ Created partitioned table 'tp' (p0, p1, p2)
  ✓ Created non-partitioned table 't'

Step 2: Insert data
  Table tp (partitioned):
    a=2, b=2, _tidb_rowid=1
    a=4, b=4, _tidb_rowid=2
    a=6, b=6, _tidb_rowid=3

  Table t (non-partitioned):
    a=12, b=2, _tidb_rowid=1
    a=14, b=4, _tidb_rowid=2
    a=16, b=6, _tidb_rowid=3

Step 3: EXCHANGE PARTITION p2 WITH TABLE t
  ✓ Partition p2 now contains rows from table t

  Table tp after exchange (showing duplicate _tidb_rowid values):
    a=2, b=2, _tidb_rowid=1
    a=4, b=4, _tidb_rowid=2
    a=6, b=6, _tidb_rowid=3
    a=12, b=2, _tidb_rowid=1  ⚠️  DUPLICATE!
    a=14, b=4, _tidb_rowid=2  ⚠️  DUPLICATE!
    a=16, b=6, _tidb_rowid=3  ⚠️  DUPLICATE!

  ⚠️  Notice: _tidb_rowid values 1, 2, 3 appear in MULTIPLE partitions!
    - Partition p0 has _tidb_rowid: 1, 2
    - Partition p1 has _tidb_rowid: 3
    - Partition p2 has _tidb_rowid: 1, 2, 3 (duplicates!)

Step 4: Create global index on column 'b'
  ✓ Created global index idx_b

Step 5: Query using the global index

  Result with GLOBAL INDEX (use index):    3 rows   ← ❌ WRONG!
  Result with TABLE SCAN (ignore index):   6 rows   ← ✓ Correct

  ❌ BUG DETECTED: Mismatch between index and table scan!
     The global index is missing rows due to key collisions.
     Rows with same (b, _tidb_rowid) from different partitions overwrote each other.

Step 6: Verify all rows are accessible
  Rows via GLOBAL INDEX:
    a=2, b=2    ← Missing a=12, b=2 from partition p2!
    a=4, b=4    ← Missing a=14, b=4 from partition p2!
    a=6, b=6    ← Missing a=16, b=6 from partition p2!

  Rows via TABLE SCAN:
    a=2, b=2
    a=4, b=4
    a=6, b=6
    a=12, b=2   ← Not in index!
    a=14, b=4   ← Not in index!
    a=16, b=6   ← Not in index!

Step 7: Run admin check table
  ❌ ADMIN CHECK FAILED: [admin:8223]data inconsistency in table: tp, index: idx_b,
     handle: 3, index-values:"" != record-values:"handle: 3, values: [KindInt64 6]"
     This confirms data/index inconsistency!

=== Summary ===
Problem: After EXCHANGE PARTITION, different partitions can have duplicate
         _tidb_rowid values. When creating a non-unique global index,
         rows with same (indexed_value, _tidb_rowid) from different
         partitions would create the same key, causing collisions.

❌ TEST FAILED - BUG IS PRESENT!
```

### Index Keys Generated (BEFORE Fix)

```
Processing Partition p0 (ID=119):
  Row (a=2, b=2, rowid=1):
    Key = [t][118][idx_b_id][b=2][IntHandleFlag(3)][1]
  Row (a=4, b=4, rowid=2):
    Key = [t][118][idx_b_id][b=4][IntHandleFlag(3)][2]

Processing Partition p1 (ID=120):
  Row (a=6, b=6, rowid=3):
    Key = [t][118][idx_b_id][b=6][IntHandleFlag(3)][3]

Processing Partition p2 (ID=116):
  Row (a=12, b=2, rowid=1):
    Key = [t][118][idx_b_id][b=2][IntHandleFlag(3)][1]  ← COLLISION! Overwrites p0's row!
  Row (a=14, b=4, rowid=2):
    Key = [t][118][idx_b_id][b=4][IntHandleFlag(3)][2]  ← COLLISION! Overwrites p0's row!
  Row (a=16, b=6, rowid=3):
    Key = [t][118][idx_b_id][b=6][IntHandleFlag(3)][3]  ← COLLISION! Overwrites p1's row!

Final Result: Only 3 keys in index!
  Key: [...][b=2][3][1] → points to (a=12, partition p2) [a=2 was overwritten]
  Key: [...][b=4][3][2] → points to (a=14, partition p2) [a=4 was overwritten]
  Key: [...][b=6][3][3] → points to (a=16, partition p2) [a=6 was overwritten]
```

### Why It Fails

The key format is: `[table_id][index_id][indexed_values][handle]`

When partition p2 is processed, it generates keys identical to those from p0 and p1 because:
- Same logical table_id (118)
- Same indexed values (b=2, b=4, b=6)
- Same handles (_tidb_rowid = 1, 2, 3)

**Result**: Keys from p2 overwrite keys from p0 and p1 in the KV store!

---

## AFTER Fix (Working Behavior)

### Test Output

```
=== Issue #65289: Exchange Partition + Global Index Bug Demo ===

Step 1: Create tables with NONCLUSTERED primary key
  ✓ Created partitioned table 'tp' (p0, p1, p2)
  ✓ Created non-partitioned table 't'

Step 2: Insert data
  [Same as before...]

Step 3: EXCHANGE PARTITION p2 WITH TABLE t
  ✓ Partition p2 now contains rows from table t
  [Same duplicate _tidb_rowid situation...]

  ⚠️  Notice: _tidb_rowid values 1, 2, 3 appear in MULTIPLE partitions!
    - Partition p0 has _tidb_rowid: 1, 2
    - Partition p1 has _tidb_rowid: 3
    - Partition p2 has _tidb_rowid: 1, 2, 3 (duplicates!)

Step 4: Create global index on column 'b'
  ✓ Created global index idx_b

Step 5: Query using the global index

  Result with GLOBAL INDEX (use index):    6 rows   ← ✓ Correct!
  Result with TABLE SCAN (ignore index):   6 rows   ← ✓ Correct

  ✅ FIX VERIFIED: Index and table scan match!
     The global index properly encodes partition ID to prevent collisions.

Step 6: Verify all rows are accessible
  Rows via GLOBAL INDEX:
    a=2, b=2
    a=4, b=4
    a=6, b=6
    a=12, b=2   ← Now in index!
    a=14, b=4   ← Now in index!
    a=16, b=6   ← Now in index!

  Rows via TABLE SCAN:
    a=2, b=2
    a=4, b=4
    a=6, b=6
    a=12, b=2
    a=14, b=4
    a=16, b=6

Step 7: Run admin check table
  ✅ ADMIN CHECK PASSED
     No data/index inconsistency detected!

=== Summary ===
Problem: After EXCHANGE PARTITION, different partitions can have duplicate
         _tidb_rowid values. When creating a non-unique global index,
         rows with same (indexed_value, _tidb_rowid) from different
         partitions would create the same key, causing collisions.

Solution: For non-unique global indexes, encode BOTH partition_id AND
          _tidb_rowid in the index key to make it unique:
          Key format: [table_id][index_id][indexed_values][partition_id][handle]

✅ ALL TESTS PASSED - FIX IS WORKING!
```

### Index Keys Generated (AFTER Fix)

```
Processing Partition p0 (ID=119):
  Row (a=2, b=2, rowid=1):
    Handle wrapped: PartitionHandle(partID=119, innerHandle=1)
    Key = [t][118][idx_b_id][b=2][PartitionHandleFlag(124)][119][IntHandleFlag(3)][1]
  Row (a=4, b=4, rowid=2):
    Handle wrapped: PartitionHandle(partID=119, innerHandle=2)
    Key = [t][118][idx_b_id][b=4][PartitionHandleFlag(124)][119][IntHandleFlag(3)][2]

Processing Partition p1 (ID=120):
  Row (a=6, b=6, rowid=3):
    Handle wrapped: PartitionHandle(partID=120, innerHandle=3)
    Key = [t][118][idx_b_id][b=6][PartitionHandleFlag(124)][120][IntHandleFlag(3)][3]

Processing Partition p2 (ID=116):
  Row (a=12, b=2, rowid=1):
    Handle wrapped: PartitionHandle(partID=116, innerHandle=1)
    Key = [t][118][idx_b_id][b=2][PartitionHandleFlag(124)][116][IntHandleFlag(3)][1]  ✓ Unique!
  Row (a=14, b=4, rowid=2):
    Handle wrapped: PartitionHandle(partID=116, innerHandle=2)
    Key = [t][118][idx_b_id][b=4][PartitionHandleFlag(124)][116][IntHandleFlag(3)][2]  ✓ Unique!
  Row (a=16, b=6, rowid=3):
    Handle wrapped: PartitionHandle(partID=116, innerHandle=3)
    Key = [t][118][idx_b_id][b=6][PartitionHandleFlag(124)][116][IntHandleFlag(3)][3]  ✓ Unique!

Final Result: All 6 keys in index!
  Key: [...][b=2][124][119][3][1] → points to (a=2, partition p0)
  Key: [...][b=2][124][116][3][1] → points to (a=12, partition p2)
  Key: [...][b=4][124][119][3][2] → points to (a=4, partition p0)
  Key: [...][b=4][124][116][3][2] → points to (a=14, partition p2)
  Key: [...][b=6][124][120][3][3] → points to (a=6, partition p1)
  Key: [...][b=6][124][116][3][3] → points to (a=16, partition p2)
```

### Why It Works

The new key format is: `[table_id][index_id][indexed_values][PartitionHandleFlag][partition_id][handle]`

Now when partition p2 is processed, it generates DIFFERENT keys from p0 and p1 because:
- Same logical table_id (118) ✓
- Same indexed values (b=2, b=4, b=6) ✓
- Same handles (_tidb_rowid = 1, 2, 3) ✓
- **DIFFERENT partition IDs** (119 vs 120 vs 116) ← **This makes keys unique!**

**Result**: All 6 rows are properly indexed with unique keys!

---

## Key Differences Summary

### BEFORE Fix:
- ❌ Index key: `[table][index][values][handle]`
- ❌ Duplicate handles create collisions
- ❌ Only 3 of 6 rows indexed
- ❌ Admin check fails
- ❌ Query results incorrect

### AFTER Fix:
- ✅ Index key: `[table][index][values][flag(124)][partition_id][handle]`
- ✅ Partition ID makes keys unique
- ✅ All 6 rows properly indexed
- ✅ Admin check passes
- ✅ Query results correct

---

## How to Run the Demo

```bash
# Enable failpoint
make failpoint-enable

# Run with BEFORE fix (comment out the fix code):
cd pkg/ddl
go test -v -run TestIssue65289Demo -tags=intest

# Run with AFTER fix (uncomment the fix code):
go test -v -run TestIssue65289Demo -tags=intest

# Disable failpoint
cd ../..
make failpoint-disable
```

The test output will clearly show "BUG DETECTED" vs "FIX VERIFIED" and display the exact row counts and admin check results.
