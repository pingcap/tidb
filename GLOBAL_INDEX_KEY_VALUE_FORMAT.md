# Global Index Key+Value Format Reference

This document describes the key and value encoding formats for global indexes on partitioned tables in TiDB, covering all combinations of clustered/non-clustered tables and unique/non-unique indexes.

## Table of Contents
1. [Background](#background)
2. [Format Matrix](#format-matrix)
3. [Detailed Formats](#detailed-formats)
4. [Examples](#examples)

---

## Background

### Key Concepts

- **Clustered Table**: Primary key columns are stored inline with the row data. The handle IS the primary key.
- **Non-Clustered Table**: Primary key is stored separately. An implicit `_tidb_rowid` (IntHandle) is used as the row handle.
- **Global Index**: Index that spans all partitions (vs local index per partition). Uses logical table ID instead of physical partition ID.
- **Distinct (Unique) Index**: No duplicate values allowed. Handle may be stored in VALUE instead of KEY.
- **Non-Distinct (Non-Unique) Index**: Duplicates allowed. Handle MUST be in KEY to ensure uniqueness.

### The Duplicate Handle Problem

After `EXCHANGE PARTITION`, different partitions can have duplicate `_tidb_rowid` values:
```
Partition p0: rows with _tidb_rowid = 1, 2
Partition p1: rows with _tidb_rowid = 3
Partition p2: rows with _tidb_rowid = 1, 2, 3  ← DUPLICATES!
```

For non-unique global indexes, this creates key collisions without the fix.

---

## Format Matrix

| Table Type | Index Type | Key Contains | Value Contains | Notes |
|------------|------------|--------------|----------------|-------|
| **Non-Clustered** | Unique (distinct=true) | `[prefix][logicalTblID][idxID][indexed_values]` | `[partition_id][handle]` | Handle in value, partition ID in value |
| **Non-Clustered** | **Non-Unique (distinct=false)** | `[prefix][logicalTblID][idxID][indexed_values][PartitionIDFlag][partition_id][handle]` | `[partition_id]` | **FIX**: Both partition_id and handle in key! |
| **Clustered** | Unique (distinct=true) | `[prefix][logicalTblID][idxID][indexed_values]` | `[partition_id][commonhandle]` | CommonHandle in value, partition ID in value |
| **Clustered** | **Non-Unique (distinct=false)** | `[prefix][logicalTblID][idxID][indexed_values][PartitionIDFlag][partition_id][commonhandle]` | `[partition_id]` | **FIX**: Both partition_id and commonhandle in key! |

---

## Detailed Formats

### 1. Non-Clustered Table, Unique Global Index

**Scenario**: `PRIMARY KEY (a) NONCLUSTERED`, `UNIQUE INDEX idx_b (b) GLOBAL`

#### Key Format:
```
[TablePrefix] + [LogicalTableID] + [IndexID] + EncodeKey(indexed_values)
```
- **TablePrefix**: `'t'` (1 byte)
- **LogicalTableID**: Table ID (8 bytes, shared across all partitions)
- **IndexID**: Index ID (8 bytes)
- **indexed_values**: Encoded indexed column values (variable length)
- **NO HANDLE** in key because it's unique (distinct=true)

#### Value Format:
```
[tailLen] + [IndexVersionFlag] + [version] + [PartitionIDFlag] + [partition_id] + [IntHandleFlag] + [handle] + [padding]
```
- **tailLen**: 1 byte indicating length of tail
- **PartitionIDFlag**: 126 (1 byte)
- **partition_id**: 8 bytes
- **IntHandleFlag**: 3 (1 byte)
- **handle**: 8 bytes (the `_tidb_rowid`)

**Example**:
```
Key:   't' + table_id(118) + index_id(2) + encode(b=2)
Value: [10] + [125,1] + [126] + partition_id(119) + [3] + handle(1) + padding
```

---

### 2. Non-Clustered Table, Non-Unique Global Index (THE FIX)

**Scenario**: `PRIMARY KEY (a) NONCLUSTERED`, `INDEX idx_b (b) GLOBAL`

#### Key Format (WITH FIX):
```
[TablePrefix] + [LogicalTableID] + [IndexID] + EncodeKey(indexed_values) +
[PartitionIDFlag] + [partition_id] + [IntHandleFlag] + [handle]
```
- **PartitionIDFlag**: 126 (1 byte) ← **Reused from value encoding**
- **partition_id**: 8 bytes (encoded with EncodeInt)
- **IntHandleFlag**: 3 (1 byte)
- **handle**: 8 bytes (the `_tidb_rowid`, encoded with EncodeInt)

#### Value Format:
```
[tailLen] + [PartitionIDFlag] + [partition_id] + [padding]
```
- Partition ID also in value for consistency
- Value is simpler since handle is in key

**Why This Fix is Needed**:

Without the partition_id in the key, rows from different partitions with the same (b, _tidb_rowid) create identical keys:

```
BEFORE FIX (BROKEN):
Partition p0, row (b=2, _tidb_rowid=1): Key = [...][b=2][3][1]
Partition p2, row (b=2, _tidb_rowid=1): Key = [...][b=2][3][1]  ← COLLISION!
                                        └→ Same key overwrites!

AFTER FIX (WORKING):
Partition p0, row (b=2, _tidb_rowid=1): Key = [...][b=2][124][p0_id][3][1]
Partition p2, row (b=2, _tidb_rowid=1): Key = [...][b=2][124][p2_id][3][1]
                                        └→ Different keys! ✓
```

**Example**:
```
Partition p0 (ID=119), row (b=2, _tidb_rowid=1):
  Key:   't' + table_id(118) + index_id(2) + encode(b=2) + [124] + partition_id(119) + [3] + handle(1)
  Value: [9] + [126] + partition_id(119) + padding

Partition p2 (ID=116), row (b=2, _tidb_rowid=1):
  Key:   't' + table_id(118) + index_id(2) + encode(b=2) + [124] + partition_id(116) + [3] + handle(1)
  Value: [9] + [126] + partition_id(116) + padding

→ Different keys prevent collision!
```

---

### 3. Clustered Table, Unique Global Index

**Scenario**: `PRIMARY KEY (a, b)`, `UNIQUE INDEX idx_c (c) GLOBAL`

#### Key Format:
```
[TablePrefix] + [LogicalTableID] + [IndexID] + EncodeKey(indexed_values)
```
- No handle in key (distinct=true)

#### Value Format:
```
[tailLen] + [IndexVersionFlag] + [version] + [CommonHandleFlag] + [handleLen] + [commonhandle] +
[PartitionIDFlag] + [partition_id] + [padding]
```
- **CommonHandleFlag**: 127 (1 byte)
- **handleLen**: 2 bytes (length of encoded common handle)
- **commonhandle**: Encoded primary key columns
- **PartitionIDFlag**: 126 (1 byte)
- **partition_id**: 8 bytes

**Example**:
```
Key:   't' + table_id(118) + index_id(2) + encode(c=100)
Value: [20] + [125,1] + [127] + handleLen(10) + encode(a=1,b=2) + [126] + partition_id(119) + padding
```

---

### 4. Clustered Table, Non-Unique Global Index (THE FIX)

**Scenario**: `PRIMARY KEY (a, b)`, `INDEX idx_c (c) GLOBAL`

#### Key Format (WITH FIX):
```
[TablePrefix] + [LogicalTableID] + [IndexID] + EncodeKey(indexed_values) +
[PartitionIDFlag] + [partition_id] + [commonhandle]
```
- **PartitionIDFlag** | **126 (1 byte) ← **NEW FLAG**
- **partition_id**: 8 bytes
- **commonhandle**: Encoded primary key columns (variable length)

#### Value Format:
```
[tailLen] + [PartitionIDFlag] + [partition_id] + [padding]
```

**Example**:
```
Partition p0 (ID=119), row (a=1, b=2, c=100):
  Key:   't' + table_id(118) + index_id(2) + encode(c=100) + [124] + partition_id(119) + encode(a=1,b=2)
  Value: [9] + [126] + partition_id(119) + padding

Partition p2 (ID=116), row (a=1, b=2, c=100):
  Key:   't' + table_id(118) + index_id(2) + encode(c=100) + [124] + partition_id(116) + encode(a=1,b=2)
  Value: [9] + [126] + partition_id(116) + padding

→ Different keys prevent collision (even though commonhandle is same)!
```

---

## Examples

### Complete Example: Non-Clustered Table After EXCHANGE PARTITION

```sql
-- Setup
CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)
PARTITION BY RANGE (a) (
  PARTITION p0 VALUES LESS THAN (5),
  PARTITION p1 VALUES LESS THAN (11),
  PARTITION p2 VALUES LESS THAN (20)
);

CREATE TABLE t (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED);

-- Insert data
INSERT INTO tp VALUES (2,2), (4,4), (6,6);    -- p0: rowid 1,2  p1: rowid 3
INSERT INTO t VALUES (12,2), (14,4), (16,6);  -- t: rowid 1,2,3

-- Exchange creates duplicate rowids!
ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t;
-- Now: p0: rowid 1,2  p1: rowid 3  p2: rowid 1,2,3 ← DUPLICATES

-- Create global index
CREATE INDEX idx_b ON tp(b) GLOBAL;
```

### Index Keys Generated:

**Without Fix (BROKEN - 3 keys instead of 6):**
```
Key: [...][b=2][3][1]  ← from p0 (a=2)
Key: [...][b=2][3][1]  ← from p2 (a=12) OVERWRITES ABOVE!
Key: [...][b=4][3][2]  ← from p0 (a=4)
Key: [...][b=4][3][2]  ← from p2 (a=14) OVERWRITES ABOVE!
Key: [...][b=6][3][3]  ← from p1 (a=6)
Key: [...][b=6][3][3]  ← from p2 (a=16) OVERWRITES ABOVE!
Result: Only 3 keys stored! Missing 3 rows!
```

**With Fix (WORKING - 6 unique keys):**
```
Key: [...][b=2][124][119][3][1]  ← from p0 (a=2, partition 119)
Key: [...][b=2][124][116][3][1]  ← from p2 (a=12, partition 116) ✓ Different!
Key: [...][b=4][124][119][3][2]  ← from p0 (a=4, partition 119)
Key: [...][b=4][124][116][3][2]  ← from p2 (a=14, partition 116) ✓ Different!
Key: [...][b=6][124][120][3][3]  ← from p1 (a=6, partition 120)
Key: [...][b=6][124][116][3][3]  ← from p2 (a=16, partition 116) ✓ Different!
Result: All 6 keys stored! All rows indexed!
```

---

## Flag Bytes Reference

| Flag Byte | Value | Usage |
|-----------|-------|-------|
| IntHandleFlag | 3 | Indicates an int64 handle follows |
| CommonHandleFlag | 127 | Indicates a common handle follows (in value) |
| **PartitionIDFlag** | **126** | **Indicates partition ID follows (in value or in key for V2+ global indexes)** |
| IndexVersionFlag | 125 | Indicates index version info follows |

---

## Implementation Files

1. **pkg/tablecodec/tablecodec.go**:
   - `PartitionIDFlag` constant - Reused for both key and value encoding
   - `GenIndexKey()` - Encodes partition ID + handle in key for V2+ global indexes
   - `decodeHandleInIndexKey()` - Decodes partition ID + handle from key

2. **pkg/ddl/index.go**:
   - `fetchRowColVals()` - Wraps handles with PartitionHandle for global indexes (line 2447-2468)

3. **pkg/table/tables/index.go**:
   - `GenIndexValue()` - Extracts partition ID from PartitionHandle for value (line 207-216)

---

## Summary

The key insight is that for **non-unique global indexes on partitioned tables**:

- ✅ **KEY must contain**: `[indexed_values][PartitionIDFlag][partition_id][handle]`
- ✅ **VALUE must contain**: `[partition_id]`

This prevents key collisions when different partitions have duplicate handles (like after EXCHANGE PARTITION), ensuring all rows are properly indexed.

For unique global indexes, the handle can remain in the value since the indexed values themselves enforce uniqueness.
