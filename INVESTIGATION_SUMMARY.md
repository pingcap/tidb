# Investigation Summary: Issue #65289

## Problem Statement
Exchange partition followed by creating a global index on a table with NONCLUSTERED primary key causes data/index inconsistency.

### Symptoms
- Global index query returns 3 rows
- Full table scan returns 6 rows (correct)
- `admin check table` reports data inconsistency

## Root Cause Analysis

### Key Finding
After EXCHANGE PARTITION on tables with NONCLUSTERED primary keys, there are **duplicate `_tidb_rowid` values** across different partitions. This is a known issue (see #65067, #64176).

### Example Scenario
```sql
-- After exchange partition:
-- p0: a=2,4 with _tidb_rowid=1,2
-- p1: a=6 with _tidb_rowid=3
-- p2: a=12,14,16 with _tidb_rowid=1,2,3 (from exchanged table)

-- Duplicate _tidb_rowid values exist: 1,2,3 appear in multiple partitions
```

### Related Fix
Issue #65067 was fixed in commit 8227b493ba for UPDATE operations. The fix prevented skipping rows based on duplicate `_tidb_rowid` when updating partitioned non-clustered tables.

## Investigation Findings

### Code Locations Examined
1. **pkg/ddl/partition.go:2938** - ID swap during exchange partition
2. **pkg/ddl/index.go:2562-2582** - Duplicate detection during index backfilling
3. **pkg/ddl/index.go:2494-2506** - Handle comparison for duplicate checking
4. **pkg/tablecodec/tablecodec.go:1006-1038** - DecodeIndexHandle with partition ID support

### Potential Issues
The backfilling process for global indexes may not correctly handle the case where:
1. Multiple partitions have the same `_tidb_rowid` values
2. The partition IDs have been swapped by EXCHANGE PARTITION

### Why Global Indexes Should Work (Theoretically)
- Global index keys encode: `tableID + indexValues + PartitionHandle(partitionID, _tidb_rowid)`
- PartitionHandle includes both partition ID and _tidb_rowid
- Even with duplicate _tidb_rowid, the full handle should be unique across partitions

### Hypothesis
There may be an issue in:
1. How partition IDs are resolved during backfilling after EXCHANGE PARTITION
2. Distributed task execution for global sort (if enabled)
3. Statistics or metadata caching that uses stale partition information
4. The iteration logic over partitions during index creation

## Test Case
Created integration test at: `tests/integrationtest/t/ddl/exchange_partition_global_index.test`

This test:
1. Creates partitioned table with nonclustered PK
2. Inserts data into partitions
3. Exchanges partition p2 with non-partitioned table
4. Creates global index
5. Verifies all rows are accessible via the index

## Proposed Fix Approach

### Investigation Steps
1. Run the integration test to confirm it reproduces the bug
2. Add detailed logging to track which partitions are processed during backfilling
3. Check if partition iteration skips any partitions
4. Verify that PartitionHandle is correctly used throughout

### Potential Fix Locations
1. **Index backfilling code** (`pkg/ddl/index.go`) - Ensure all partitions are processed with correct IDs
2. **Duplicate detection logic** - May need similar fix to #65067 for index creation
3. **Partition iteration** (`getNextPartitionInfo`, `findNextPartitionID`) - Verify correct partition ID resolution

### Similar to #65067 Fix
The fix may need to:
- Detect when a partitioned table has non-clustered index AND may have been subject to EXCHANGE PARTITION
- Ensure deduplication uses full PartitionHandle, not just _tidb_rowid
- Process all partitions even when duplicate _tidb_rowid values exist

## Next Steps
1. Run the integration test: `pushd tests/integrationtest && ./run-tests.sh -r ddl/exchange_partition_global_index && popd`
2. If test fails (expected), add debug logging to identify which partition's data is missing from the index
3. Implement fix based on findings
4. Verify fix with the integration test

## References
- Issue #65289: https://github.com/pingcap/tidb/issues/65289
- Issue #65067: Duplicate _tidb_rowid causing UPDATE to miss rows
- Issue #64176: Duplicate key error on _tidb_rowid
- Fix commit 8227b493ba: UPDATE fix for duplicate _tidb_rowid
