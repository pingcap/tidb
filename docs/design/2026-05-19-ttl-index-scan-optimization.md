# TTL Secondary Index Scan Optimization

## Background

TiDB's TTL (Time-To-Live) feature is designed for tables where the TTL column does not have a secondary index. When a secondary index exists on the TTL column, the current TTL design cannot take advantage of it and may actually degrade performance.

The root cause is that TTL splits scan tasks and batches queries based on the **primary key** order:

```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

When the optimizer chooses a secondary index on the TTL column, this query structure causes two problems:

1. **Duplicate scan by split range**: The scan task is split into multiple subtasks (default 64) by primary key ranges. Each subtask scans the index independently, causing at least 64 full index range scans.

2. **Duplicate scan by pagination**: Each subtask is paginated by primary key with a limited batch size (`tidb_ttl_scan_batch_size`, default 500). Every page requires re-scanning and re-sorting the index by primary key. If there are many expired rows, the index range is scanned `EXPIRED_DATA_ROWS / 500` times per subtask.

In total, the index may be scanned `(EXPIRED_DATA_ROWS / 500) * subtask_count` times. When `(TTL_JOB_INTERVAL / DATA_LIFE_TIME)^2 * TABLE_SIZE` is greater than 1, using the secondary index becomes slower than a full table scan.

This issue has been encountered by multiple customers.

## Goals

1. Allow TTL to leverage secondary indexes on the TTL column by ordering and splitting scans using the TTL column instead of the primary key.
2. Provide a global variable switch so administrators can opt in or out.
3. Automatically identify suitable secondary indexes that contain the TTL column as a prefix.
4. Enable the optimization by default when a suitable index exists.

## Non-goals

1. Support composite indexes where the TTL column is not the prefix.
2. Per-table or per-column configuration of index usage (for this phase).
3. Automatically detect whether using the index is faster than a table scan and dynamically switch.
4. Change the TTL delete logic or the `mysql.tidb_ttl_job_history` schema.

## Design

### Overview

When the global variable `tidb_ttl_enable_index_scan` is enabled, the TTL module:

1. For each TTL table, automatically identifies a usable secondary index that contains the TTL column as its prefix.
2. If such an index exists, generates scan SQLs with `ORDER BY (ttl_column, primary_key)` and a `FORCE INDEX` hint.
3. Splits scan subtasks by the TTL column value ranges instead of primary key ranges.
4. Stores the selected index ID in the `split_by` column of `mysql.tidb_ttl_task` so workers know which ordering to use.

If no suitable index exists, or if the global switch is `OFF`, the system falls back to the existing primary-key-based scan behavior.

### Global Variable Switch

| Variable Name | Scope | Type | Default | Description |
|--------------|-------|------|---------|-------------|
| `tidb_ttl_enable_index_scan` | Global | Bool | `ON` | When `ON`, TTL will attempt to use secondary indexes on the TTL column for scan tasks. |

The switch is global because TTL jobs run as background tasks without a user session context. The default is `ON` so that TTL automatically leverages available secondary indexes.

### Index Selection

A secondary index is considered usable if:

1. The index contains the TTL column.
2. If the index is composite, the TTL column must be its first column (prefix).

The index selection is performed at job scheduling time via `PhysicalTable.FindTTLIndex()`. If the selected index is later dropped, the TTL worker reports an error for the affected task.

### Scan SQL Structure

**Without index scan (existing behavior):**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

**With index scan enabled:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

**Pagination within the same task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE (`created_time`, `id`) > (?, ?)
  AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

The `FORCE INDEX` hint ensures the optimizer does not fall back to a different plan. The scan selects both the TTL column and the primary key columns so that pagination can continue from the composite tuple `(last_time, last_key)`.

### Task Splitting

- **Without index scan (default):** Tasks are split by `rowid` / clustered primary key ranges. `split_by` is `NULL`.
- **With index scan enabled:** Tasks are split by the TTL column's value distribution. `split_by` stores the selected index ID.

The `split_by` column in `mysql.tidb_ttl_task` is a `bigint` with default `NULL`. It stores the selected index ID so that workers know whether to use index-ordered or primary-key-ordered scan SQL generation.

### Type-Specific Minimum Time for Index Range Splitting

When splitting index scan ranges by the TTL column, the implementation uses type-aware minimum values as the lower bound:

- **TIMESTAMP**: `1970-01-01 00:00:00` (the physical minimum for the TIMESTAMP type)
- **DATETIME / DATE**: `0001-01-01 00:00:00` (the type minimum)

Previously, the code hardcoded `1970-01-01` as the universal minimum, which silently skipped valid pre-1970 data for DATETIME and DATE columns.

## Compatibility

- The default value of `tidb_ttl_enable_index_scan` is `ON`, so existing behavior is unchanged.
- The new `split_by` column in `mysql.tidb_ttl_task` defaults to `NULL`, compatible with old tasks.
- No TiKV changes are required.
- No changes to the TiDB SQL protocol or client interfaces.

