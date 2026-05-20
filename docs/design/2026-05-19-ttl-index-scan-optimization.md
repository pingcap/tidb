# TTL Secondary Index Scan Optimization

## Background

TTL scan tasks split and paginate by **primary key** order:

```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

When the optimizer uses a secondary index on the TTL column, this query structure causes redundant scans: each subtask (default 64) independently scans the full TTL-column index range, and every pagination page (default 500 rows) re-sorts by primary key. For tables with many expired rows, the total index scans can exceed a full table scan.

## Goals

- Use the TTL column (with its secondary index) as the scan ordering and split boundary.
- Provide a global variable to enable/disable the optimization.
- Fall back to PK-based scan when no suitable index exists.

## Non-goals

- Composite indexes where the TTL column is not the prefix.
- Per-table or per-column configuration.
- Dynamic cost-based switching.

## Design

### Global Variable

| Variable | Scope | Default | Description |
|---|---|---|---|
| `tidb_ttl_enable_index_scan` | Global | `ON` | Enable index-based TTL scan. |

### Index Selection

A usable secondary index must have the TTL column as its **first** column. The scheduler calls `PhysicalTable.FindTTLIndex()` at job creation time. If the selected index is dropped later, the worker reports an error for the affected task.

### Scan SQL

**PK scan (existing behavior):**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

**Index scan:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

**Pagination within the same task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE (`created_time`, `id`) > (?, ?) AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

The `FORCE INDEX` hint prevents the optimizer from choosing a different plan. The scan selects both the TTL column and the PK columns so pagination can continue from the composite tuple `(last_time, last_key)`.

### Task Splitting

- **PK scan:** tasks are split by PK ranges; `split_by` is `NULL`.
- **Index scan:** tasks are split by TTL column value ranges; `split_by` stores the selected **index ID** (`bigint`).

The `split_by` column in `mysql.tidb_ttl_task` is added as `bigint DEFAULT NULL`. Workers read it to decide which ordering to use.

### Type-Specific Minimum Time

When splitting index scan ranges, the lower bound uses type-specific minima to avoid skipping data:

| Type | Minimum |
|---|---|
| TIMESTAMP | `1970-01-01 00:00:00` |
| DATETIME / DATE | `0001-01-01 00:00:00` |

A hardcoded `1970-01-01` for all types would silently skip pre-1970 DATETIME/DATE data.

## Compatibility

- `tidb_ttl_enable_index_scan` defaults to `ON`; existing behavior is unchanged when no suitable index exists.
- `split_by` defaults to `NULL`, compatible with old tasks.
- No TiKV or protocol changes.
