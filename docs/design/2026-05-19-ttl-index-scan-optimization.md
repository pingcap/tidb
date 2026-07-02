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

A usable index must be a public, visible secondary index whose first column is the TTL column. The scheduler calls `PhysicalTable.FindTTLIndex()` at job creation time. If the selected index is dropped later, the worker reports an error for the affected task.

### Scan SQL

**PK scan (existing behavior):**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

**Index scan for the first page of a split task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE `created_time` >= ? AND `created_time` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

**Pagination within the same task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE (`created_time`, `id`) > (?, ?) AND `created_time` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `id` ASC LIMIT ?;
```

The `FORCE INDEX` hint prevents the optimizer from choosing a different plan. Each index task scans one TTL-column range `[start, end)`. The first page applies the lower and upper bounds, and later pages continue from the composite tuple `(last_time, last_key)` while keeping the upper bound. The scan selects both the TTL column and the PK columns so the delete phase can still delete by primary key.

### Task Splitting

- **PK scan:** tasks are split by PK ranges; `split_by` is `NULL`.
- **Index scan:** tasks are split by the selected secondary index's TiKV region distribution; `split_by` stores the selected **index ID** (`bigint`).

The `split_by` column in `mysql.tidb_ttl_task` is added as `bigint DEFAULT NULL`. Workers read it to decide which ordering to use. A non-`NULL` value is interpreted as the index ID; if the index no longer exists when the task runs, the worker returns an error for that task.

In PK scan mode, `scan_range_start` and `scan_range_end` encode primary-key boundaries. In index scan mode, they encode TTL-column time boundaries decoded from secondary-index region boundaries, so task deserialization uses table metadata from the information schema cache to decode them back as the TTL column's time type.

For index scan splitting, the scheduler locates TiKV regions in the raw secondary-index key range from `MinNotNull` to the encoded expire time. The lower bound excludes `NULL` TTL-column entries, which cannot satisfy `ttl_col < expire`; the upper bound is exclusive, so entries with `ttl_col == expire` are not included. Region end keys are decoded back to TTL-column datums and used as task boundaries. If the store is not TiKV, the selected index is missing, or the region split cannot produce useful boundaries, the scheduler falls back to a single full TTL-column range while still using index-ordered scan for that task.

## Compatibility

- `tidb_ttl_enable_index_scan` defaults to `ON`. Tables with a suitable TTL-column secondary index will use index-ordered scans; tables without one keep the existing PK-ordered scan behavior.
- `split_by` defaults to `NULL`, compatible with old tasks.
- No TiKV or protocol changes.
