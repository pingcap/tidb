* Author(s): @lcwangchao, @time-and-fate, @xhebox, @hongyunyan, @flowbehappy
* Last updated: 2025-11-05
* Tracking Issue: [#64281](https://github.com/pingcap/tidb/issues/64281)

# Design Summary

The key requirement is to enable an “Active-Active Deployment” capability in TiDB. The goal is to build a global database solution that supports multiple TiDB clusters distributed across regions, with the ability to handle conflicting writes.

In this setup, an application can connect to any TiDB cluster to perform both read and write operations on any data. Each transaction is processed locally in the TiDB cluster to which the application connects. TiCDC is then responsible for replicating the data changes to other TiDB clusters.

This design provides eventual consistency, and any write conflicts are resolved using the Last Write Wins (LWW) strategy.

The following example illustrates multiple TiDB clusters forming an active-active deployment, referred to as an “active-active group.”

![active-active-group.png](active-active-group.png)

Since a transaction happens only inside a TiDB cluster, and the commit_tss are generated from local PD node which has no direct connection to other TiDB clusters' PD nodes, the solution does't provide global transactional consistency. A simple example: A client connects to cluster A and sets value to x, then connects to cluster B and sets the same value to y in a very short time (before the update is replicated to B by TiCDC). The system does not guarantee the final result of value is y. It only guarantees the final value of each cluster is the same, no matter x or y, i.e. eventual consistency.
To guarantee eventual consistency, the system must address the write conflicts between multiple TiDB clusters. E.g. The same PK rows are modified in more than one cluster simultaneously. The written conflicts are resolved by a strategy named "Last Write Wins (LWW)". An example of how LWW addresses conflict:

![active-active-last-write-wins.PNG](active-active-last-write-wins.PNG)

# Design Details

This section introduces the detailed design in two major parts: Soft-delete and Active-Active. Soft-delete is a feature enabling the deleted rows to be recovered in a retention duration. Active-Active describes how we guarantee eventual consistency between multiple TiDB clusters with TiCDC.

## Soft-Delete

### Introduction

In TiDB, after a row is deleted by DELETESQL, it is no longer accessible unless by [historical read](https://docs.pingcap.com/tidb/stable/read-historical-data/). After the `tidb_gc_life_time` passed, it is no longer accessible or recoverable by any means because the MVCC versions are cleaned up by GC jobs . But some businesses would like to have a much longer retention duration, e.g. 2 weeks. It is the initial intention of intorducting soft-delete feature.
A row is soft deleted means it is marked tombstoned but not marked deleted yet (i.e. no delete mark in MVCC list). The MVCC GC process will never clean it up until it is officially hard deleted (i.e. append a delete mark in MVCC list) after the soft delete retention expired.

Another requirement for soft delete feature comes from the LWW strategy. During LWW, we need to compare the DELETE operations with INSERT and UPDATE operations by SQLs, as TiCDC replicates changes by pure SQLs. The issue is that after a row is hard deleted, normal SQL cannot retrieve the `commit_ts` of DELETE operation any more. And soft delete can help to resolve the issue: the row is not actually hard deleted, so we can make SQLs to retrieve the soft deleted row to do commit_ts comparsions. You can find more details in the CDC sections below. 

In the design, we use the SQL rewrite approach to implement the soft-delete. A hidden `_tidb_softdelete_time` column is added to soft-delete tables, and we rewrite the DML SQLs on the soft-delete table.

### How to Use

First, one needs to create a softdelete table. It is either inherited from db options automatically:

```
CREATE DATABASE t SOFTDELETE RETENTION 7 DAY;
USE t;
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
);
```

Or specify options directly for CREATE TABLE:

```
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
) SOFTDELETE RETENTION 7 DAY;
```

Additionally, one can also specify clean up job intervals:

```
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
) SOFTDELETE RETENTION 7 DAY SOFTDELETE_JOB_INTERVAL '24h' SOFTDELETE_JOB_ENABLE 'ON';

ALTER TABLE message SOFTDELETE_JOB_INTERVAL '24h';
ALTER TABLE message SOFTDELETE_JOB_ENABLE 'ON';
```

Then, one needs to enable softdelete synmatics if it is not:

```
set @@session.tidb_translate_softdelete_sql=on;
set @@global.tidb_translate_softdelete_sql=on; -- it is default on
```

With `tidb_translate_softdelete_sql=on`, DELETE statements will be rewroten internally to mark rows as deleted by modifying an internal hidden column. And SELECT statements will filter those rows. INSERT/UPDATE will treat softdeleted rows as deleted. And you can recover softdeleted rows before the expiration by the following new syntax.

```
RECOVER VALUES FROM message WHERE id = "1001";
```

Softdeleted rows will be deleted after the expiration, i.e. hard deleted by the cleanup jobs.

Internally, if a table is created as softedelete table,  TiDB creates an additional internal hidden column named `_tidb_softdelete_time` for it, to store the expired timestamp now().

All sqls in session with `tidb_translate_softdelete_sql=on` will be translated automatically, including business TTL of TiDB. These change are visible in `explain ...` statement.

For example, `SELECT * from message WHERE ...` may be translated to `SELECT * FROM message WHERE _tidb_softdelete_time is NULL AND (...)`. 

```
explain SELECT * FROM message WHERE text like '%43%';
+---------------------+----------+-----------+---------------+-------------------------------------+
| id                  | estRows  | task      | access object | operator info                       |
+---------------------+----------+-----------+---------------+-------------------------------------+
| TableReader_7       | 8000.00  | root      |               | data:Selection_6                    |
| └─Selection_6       | 8000.00  | cop[tikv] |               | like(test.message.text, "%43%", 92), isnull(test.message._tidb_softdelete_time) |
|   └─TableFullScan_5 | 10000.00 | cop[tikv] | table:message | keep order:false, stats:pseudo      |
+---------------------+----------+-----------+---------------+-------------------------------------+
```
For mysql compatibility, the result of SHOW CREATE TABLE uses comment format:

```
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
) /*T![softdelete] SOFTDELETE RETENTION 7 DAY*/;
```
And you can disable the cleanup job by:

```
ALTER TABLE message SOFTDELETE JOB INTERVAL 0;
```

You can turn off the softdelete status on database level, and the following created tables in the database are not softdelete table automatically. While turning off softdelete status at table level is not supported.

```
ALTER DATABASE t SOFTDELETE = 'OFF';
ALTER TABLE t SOFTDELETE = 'OFF'; -- Phase 1 not supported
```
And a new system var `tidb_softdelete_job_enable` is introduced to control the soft delete cleanup jobs running or not.

#### Optimize the Query Execution 

Because tombstone rows remain in the cluster for a period of time, they may impact query performance when the system needs to skip over them. For example:

```
CREATE TABLE t(id int primary, v int) SOFTDELETE RETENTION 7 DAY;
-- insert 10000 rows with id (1 .. 10000)
INSERT INTO t VALUE (1, 1), (2, 2) ...;
-- DELETE some rows
DELETE FROM t WHERE id < 5000;

-- Will be translated to SELECT * FROM t WHERE id > 0 AND _tidb_tombstone_time IS NULL LIMIT 1;
EXPLAIN SELECT * FROM t WHERE id > 0 LIMIT 1;
+---------------------------+---------+-----------+---------------+--------------------------------------------------+
| id                         | estRows | task      | access object | operator info                                    |
+----------------------------+---------+-----------+---------------+--------------------------------------------------+
| Limit_9                    | 1.00    | root      |               | offset:0, count:1                                |
| └─TableReader_16          | 1.00    | root      |               | data:Limit_15                                   |
|   └─Limit_15              | 1.00    | cop[tikv] |               | offset:0, count:1                               |
|     └─Selection_14        | 1.00    | cop[tikv] |               | isnull(test.t._tidb_tombstone_time)             |
|       └─TableRangeScan_13 | 5000.00  | cop[tikv] | table:ta      | range:(2,+inf], keep order:false, stats:pseudo |
+---------------------------+---------+-----------+---------------+------------------------------------------------+
```

The SELECT statement above is rewritten to include the condition _tidb_tombstone_time IS NULL in order to skip tombstone rows. However, even with a LIMIT 1 clause, TiDB still needs to scan and skip all 5,000 tombstone rows.

To improve scan efficiency, you can create an index as follows:

```
ALTER TABLE t ADD INDEX i1(_tidb_tombstone_time, id);
```
With this index, the query can use i1 to scan efficiently without reading all the tombstone rows:

```
EXPLAIN SELECT * FROM t WHERE id > 0 LIMIT 1;
+----------------------------+---------+-----------+---------------------------------------------+----------------------------------------------------------+
| id                         | estRows | task      | access object                               | operator info                                            |
+----------------------------+---------+-----------+---------------------------------------------+----------------------------------------------------------+
| IndexLookUp_20             | 1.00    | root      |                                             | limit embedded(offset:0, count:1)                        |
| ├─Limit_19(Build)          | 1.00    | cop[tikv] |                                             | offset:0, count:1                                        |
| │ └─IndexRangeScan_17      | 1.00    | cop[tikv] | table:t, index:i1(_tidb_tombstone_time, id) | range:(NULL 0,NULL +inf], keep order:false, stats:pseudo |
| └─TableRowIDScan_18(Probe) | 1.00    | cop[tikv] | table:t                                     | keep order:false, stats:pseudo                           |
+----------------------------+---------+-----------+---------------------------------------------+----------------------------------------------------------+
```
Please note that TiDB does not create any indexes automatically—you need to create them manually when appropriate. The reason is that the optimal index depends on your specific business workload.

For example, if you frequently query rows where v > 1000 and want to optimize such queries, you can create an additional index like this:

```
ALTER TABLE t ADD INDEX i2(_tidb_tombstone_time, v);
```

#### Scenes to Operate the Table with tidb_translate_softdelete_sql='OFF'

Most applications should keep `tidb_translate_softdelete_sql` set to ON when working with soft-delete tables, allowing TiDB to automatically manage tombstone rows. However, there are certain scenarios where you may want to disable it, such as:

- Debugging during application development. E.g. Select the soft-deleted rows in a range of time.
- Manual maintenance by DBAs—for example, updating `_tidb_tombstone_time` to an earlier value to accelerate cleanup of specific rows, or to a later value to preserve them longer.
- Disaster recovery or emergency analysis, where querying tombstone rows can help retrieve historical information.

#### Explicit DML for Hard vs Soft Deletes

This section explores introducing explicit DML to control hard vs soft deletes. The key idea being that rather than relying on a session variable, we should be explicit with the DML for the most common cases. This is an supplement to the use of `@@session.tidb_translate_softdelete_sql` to control the soft delete behavior explicitly. It can coexist with that session variable if desired, but a more explicit DML is used to show the express intent of the user.
The current MySQL/TiDB DELETE syntax is:

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name [[AS] tbl_alias]
    [PARTITION (partition_name [, partition_name] ...)]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

The proposed change would be to add a `HARD` key word to the DELETE statement:

```
DELETE [HARD] [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name [[AS] tbl_alias]
    [PARTITION (partition_name [, partition_name] ...)]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

This would only have an impact on tables where the soft delete capability is enabled. For all other tables, it would be ignored and treated as a normal delete. For tables where soft delete is enabled, then this statement would perform an actual delete of the data (not a soft delete) and so would not be rewritten as a regular DELETE would be.


#### Notes

1. Since `tidb_translate_softdelete_sql` affects the final executing SQLs, the modification to the value of the var affects [Virtual View](https://docs.pingcap.com/tidb/stable/views/#views)' result and [plan cache](https://docs.pingcap.com/tidb/stable/sql-prepared-plan-cache/).
2. If a table is a softdelete table, then the delete operations triggered by [TTL](https://docs.pingcap.com/tidb/stable/time-to-live/) background jobs are always soft deletes, which do not follow the setting of `tidb_translate_softdelete_sql`
3. Softdelete shares the same system vars with TTL. Including all the vars `tidb_ttl_xxx` from [here](https://docs.pingcap.com/tidb/stable/system-variables/#tidb_ttl_running_tasks-new-in-v700). E.g. `tidb_ttl_delete_rate_limit`,  `tidb_ttl_delete_batch_size`, `tidb_ttl_delete_worker_count`, etc
  1. And `tidb_ttl_job_enable` and `tidb_softdelete_job_enable` controls TTL and soft delete jobs separately.

### Observability 

#### Metrics

Prometheus metrics:

- `softdelete_queries`: num of issued softdelete queries
- `softdelete_processed_rows`: rows that are processed by softdelete, labeled with deleted or not.
- `softdelete_query_duration`: duration of issued softdelete queries.
- `ttl_job_status,ttl_phase_time`,`ttl_insert_rows`,`ttl_event_count` will be shared between TTL and softdelete. Or this can be separated.

There wil the following grafana dashboards:

- Softdelete QPS: using softdelete_queries , grouped by SQL type.
- Softdelete process rows per second: using softdelete_processed_rows, separated into expired, total.
- Softdelete process rows per hour/per day: large scale views of cleanup job performance.
- Softdelete scan duration: using softdelete_query_duration, grouped into 50%,80%,90%,99%
- Softdelete delete duration: similar to scan duration board, but for delete SQL.
- Softdelete scan phase duration/delete phase duration: internal metrics that can be used for performance diagnostics.
- Softdelete job count/task count: the current live cleanup jobs/tasks.
- Softdelete OPM: operations count triggered by cleanup job.

#### Stats

Introduce a new system table tidb_softdelete_table_stats in information_schema. The table definition is as follows:

```
CREATE TABLE tidb_softdelete_table_stats (
  db_name varchar(64),
  table_name varchar(64),
  partition_name varchar(64),
  estimated_total_row_count bigint unsigned,
  estimated_softdeleted_row_count bigint unsigned
)
```
The user can access the row count information from this system table.

Internally, the information would be from optimizer statistics.
When the user access this system table, tidb access the corresponding optimize statistics from the cache in tidb memory. If the corresponding statistics are collected by not loaded into memory yet, tidb will try to load it from storage.

If the statistics for a table are not collected, or if it's not a softdelete table, tidb can't display information for this table.
If there are valid statistics for the queried softdelete table, tidb would use the statistics on the `_tidb_softdelete_time` column to calculate the `total_row_count` and `soft_deleted_row_count`. Since `_tidb_softdelete_time IS NULL` means it's not softdeleted, we can get `soft_deleted_row_count` by subtracting the NULL count from the total count.

Since we rely on optimizer statistics, there are some limitations:

- If the corresponding table is not ANALYZEd (manually or automatically), there will be no statistics to display.
- The displayed information only reflects the row count when the last ANALYZE happen.

### Soft-delete Limitations

1. Foreign keys are not supported.  (Currently unsupported and not planned.)
2. The DELETE ... JOIN ..., which involves multiple tables, is not supported. (Currently unsupported and not planned.)

```
-- The below statements are not allowed for soft-delete table
DELETE t,t1 FROM t JOIN t1 ON t.id=t1.id WHERE t.id=1; -- not allowed
```

The main reason is that for DELETE statements involving multiple tables, the translation logic would be much more complicated.

A simpler case is that if we have a DELETE stmt with multiple soft-delete tables, we need to check which of them needs to be updated.For example, we need to rewrite it similar to the logic below:

```
DELETE t1, t2 FROM t1 JOIN t2 ON t1.a = t2.a;
DELETE t1 FROM t1 JOIN t2 ON t1.a = t2.a;
-- translates to
UPDATE t1 JOIN t2 ON t1.a = t2.a SET t1._tidb_softdelete_time = NOW(), t2._tidb_softdelete_time = NOW();
UPDATE t1 JOIN t2 ON t1.a = t2.a SET t1._tidb_softdelete_time = NOW();
```

If we have a DELETE stmt with soft-delete tables and non-soft-delete tables, it would be harder to implement because that means we need the executor to handle Update logic and Delete logic at the same time.

3. Unique Keys besides the primary key are not supported in phase 1. (phase1 not supported)
  1. It could be supported in the future phases. However, Unique Key is still an issue in active-active setup and cannot be supported later. See the "Active-Active Limitations" section for more information.
4. A soft-delete table must have a primary key; the primary key can be clustered or nonclustered. (Currently unsupported and not planned, no primary key)
5. UPDATE or INSERT ... ON DUPLICATE KEY UPDATE ... do not support changing the primary key when updating an existing row. (phase1 not supported)
  1. If a relevant SQL is sent to the table, an error message will return.
6. Some DDL operations are forbidden for column `_tidb_softdelete_time` : (Currently unsupported and not planned.)
  1. DROP COLUMN
  2. DDLs that change the column meta, such as ALTER COLUMN, RENAME COLUMN ...
  3. Constraint checks that contain the column `_tidb_softdelete_time`
  4. Though unique-index is not supported, we support adding non-unique indexes for both normal and hidden columns.


### How SQL Rewrite Works

#### SELECT 

This part also applies to SELECT statements in DML statements, like INSERT ... SELECT statements.

##### Filtering soft-deleted rows

According to the soft-delete semantic, the rows with a non-null `_tidb_softdelete_time` is deleted and should not be output when the user queries this table, so we should explicitly filter these rows internally. Besides, this filtering behavior can be disabled by turning off `tidb_translate_softdelete_sql`.

If `tidb_translate_softdelete_sql` is enabled, in `PredicatePushDown()` for `DataSource`, we will check if it's a soft-delete-enabled table, if it is, then add an extra `_tidb_softdelete_time IS NULL` filter to the predicates for this DataSource.

##### Filtering hidden columns

Internally, the new hidden column `_tidb_softdelete_time` is a normal public column in its nature. This column should not be output for any queries (even when `tidb_translate_softdelete_sql` is disabled), unless the user explicitly specifies this column in the select fields.

For implementation, the only thing we need to consider is the behavior of the `SELECT *`. Specifically, we should not contain the `_tidb_softdelete_time` column in the select fields when we are unfolding the `*` in the select fields. We don't need restrictions or behavior changes for anything else.

In `unfoldWildStar()`, we should check the column name (since the user can't create a column with this name, so we don't need any extra flags and checking the name is enough), and skip it if it's `_tidb_softdelete_time`.

##### Compatibility with the plan cache

When `tidb_translate_softdelete_sql` is enabled or disabled, the semantic of the SQL is changed and the execution plan can't be reused between the two cases, so we need special handling in the plan cache module.

If there are soft-delete enabled table in a query, we will include the value of the `tidb_translate_softdelete_sql` in the plan cache key, so that it will have different entries in the plan cache when `tidb_translate_softdelete_sql` is enabled or disabled.

#### INSERT

The insert SQL (without `ON DUPLICATE`) should check the old row's tombstone to determine whether to perform an insert or a replace, or return a dupkey error:

```
EXPLAIN INSERT INTO t(id, a) VALUES(1, 2); -- original SQL

+----------+---------+------+---------------+----------------------------------------------------------+
| id       | estRows | task | access object | operator info                                            |
+----------+---------+------+---------------+----------------------------------------------------------+
| Insert_1 | N/A     | root |               | replace-conflict-if:not(isnull((t._tidb_softdelete_time) |
+----------+---------+------+---------------+----------------------------------------------------------+
```

Notice the `replace-conflict-if:not(isnull((t._tidb_softdelete_time)` in the explain result. During execution, TiDB attempts to locate duplicate rows and checks whether the existing row’s `_tidb_softdelete_time` value is not NULL. The behavior can be divided into the following cases:

- No duplicate rows found: Behaves the same as a regular INSERT.
- Duplicate rows found:
  - If all duplicate rows have `_tidb_softdelete_time` is not NULL, delete those rows and insert the new one (i.e., replace).
  - If any duplicate row has `_tidb_softdelete_time is NULL`, return a duplicate key error, or skip updating that row if `IGNORE` is specified.

To achieve the above goal, we should add some new fields for `InsertExec`

```
type InsertExec struct {
    *InsertValues
+   replaceConflictIf expression.Expression   
    ...
}
```

The `replaceConflictIf` field is an expression that indicates whether an old row should be replaced if it meets the condition. By default, it is nil, preserving the original behavior. For soft delete tables, however, it is set to the expression `_tidb_softdelete_time` is not NULL.
If `replaceConflictIf` is set, `InsertExec` performs the following steps:

1. Iterate over each row in the values list.
2. For each row:
  1. Identify any conflicting rows based on the inserted values. Since currently only the primary key is considered unique, there can be at most one conflicting row.
  2. If no conflicting row is found, insert the new row and proceed to the next one.
  3. If a conflicting row is found, check whether the old row satisfies the `replaceConflictIf` condition.
  4. If the old row **does not** satisfy `replaceConflictIf`, return a conflict error or append a warning if `INSERT IGNORE` is specified.
  5. If the old row **does** satisfy `replaceConflictIf`, delete the old row and insert the new one.
  6. Continue to the next row in the values list and repeat the process.

#### INSERT ... ON DUPLICATE KEY UPDATE ...

Similar to normal insert, the upsert case also uses the replaceConflictIf to check whether a row is a tombstone or not. But upsert should handle the update cases if the non-soft-delete conflict happens.

It performs the following steps:

1. Iterate over each row in the values list.
2. For each row:
  1. Identify any conflicting rows based on the inserted values. Since currently only the primary key is considered unique, there can be at most one conflicting row.
  2. If a conflicting row is found, check whether the old row satisfies the `replaceConflictIf` condition.
  3. If the old row **does not** satisfy `replaceConflictIf`, update the conflict row as the specified value. Because currently changing a primary key is not allowed and there is no other unique indexes, the update phase should have no conflict.
  4.  If the old row **does** satisfy `replaceConflictIf`, delete the old row and insert the new one.
  5. Continue to the next row in the values list and repeat the process.

#### UPDATE

We only need to rewrite the SQL to make sure the update will skip updating the tombstone rows, for example:

```
-- original SQL
UPDATE t SET v = v + 1 WHERE id = 1;

-- rewrite SQL for soft-delete table
UPDATE t SET v = v + 1 WHERE id = 1 and _tidb_softdelete_time IS NULL;
```

Currently, updating a soft-delete table will not meet any conflict because:

1. Changing the primary key is not allowed for a soft-delete table.
2. There is no unique index besides the primary key.

#### REPLACE

The behavior of a REPLACE statement on a soft-delete-enabled table will be the same as a normal one, which is:

1. If there are conflicts in the existing rows with the specified values for a primary key or a unique key, those existing rows will be deleted.
2. The specified rows are inserted.
Note that, similar to the INSERT statement above, any conflicting rows will be hard-deleted and not be able to be recovered.

#### DELETE

If `tidb_translate_softdelete_sql` is enabled,  a DELETE statement translates to an UPDATE with the same query part plus SET `_tidb_softdelete_time = NOW()` internally.

For example,

```
-- User queries:
DELETE FROM t WHERE col_a = 10;
DELETE FROM t WHERE col_a = 10 ORDER BY col_b LIMIT 1;


-- internally translates to:
UPDATE t SET _tidb_softdelete_time = NOW() WHERE col_a = 10;
UPDATE t SET _tidb_softdelete_time = NOW() WHERE col_a = 10 ORDER BY col_b LIMIT 1;
```

The privilege check is conducted as if it's a normal DELETE statement, i.e., we check the DELETE privilege instead of the UPDATE privilege.

#### RECOVER

Introduce a new SQL syntax `RECOVER VALUES FROM <table_name> WHERE <expr>`

Internally, this SQL translates to `UPDATE <table_name> SET _tidb_softdelete_time = NULL WHERE <expr>` (without further applying the rewriting for normal UPDATE statements mentioned above). The privilege check and query result will be the same as that.

### How Softdelete Cleanup Jobs Work 

There is a minimum for the job interval, 1h. A background task will scan all tables with softdelete enabled, and see if `SOFTDELETE_JOB_INTERVAL` is met since the TiDB startup.

New fields will be added to `mysql.tidb_ttl_table_status` and `mysql.tidb_ttl_tasks` to reuse the existing distributed TTL framework. One can tweak the concurrency, schedule time like TTL, using the same set of variables.

The whole process is two-phased: dispatch, scan&delete. Similar to TTL, one instance will dispatch up to 64 tasks per table, and store that information into `tidb_ttl_tasks` table. And scan&deletion tasks will be distributed to the whole cluster, reporting to `tidb_ttl_table_status`.

Note that the dispatch manager will either issue softdelete cleanup jobs, or TTL jobs. This is a technical limitation, and may be solved in the future.

When collaborating with active-active synchronization and TiCDC, i.e. with `ACTIVE_ACTIVE=ON` on the table meta, cleanup worker should also query the minimum `checkpoint_ts` from `ticdcProgresTable`. Every hard deletion requires `min(current_tso, checkpoint_ts) >= IFNULL(_tidb_origin_ts, _tidb_commit_ts)`. Otherwise there may be inconsistency.

### How to migrate existing tables to soft-delete tables 

(Phase 1 not supported, might support in the future phases)

One may want to migrate existing tablesto softdelete for active-active usage or so.

It could be done by:

```
// enable softdelete for random tables
ALTER TABLE t ENABLE SOFTDELETE SOFTDELETE_RETENTION '7d';
```
`ALTER TABLE ENABLE` statements will try to create the hidden columns(or indexes) automatically if it does not exist. And will do nothing if it exists.

And one can also disable it anytime:

```
// disable it
ALTER TABLE t DISABLE SOFTDELETE;
```

`ALTER TABLE DISABLE` statements will not delete hidden columns or indexes, but just disable the functionality on this table.

That said, to completely disable softdelete, one needs to manually drop columns and indexes using admin:

```
ALTER TABLE t DROP COLUMN _tidb_softdelete_time;
```

You could also amend the table manually to correct any error/inconsistency.

### Pros

1. The project risk is under control.
  1. Major impact components are SQL, planner, and executor layer, mainly in the TiDB repo. The technical impact to other compoenents are limited. 
  2. No changes to the storage layer. Good compatibility gaurantee for BR, TiFlash, TiDB X (next-gen TiDB).
2. Tombstoned rows are indexed normally on the extra column `_tidb_softdelete_time`. So the query performance on tombstoned rows is guaranteed. 
3. The mechanism can be reused by Instant TTL requirement. We don't need to make too much effort to implement Instant TTL later.

### Cons

1. Userbility compromised. It introduce complexity and limitations to the user interface, especially for DBA during the clusters setting up. 
  1. The setting up of active-active requires configurations on both TiDB and TiCDC sides.

## Active-Active Synchronization

### Introduction

In this section we introduce how to build active-active synchronization in a TiDB cluster group. TiDB already supports [CDC bi-directional](https://docs.pingcap.com/tidb/stable/ticdc-bidirectional-replication/#deploy-bi-directional-replication) to replicate changes between two TiDB clusters. The main challenge is how to achieve eventual consistency by "Last Write Wins(LWW)" strategy. 

The basic idea of this design is that before TiCDC applying a change from upstream TiDB cluster to a target TiDB cluster, TiCDC compares the `commit_ts` of the change to the latest existing MVCC version's `commit_ts` and only applies it when the former `commit_ts` is larger. And since we cannot directly use the `commit_ts` from upstream TiDB cluster as the target MVCC version's `commit_ts`(It breaks TiDB's transaction assumption and might cause transactional inconsistency),  the design adds a new hidden column `tidb_origin_ts` to store the original `commit_ts` for further comparison.

### How to Create an Active-Active Table

You can use the table option `ACTIVE_ACTIVE` to create a table that supports active-active synchronization. Note that the `SOFTDELETE` option is also required when defining an active-active table:

```
-- Create a table that supports active-active sync
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
) ACTIVE_ACTIVE = "ON" SOFTDELETE RETENTION = 7 DAY;

-- Option ACTIVE_ACTIVE requires SOFTDELETE enable.
-- The below statement will fail because SOFTDELETE is not enabled:
CREATE TABLE message ( -- returns ERROR!
    id int PRIMARY KEY,
    text varchar(10)
) ACTIVE_ACTIVE = "ON"; 
```

The output `/*T![active-active] ACTIVE_ACTIVE='ON' */ /*T![soft-delete] SOFTDELETE RETENTION=7 DAY */` of `SHOW CREATE DATABASE` is in comment format, which is also legal.

Another option is to add the option `ACTIVE_ACTIVE` to the database. The newly created table will then inherit  the `ACTIVE_ACTIVE` automatically:

```
 -- Create a database with active-active option
 CREATE DATABASE test ACTIVE_ACTIVE = 'ON' SOFTDELETE RETENTION = 7 DAY;
 
 SHOW CREATE DATABASE test;
+----------+------------------------------------------------------------------------------------------------------------------+
| Database | Create Database                                                                                                  |
+----------+------------------------------------------------------------------------------------------------------------------+
| test     | CREATE DATABASE `test` /*T![active-active] ACTIVE_ACTIVE='ON' */ /*T![soft-delete] SOFTDELETE RETENTION=7 DAY */ |
+----------+------------------------------------------------------------------------------------------------------------------+
 
-- Create a new table under the database test
USE test;
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
);

-- The newly created table should inherit the active-active option automatically
SHOW CREATE TABLE message;
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table   | Create Table                                                                                                                                          |
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
| message | CREATE TABLE `message` (                                                                                                                              |
|         |   `id` int NOT NULL,                                                                                                                                  |
|         |   `text` varchar(10) DEFAULT NULL,                                                                                                                    |
|         |   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */                                                                                               |
|         | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![active-active] ACTIVE_ACTIVE='ON' */ /*T![soft-delete] SOFTDELETE RETENTION=7 DAY */ |
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
```

You can also use `ALTER DATABSE` statement to change the active-active option. Please note that it only affects the newly created table under this database.

```
-- Create a normal database
CREATE DATABASE test;
USE test;
CREATE TABLE message (
    id int PRIMARY KEY,
    text varchar(10)
);

-- Alter database with active-active option
ALTER DATABASE ACTIVE_ACTIVE='ON' SOFTDELETE RETENTION=7 DAY;

-- The created table before ALTER should not be affected
SHOW CREATE TABLE message;
+---------+-------------------------------------------------------------+
| Table   | Create Table                                                |
+---------+-------------------------------------------------------------+
| message | CREATE TABLE `message` (                                    |
|         |   `id` int NOT NULL,                                        |
|         |   `text` varchar(10) DEFAULT NULL,                          |
|         |   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */     |
|         | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin |
+---------+-------------------------------------------------------------+

-- Only the newly created table should be affected
CREATE TABLE message2 (
    id int PRIMARY KEY,
    text varchar(10)
);
SHOW CREATE TABLE message2;
+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table    | Create Table                                                                                                                                          |
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------+
| message2 | CREATE TABLE `message2` (                                                                                                                             |
|          |   `id` int NOT NULL,                                                                                                                                  |
|          |   `text` varchar(10) DEFAULT NULL,                                                                                                                    |
|          |   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */                                                                                               |
|          | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![active-active] ACTIVE_ACTIVE='ON' */ /*T![soft-delete] SOFTDELETE RETENTION=7 DAY */ |
+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Only the database level `ACTIVE_ACTIVE` option is allowed to be altered:

```
-- Alter database to add or drop ACTIVE_ACTIVE options is allowed
ALTER DATABASE test ACTIVE_ACTIVE='ON' SOFTDELETE RETENTION=7 DAY;
ALTER DATABASE test ACTIVE_ACTIVE='OFF';

-- Alter the table to add or drop ACTIVE_ACTIVE options is not allowed
ALTER TABLE test ACTIVE_ACTIVE='ON' SOFTDELETE RETENTION=7 DAY; -- returns ERROR!
ALTER TABLE test ACTIVE_ACTIVE='OFF'; -- returns ERROR!
```

### `_tidb_origin_ts` and `_tidb_commit_ts`

If a table is created with `ACTIVE_ACTIVE`, a new hidden column `_tidb_origin_ts` will be added. The column `_tidb_origin_ts` indicates the original `commit ts` if this row is synced from the upstream, and it has the following definition:

```
`_tidb_origin_ts` BIGINT DEFAULT NULL
```

- By default, `_tidb_origin_ts` is NULL, indicating that the row was last inserted or updated by a local transaction.
- If `_tidb_origin_ts` is not NULL, it means the row was synchronized from an upstream cluster via CDC. In this case, the value of `_tidb_origin_ts` represents the commit timestamp of the original row in the upstream cluster.

We also propose a read-only column, `_tidb_commit_ts`, to represent the row’s commit timestamp in the local cluster. The `_tidb_commit_ts` column has the same definition as `_tidb_origin_ts`:

```
 `_tidb_commit_ts` BIGINT DEFAULT NULL
```

- In most cases, `_tidb_commit_ts` shows the row’s commit timestamp in the local cluster.
- If `_tidb_commit_ts` is NULL, it indicates that the row has not yet been committed. It happens only inside TiDB as temporary transaction status.

The two columns above are hidden by default, but you can still query them by explicitly specifying their column names:

```
-- The `_tidb_origin_ts` and `_tidb_commit_ts` is hidden by default.
> SELECT * FROM message;
+----+-------+
| id | text  |
+----+-------+
| 1  | hello |
+----+-------+

-- You can query the hidden columns by specifying the column name:
> SELECT id, _tidb_origin_ts, _tidb_commit_ts FROM message;
+----+-------------------+--------------------+
| id | _tidb_origin_ts | _tidb_commit_ts    |
+----+-------------------+--------------------+
| 1  | NULL              | 460711508282966017 |
+----+-------------------+--------------------+
```

After introducing the columns `_tidb_commit_ts` and `_tidb_origin_ts`, the "actual commit timestamp" of a row can be represented as follows:

```
IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`)
```

- When CDC synchronizes a row to a downstream cluster, it compares the upstream commit timestamp with the downstream’s actual commit timestamp and only applies the change if the upstream timestamp is greater.
- When a local transaction updates a row, it must always overwrite the existing row and commit with a new commit timestamp greater than the actual commit timestamp. Then the _tidb_origin_ts will be set to NULL, indicating the row is updated by the local transaction.

```
-- Suppose an old row is from an upstream cluster
> SELECT id, message, _tidb_origin_ts, _tidb_commit_ts FROM message;
+----+-------+--------------------+--------------------+
| id | text  | _tidb_origin_ts  | _tidb_commit_ts    |
+----+-------+--------------------+--------------------+
| 1  | hello | 461617167984492544 | 461617258860118016 |
+----+-------+--------------------+--------------------+

-- Local transaction will always overwrite the old row.
> UPDATE message SET text = "world" WHERE id = 1;
-- The new row should have:
-- `_tidb_origin_ts` is NULL
-- `_tidb_commit_ts` > old `_tidb_origin_ts`
> SELECT id, message, _tidb_origin_ts, _tidb_commit_ts FROM message;
+----+-------+--------------------+--------------------+
| id | text  | _tidb_origin_ts  | _tidb_commit_ts    |
+----+-------+--------------------+--------------------+
| 1  | world | NULL               | 461617262510997504 |
+----+-------+--------------------+--------------------+

-- Set _tidb_origin_ts explictly is allowed
-- DO NOT do it only if you know what you are doing, as it may cause the final result to be inconsistent between clusters!
> UPDATE message SET _tidb_origin_ts = 461620485322702848 WHERE id = 1;
> SELECT id, message, _tidb_origin_ts, _tidb_commit_ts FROM message;
+----+-------+--------------------+--------------------+
| id | text  | _tidb_origin_ts  | _tidb_commit_ts    |
+----+-------+--------------------+--------------------+
| 1  | world | 461620485322702848 | 461617262510997504 |
+----+-------+--------------------+--------------------+
```

These two rules together ensure the `Last Writer Wins (LWW)` policy—that is, the write with the greater timestamp always overwrites the older value.

### Active-Active Limitations

- An Active-Active table needs also to be a soft-delete table.  (Currently unsupported and not planned to be a non-soft-delete table)
- All secondary indexes should not be unique in the Active-Active table. For example, it's to figure out how to resolve the conflict below:  (Unique indexes currently unsupported and not planned) You can find more discussions from section "(Unsolved) To Support Secondary Unique Index"

```
-- Suppose a table with an extra unique index
CREATE TABLE t(id int primary key, u int unique);

-- ClusterA
INSERT INTO t VALUES(1, 10); -- commit_ts: ts1
-- ClusterB
INSERT INTO t VALUE(1, 20); -- commit_ts: ts2
-- ClusterA
INSERT INTO t VALUES(2, 20); -- commit_ts: ts3

-- Suppose ts1 < ts2 < ts3.
-- We have a dilemma when syncing (1, 20) from cluster B to cluster A:
--   A should overwrite (1, 10) because ts2 > ts1, and we should remove the conflict row (2, 20) first
--   But we cannot remove (2, 20) because (2, 20) has a newer version (ts2 < ts3)
```

- The columns `_tidb_origin_ts` and `_tidb_commit_ts` can not be modified with DDL.  (Currently unsupported and not planned to alter these hidden columns)
- Changing table's `Active-Active` enable status after creation is not supported.  (Currently unsupported and not planned.)

### How Local DML Works 
#### How PD TSO Allocation Works

The values of `_tidb_origin_ts` and `_tidb_commit_ts` are both TSO allocated by PD. However, since each cluster has its own PD instance, we must ensure that timestamps generated by different PDs are comparable. To achieve this, each PD instance must allocate globally unique timestamp values.

To support this, two configuration parameters are introduced: `tso-max-index` and `tso-unique-index`, which define the number of active-active clusters and their identities.

- `tso-unique-index` identifies each cluster in an active-active setup and must be unique across clusters.
- `tso-max-index` specifies the maximum possible value for tso-unique-index.

Usually, a TSO value is generated according to the following rule:

```
TSO = (physical_timestamp << 18) + logical_num
logical_num = tso_unique_index + (tso_max_index * N) 
// N >= 0 && tso_unique_index <= tso_max_index && logical_num < (1 << 18)
```

- physical_timestamp is derived from the system clock and increases monotonically.
- logical_num also increases monotonically for the same physical_timestamp within a PD instance. The logical_num values generated by different PD instances must never be the same.  

The TSO generation algorithm can be seen below:

```
// last_physical_timestamp is the physical _imestamp for the last TSO request
var last_physical_timestamp
// last_logical_num is the logical_num for the last TSO request
var last_logical_num

for each TSO request:
    current_physical_timestamp = get_current_physical_timestamp()
    if current_physical_timestamp != last_physical_timestamp
        // for each new physical timestamp, the logical_num is initialized from tso_unique_index
        current_logical_num = tso_unique_index
    else
        // The logical_num should increase by tso_max_index for each request
        current_logical_num += tso_max_index
    
    last_physical_timestamp = current_physical_timestamp
    last_logical_num = current_logical_num
    response(current_physical_timestamp << 18 + current_logical_num)
```

Because each PD produces distinct `logical_num` values, the resulting TSO values are guaranteed to be unique across clusters.

A frequent TSO allocation may cause the `logical_num` to grow quickly.  Generally, it is better to ensure `tso-max-index` is less than 10 to avoid the logical_num being exhausted before physical timestamp advances.

#### How Local DML Executor Works

As mentioned above, a local transaction must ensure that its commit timestamp is greater than the old row’s `_tidb_origin_ts`. To achieve this, the internal implementation should allow DML operations to set a "minimum commit timestamp" for the current transaction.

A new transaction option, MinCommitTS, is proposed. DML operations can use SetOption to enforce that the commit timestamp is greater than a specified value:

```
// When found the old row's `_tidb_origin_ts` is not NULL 
// while executing a local DML.
txn.SetOption(kv.MinCommitTS, oldRow.TiDBOriginalTS)
```

If `kv.MinCommitTS` is set, during transaction commit, if PD returns a TSO that is not greater than the specified value, the transaction will wait briefly and retry fetching a new TSO until the returned timestamp meets the requirement.

In most cases, the clock drift between clusters should be minimal. However, if PD returns a TSO that is significantly smaller than the old row’s original timestamp (for example, by more than 500 ms), the commit should fail to prevent excessively long waiting times. In this case, there must be a serious clock drift between PD instances and the system administrator has to fix the issue before proceeding new updates to databases.

### How CDC Sync Rows 
#### How to Create a Changefeed in LWW Mode

We will add a new changefeed configuration item, `enable-active-active`, with a default value of false. When `enable-active-active` is true, CDC will synchronize data downstream in LWW mode. 

Users need to ensure that all tables that need to be synchronized in this changefeed are created upstream in `ACTIVE_ACTIVE` mode. `enable-active-active` mode only supports tidb downstream now.

#### How to Synchronization to TiDB Downstream in LWW Mode

For Active-Active scenarios, TiCDC uniformly transforms upstream data changes into SQL statements with LWW conflict resolution capabilities for writing to the downstream TiDB clusters.

1. DML Statement Handling (Insert & Update)
2. 
All upstream changes, including native INSERT and UPDATE statements, as well as UPDATE statements resulting from the internal Soft Delete mechanism, will be uniformly translated into the INSERT ... ON DUPLICATE KEY UPDATE pattern by TiCDC.

Key LWW Logic: TiCDC embeds the logic to compare `_tidb_origin_ts`, `_tidb_softdelete_time` and `commit_ts` within the statement. An update is only executed if the `_tidb_origin_ts` (or `commit_ts`) of the incoming change is greater than or equal to the `_tidb_origin_ts` or `commit_ts` of the existing row in the downstream. This strictly enforces the LWW strategy.

Additionally, considering that `_tidb_softdelete_time` might be directly modified in the statement generated by TiCDC. in LWW mode, we will set `@@session.tidb_translate_softdelete_sql=off;` when writing data downstream.

Example LWW SQL:

```SQL
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (....) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF(@cond, VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF(@cond, VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF(@cond, VALUES(_tidb_origin_ts), test._tidb_origin_ts);
```

2. Handling of Hard Delete Statements

For physical DELETE statements generated by the TiDB Soft Delete TTL job after the Soft Delete retention period expires:

- TiCDC will directly ignore these Hard Delete changes.
  - Each cluster in the Active-Active group independently handles its own Hard Delete operations based on local TTL configuration and safety checks. Ignoring these statements prevents a flood of physical DELETE statements from impacting TiCDC's synchronization performance.

#### How to Guarantee Data Correctness During Hard Delete in LWW Mode

To mitigate the Corner Case where high TiCDC latency (e.g., during changefeed stops or abnormal conditions) allows a downstream cluster to perform a premature Hard Delete, leading to data inconsistency, we introduce a Hard Delete Safety Watermark mechanism.

1. Corner Case Description

The risk is that Cluster B executes a Hard Delete on a row based on local Soft Delete TTL before it receives a prior UPDATE (not a Soft Delete update) from Cluster A, which occurred before the Soft Delete that B is currently processing. This results in data inconsistency between the two clusters.

![active-actve-lww-delete.png](active-actve-lww-delete.png)

Therefore, we must ensure that a row is Hard Deleted only when the synchronization progress for that table in the current cluster, from all upstream cluster, is larger then the CommitTS of when the row was Soft Delete.

2. TiCDC Progress Table (`ticdcProgressTable`)

To provide this safety watermark, TiCDC, in `Active-Active` mode, will write to a special table in the downstream TiDB cluster: ticdcProgressTable.

- The table records the current synchronization progress (`checkpoint_ts`) for every table, in every changefeed, from every upstream cluster.
- The semantic of `checkpoint_ts` is: All data changes up to and including this timestamp have been safely synchronized to the downstream.

```
CREATE TABLE ticdcProgressTable (     
    changefeed_id VARCHAR(255) NOT NULL COMMENT 'Unique identifier for the changefeed synchronization task',     
    upstreamID VARCHAR(255) NOT NULL COMMENT 'Unique identifier for the upstream cluster',     
    database_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream database', 
    table_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream table',     
    checkpoint_ts BIGINT UNSIGNED NOT NULL COMMENT 'Safe watermark CommitTS indicating the data has been synchronized',     
    PRIMARY KEY (changefeed_id, upstreamID, database_name, table_name) 
) COMMENT='TiCDC synchronization progress table for HardDelete safety check';
```

3. TiDB Soft Delete TTL Collaboration
TiDB's Soft Delete TTL cleanup module will collaborate with this mechanism:
- Before performing a Hard Delete on a Soft Deleted row, TiDB queries the `ticdcProgressTable` to find the minimum `checkpoint_ts` across all active upstream changefeeds for that table.
- The row is only permitted to be Hard Deleted if this minimum checkpoint_ts is greater than or equal to the row's commitTs at the time of its Soft Delete. Otherwise, the row is temporarily skipped, awaiting progress from TiCDC.

4. Performance Assurance

To avoid placing additional read/write burden on the downstream TiDB cluster, TiCDC will update the `ticdcProgressTable` in a sparse manner, such as once every 30 minutes for all tables. Given that Soft Delete retention periods are typically measured in days, this sparse update frequency will not significantly impact the timeliness of the TTL cleanup.

#### How to Perform DDL Operations in LWW Mode

Executing DDL in LWW mode must comply with the DDL requirements([ticdc-bidirectional-replication](https://docs.pingcap.com/tidb/stable/ticdc-bidirectional-replication/)) for bidirectional synchronization.

We have two following options:

1. Choose one cluster as the PRIMARY cluster, and only perform replicable DDLs on the primary cluster and synchronize them to the SECONDARY cluster through CDC. For non-replicable DDLs, we should perform this way [replication-scenarios-of-non-replicable-ddls](https://docs.pingcap.com/tidb/stable/ticdc-bidirectional-replication/#replication-scenarios-of-non-replicable-ddls).
2. Don't set BDR Mode for all clusters, and perform all the ddls this way [replication-scenarios-of-non-replicable-ddls](https://docs.pingcap.com/tidb/stable/ticdc-bidirectional-replication/#replication-scenarios-of-non-replicable-ddls). This approach requires users to stop all writes to all the clusters until finishing running DDLs in all clusters.

#### How to Synchronization Downstream in Normal Mode

When an LWW table needs to be synchronized to external downstreams (such as data warehouses), TiCDC must revert the internal LWW representation back to standard events.

- Native DML Restoration: Upstream INSERT and UPDATE statements are translated into standard INSERT and UPDATE events formatted for the respective downstream.
- Soft Delete Restoration: The internal UPDATE statement that represents a logical DELETE in TiDB will be restored to a standard DELETE event for the external downstream system.
- Hard Delete: These are also directly ignored.

#### How to Guarantee Cross-Table Transaction Consistency

How to Enable Cross-Table Transaction Consistency in LWW Mode

- Enablement: The Cross-Table Transaction capability will be enabled via a new parameter in the Sink URI: `enable-transaction-atomic`. Once enabled, all data for that Changefeed will be written downstream at transaction granularity. We only support TiDB Downstream now.
- Filter Updates: Users must follow a `pause → update filter → resume sequence` to safely modify the tables included in the Changefeed.

##### Overall Architecture: Transaction  Combinator

To support Cross-Table Transactions Consistency, we added a transaction combinator layer between the dispatcher layer and the sink to combine transactions. The transaction combinator receives all data from the dispatcher layer, restores it to transaction granularity, and then writes it to various sink modules to generate the corresponding output format for downstream.

The Transaction Combinator consists of two core components: Event Storage, Transaction Processing.

![active-active-cdc-arch.jpeg](active-active-cdc-arch.jpeg)

1. Event Storage
    - Function: A Memory + Disk hybrid KV store used for temporarily holding events received from various Dispatchers.（Each Dispatcher corresponds to each table in the changefeed, is responsible for receiving the Event in the corresponding table and sending it to the Sink module.)
    - Key Structure: The Key design guarantees efficient retrieval and sorting based on temporal order. We will use CommitTS and StartTS as the primary sorting prefixes.
    - Data Flow: Events are preferentially stored in the memory layer. When memory utilization exceeds a predefined threshold, data spills over to the Disk Store.
    - Cleanup Mechanism: The storage data is periodically cleaned. After the Transaction Processing module successfully reads and confirms the processing of events, it instructs the Storage to delete all events with CommitTS ≤ ProgressTs, maintaining a manageable storage size (with the goal of maximizing data residency in memory for performance).

2. Transaction Processing
    - Progress Driver: This module is driven by the latest ProgressTs pushed from the Dispatcher Layer. ProgressTs acts as a Changefeed-level safe watermark, ensuring all events with CommitTS ≤ ProgressTs have been safely written to Event Storage.
    - Txn Rebuilder: The Processing module extracts all events with CommitTS ≤ ProgressTs from the Storage. It uses CommitTS and StartTS as unique identifiers to accurately combine fragmented events into complete transactions (Txn).
    - Cleanup Trigger: After reading the necessary data, this module triggers Event Storage to perform the cleanup of events with CommitTS ≤ ProgressTs.

##### DDL and DML Event Ordering

DDL and DML events never share a single transaction. The Transaction Processing module must strictly maintain DDL sequence during transaction reassembly:

- The Processing Layer identifies a DDL transaction.
- It must ensure that all DML transactions with a CommitTS less than or equal to the DDL transaction's CommitTS have been successfully written to the downstream TiDB.
- Once confirmation of DML synchronization is received, the DDL transaction is allowed to be dispatched to Sink.
- Subsequent DML transactions can only be processed after the DDL transaction has successfully executed downstream.


##### Node Restart and Checkpoint Mechanism

The Transaction Sink's restart mechanism ensures data consistency and preserves LWW correctness.

- Checkpoint Storage: The Transaction Sink's CheckpointTs is stored in etcd, serving as the startTs for data retrieval upon node restart.
- Checkpoint Semantics: The stored CheckpointTs signifies that all events with CommitTS less than or equal to this timestamp have been successfully synchronized to the downstream TiDB at the transaction granularity.
- Fault Tolerance: The CheckpointTs stored in etcd will theoretically lag slightly behind the actual progress. Upon restart, partial data already processed will be re-pulled. Because the Transaction Sink generates LWW-enabled SQL, these redundant writes of stale transactions will be safely ignored downstream, ensuring eventual correctness and simplifying the recovery process.


##### Scheduling Strategy

To guarantee Cross-Table Transaction Atomicity, it is mandatory that all tables associated with a single Changefeed are synchronized on the same TiCDC node.

- Implementation: The Maintainer is responsible for creating all Dispatchers for the Changefeed on its own node.
- Coordinator Role: The Coordinator manages the load balance of Maintainer instances across nodes (as a preliminary goal). Further balancing based on Transaction Sink throughput may be considered later.

#### Limitations and Constraints

- Performance Limits: Due to the single-node centralized scheduling bottleneck, the maximum throughput for a single changefeed enabling this feature is wide table 100MiB/s and narrow table 30MiB/s, supporting a maximum of 10K tables.
- Conflict Constraint: It is not supported to configure multiple changefeeds with this feature enabled to synchronize the same table into the same downstream cluster.
- Feature Gaps: This initial design does not support Syncpoint or Redo log functionalities.
  - Since we already guarantee cross table transaction atomacity, Redo log is not neccessary.
  - SyncPoint is not possible to create an identical consistent view between two clusters in active-active setup.


### (Unsolved) To Support Secondary Unique Index

Currently, active-active mode does not support secondary unique indexes until a well-defined mechanism is introduced to handle cross-row conflicts. For example:

```
-- Suppose a table with an extra unique index
CREATE TABLE t(id int primary key, u int unique);

-- ClusterA
INSERT INTO t VALUES(1, 10); -- commit_ts: ts1
-- ClusterB
INSERT INTO t VALUE(1, 20); -- commit_ts: ts2
-- ClusterA
INSERT INTO t VALUES(2, 20); -- commit_ts: ts3

-- Suppose ts1 < ts2 < ts3.
-- We have a dilemma when syncing (1, 20) from cluster B to cluster A:
--   A should overwrite (1, 10) because ts2 > ts1, and we should remove the conflict row (2, 20) first
--   But we cannot remove (2, 20) because (2, 20) has a newer version (ts2 < ts3)
```

I believe the resolution strategy should be tailored to the specific business logic. But if a defined behavior is required, we may introduce additional rules as follows:

- When CDC synchronizes a row, it should check for all potential conflicts:
  - If the incoming row's commit timestamp is greater than all conflicting rows, it should remove the old conflicting rows and insert the synchronized row.
  - If the incoming row's commit timestamp is less than any conflicting row, it should skip synchronizing the current row.

In the example above, ts2 should be skipped because the column u conflicts with the existing row (2, 20), which has a greater commit timestamp.

Unfortunately, the above rule cannot guarantee eventual consistency across clusters. For example:

```
create table t(id int primary key, u int unique);
-- clusterA
insert into t values(1, 20); -- commit_ts1
--clusterB
insert into t values(1, 10); -- commit_ts2
--clusterC
insert into t values(2, 10); -- commit_ts3

-- suppose commit_ts1 < commit_ts2 < commit_ts3

-- ClusterA has the apply sequence: (1, 20), (1, 10), (2, 10)
-- (1, 10) will overwrite (1, 20)
-- (2, 10) will overwrite (1, 10) 
-- The final result is: (2, 10)

-- ClusterC has the apply sequence: (2, 10), (1, 20), (1, 10)
-- (1, 20) be inserted because no conflict
-- (1, 10) will be skipped because its commit_ts is less than (2, 10)
-- The final result is (2, 10), (1, 20)
```

This issue can still occur even with only two clusters:

```
-- clusterA
insert into t values(1, 20); -- commit_ts1
--clusterB
insert into t values(1, 10); -- commit_ts2
replace into t values(2, 10); -- commit_ts3

-- supporse commit_ts1 < commit_ts2 < commit_ts3
-- A apply sequence (A: 1, 20) -> (B: 1, 10) -> (B: 2, 10)
-- The final result is: (2, 10)

-- B apply sequence (B: 1, 10) -> (B: 2, 10) -> (A: 1, 20), 
-- The final result is: (1, 20), (2, 10)
```

### CASE Scenario Description

In this section, we will explain in detail the behavior of each case in active-to-active mode. We use this table to explain in the following cases for simplify.

Note: All values in the following SQL cases are just for demonstration, instead of the actual values, especially the ts-related values.

```
CREATE TABLE IF NOT EXISTS test (
    id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
);
```

#### Case 1

![active-active-case1.png](active-active-case1.png)


```
--Region A
INSERT INTO test (id, first_name) VALUES(1, "Ben");

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |     Ben     |    NULL    |    NULL         |       1          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region B
INSERT INTO test (id, first_name) VALUES(1, "Alice");

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |    NULL         |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Ben", NULL, 1, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |    NULL         |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--TiCDC Sync from Region B to Region A
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Alice", NULL, 2, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+
```

#### Case 2 

![active-active-case2.png](active-active-case2.png)


```
--pre State

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |      NULL       |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region A
UPDATE test SET first_name = "Mary" where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |     Mary    |    NULL    |    NULL         |       4          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region B
UPDATE test SET last_name = "Smith" where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |   Smith    |    NULL         |       5          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Mary", NULL, 4, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |   Smith    |    NULL         |       5          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--TiCDC Sync from Region B to Region A
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Alice", "Smith", 5, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |   Smith    |       5         |       6          |
+----+-------------+------------+-----------------+------------------+
```

#### Case 3

![active-active-case3.png](active-active-case3.png)

```
--pre State

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |       2         |       3          |
| 2  |   Alice     |    NULL    |       2         |       3          |
| 3  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |      NULL       |       2          |
| 2  |   Alice     |    NULL    |      NULL       |       2          |
| 3  |   Alice     |    NULL    |      NULL       |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region A
BEGIN;
UPDATE test SET first_name = "Mary" where id = 1;
UPDATE test SET first_name = "Mary" where id = 2;
COMMIT;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Mary      |    NULL    |      NULL       |       4          |
| 2  |   Mary      |    NULL    |      NULL       |       4          |
| 3  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region B
BEGIN;
UPDATE test SET first_name = "John" where id = 2;
UPDATE test SET first_name = "John" where id = 3;
COMMIT;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |      NULL       |       2          |
| 2  |   John      |    NULL    |      NULL       |       5          |
| 3  |   John      |    NULL    |      NULL       |       5          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

-- TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Mary", NULL, 4, NULL), (2, "Mary", NULL, 4, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

-- Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Mary      |    NULL    |       4         |       6          |
| 2  |   John      |    NULL    |      NULL       |       5          |
| 3  |   John      |    NULL    |      NULL       |       5          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

-- TiCDC Sync from Region B to Region A
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (2, "John", NULL, 5, NULL), (3, "John", NULL, 5, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);
-- Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Mary      |    NULL    |      NULL       |       4          |
| 2  |   John      |    NULL    |       5         |       7          |
| 3  |   John      |    NULL    |       5         |       7          |
+----+-------------+------------+-----------------+------------------+
```

#### Case 4

![active-active-case4.png](active-active-case4.png)

```
-- Region A
INSERT INTO test (id, first_name) VALUES(1, "Mary");

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |     Mary    |    NULL    |    NULL         |       4          |
+----+-------------+------------+-----------------+------------------+

UPDATE test SET first_name = "John" where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |     John    |    NULL    |    NULL         |       5          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

-- TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time)  VALUES (1, "Mary", NULL, 4, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);
           
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "John", NULL, 5, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

-- Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |     John    |    NULL    |      5          |       7          |
+----+-------------+------------+-----------------+------------------+
```

#### Case 5

![active-active-case5.png](active-active-case5.png)

This case is just about TiDB transactions. TiDB behavior defined by the transaction mode:
- [PESSIMISTIC](https://docs.pingcap.com/tidb/stable/pessimistic-transaction/) (default)
- [OPTIMISTIC](https://docs.pingcap.com/tidb/stable/optimistic-transaction/)

#### Case 6

![active-active-case6.png](active-active-case6.png)

This will never happen in our design; TiDB will guarantee the Region A to generate T1 that always has a greater value than T2.

#### Case 7

![active-active-case7.png](active-active-case7.png)

```
--pre State

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |      NULL       |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region A
DELETE FROM test where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts, _tidb_softdelete_time from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     Mary    |    NULL    |    NULL         |       4          |           4          |
+----+-------------+------------+-----------------+------------------+---------------------+

------------------------------------------------------------------------------
--Region B
UPDATE test SET first_name = "John", last_name = "Smith" where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts, _tidb_softdelete_time from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     John    |    Smith   |    NULL         |       5          |         NULL         |
+----+-------------+------------+-----------------+------------------+----------------------+

------------------------------------------------------------------------------

--TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "Mary", NULL, 4, 4) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);
 
--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     John    |    Smith   |    NULL         |       5          |         NULL         |
+----+-------------+------------+-----------------+------------------+----------------------+

           
------------------------------------------------------------------------------        
--TiCDC Sync from Region B to Region A
           
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time)  VALUES (1, "John", NULL, 5, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     John    |    Smith   |       5         |       6          |         NULL         |
+----+-------------+------------+-----------------+------------------+----------------------+

```

#### Case 8

![active-active-case8.png](active-active-case8.png)

```
--pre State

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |       2         |       3          |
+----+-------------+------------+-----------------+------------------+

--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  |
+----+-------------+------------+-----------------+------------------+
| 1  |   Alice     |    NULL    |      NULL       |       2          |
+----+-------------+------------+-----------------+------------------+

------------------------------------------------------------------------------

--Region A
UPDATE test SET first_name = "John", last_name = "Smith" where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts, _tidb_softdelete_time from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     John    |    Smith   |    NULL         |       4          |         NULL         |
+----+-------------+------------+-----------------+------------------+----------------------+

------------------------------------------------------------------------------
--Region B

DELETE FROM test where id = 1;

select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts, _tidb_softdelete_time from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     Mary    |    NULL    |    NULL         |       5          |           5          |
+----+-------------+------------+-----------------+------------------+---------------------+
------------------------------------------------------------------------------

--TiCDC Sync from Region A to Region B
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time) VALUES (1, "John", "Smith", 4, NULL) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);
 
--Region B
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     Mary    |    NULL    |    NULL         |       5          |           5          |
+----+-------------+------------+-----------------+------------------+---------------------+

           
------------------------------------------------------------------------------        
--TiCDC Sync from Region B to Region A
           
INSERT INTO test (id, first_name, last_name, _tidb_origin_ts, _tidb_softdelete_time)  VALUES (1, "Mary", NULL, 5, 5) ON DUPLICATE KEY
    UPDATE first_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(first_name), test.first_name),
           last_name = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(last_name), test.last_name),
           _tidb_softdelete_time = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_softdelete_time), test._tidb_softdelete_time),
           _tidb_origin_ts = IF((@cond := (IFNULL(test._tidb_origin_ts, test._tidb_commit_ts)<= VALUES(_tidb_origin_ts))), VALUES(_tidb_origin_ts), test._tidb_origin_ts);

--Region A
select id, first_name, last_name, _tidb_origin_ts, _tidb_commit_ts from test;
+----+-------------+------------+-----------------+------------------+ ---------------------+
| id |  first_name | last_name  | _tidb_origin_ts | _tidb_commit_ts  | _tidb_softdelete_time
+----+-------------+------------+-----------------+------------------+ ---------------------+
| 1  |     Mary    |    NULL    |     5           |       6          |           5          |
+----+-------------+------------+-----------------+------------------+---------------------+
```

#### Case 9

![active-active-case9.png](active-active-case9.png)

The data type has no impact to the final result in Active-Active scenario.

### How to setup an active-active group from scratch

This section describes how to build an active-active group with multiple TiDB clusters based on  the ability of phase 1. Since it only supports creating active-active tables when they are created at phase 1, a user don't have to do data migration between TiDB clusters.

1. Setup several TiDB clusters, and configure the `tso-unique-index` and  `tso-max-index` for each of them.
2. Create the same tables with `active-active` enabled in all clusters.
3. [Optional] Load the existing data from original systems into all tables in separete TiDB clusters. SyncDiff tool could be used to check the data consistency between clusters.
4. Set up TiCDC changefeeds between clusters, with the `enable-active-active` option turned on.
5. Start writing to tables in any clusters.

### How to migrate existing tables to active-active tables

This section describes how to switch some tables that are not in active-active mode to active-active mode. You can refer to the following steps. 

Note: It's not supported in Phase 1, and it might be supported in the future phases.

1. The tables that need to be modified to active-active mode should be made completely consistent in both two TiDB clusters. The steps are as follows:
  1. Use BR to back up a cluster snapshot containing these tables ([back-up-cluster-snapshots](https://docs.pingcap.com/tidb/stable/br-snapshot-guide/#back-up-cluster-snapshots.)). Besides, record the TSO when the backup was performed, or get TSO by [get-the-backup-time-point-of-a-snapshot-backup](https://docs.pingcap.com/tidb/stable/br-snapshot-guide/#get-the-backup-time-point-of-a-snapshot-backup).
  2. Restore these tables in the other clusters([restore-a-database-or-a-table](https://docs.pingcap.com/tidb/stable/br-snapshot-guide/#restore-a-database-or-a-table).)
  3. If there are some write operations on these tables after starting the backup, we need to use CDC to synchronize this incremental data to the downstream cluster, by specifying `--startTs` as the TSO that start backup([changefeed-cli-parameters](https://docs.pingcap.com/tidb/stable/ticdc-changefeed-config/#changefeed-cli-parameters)).
  4. The data in the two tables will only become consistent once changefeed catches up with the synchronization progress of the corresponding table. You can use SyncDiff to check if the data in the tables of the two clusters is the same(sync-diff-inspector-user-guide).
  5. Then you should stop writing data to the corresponding tables and delete the corresponding changefeed.
2. Execute DDL in both TiDB Clusters to adjust table status.

```
-- It's not supported in Phase 1
ALTER TABLE <> ACTIVE_ACTIVE='ON' SOFTDELETE RETENTION=7 DAY;
```

3. Once both cluster DDL operations have been successfully executed, you can create a changefeed to sync these tables in active-active LWW mode.
4. Start writing to tables in any clusters.

### How to migrate active-active tables to non-active-active tables

This section describes how to switch some tables that are in active-active mode to non-active-active mode. You can refer to the following steps. 

Note: It's not supported in Phase 1, and it might be supported in the future phases.

1. Delete the changefeed or modify its configuration to remove these tables from the changefeed that requires active-active LWW mode synchronization.
  1. When changefeed is in active-active LWW mode, CDC is responsible for checking whether these tables are in active-active. If not, it will report an error and stuck the changefeed.
  2. When the CDC writes data downstream in active-active LWW mode, the downstream TiDB is responsible for checking whether the corresponding table is also in active-active mode. If not, the write fails and an error message is returned.
2. Execute DDL in both TiDB Clusters to adjust table status.

```
-- It's not supported in Phase 1
ALTER TABLE <> ACTIVE_ACTIVE='OFF';
```

After an `active-active` table is altered to a normal table, the hidden column `_tidb_origin_ts` still exists. If the customer think `_tidb_origin_ts` is not needed anymore, he can drop this column by `ALTER TABLE xxx DROP COLUMN _tidb_origin_ts`.

Notice we still have the limitations for DDL operations on `_tidb_origin_ts`
- This column is only allowed to be dropped when the table is not an `active-active` one.
- Column `_tidb_origin_ts` cannot be created manually without the option `ACTIVE_ACTIVE`. If a non-active-active table has this column, it means the  table is altered from an active-active table before. If you alter this table to `active-active` mode again, this column will be reused with all previous origin ts values.

## MySQL compatibility

1. In active-active mode, a MySQL instance cannot join the TiDB clusters as one of the service regions.
2. A MySQL instance can still be downstream of one TiDB cluster of the active-active group and receive the changes from all TiDB clusters. I.e. All changes in the TiDB clusters are applied in any one TiDB cluster, so that the MySQL instance only needs to watch one TiDB cluster to receive all changes.


# Limitation from User Perspective

The active-active mode does not provide serializable, linearizability,  or causal consistency across clusters. For example, it can lead to a “lost update” scenario when two clusters concurrently update the same row:

```
-- A counter table
CREATE TABLE counter (id INT PRIMARY KEY, count INT);
-- Suppose both clusters initially contain the same row:
-- (1, 10)

-- Cluster 1 increments the counter
UPDATE counter SET count = count + 1 WHERE id = 1; -- Result: (1, 11)

-- Cluster 2 increments the counter
UPDATE counter SET count = count + 100 WHERE id = 1; -- Result: (1, 110)

-- The final count value will be either 11 if Cluster 1’s commit has the greater timestamp,
-- or 110 if Cluster 2’s commit has the greater timestamp.
-- However, the correct result required by the business should be 111 — meaning one of the updates is lost.
```

You should not use active-active replication in financial scenarios such as account balance management. The following example illustrates why:

```
-- The balance table
CREATE TABLE balance (
    user_id INT PRIMARY KEY,
    balance INT
);

-- Suppose user1 and user2 initially both have 100 dollars.
-- Both clusters initially contain the same rows:
-- (1, 100), (2, 100)

-- In cluster1, user1 transfers 50 to user2
BEGIN;
UPDATE balance SET balance = balance - 50 WHERE user_id = 1;
UPDATE balance SET balance = balance + 50 WHERE user_id = 2;
COMMIT;

-- In cluster2, user2 transfers 49 to user1
BEGIN;
UPDATE balance SET balance = balance + 49 WHERE user_id = 1;
UPDATE balance SET balance = balance - 49 WHERE user_id = 2;
COMMIT;

-- The expected result when executed serially should be (1, 99), (2, 101)
-- But the possible results under Active-Active with LWW are either
-- (1, 50), (2, 150) or (1, 149), (2, 51)
```

