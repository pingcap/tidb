# Proposal: Support Analyze Predicate Columns

- Author(s): [xuyifangreeneyes](https://github.com/xuyifangreeneyes)
- Discussion PR: 
- Tracking Issue: https://github.com/pingcap/tidb/issues/27828

## Abstract



## Background

The `ANALYZE` statement would collect the statistics for all columns currently. If the table is big and wide, executing `ANALYZE` would consume lots of time, memory and CPU(see https://github.com/pingcap/tidb/issues/27358 for details). However, only some columns' statistics are used in making query plans while others' statistics are not used. Predicate columns are the columns whose statistics are used in making query plans, which usually occur in where conditions, join conditions and so on. If `ANALYZE` only collect the statistics for predicate columns and indexed columns(statistics of indexed columns are important for index selection), the cost of `ANALYZE` can be reduced.

## Proposal

We want to support the `ANALYZE` option to only collect the statistics of predicate columns and indexed columns. Sometimes predicate columns may vary since new queries come in, so we want another option to only collect the statistics of user-specified columns and indexed columns. 

### ANALYZE Syntax

```
ANALYZE TABLE TableNameList (PREDICATE COLUMNS | COLUMNS ColumnNameList)? AnalyzeOptionListOpt
ANALYZE TABLE TableName (PARTITION PartitionNameList)? (PREDICATE COLUMNS | COLUMNS ColumnNameList)? AnalyzeOptionListOpt
```
1. If `PREDICATE COLUMNS` occurs, the statistics of predicate columns and indexed columns would be collected. If there is no predicate columns in record, TiDB would collect the statistics of all columns and give a warning to the client.
2. If `COLUMNS ColumnNameList` occurs, the statistics of the columns in `ColumnNameList` and indexed columns would be collected.
3. Otherwise, the statistics of all columns would be collected.

### Track Predicate Columns

We build a new system table `mysql.column_stats_usage` to track predicate columns.
```sql
CREATE TABLE IF NOT EXISTS mysql.column_stats_usage (
	table_id BIGINT(64) NOT NULL,
	column_id BIGINT(64) NOT NULL,
	last_used_at TIMESTAMP,
	last_analyzed_at TIMESTAMP,
	PRIMARY KEY (table_id, column_id) CLUSTERED
);
```
`table_id` and `column_id` are the IDs of the table and the column respectively. `last_used_at` is the timestamp at which the column stats were used(needed). `last_analyzed_at` is the timestamp at which the column stats are updated.

During the plan optimization phase, we track which column stats are used(needed) in memory. `updateStatsWorker` periodically dumps column stats usage(in memory) into `mysql.column_stats_usage`(in disk).

Each time when executing `ANALYZE PREDICATE COLUMNS`, we read predicate columns(the columns whose `last_used_at` is not null) from `mysql.column_stats_usage` and then decide which columns' statistics need collecting.

If a table or a column is deleted, the related rows in `mysql.column_stats_usage` also need to be deleted.

### Auto Analyze Logic

By default, auto-analyze would collect the statistics of all columns. We can modify the `ANALYZE` configuration for a specific table. The configuration includes the column option(all columns/predicate columns/user-specified columns). Once we set the configuration for the table, auto-analyze can decide which columns' statistics need to be collected according to the column option.

### Modify Count And Outdated Statistics

`modify_count` is currently at the table level. Each time when the table is analyzed, `modify_count` is set to 0. After introducing the predicate/user-specified columns option for `ANALYZE`, some columns' statistics are updated while others' statistics are not(maybe even not exist), but we still set `modify_count` to 0 after `ANALYZE` is finished, which breaks the table level meaning of `modify_count`. A method is to make each column have its own `modify_count` but we don't consider that currently since it involves many logic changes. Another method is to delete the outdated statistics of the columns which are excluded in the current `ANALYZE` statement but we don't adopt that since deleting the outdated statistics may bring the risk of changing plans. Therefore, our behavior is to set `modify_count` to 0 and remain outdated statistics, though it breaks the table level meaning of `modify_count`. It should be emphasized that users should list **all the columns whose statistics need to be collected** rather than parts of them, and TiDB will give a note-level warning to the client each time when `ANALYZE COLUMNS ColumnNameList` is executed. 

## Rationale



## Compatibility

It does not affect the compatibility.

## Implementation
