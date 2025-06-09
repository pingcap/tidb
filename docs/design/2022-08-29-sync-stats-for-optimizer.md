# Proposal: Sync Stats For Optimizer

- Author(s): [chrysan](https://github.com/chrysan), [yisaer](https://github.com/yisaer)
- Tracking Issue: https://github.com/pingcap/tidb/issues/37434

## Abstract

This proposes a design of how to retrieve stats for query during optimizing in order to make generated plan stable.

## Background

TiDB maintained stats so that it can use stats during optimizing for generating execution plans for each query.
However, not all the stats are remained in the tidb-server memory, most of the stats of columns are only persisted in the storage and retrieved into memory by other asynchronous loading mechanism.
This may let tidb-server has no required stats during optimizing and generated bad or pseudo plan for queries.
To solve this problem, we introduce `Sync Stats` for optimizer to retrieve stats during optimizing in order to make plan stable.

### Goal

- Sync Stats will load required stats during optimizing
- Considering Sync Stats located in key path of executing a query, it should be efficient performance.
- Sync Stats should expose its result if any error happened for easy diagnosis.

## Proposal

### Design

To achieve the goals, `Sync Stats` should record the required columns and indices in each query and retrieved the stats before physical optimizing.
Thus, in logical optimizing, we added 2 new rule `collectPredicateColumnsPoint` and `syncWaitStatsLoadPoint` to collect the required stats and retrieve the stats from storage.

#### Collect the required columns and indices

In `collectPredicateColumnsPoint` rule phase, we collect the required stats by transferring the plan tree, and extract the columns and indices from the conditions in each logical operator in the plan tree.

For example, for the query `select a from t where b = 1 and c = 1` and its execution plan is like following:

```sql 
+---------------------------+----------+-----------+---------------+----------------------------------+
| id                        | estRows  | task      | access object | operator info                    |
+---------------------------+----------+-----------+---------------+----------------------------------+
| Projection_4              | 0.01     | root      |               | test.t.a                         |
| └─TableReader_7           | 0.01     | root      |               | data:Selection_6                 |
|   └─Selection_6           | 0.01     | cop[tikv] |               | eq(test.t.b, 1), eq(test.t.c, 1) |
|     └─TableFullScan_5     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo   |
+---------------------------+----------+-----------+---------------+----------------------------------+
```

The column `b` and `c` are maintained in the `Selection_6` and `collectPredicateColumnsPoint` will extract them in the operator so that we can know what's the required stats.
After collecting the columns and indices from the queries, we will also check them whether they are necessary to retrieve the stats.
For each column and index maintained in the tidb-server, tidb-server will also maintain the stats loading status in memory as `StatsLoadedStatus`.
If the `StatsLoadedStatus` of column or index is full loaded, it will not to load stats during logical optimizing.

#### Retrieve the stats

After collecting the necessary columns and indices, tidb-server will collect the stats during `syncWaitStatsLoadPoint` phase.
For each tidb-server, it will start several sub sync stats loaders to load stats for columns and indices given by the above phase.
As each query may send loading stats request to sync stats loaders, the required columns and indices might be duplicated between each query.
Thus in order to improve the performance for sync stats loaders and reduce loading duplicated stats for columns and indices, each sync stats loader will check other loaders in order to avoid loading duplicated stats.
If the required stats are loaded by one loader, other loaders will not load this stats again.

## Diagnosability

If any error happened during `Sync Stats`, tidb-server should provide the ability to diagnose the error happened in the sync stats.

### Slow Log

If any error happened during sync stats, it will set up a flag in slow log as `IsSyncStatsFailed` to indicate the there existed error during sync stats for this query.

And the stats info will also be recorded in the slow log including its loading memory status, this will also be helpful to check whether stats is loaded successfully.

### Plan's Explain Info

In plan's explain info, the memory loading status of columns or indices for datasource operator would also be exposed.
This would also indicate the result of sync stats.
