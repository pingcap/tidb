# Index Usage

- Author： [YangKeao](https://github.com/YangKeao)
- Tracking Issue:
  - <https://github.com/pingcap/tidb/issues/49830>

## Motivation

It's a common scenario that multiple indexes are created on one table.  Some of them cost space and slow down DML, while it's never selected.  
Basically, there are 2 types of indexes we should care about

- Unused (or inefficient) indexes: the indexes that are not being selected by optimizer. We can only tell from runtime statistics collected.  
- Redundant indexes: the indexes could be covered by other indexes (same prefix or ...). This can be tell from the definitions of the indexes.

This RFC will focus on the first kind of indexes. By collecting enough runtime statistics, we can provide evidences to show that an index is unused or inefficient, so that the users can drop these indexes confidently.

## Design Goals

- Offer a view to decide which indexes are not being accessed cluster-wide. 
- The overhead to latency is minimized (~0.1%).

## Detailed Design

### User Interface

Re-use a global system variable / config [`tidb_enable_collect_execution_info`](https://docs.pingcap.com/tidb/stable/system-variables#tidb_enable_collect_execution_info) to enable or disable collecting index usages.

Provide system tables `information_schema.tidb_index_usage` and `information_schema.cluster_tidb_index_usage` to show the index usages for the index on a node and cluster-wide. The schema of this table is:

|Field|DataType|Description|
|-|-|-|
|INSTANCE|varchar(64)|For `cluster_` table to represent node|
|TABLE_SCHEMA|varchar(64)|The corresponding `schema` of the table|
|TABLE_NAME|varchar(64)|The name of the table|
|INDEX_NAME|varchar(64)|The name of the index|
|QUERY_TOTAL|bigint|The count of all queries using this index|
|KV_REQ_TOTAL|bigint|The count of all KV requests using this index|
|ROWS_ACCESS_TOTAL|bigint|The count of all rows accessed by reading this index|
|PERCENTAGE_ACCESS_0|bigint|The number of occurrences where the ratio of accessed rows to total rows is 0%|
|PERCENTAGE_ACCESS_0_1|bigint|The number of occurrences where the ratio of accessed rows to total rows falls into (0% ~ 1%).|
|PERCENTAGE_ACCESS_1_10|bigint|The number of occurrences where the ratio of accessed rows to total rows falls into [1% ~ 10%).|
| PERCENTAGE_ACCESS_10_20    | bigint   | The number of occurrences where the ratio of accessed rows to total rows falls into [10% ~ 20%).          |
| PERCENTAGE_ACCESS_20_50    | bigint   | The number of occurrences where the ratio of accessed rows to total rows falls into [20% ~ 50%).          |
| PERCENTAGE_ACCESS_50_100   | bigint   | The number of occurrences where the ratio of accessed rows to total rows falls into [50% ~ 100%).         |
| PERCENTAGE_ACCESS_100      | bigint   | The number of occurrences where the ratio of accessed rows to total rows is 100%.                          |
| LAST_ACCESS_TIME         | datetime | The datetime this index was last accessed.                                                                  |

The percentage is calculated by dividing the count of accessed rows by the count of all rows in the table. The count of rows in table is an assumption value by managing table row deltas, which has already been used by optimizer.

These data are all stored in TiDB memory. Therefore, the data on an instance are lost when the TiDB instance shutdown or reboot.

Also, we provide a view to help users identify the unused index quickly: the `sys.schema_unused_indexes`. It contains the following columns:

|Field|DataType|Description|
|-|-|-|
|TABLE_SCHEMA|varchar(65)|The corresponding `schema` of the table|
|TABLE_NAME|varchar(65)|The name of the table|
|INDEX_NAME|varchar(65)|The name of the index|

It relies on the `information_schema.cluster_tidb_index_usage`. It'll list the indexes which haven't been used since the last startup of tidb instances.

### Data source

We need to get / calculate the following data, and record them in a per-node memory structure:

1. `KV_REQ_TOTAL`. It is equal to the `totalTasks` in the `basicCopRuntimeStats`. A new method is needed to expose it.
2. `QUERY_TOTAL`. We need a structure on `StmtCtx` to make sure a query is only counted once.
3. `READ_SELECTION_RATE`, `ACCESS_SELECTION_RATE`. They are stored in a histogram, and are calculated by access_rows / total_rows.

  1. The rows returned by the following plan should be recorded as access_rows
    1. `PhysicalIndexScan`
    2. `PointGet` if the object is an index.
  2. `total_rows` can be dumped from `(*statistics.HistColl).RealtimeCount`.The RealtimeCount in `sc.usedStatsInforecords` the used stats in current statement, but I'm not sure whether it's reliable.

The existing `RuntimeStatsColl` records the execution summary of each physical plan, and is accessed by physical plan ID. We can have multiple options on when / how to re-order the data into index usages.

These physical plans have a corresponsing executor. We need to consider the following executors:

1. `IndexReaderExecutor`
2. `IndexLookUpExecutor`
3. `IndexMergeReaderExecutor`
4. `PointGetExecutor`
5. `BatchPointGetExecutor`

Then the rows returned by the `Index(Range|Full)Scan` inside can be recorded as `access_rows`.

This information is reported and collected when the corresponding executor is closed. For example:

```go
func (e *IndexLookUpExecutor) Close() error {
    // ...
    accessRows := runtimeStatsColl.GetCopStats(e.idxPlans[0].ID()).GetActRows()
    returnRows := accessRows
    if len(e.idxPlans) > 1 {
        selID := e.idxPlans[len(e.idxPlans)-1].ID()
        returnRows = runtimeStatsColl.GetCopStats(selID).GetActRows()
    }
    // record the `accessRows` and `returnRows`
    // ...
```

### Index Usage Collector

As discussed in the previous section, we'll need both a global structure to record the usages on the current node and a per-statement structure to report the usage to the global structure and count `QEURY_TOTAL`.

#### Node Index Usage Collector

This feature needs a global structure to track the stats of every "existing" index. It should be synchronized with the information schema. However, keeping real synchronization is pretty hard and unnecessary (one option is to attach the stats to info schema, so it can be created automatically after loading the index meta, which sounds bad). 

In order to achieve similar goals, we need to consider the following situations:

1. In `(Executor).Close`, it reports the stats to an index which exists in node index usages.
In this situation, we should simply merge the stats with the existing stats.
2. In `(Executor).Close`, it reports the stats to an index which *doesn't* exist in node index usages.
In this case, we should create a new entry to store the stats information of this index *without* considering whether this index actually exists.
3. The index exists in node index usages, but it has been removed.
A global cron job can be used to remove the disappeared tables and index.

When the user is querying the table `tidb_index_usagetable`, these stats will be filtered by doing an intersection with the info schema in the query transaction, so that the user won't see the already removed table / index.

To achieve these functions, it can be organized in the following structure:

```go
type IndexKey struct {
    TableID int64
    IndexID int64
}

type IndexUsage struct {
    QueryTotal atomic.Uint64
    KvReqTotal atomic.Uint64

    RowAccessTotal   atomic.Uint64
    // 0, 0-1, 1-10, 10-20, 20-50, 50-100, 100
    PercentageAccess [7]atomic.Uint64
}

type indexUsageCollector struct {
    mu     sync.RWMutex
    usages map[IndexKey]*IndexUsage
}

type IndexRowUsageCollector interface {
    // ReportIndex reports one usage of the index
    ReportIndex(key IndexKey, kvReqTotal uint64, rowAccess uint64, tableTotalRows uint64)
}

type IndexUsageCollector interface {
    IndexRowUsageCollector
    ReportQuery(key IndexKey, queryTotal uint64)
    GetUsage(key IndexKey) *IndexUsage

    StartGCWorker() IndexUsageGCWorker
}

type IndexUsageGCWorker interface {
    Stop()
}
```

As we don't need to keep consistent between multiple fields of `IndexUsage`. The method `ReportIndex`, `ReportQuery` and `GetUsage` will only need a read lock (but need to upgrade to write lock when the index is not found in the usages map), so that each statement will never block each other. The `RunGCWorker` will take the write lock for each several minutes (IMO 15 or 30 minutes is enough). The `RunGCWorker` will be called when the domain is started, and `StopGCWorker` is called when the domain is about to stop.

#### Per-statement Index Usage Collector

A per-statement structure is needed to track whether an index has been used / reported in the current stmt. As the executors will not be Closed concurrently, it's safe to not protect indexUsage .

```go
type stmtIndexUsages struct {
    usages map[IndexKey]struct{}
}

var _ IndexRowUsageCollector = &stmtIndexUsages{}
```

It'll only implement `ReportIndex`. The method `ReportIndex` works like a wrapper of `(IndexUsageCollector).ReportIndex`. It'll report the index usage to the global `IndexUsageCollector` and record the index in `stmtIndexUsages.usages`. In `FinishExecuteStmt`, it'll be able to call ReportQuery for each used index.
