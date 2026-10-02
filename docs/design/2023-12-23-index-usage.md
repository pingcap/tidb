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

  1. The rows returned by the following plan should be recorded as `access_rows`
     1. `PhysicalIndexScan`
     2. `PointGet` if the object is an index.
  2. `total_rows` can be dumped from `(*statistics.HistColl).RealtimeCount`.The RealtimeCount in `sc.usedStatsInforecords` the used stats in current statement.

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

We already have a similar RFC [2020-09-30-index-usage-information.md](https://github.com/pingcap/tidb/blob/master/docs/design/2020-09-30-index-usage-information.md). It gives a good idea of collecting index usages without locking.

This RFC uses a similar way to track the index usages: allocate an index usage collector for each session, and organize them into a linked list:

```go
type sessionIndexUsageCollector struct {
	sync.Mutex

	mapper indexUsage
	next   *SessionIndexUsageCollector

	deleted bool
}

// Sample is the data structure to store index usage information.
type Sample struct {
	LastUsedAt     time.Time
	QueryTotal     uint64
	KvReqTotal     uint64
	RowAccessTotal uint64
	// 0, 0-1, 1-10, 10-20, 20-50, 50-100, 100
	PercentageAccess [7]uint64
}

type GlobalIndexID struct {
	TableID int64
	IndexID int64
}

type indexUsage map[GlobalIndexID]IndexUsageInformation
```

It provides the following method:

```go
type SessionIndexUsageCollector interface {
    Update(tableID int64, indexID int64, sample *Sample)
    Delete()
}
```

The `Update` method will increase the count of index usages inside. The `Delete` method means this session has been closed, and it's safe to remove this session.

To avoid increasing the `queryTotal` in a query multiple times, we'll also have a statement-level tracker to track the usage of index in a single statement:

```go
type StmtIndexUsageCollector interface {
    Update(tableID int64, indexID int64, sample *Sample)
}

type stmtIndexUsages struct {
    recordedIndex map[IndexKey]struct{}
    sessionCollector SessionIndexUsageCollector
}

var _ StmtIndexUsageCollector = &stmtIndexUsages{}
```

It'll transparently update the usage on `SessionIndexUsageCollector`. The value of `queryTotal` will depend on whether this index has been recorded in the `recordedIndex` map.

#### Sweep index usage list

We'll need to sweep the index usage list in the following cases:

1. Periodically sweep, to avoid keeping too many deleted `SessionIndexUsageCollector` in memory.
2. When the user is reading `tidb_index_usage` table.

The cost of sweeping index usage list is locking the session collector one-by-one. All these information should be collected into a node-scope index usage collector, which is represented as the head of the session list.

#### GC removed index

The schema, table and index can be dropped. After dropping them, the related memory should be released. It's fine to keep some unused index in memory, as we can filter them out when we are retrieving `tidb_index_usage`.

The GC should happen periodically. 20min or more is enough, as `ADD INDEX/DROP INDEX` can be regarded as an infrequent operation.

### Remove redundant implementation

As we already have a partial implementation of index usage for [2020-09-30-index-usage-information.md](https://github.com/pingcap/tidb/blob/master/docs/design/2020-09-30-index-usage-information.md), we should remove / modify some of the codes:

1. Remove the table `SCHEMA_INDEX_USAGE`.
2. Deprecate the config `index-usage-sync-lease`.
3. Remove the codes related with persist index usages to table.
