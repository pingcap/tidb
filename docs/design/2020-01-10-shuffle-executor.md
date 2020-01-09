# Shuffle Executor

- Author:             [pingyu](https://github.com/pingyu)  (Ping Yu)
- Last updated:  2020-01-10

## Abstract
This article will describe the design of Shuffle Executor in TiDB.

## Background
As described in [#12966](https://github.com/pingcap/tidb/issues/12966), the performance of Window operator in TiDB has much space to improve.
So we implemented a Shuffle Executor, which is proposed [here](https://github.com/pingcap/tidb/pull/14238#issuecomment-569880893) by [SunRunAway](https://github.com/SunRunAway) (Feng Liyuan) , using multi-thread hash grouping, to run each window partition in parallel.
Furthmore, the Shuffle Executor is designed be a general purpose executor, which can also be used for other scenario, such as Parallel Sort-Merge Join, in the future.

## Implementation

The implementation consists of 3 parts:
1. Planning.
2. Building executor.
3. Executing.

### Planning
In CBO procedure (to be specific, in `(*baseLogicalPlan).findBestTask`), we inserts a new physical plan `PhysicalShuffle`, which represents the executing plan of Shuffle Executor, on the top of the very executor to be "shuffled".

Take window operator as example, when table `t` was created as `create table t (i int, j int)`, the plan of `select j, sum(i) over w from t window w as (partition by j)` would be:
```
mysql> explain select j, sum(i) over w from t window w as (partition by j);
+--------------------------+----------+-----------+------------------------------------------------------------+
| id                       | count    | task      | operator info                                              |
+--------------------------+----------+-----------+------------------------------------------------------------+
| Projection_7             | 10000.00 | root      | test.t.j, Column#5                                         |
| └─Window_8               | 10000.00 | root      | sum(cast(test.t.i))->Column#5 over(partition by test.t.j)  |
|   └─Sort_11              | 10000.00 | root      | test.t.j:asc                                               |
|     └─TableReader_10     | 10000.00 | root      | data:TableScan_9                                           |
|       └─TableScan_9      | 10000.00 | cop[tikv] | table:t, range:[-inf,+inf], keep order:false, stats:pseudo |
+--------------------------+----------+-----------+------------------------------------------------------------+
```
To "shuffle" the window operator, the plan would changed to:
```
mysql> explain select j, sum(i) over w from t window w as (partition by j);
+----------------------------+----------+-----------+------------------------------------------------------------+
| id                         | count    | task      | operator info                                              |
+----------------------------+----------+-----------+------------------------------------------------------------+
| Projection_7               | 10000.00 | root      | test.t.j, Column#5                                         |
| └─Shuffle_12               | 10000.00 | root      | execution info: concurrency:4, data source:TableReader_10  |
|   └─Window_8               | 10000.00 | root      | sum(cast(test.t.i))->Column#5 over(partition by test.t.j)  |
|     └─Sort_11              | 10000.00 | root      | test.t.j:asc                                               |
|       └─TableReader_10     | 10000.00 | root      | data:TableScan_9                                           |
|         └─TableScan_9      | 10000.00 | cop[tikv] | table:t, range:[-inf,+inf], keep order:false, stats:pseudo |
+----------------------------+----------+-----------+------------------------------------------------------------+
```
(_See "Building Executor" for what `data source` means_)

The `PhysicalShuffle` is inserted when the following conditions are met:

- The variable for the operator to be executing in parallel manner is enable. As to window operator, the value of variable `tidb_window_concurreny` should be more than 1.
- The NDV (number of dinstict value) of "partition by" row(s) should be more than 1.
- The parallel should be effective enough. As to window operator, when the child of window operator meets the property requirement (i.e. enforced sorting is not necessary), the parallel is not effective enough. See "Benchmarks" for detail.

### Building Executor
TODO

### Executing

The executing steps of Shuffle Executor:

1. It fetches chunks from `DataSource`.
2. It splits tuples from `DataSource` into N partitions (Only "split by hash" is implemented so far).
3. It invokes N workers in parallel, assign each partition as input to each worker and execute child executors.
4. It collects outputs from each worker, then sends outputs to its 
```
                                +-------------+
                        +-------| Main Thread |
                        |       +------+------+
                        |              ^
                        |              |
                        |              +
                        v             +++
                 outputHolderCh       | | outputCh (1 x Concurrency)
                        v             +++
                        |              ^
                        |              |
                        |      +-------+-------+
                        v      |               |
                 +--------------+             +--------------+
          +----- |    worker    |   .......   |    worker    |  workers (N Concurrency)
          |      +------------+-+             +-+------------+   eg. WindowExec (+SortExec)
          |                 ^                 ^
          |                 |                 |
          |                +-+  +-+  ......  +-+
          |                | |  | |          | |
          |                ...  ...          ...  inputCh (Concurrency x 1)
          v                | |  | |          | |
    inputHolderCh          +++  +++          +++
          v                 ^    ^            ^
          |                 |    |            |
          |          +------o----+            |
          |          |      +-----------------+-----+
          |          |                              |
          |      +---+------------+------------+----+-----------+
          |      |              Partition Splitter              |
          |      +--------------+-+------------+-+--------------+
          |                             ^
          |                             |
          |             +---------------v-----------------+
          +---------->  |    fetch data from DataSource   |
                        +---------------------------------+
```

## Benchmarks
TODO

