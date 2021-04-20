# Temporary Table Design

- Authors: [Kangli Mao](http://github.com/tiancaiamao), [Ming Zhang](http://github.com/djshow832)
- Discussion PR: https://github.com/pingcap/tidb/pull/24168
- Tracking Issue: https://github.com/pingcap/tidb/issues/24169

# Introduction

A temporary table is a special table whose rows data are only temporarily available. 

For a local temporary table, it is visible only within the current session, and the table is dropped automatically when the session is closed.

For a global temporary table, the table schema is visible to all the sessions, but the changes into the table are only available within the current transaction, when the transaction commits, all changes to the global temporary table are discarded.

## Syntax

The syntax of creating a global  temporary table:

```sql
CREATE GLOBAL TEMPORARY TABLE tbl_name (create_definition) 
ON COMMIT DELETE ROWS
[ENGINE=engine_name];
```

The syntax of creating a session(local) temporary table:

```sql
CREATE TEMPORARY TABLE tbl_name (create_definition) 
[ENGINE=engine_name];
```

In the following sections, session temporary tables and local temporary tables are used interchangeably.

## Visibility of table definition

There are 2 kinds of table definition visibility.

- Global: global temporary tables are visible to all sessions. These tables only need to be created once and the metadata is persistent.
- Local: local temporary tables are visible to the session that creates them. These tables must be created in the session before being used and the metadata is only kept in memory.

## Visibility of data

There are 2 kinds of data visibility:

- Session: when the table is a session temporary table, the data will be kept after a transaction commits. Subsequent transactions will see the data committed by the previous transactions.
- Transaction: when the table is defined as `ON COMMIT DELETE ROWS`, this data will be cleared automatically after a transaction commits. Subsequent transactions won’t see the data committed by the previous transactions.

## Storage engines

TiDB uses TiKV and TiFlash to store the data of normal tables. This is also true for temporary tables by default because MySQL does so. However, temporary tables sometimes are used to boost performance, so it's also reasonable to support in-memory temporary tables. There are 2 kinds of storage engines available:

- Memory or TempTable: TiDB keeps data in memory, this is the default setting. If the memory consumed by a temporary table exceeds a threshold, then the data will be spilled to the local disk on TiDB.
- InnoDB: TiDB stores data on TiKV and TiFlash with more than one replica, just like normal tables.

# Motivation or Background

Temporary tables are useful in applications where a result set is to be buffered (temporarily persisted), perhaps because it is constructed by running multiple DML operations.

- What's the goal?
- usability
- performance
- infrastructure for CTE

# Detailed Design

## Metadata

The table ID of global temporary tables must be globally unique, while the local temporary tables don’t. However, logical or physical plans involve table IDs, which means the temporary table IDs must be different from the normal table IDs. To achieve this goal, i'’s straightforward to also allocate local temporary tables globally.

Name conflict?

Since the metadata of global temporary tables are persistent on TiKV, it's straightforward to execute DDL in the same procedure as normal tables. However, the metadata of local temporary tables are only kept in the memory of the current TiDB instance, so we can bypass the complex online DDL procedure. We need only to generate the metadata locally and then merge it into the information schema. Thus, users cannot see the DDL jobs of local temporary tables through `ADMIN SHOW DDL JOBS`.

Local temporary tables don’t support altering table operations because few users will do that. TiDB should report errors when users try to do that.

As all DDL statements do, any DDL will automatically commit the current transaction before being executed.

Truncating global temporary tables also conforms to the online DDL procedure, which affects other sessions. However, it's different for local temporary tables because the metadata is kept in memory. Truncating local temporary tables just drops the current metadata and creates a new one in memory.

DDL operations, including those using `INSTANT` and `COPY` algorithms, are simple to apply on global temporary tables. For example, adding an index on a global temporary table is easy, because the table must be empty before adding the index. This benefits from automatically committing the current transaction before adding the index. Session-specific temporary tables, on the other hand, do support adding indexes, as other altering table operations.

Some options in `CREATE TABLE` statements are not suitable for temporary tables. These options include: `auto_random`, `shard_row_id_bits`, `pre_split_regions`, `partition by`. Similarly, some related DDL operations are not supported, such as `split table`, `show table regions`, `alter placement policy`. Table partition option is useless to a temporary table in the real use cases, so it's also not supported. Errors should be reported when users declare such options in the statements.

Since some options are not suitable for temporary tables, when a user creates a temporary table from the `CREATE TABLE LIKE` statement and the source table has these options, an error should be reported.

Other options will be kept. For example, clustered indexes and secondary indexes are kept because they can improve performance. Foreign keys, as normal tables, won’t take effect.

- Altering table types is not allowed, since few users will do that:
- Altering a temporary table to a normal table or conversely.
- Altering a global temporary table to a local temporary table or conversely.
- Altering the storage engine of temporary tables.

As the metadata of global temporary tables are persistent on TiKV, the binlog of DDL should also be exported. However, this is unnecessary for local temporary tables.

information_schema?

## Optimizer

Statistics of temporary tables are very different from those of normal tables. The data of each temporary table is invisible to other sessions, so it is unnecessary to persist statistics. On the other hand, even if the sizes of temporary tables are relatively small, it's also necessary to consider statistics since it may improve the query performance significantly.

Updating statistics is a little different from normal tables. We can’t rely on `AUTO ANALYZE` anymore, because the lifecycle of one session is relatively short, i'’s unlikely to wait for `AUTO ANALYZE`. What’s more, `AUTO ANALYZE` runs in background sessions, which means they can’t visit the temporary data.

It's also unreasonable to force users to run `ANALYZE TABLE` periodically in applications. Intuitively, there are 2 ways to maintain statistics:
Update statistics once updating the table. When users run `ANALYZE TABLE`, TiDB updates statistics in the current session, instead of in background sessions.
Instead of maintaining statistics, collecting needed statistics before each query is another option. The collection can be boosted by sampling. `ANALYZE TABLE` needs to do nothing in this way.

Obviously, the cost of reading temporary tables is much lower than reading normal tables, since TiDB doesn’t need to request TiKV or consume any network resources. So the factors of such operations should be lower or 0.

SQL binding is used to bind statements with specific execution plans. Global SQL binding affects all sessions and session SQL binding affects the current session. Since local temporary tables are invisible to other sessions, global SQL binding is meaningless. TiDB should report errors when users try to use global SQL binding on local temporary tables.

Baseline is used to capture the plans of statements that appear more than once. The appearance of statements is counted by SQL digests. Even if all sessions share the same global temporary table definitions, the data and statistics is different from one session to another. Thus baseline and SPM is useless for temporary tables. TiDB will ignore this feature for temporary tables.

Prepared plan cache is used to cache plans for prepared statements to avoid duplicate optimization. Each session has a cache and the scope of each cache is the current session. Even if the cached plan stays in the cache after the temporary table is dropped, the plan won’t take effect and will be removed by the LRU list finally.

explain and explain analyze?

## Storage

Before going further to the executor, we need to determine the storage form of temporary tables.

Basically, there are a few alternatives to store the data of temporary tables, and the most promising ones are:

- Unistore. Unistore is a module that simulates TiKV on a standalone TiDB instance.
- UnionScan. UnionScan is a module that unites membuffer and TiKV data. Membuffer buffers the dirty data a transaction writes. Query operators read UnionScan and UnionScan will read both buffered data and persistent data. Thus, if the persistent part is always empty, then the UnionScan itself is a temporary table.


|                  | Unistore        | UnionScan |
| -------------    | :-------------: | -----:    |
| execution        | Y               | Y         |
| indexes          | Y               | Y         |
| spilling to disk | Y               | N         |
| MVCC             | Y               | N         |


For convenience, TiDB uses UnionScan to store the data of temporary tables.

Nothing needs to be changed for the KV encoding, the temporary table uses the same strategy with the normal table.

When the memory consumed by a temporary table exceeds a threshold, the data should be spilled to the local disk to avoid OOM. The threshold is defined by system variable `temptable_max_ram`, which is 1G by default. Membuffer does not support disk storage for now, so we need to implement it.

A possible implementation is to use the AOF (append-only file) persistent storage, the membuffer is implemented as a red-black tree and its node is allocated from a customized allocator. That allocator manages the memory in an arena manner.  A continuous block of memory becomes an arena block and an arena consists of several arena blocks. We can dump those blocks into the disk and maintain some LRU records for them. A precondition of the AOF implementation is that once a block is dumped, it is never modified. Currently, the value of a node is append-only, but the key is modified in-place, so some changes are necessary. We can keep all the red-black tree nodes in memory, while keeping part of the key-value data in the disk.

Another option is changing the red-black tree to a B+ tree, this option is more disk friendly but the change takes more effort.

After a temporary table commits, its disk data should be cleared. That can be done asynchronously by a new goroutine. The disk files are named by their create date, so even in some crash recovery scenario, they can be cleaned up.

## Executor

For normal tables, auto-increment IDs are allocated in batches from TiKV, which significantly improves performance. This procedure also applies to temporary tables, since the overhead of requiring auto-increment IDs from TiKV is minor.

Since the data of in-memory temporary tables are not needed to be cached anymore, coprocessor cache and point-get cache are ignored. They still work for on-disk temporary tables.

Follower read indicates TiDB to read the follower replicas to release the load of leaders. For in-memory temporary tables, this hint is ignored. It still works for on-disk temporary tables..

Users can also choose the storage engine to read by setting `tidb_isolation_read_engines`. For in-memory temporary tables, this setting will also be ignored. It still works for on-disk temporary tables.

Since in-memory temporary tables are not persistent on TiKV or TiFlash, writing binlog for DML is also unnecessary. This also stays true for on-disk temporary tables, because data is invisible to other sessions.

When ecosystem tools export or backup data, the data should not be read. However, on-disk temporary tables are stored on TiKV and TiFlash, they need to be ignored by those tools.

It's straightforward to support MPP on on-disk temporary tables, because the data is also synchronized to TiFlash. Most operators on in-memory temporary tables can still be processed in TiDB, such as Aggregation and TopN. These operators will not cost much memory because the sizes of in-memory temporary tables are relatively small.

However, joining normal tables with in-memory temporary tables might be a problem, because the sizes of normal tables might be huge and thus merge sort join will cause OOM, while hash join and index lookup join will be too slow. Supporting broadcast join and shuffled hash join on in-memory temporary tables is very difficult. Fortunately, MPP typically happens in OLAP applications, where writing and scanning duration is relatively short compared to computing duration. So users can choose to define on-disk temporary tables in this case.

Memory track?

## Transaction

TSO：the temporary table does not really require the TSO, but for the sake of simplify, we just handle it like other DML operation（point get without TSO）

MVCC: No flashback / recover / stale read / historical read

commit: do not commit the temporary table data to the TiKV

rollback: the operation to the normal table and the temporary table should success or fail together

global txn / local txn: no need to change

schema change during transaction commit: ignore this event for the global temporary; this event would never happen for the local temporary table.

compatibility: async commit & 1PC

## Privileges

DDL：Creating a local temporary table checks the `CREATE TEMPORARY TABLES` privilege.
DML：A global temporary table checks the privileges like the normal table; There is no privilege check for a local temporary table。


## Troubleshooting

telemetry:

statements summary / slow log:

## Interface

Internal temporary tables

internal_tmp_mem_storage_engine
CTE：提供通过 projection（而不是 AST）建临时表的接口；分配 table ID 涉及写数据，如果查询是 read-only 的，不合理。
Derived table
UNION
https://dev.mysql.com/doc/refman/8.0/en/internal-temporary-tables.html


## Modification to the write path
The data of the temporary table is all in the membuffer of UnionScan. Writing to the temporary table is writing to the transaction cache.

The data writing path in TiDB is typically through the `table.Table` interface, rows data are converted into key-values, and then written to TiKV via 2PC. For normal transactions, the data is cached in the membuffer until the transaction commits. The temporary table should never be written to the real TiKV.

A transaction can write to both the temporary table and the normal table. Should the temporary table share the  membuffer with the normal table, or use a separate one?
It's better to share the membuffer so the commit/rollback operation can be atomic. The risk is that in case of a bug, the temporary table data may be written to the real table.

For transaction-specific temporary tables, since the transaction commits automatically clears the temporary table, we can filter out and discard the transaction cache data from the temporary table when committing, and the implementation is relatively simple.

For session-specific temporary tables, the temporary table data is cleared at the end of the session, so the data should survive the transaction’s commit. We can copy the key-value data belonging to the temporary table to another place and use it in the later read operation.

## Modification to the read path

In TiDB, reading is implemented by the coprocessor. Read operations should see their own writes. TiDB uses a UnionScan operator on top of the coprocessor's executor. This operator takes the data read by the coprocessor as a snapshot, then merges it with the membuffer, and passes the result to its parent operator.

For transactional temporary tables, there is no need to do any additional changes, the current code works well.

For the session-specific temporary table, the key-value data of the temporary table in the current transaction should be taken out. We keep a temporary table membuffer in the session variable and if it is not null, the UnionScan operator needs to combine it with the original data. So now the data hierarchy looks like this:

    TiKV snapshot => Old membuffer => current transaction’s membuffer

## DDL

Support the new syntax for the temporary table. Disable many of the features when the table is temporary.

For the local temporary table, any `alter table` operation is prohibited once the table is created. This is incompatible with MySQL, the user should receive a proper error message.

The `show table` operation for a local temporary table does not work, just like MySQL. The motivation is that this table is only for temporary usage, so the user should know what he is doing.

For a global temporary table, its name should not be duplicated with a normal table. For a local temporary table, when its name conflicts with an existing table, it will take a higher priority. We can keep the temporary tables in a local schema, and overload the original one. 
Other feature compatibility issues

The temporary table does not support MVCC, so reading the temporary table after setting the `tidb_snapshot` session variable should return an error.

For `INFORMATION_SCHEMA.TABLES`, there is nothing like a “temporary” column, so we can not distinguish a normal table with a global temporary table in this way. Fortunately, the HTTP API shows the json format of table meta information.

TiDB comes with an optimization when the variable `tidb_constraint_check_in_place` is disabled: checking for duplicate values in UNIQUE indexes is deferred until the transaction commits. Since the temporary table does not do 2PC really, this optimization should be disabled on the temporary table.

`admin check table` is a SQL command to check the data integrity. The temporary table is only for the temporary usage purpose, so for the most of the time, we do not need this command。


# Test Design


A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.

## Functional Tests

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

- DDL
  - Create table / create table like
  - Drop table
  - Truncate table
  - Other DDL (for transaction-specific)
- DML
  - Insert / replace / update / delete / load data
  - All kinds of query operators
  - Show create table / show tables
- Transaction
  - Atomicity (for session-specific)
  - Isolation (RC / SI, linearizability)
  - Data visibility between transactions
  - Optimistic / Pessimistic transactions
- Information_schema
  - Tables
  - Columns
- Privileges

## Scenario Tests

It's used to ensure this feature works as expected in some common scenarios.

Before we implement the spilling to disk feature, we have to know the memory usage for some scenarios. For example, 100M for each temporary table, and 500-600 concurrent connections, how much memory does TiDB use.

## Compatibility Tests



| Feature                                              | Compatibility              | Temporary Table type | Reason                                                                             |
| -------------                                        | :-------------:            | :-----:              | :---:                                                                              |
| placement rules                                      | Report error: not support  |                      | Meaningless                                                                        |
| partition                                            | Report error: not support  |                      | Meaningless                                                                        |
| show table regions / split table / pre_split_regions | Report error: not support  |                      | Meaningless                                                                        |
| stale read / historical read                         | Report error: not support  |                      | Meaningless                                                                        |
| auto_random / `shard_row_id_bits`                    | Report error: not support  |                      | No need to release writing hotspots                                                |
| flashback / recover table                            | Report error: not support  |                      | Meaningless                                                                        |
| global SQL binding                                   | Report error: not support  | local                | Bindings are meaningless after session ends & Tables are different amo ng sessions |
| view                                                 | Report error: not support  | local                | Views are meaningless after session ends & Tables are different amo ng sessions    |
| view                                                 | Need to deal with          | global               | Drop views on dropping temporary tables                                            |
| copr cache                                           | Ignore this setting        |                      | No need to cache                                                                   |
| point get cache                                      | Ignore this setting        |                      | No need to cache                                                                   |
| follower read                                        | Ignore this setting        |                      | Data is neither on TiKV nor  on TiFlash                                            |
| read engine                                          | Ignore this setting        |                      | Data is neither on TiKV nor  on TiFlash                                            |
| GC                                                   | Ignore this setting        |                      | No need to GC data                                                                 |
| select for update / lock in share mode               | Ignore this setting        |                      | No lock is needed                                                                  |
| broadcast join / shuffle hash join                   | Ignore this setting        |                      | Data is not on TiFlash                                                             |
| baseline / SPM                                       | Ignore this setting        |                      |                                                                                    |
| Tables / stats are different among sessions          |                            |                      |                                                                                    |
| `tidb_constraint_check_in_place`                     |                            | Ignore this setting  | Data is not committed on TiKV                                                      |
| auto analyze                                         | Ignore this setting        |                      | Background sessions can’t access private data                                     |
| add index                                            | Need to deal with          |                      |                                                                                    |
| global session-specific                              | Need to backfill in memory |                      |                                                                                    |
| global txn / local txn                               | Need to deal with          |                      | No limitation for read / write                                                     |
| analyze                                              | Need to deal with          |                      | Update in-memory stats in the current session instead of in system sessions        |
| `auto_increment` / `last_insert_id`                  | Need to deal with          |                      | AutoID is allocated locally                                                        |
| telemetry                                            | Need to deal with          |                      | Report the usage of temporary tables                                               |
| all hints                                            | Need to test               |                      |                                                                                    |
| plan cache                                           | Need to test               |                      | plan cache is session-scope                                                        |
| show fields / index / keys                           | Need to test               |                      |                                                                                    |
| SQL binding                                          | Need to test               |                      |                                                                                    |
| clustered index                                      | Need to test               |                      |                                                                                    |
| async commit / 1PC                                   | Need to test               |                      |                                                                                    |
| checksum / check table                               | Need to test               |                      |                                                                                    |
| collation / charset                                  | Need to test               |                      |                                                                                    |
| batch insert                                         | Need to test               |                      |                                                                                    |
| feedback                                             | Need to test               |                      |                                                                                    |
| statements_summary / slow_query                      | Need to test               |                      |                                                                                    |
| SQL normalization                                    |                            |                      |                                                                                    |
| big transaction                                      | Need to test               |                      |                                                                                    |
|                                                      |                            |                      |                                                                                    |

Compatibility with other external components, like TiDB, PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.

Upgrade compatibility

Downgrade compatibility

## Benchmark Tests

The following two parts need to be measured:

- measure the performance of this feature under different parameters
- measure the performance influence on the online workload

sysbench for the temporary table, comparing its performance with the normal table. It means comparing the performance of an in-memory system with a distributed system.

# Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/internal-temporary-tables.html) about the temporary table

[CockroachDB](https://github.com/cockroachdb/cockroach/issues/5807) just uses the normal table as the temporary table. All the temporary tables are stored in a special schema, and it is scanned and cleaned periodically. If a session is finished, the temporary tables of that session are also cleaned.

[Oracle](https://docs.oracle.com/cd/B28359_01/server.111/b28310/tables003.htm#ADMIN11633) uses global temporary tables in the old versions, and later on they also have the private temporary tables. The private temporary table in Oracle looks like MySQL, those tables are visible to the session rather than global. For global temporary tables, Oracle does not need to handle the schema change, because `alter table` is not allowed when some transactions are using the table.

# Development Plan

Stage 1（Support transactional, in-memory temporary table）

- The architecture design for global/local, session/transaction, in-memory/disk spilling (L)
- DDL（2XL)
  - Basic DDL e.g. create/drop table/add index etc
  - Show tables/show create table for global temporary table
- DML (2XL)


- Backup and recover the temporary table meta information (L)
- Sync the temporary table DDL through CDC or Binlog to the downstream
- Privilege (L)
- Compatibility with other features (2XL)
- Performance test (L)

Stage 2 （Support for session-specific temporary table)

- create/drop session temporary table (3XL)
- Duplicated table name handling
- Compatibility with MySQL 8.0 temporary table (in-memory)
- Compatibility with other features (2XL)

Stage 3

- Support spilling to disk for all kind of temporary tables (4XL)


# Unresolved Questions

The detailed implementation plan for spilling to disk, and the related change for the statistic component.

# Design Review
