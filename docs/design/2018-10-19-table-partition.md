# Proposal: Table partition

- Author(s):     [tiancaiamao](https://github.com/tiancaiamao)
- Last updated:  2018-10-19
- Discussion at: https://github.com/pingcap/tidb/issues/7907

## Abstract

This proposal proposes to support the table partition feature.

## Background

MySQL has the [table partition](https://dev.mysql.com/doc/refman/8.0/en/partitioning.html) feature, if this feature is supported in TiDB, many issues could be addressed. For example, drop ranged partitions could be used to remove old data; partition by hash could address the hot data issue thus improve the write performance; query on partitioned table could be faster than on manual sharding tables because of the partition pruning.

## Proposal

Implement the table partition feature step by step. Partition by range first, then partition by hash.

For current implementation in this proposal, subpartition is not being considered. Reorganize partition involving data movement remains unsupported.

## Rationale

The new table partition feature should be problem solving oriented, it may not cover all MySQL counterpart at first.

The syntax should be always MySQL compatible, as TiDB's promise from the beginning.

For those partitioned tables contain unimplemented feature, TiDB parses the SQL but ignore its partition attributes, regard the table as a normal one.

## Compatibility

In the old version, TiDB parses `create table ... partition by ...` and handle it as normal table, the `partition by ...` option is discard.
Now that table partition is adding, we have to consider the following cases:

1. New TiDB runs on the old cluster data
2. Old TiDB runs on the new cluster data
3. Upgrading phases during part of the features is implemented

A new `PartitionInfo.Enable` flag is introduced to the `TableInfo`, and it's persisted.
New TiDB runs on the old cluster will check this flag and find `PartitionInfo.Enable` is false, so it handles the table as before.
Old TiDB will not be able to run with partitioned table data, so **the upgrading is incompatible**. (Note that if no partitioned table is created with the new TiDB, i.e, the feature is not used, the cluster is still able to degrade)

If partition by range is implemented while partition by hash is not, TiDB will not set `PartitionInfo.Enable` to true for `create table ... partition by hash ...`, but it will set do that for `create table ... partition by range ...`.

In a word, only when the persisted `PartitionInfo.Enable` is true, and the code could handle partitioned table, will the table partition feature be supported.

## Implementation

### How the partitioned table is stored

There are two levels of mapping in TiDB: SQL data to a logical key range, logical key range to the physical storage.
When TiDB maps table data to key-value storage, it first encodes `table id + row id` as key, row data as value. Then the logical key range is split into regions and stored into TiKV.

Table partition works on the first level of mapping, partition ID will be made equivalent of the table ID. A partitioned table row uses `partition id + row id` as encoded key.

Table ID and partition IDs relationship should be maintained in the `TableInfo`.

When a new row is inserted into a partitioned table, if the row doesn't  belong to any partition of the table, the operation fails.

Handling `NULL` values refers to MySQL document https://dev.mysql.com/doc/refman/8.0/en/partitioning-handling-nulls.html

### How to read from the partitioned table

```
create table t (id int) partition by range (id)
(partition p1 values less than (10),
partition p2 values less than (20),
partition p3 values less than (30))
```

This SQL query

```
select * from t 
```

is equal to

```
select * from (union all
select * from p1 where id < 10
select * from p2 where id < 20
select * from p3 where id < 30)
```

During logical optimize phase, the `DataSource` plan is translated into `UnionAll`, then each partition generates its own `TableReader` in the physical optimize phase.

The drawbacks of this implementation are:

* If the table has many partitions, there will be many readers, then `explain` result is not friendly to user
* `UnionAll` executor can't keep order, so if some executor need ordered result such as `IndexReader`, an extra `Sort` executor is needed

If [partition selection](https://dev.mysql.com/doc/refman/5.7/en/partitioning-selection.html) is used, planner should translate table ID to partition ID, and turn off partition pruning.

#### Partition pruning

During logical optimize phase, after predicate push down, partition pruning is performed.

For range partition, partition pruning is based on the range filter conditions of the partition column. For hash partition, if the filter condition is something like `key = const`, partition pruning could work.

### How to write to the partitioned table

All the write operation calls functions like `table.AddRecord` eventually, so implement write operation on a partitioned table simply implement this interface method on the `PartitionedTable` struct.

`PartitionedTable` implements the `table.Table` interface, overloads the `AddRecord` method. It also comes with a `locatePartition` method, to decide which partition a row should be insert into.

Each partition maintains its own index data, the insert operation should keep data and index consistent.

### DDL operations

DROP/TRUNCATE/ADD, those three operations works on range partition.

`DROP PARTITION` is similar to `DROP TABLE`, except that partition ID is used. `TableInfo` should be updated after the drop partition operation. Note that if one want to drop the last partition in a table, he should use drop table rather than drop partition.

Trancate partition truncates all data and index in a partition, but keep the partition itself.

### Partition management

There are many statements for partition management, current implementation could just parse the SQL and ignore them.

```
ALTER TABLE ... REBUILD PARTITION ...
ALTER TABLE ... OPTIMIZE PARTITION ...
ALTER TABLE ... ANALYZE PARTITION ...
ALTER TABLE ... REPAIR PARTITION ...
ALTER TABLE ... CHECK PARTITION ...
ALTER TABLE ... DROP/TRUNCATE PARTITION
ALTER TABLE ... ADD PARTITION
SHOW CREATE TABLE
SHOW TABLE STATUS
INFORMATION_SCHEMA.PARTITIONS table
```

### Limitations

Refer to MySQL https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html

The partition key must be an integer column, or an expression which evaluate to an integer (or NULL).

The partition key must not be a subquery, even a subquery which evaluate to an integer.

The maximum partitions count of a table is 8192 in MySQL, but 1024 in TiDB.

Functions allowed in partition expressions https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-functions.html

## Open issues (if applicable)

[https://github.com/pingcap/tidb/issues/7516](https://github.com/pingcap/tidb/issues/7516)
