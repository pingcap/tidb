# Proposal: Table Partition

- Author(s):     [tiancaiamao](https://github.com/tiancaiamao)
- Last updated:  2018-10-19
- Discussion at: https://github.com/pingcap/tidb/issues/7907

## Abstract

This proposal proposes to support the table partition feature.

## Background

MySQL has the [table partition](https://dev.mysql.com/doc/refman/8.0/en/partitioning.html) feature. If this feature is supported in TiDB, many issues could be addressed. For example, drop ranged partitions could be used to remove old data; partition by hash could address the hot data issue thus improve the write performance; query on partitioned table could be faster than on unpartitioned tables because of the partition pruning.

## Proposal

Implement the table partition feature step by step. Partition by range first, then partition by hash.

For current implementation in this proposal, subpartition is not considered. Reorganize partition involving data movement remains unsupported.

## Rationale

The new table partition feature should be problem-solving oriented, and it may not cover all MySQL counterpart at first.

The syntax should be always MySQL compatible, as TiDB's promise from the beginning.

For those partitioned tables which contain unimplemented features, TiDB parses the SQL but ignores its partition attributes, regards the table as a normal one.

## Compatibility

In the old version, TiDB parses `create table ... partition by ...` and handles it as a normal table; the `partition by ...` option is discarded.
Now that table partition is added, and we have to consider the following cases:

1. New TiDB runs on the old cluster data
2. Old TiDB runs on the new cluster data
3. The upgrading phases during part of the features is implemented

A new `PartitionInfo.Enable` flag is introduced to the `TableInfo`, and it's persisted.
New TiDB running on the old cluster will check this flag and find `PartitionInfo.Enable` is false, so it handles the table as before.
Old TiDB will not be able to run with partitioned table data, so **the upgrading is incompatible**. (Note that if no partitioned table is created with the new TiDB, i.e, the feature is not used, the cluster is still able to be degraded)

If partition by range is implemented while partition by hash is not, TiDB will not set `PartitionInfo.Enable` to true for `create table ... partition by hash ...`, but it set `PartitionInfo.Enable` to true for `create table ... partition by range ...`.

In a word, only when the persisted `PartitionInfo.Enable` is true, and the code could handle partitioned table, will the table partition feature be supported.

## Implementation

### How the partitioned table is stored

There are two levels of mapping in TiDB: SQL data to a logical key range, logical key range to the physical storage.
When TiDB maps table data to key-value storage, it first encodes `table id + row id` as key, row data as value. Then the logical key range is split into regions and stored into TiKV.

Table partition works on the first level of mapping, partition ID will be made equivalent of the table ID. A partitioned table row uses `partition id + row id` as encoded key. Partition ID is unique in cluster scope.

The relationship of Table ID and partition ID should be maintained in the `TableInfo`.

When a new row is inserted into a partitioned table, if the row doesn't belong to any partition of the table, the operation fails.

Refer to the MySQL document https://dev.mysql.com/doc/refman/8.0/en/partitioning-handling-nulls.html for handling `NULL` values.

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

During logical optimize phase, the `DataSource` operator is translated into `UnionAll` operator, and then each partition generates its own `TableReader` in the physical optimize phase.

The drawbacks of this implementation are:

* If the table has many partitions, there will be many readers, then `explain` result is not friendly to user
* The `UnionAll` operator can't keep order, so if some operator need ordered result such as `IndexReader`, an extra `Sort` operator is needed

If [partition selection](https://dev.mysql.com/doc/refman/5.7/en/partitioning-selection.html) is used, planner should translate table ID to partition ID, and turn off partition pruning.

#### Partition pruning

During logical optimization phase, after predicate push down, partition pruning is performed.

For range partition, partition pruning is based on the range filter conditions of the partition column. For hash partition, if the filter condition is something like `key = const`, partition pruning could work.

### How to write to the partitioned table

All the write operations call functions like `table.AddRecord` eventually, so implement write operation on a partitioned table simply implements this interface method on the `PartitionedTable` struct.

`PartitionedTable` implements the `table.Table` interface, and overloads the `AddRecord` method. It also comes with a `locatePartition` method, to decide which partition a row should be insert into.

Each partition maintains its own index data, and the insert operation should keep data and index consistent.

### DDL operations

DROP/TRUNCATE/ADD, those three operations works on range partition.

`DROP PARTITION` is similar to `DROP TABLE`, except that partition ID is used. `TableInfo` should be updated after the drop partition operation. Note that if one wants to drop the last partition in a table, he should use drop table rather than drop partition.

Trancate partition truncates all data and indexes in a partition, but keeps the partition itself.

### Partition management

There are many statements for partition management, but the current implementation could just parse the SQL and ignore them.

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

The partition key cannot be an integer column, or an expression which resolves to an integer (or NULL).

The partition key must not be a subquery, even a subquery which resolves to an integer.

The maximum partitions count of a table is 8192 in MySQL, but 1024 in TiDB.

Functions allowed in partition expressions https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-functions.html

## Open issues (if applicable)

[https://github.com/pingcap/tidb/issues/7516](https://github.com/pingcap/tidb/issues/7516)
