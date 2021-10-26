# Proposal: Support global index for partition table

- Author(s):    [ldeng-ustc](https://github.com/ldeng-ustc)
- Last updated:  Jul. 22, 2020
- Discussion at:  

## Abstract

This document proposes to support global index for partition table, which can remove some constraints of indexes for partition table.

## Background

### Local index and global index

> **Global Index:** A global index is a one-to-many relationship, allowing one index partition to map to many table partitions. This means global index can be partitioned by the range or hash method, and it can be defined on any type of partitioned, or non-partitioned, table.
>
> **Local Index:** A local index is a one-to-one mapping between a index partition and a table partition, which means index is also partition by the same conditions of table.

### Partitioned table index in TiDB

In TiDB, index entries are encoded to key-value pairs, like: (In practice, index encoding is more complex, see [this article](https://pingcap.com/blog/2017-07-11-tidbinternal2/#map) and two proposals about [full collations](https://github.com/pingcap/tidb/pull/14574) and [cluster index](https://github.com/pingcap/tidb/blob/master/docs/design/2020-05-08-cluster-index.md) for more information.)

```
Key: tablePrefix{tableID}_indexPrefixSep{indexID}_indexedColumnsValue
Value: rowID
```

Each partition of the partitioned table is treated as an independent physical table in the TiKV layer, with its own `partitionID` . Therefore, the index entries of the partitioned table are also encoded as:

```
Key: tablePrefix{partitionID}_indexPrefixSep{indexID}_indexedColumnsValue
Value: rowID
```

So, in the current table partition implementation, a partition and its indexes are encoded to the same key-value range. Or we can say, TiDB only has local index.

That design is compatible with the MySQL, but also makes TiDB have the same [constraint](https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html) as MySQL, which says:

> Every unique key on the table must use every column in the table's partitioning expression.

For example, here is a partitioned table:

```sql
CREATE TABLE t (
    A int,
    B int,
    UNIQUE KEY (B)
)
PARTITION BY RANGE (A) (
 PARTITION p0 VALUES LESS THAN (10),
 PARTITION p1 VALUES LESS THAN (20),
 PARTITION p2 VALUES LESS THAN (maxvalue)
);
```

Then we insert 2 records into it.

```sql
INSERT INTO t (A, B) VALUES (1, 23);
INSERT INTO t (A, B) VALUES (11, 23);
```

The first record will be inserted into `p0`, the second will be inserted into `p1`. The index on column `B` will create two entries which keys are:

```
p0_idxB_23
p1_idxB_23
```

Because index entries are built on different partitions, the uniqueness of the index is broken. So, In MySQL and current TiDB, these indexes are not allowed.

## Proposal

### Overview

This document proposes to support the global index for partitioned tables. Global index use `tableID` instead of `partitionID`  as the prefix of  index entries. Since all entries in a partitioned table use the same prefix, it can ensure index entry is unique in the whole table, thus removing the constraint in partitioned tables indexes.

### Encoding

#### Index key-value Layout

We want global index entry keys to begin with `TableID`, so we have to add `PartitionID` to another place. New index encoding layout:

```
Key: tableID_indexID_ColumnValues
Value: handle_partitionID
```

**Pros:** This key layout is compatible with current index key encoding.

**Cons:**  Hard to drop and exchange partition, because index entries of a partition is not continuous.

##### **Other Alternative:**

```
Key: tableID_partitionID_indexID_ColumnValues
Value: handle
```

Or

```
Key: tableID_indexID_partitionID_ColumnValues
```

**Pros:** Easy to implement partition operators such as drop partition.

**Cons:** Since `partitionID` is unknown when building a query, it needs to scan all partitions of a table when handling point-get or index scan. Also, we need special checks when inserting a row into a table to keep the index unique. It likes another implement of local index but not global index, So we reject it.

#### Detail of value layout

Now, in TiDB, we have 6 kinds of index value layout and use 3 different functions to decode it. This not only makes it hard to add new features for index (we should add at least two layouts and one function), but also makes the code and comments less readable and confusing (`index.Create` has an extreme long comment, and is still getting longer). 

Here is the new description of the index value layout, which is fully compatible with the current version, but easier to understand and extend. (`PartitionID` has been added at `Global Index Segment` )

```
// Value layout:
//		+--New Encoding (with restore data, or common handle, or index is global)
//		|
//		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
//		|  Length:   1     | len(options) | len(padding) |    8        |     1
//		|
//		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
//		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//		|                 See below for more information.
//		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
//		|  IntHandle:     Only exists when table use int handles and index is unique.
//		|  UntouchedFlag: Only exists when index is untouched.
//		|
//		|  Layout of Options:
//		|
//		|     Segment:             Common Handle                 |     Global Index      | New Collation
//		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID | restoreData
//		|     Length:     1         | 2           | len(CHandle) |    1    |    8        | len(restoreData)
//		|
//		|     Common Handle Segment: Exists when unique index used common handles.
//		|     Global Index Segment:  Exists when index is global.
//		|     New Collation Segment: Exists when new collation is used and index contains non-binary string.
//		|
//		+--Old Encoding (without restore data, integer handle, local)
//
//		   Layout: [Handle] | [UntouchedFlag]
//		   Length:   8      |     1
//
//		   Handle:        Only exists in unique index.
//		   UntouchedFlag: Only exists when index is untouched.
//
//		   If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
//		   Length of value <= 9, use to distinguish from the new encoding.
//
```

 

### DDL operations

#### Add/Drop Index

All index entries in add by `index.Create`. We modify it to support new encoding, and add `IndexInfo.Global` field to notify workers add global index entries.

Same goes for dropping index.

#### Drop Partition

In global index, all index entries in a partition is not continuous. It is impossible to delete all index entries of a partition in a single  range deletion. So, we add a reorg state (just like add index), which scan records in the partition and remove relative index entries. 

#### Exchange Partition

Also, this needs a reorg state. It is not supported for the time being because of complexity.

#### Drop Table

Add extra one range deletion to clean global indexes.

### Read from global index

In TiDB, operators in the partitioned table will be translated to UnionAll in the logical plan phase. But an in-progress project plan to translate operators to new partitioned table executors (see issue: [#18016](https://github.com/pingcap/tidb/issues/18016)). we will follow this project and add global index support in new executors.

## Rationale

[Global index in Oracle](https://docs.oracle.com/database/121/VLDBG/GUID-D1E775A0-669B-4E51-8D40-858847B64BEF.htm#VLDBG1256)
[Global index in OceanBase](https://zhuanlan.zhihu.com/p/48745358)

## Compatibility

MySQL does not support global index, which means this feature may cause some compatibility issues. We add an option  `enable_global_index` in `config.Config`  to control it. The default value of this option is `false`, so TiDB will keep consistent with MySQL, unless the user open global index feature manually.

## Implementation

DDL operations will be implemented first.  Plan and executor supports  will be implemented after [new PartitionTable executor](https://github.com/pingcap/tidb/issues/18016)  is completed.
