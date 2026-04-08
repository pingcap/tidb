# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/issues/38535
- Tracking Issue: https://github.com/pingcap/tidb/issues/15000

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Schema change states for REORGANIZE PARTITION](#schema-change-states-for-reorganize-partition)
    * [Error Handling](#error-handling)
    * [Notes](#notes)
* [Test Design](#test-design)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)

## Introduction

Support ALTER TABLE t REORGANIZE PARTITION p1,p2 INTO (partition pNew1 values...)

## Motivation or Background

TiDB is currently lacking the support of changing the partitions of a partitioned table, it only supports adding and dropping LIST/RANGE partitions.
Supporting REORGANIZE PARTITIONs will allow RANGE partitioned tables to have a MAXVALUE partition to catch all values and split it into new ranges. Similar with LIST partitions where one can split or merge different partitions.

When this is implemented, it will also allow future PRs transforming a non-partitioned table into a partitioned table as well as remove partitioning and make a partitioned table a normal non-partitioned table, as well as COALESCE PARTITION and ADD PARTITION for HASH partitioned tables, which is different ALTER statements but can use the same implementation as REORGANIZE PARTITION

The operation should be online, and must handle multiple partitions as well as large data sets.

Possible usage scenarios:
- Full table copy
  - merging all partitions to a single table (ALTER TABLE t REMOVE PARTITIONING)
  - splitting data from many to many partitions, like change the number of HASH partitions
  - splitting a table to many partitions (ALTER TABLE t PARTITION BY ...)
- Partial table copy (not full table/all partitions)
  - split one or more partitions
  - merge two or more partitions

These different use cases can have different optimizations, but the generic form must still be solved:
- N partitions, where each partition has M indexes

First implementation should be based on the merge-txn (row-by-row batch read, update record key with new Physical Table ID, write) transactional batches and then create the indexes in batches index by index, partition by partition.
Later we can implement the ingest (lightning way) optimization, since DDL module are on the way of evolution to do reorg tasks more efficiency.

## Detailed Design

There are two parts of the design:
- Schema change states throughout the operation
- Reorganization implementation, which will be handled in the StateWriteReorganization state.

Where the schema change states will clarify which different steps that will be done in which schema state transitions.

### Schema change states for REORGANIZE PARTITION

Since this operation will:
- create new partitions
- copy data from dropped partitions to new partitions and create their indexes
- change the partition definitions
- drop existing partitions

It will use all these schema change stages:

      // StateNone means this schema element is absent and can't be used.
      StateNone SchemaState = iota
      - Check if the table structure after the ALTER is valid
      - Use the generate physical table ids to each new partition (that was generated already by the client sending the ALTER command).
      - Update the meta data with the new partitions (AddingDefinitions) and which partitions to be dropped (DroppingDefinitions), so that new transactions can double write.
      - Set placement rules
      - Set TiFlash Replicas
      - Set legacy Bundles (non-sql placement)
      - Set the state to StateDeleteOnly
      
      // StateDeleteOnly means we can only delete items for this schema element (the new  partition).
      StateDeleteOnly
      - Set the state to StateWriteOnly

      // StateWriteOnly means we can use any write operation on this schema element,
      // but outer can't read the changed data.
      StateWriteOnly
      - Set the state to StateWriteReorganization
      
      // StateWriteReorganization means we are re-organizing whole data after write only state.
      StateWriteReorganization
      - Copy the data from the partitions to be dropped (one at a time) and insert it into the new partitions. This needs a new backfillWorker implementation.
      - Recreate the indexes one by one for the new partitions (one partition at a time) (create an element for each index and reuse the addIndexWorker). (Note: this can be optimized in the futute, either with the new fast add index implementation, based on lightning. Or by either writing the index entries at the same time as the records, in the previous step, or if the partitioning columns are included in the index or handle)
      - Replace the old partitions with the new partitions in the metadata when the data copying is done
      - Set the state to StateDeleteReorganization

      // StateDeleteReorganization means we are re-organizing whole data after delete only state.
      StateDeleteReorganization - we are using this state in a slightly different way than the comment above says.
      This state is needed since we cannot directly move from StateWriteReorganization to StatePublic.
      Imagine that the StateWriteReorganization is complete and we are updating the schema version, then if a transaction seeing the new schema version is writing to the new partitions, then those changes needs to be written to the old partitions as well, so new transactions in other nodes using the older schema version can still see the changes.
      - Remove the notion of new partitions (AddingDefinitions) and which partitions to be dropped (DroppingDefinitions) and double writing will stop when it goes to StatePublic.
      - Register the range delete of the old partition data (in finishJob / deleteRange).
      - Set the state to StatePublic
      
      // StatePublic means this schema element is ok for all write and read operations.
      StatePublic
      - Table structure is now complete and the table is ready to use with its new partitioning scheme
      - Note that there is a background job for the GCWorker to do in its deleteRange function.

During the reorganization happens in the background the normal write path needs to check if there are any new partitions in the metadata and also check if the updated/deleted/inserted row would match a new partition, and if so, also do the same operation in the new partition, just like during adding index or modify column operations currently does. (To be implemented in `(*partitionedTable) AddRecord/UpdateRecord/RemoveRecord`)

Example of why an extra state between StateWriteReorganize and StatePublic is needed:

```sql
-- table:
CREATE TABLE t (a int) PARTITION BY LIST (a) (PARTITION p0 VALUES IN (1,2,3,4,5), PARTITION p1 VALUES IN (6,7,8,9,10));
-- during alter operation:
ALTER TABLE t REORGANIZE PARTITION p0 INTO (PARTITION p0a VALUES IN (1,2,3), PARTITION p0b VALUES IN (4,5));
```

Partition within parentheses `(p0a [1] p0b [0])` is hidden or to be deleted by GC/DeleteRange. Values in the brackets after the partition `p0a [2]`.

If we go directly from StateWriteReorganize to StatePublic, then clients one schema version behind will not see changes to the new partitions:

| Data (TiKV/Unistore)                    | TiDB client 1                        | TiDB client 2                                                |
| --------------------------------------- | ------------------------------------ | ------------------------------------------------------------ |
| p0 [] p1 [] StateWriteReorganize        |                                      |                                                              |
| p0 [] p1 [] (p0a [] p0b [])             |                                      |                                                              |
| (p0 []) p1 [] p0a [] p0b [] StatePublic |                                      |                                                              |
| (p0 []) p1 [] p0a [2] p0b []            | StatePublic INSERT INTO T VALUES (2) |                                                              |
| (p0 []) p1 [] p0a [2] p0b []            |                                      | StateWriteReorganize SELECT * FROM t => [] (only sees p0,p1) |


But if we add a state between StateWriteReorganize and StatePublic and double write to the old partitions during that state it works:


| Data (TiKV/Unistore)                              | TiDB client 1                                  | TiDB client 2                                                         |
| ------------------------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------- |
| p0 [] p1 [] (p0a [] p0b []) StateWriteReorganize  |                                                |                                                                       |
| (p0 []) p1 [] p0a [] p0b [] StateDeleteReorganize |                                                |                                                                       |
| (p0 [2]) p1 [] p0a [2] p0b []                     | StateDeleteReorganize INSERT INTO T VALUES (2) |                                                                       |
| (p0 [2]) p1 [] p0a [2] p0b []                     |                                                | StateWriteReorganize SELECT * FROM t => [2] (only sees p0,p1)       |
| (p0 [2]) p1 [] p0a [2] p0b [] StatePublic         |                                                |                                                                       |
| (p0 [2]) p1 [] p0a [2] p0b [4]                    | StatePublic INSERT INTO T VALUES (4)           |                                                                       |
| (p0 [2]) p1 [] p0a [2] p0b [4]                    |                                                | StateDeleteReorganize SELECT * FROM t => [2,4] (sees p0a,p0b,p1) |


### Error handling

If any non-retryable error occurs, we will call onDropTablePartition and adjust the logic in that function to also handle the roll back of reorganize partition, in a similar way as it does with model.ActionAddTablePartition.

### Notes

Note that parser support already exists.
There should be no issues with upgrading, and downgrade will not be supported during the DDL.

Notes:
- statistics should be removed from the old partitions.
- statistics will not be generated for the new partitions (future optimization possible, to get statistics during the data copying?)
- the global statistics (table level) will remain the same, since the data has not changed.
- this DDL will be online, while MySQL is blocking on MDL.

## Test Design

Re-use tests from other DDLs like Modify column, but adjust them for Reorganize partition.
A separate test plan will be created and a test report will be written and signed off when the tests are completed.

### Benchmark Tests

Correctness and functionality is higher priority than performance.

## Impacts & Risks

Impacts:
- better usability of partitioned tables
- online alter in TiDB, where MySQL is blocking
- all affected data needs to be read (CPU/IO/Network load on TiDB/PD/TiKV), even multiple times in case of indexes.
- all data needs to be writted (duplicated, both row-data and indexes), including transaction logs (more disk space on TiKV, CPU/IO/Network load on TiDB/PD/TiKV and TiFlash if configured on the table).

Risks:
- introduction of bugs
  - in the DDL code
  - in the write path (double writing the changes for transactions running during the DDL)
- out of disk space
- out of memory
- general resource usage, resulting in lower performance of the cluster
