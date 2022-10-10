# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/38314
- Tracking Issue: https://github.com/pingcap/tidb/issues/15000

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Support ALTER TABLE t REORGANIZE PARTITION p1,p2 INTO (partition pNew1 values...)

## Motivation or Background

TiDB is currently lacking the support of changing the partitions of a partitioned table, it only supports adding and dropping LIST/RANGE partitions.
Supporting REORGANIZE PARTITIONs will allow RANGE partitioned tables to have a MAXVALUE partition to catch all values and split it into new ranges. Similar with LIST partitions where one can split or merge different partitions.

When this is implemented, it will also allow transforming a non-partitioned table into a partitioned table as well as remove partitioning and make a partitioned table a normal non-partitioned table, which is different ALTER statements but can use the same implementation as REORGANIZE PARTITION

The operation should be online, and must handle multiple partitions as well as large data sets.

Possible usage scenarios:
- Full table copy
  - merging all partitions to a single table (ALTER TABLE t REMOVE PARTITIONING)
  - splitting data from many to many partitions, like change the number of hash partitions
  - splitting a table to many partitions (ALTER TABLE t PARTITION BY ...)
- Partial table copy (not full table/all partitions)
  - split one or more partitions
  - merge two or more partitions

These different use cases can have different optimizations, but the generic form must still be solved:
- N partitions, where each partition has M indexes

First implementation should be based on the merge-txn (row-by-row read, update record, write) transactional batches.
Later we can implement the ingest (lightning way) optimization, since DDL module are on the way of evolution to do reorg tasks more efficiency.

## Detailed Design

There are two parts of the design:
- Schema change states throughout the operation
- Reorganization implementation

Where the schema change states will clarify which different steps that will be done in which schema state transitions.

### Schema change states for REORGANIZE PARTITION

Since this operation will:
- create new partitions
- drop existing partitions
- copy data from dropped partitions to new partitions
- 
it will use all these schema change stages:

      // StateNone means this schema element is absent and can't be used.
      StateNone SchemaState = iota
      - Check if the table structure after the ALTER is valid
      - Generate physical table ids to each new partition
      - Update the meta data with the new partitions and which partitions to be dropped (so that new transaction can double write)
      - TODO: Should we also set placement rules? (Lable Rules)
      - Set the state to StateDeleteOnly
      
      // StateDeleteOnly means we can only delete items for this schema element.
      StateDeleteOnly
      - Set the state to StateWriteOnly

      // StateWriteOnly means we can use any write operation on this schema element,
      // but outer can't read the changed data.
      StateWriteOnly
      - Set the state to StateWriteReorganization
      
      // StateWriteReorganization means we are re-organizing whole data after write only state.
      StateWriteReorganization
      - Copy the data from the partitions to be dropped and insert it into the new partitions [1]
      - Write the index data for the new partitions [2]
      - Replace the old partitions with the new partitions in the metadata
      - Add a job for dropping the old partitions data (which will also remove its placement rules etc.)
      
      // StateReplicaOnly means we're waiting tiflash replica to be finished.
      StateReplicaOnly
      - TODO: Do we need this stage? How to handle TiFlash for REORGANIZE PARTITIONS?

- [1] will read all rows, update their physical table id (partition id) and write them back.
- [2] can be done together with [1] to avoid duplicating the work for both reading and getting the new partition id


During the reorganization happens in the background the normal write path needs to check if there are any new partitions in the metadata and also check if the updated/deleted/inserted row would match a new partition, and if so, also do the same operation in the new partition, just like during adding index or modify column operations currently does. (To be implemented in `(*partitionedTable) AddRecord/UpdateRecord/RemoveRecord`)

Note that parser support already exists.
There should be no issues with upgrading, and downgrade will not be supported during the DDL.

TODO:
- How does DDL affect statistics? Look into how to add statistics for the new partitions (and that the statistics for the old partitions are removed when they are dropped)
- How to handle TiFlash?

## Test Design

Re-use tests from other DDLs like Modify column, but adjust them for Reorganize partition.

### Functional Tests

TODO

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

### Scenario Tests

Note that this DDL will be online, while MySQL is blocking on MDL.

TODO

It's used to ensure this feature works as expected in some common scenarios.

### Compatibility Tests

TODO

A checklist to test compatibility:
- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Benchmark Tests

Correctness and functionality is higher priority than performance, also better to not influence performance of normal load during the DDL.

TODO

The following two parts need to be measured:
- The performance of this feature under different parameters
- The performance influence on the online workload

## Impacts & Risks

Impacts:
- better usability of partitioned tables
- online alter in TiDB, where MySQL is blocking
- all affected data needs to be read (CPU/IO/Network load on TiDB/PD/TiKV)
- all data needs to be writted (duplicated, both row-data and indexes), including transaction logs (more disk space on TiKV, CPU/IO/Network load on TiDB/PD/TiKV and TiFlash if configured on the table).

Risks:
- introduction of bugs
  - in the DDL code
  - in the write path (double writing the changes for transactions running during the DDL)
- out of disk space
- out of memory
- general resource usage, resulting in lower performance of the cluster

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions

What parts of the design are still to be determined?
- TiFlash handling
