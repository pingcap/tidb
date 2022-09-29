# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
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

Should we implement both the ingest (lightning way) as well as the merge-txn (row-by-row internal insert batches)?

## Detailed Design

There are two parts of the design:
- Schema change states throughout the operation
- Reorganization implementation

Where the schema change states will clarify which different steps that will be done in which schema state transisions.

### Schema change states for REORGANIZE PARTITION

Since this operation will:
- create new partitions
- drop existing partitions
- copy data from dropped partitions to new partitions
it will use all these schema change stages:
      // StateNone means this schema element is absent and can't be used.
      StateNone SchemaState = iota
      // StateDeleteOnly means we can only delete items for this schema element.
      StateDeleteOnly
      // StateWriteOnly means we can use any write operation on this schema element,
      // but outer can't read the changed data.
      StateWriteOnly
      // StateWriteReorganization means we are re-organizing whole data after write only state.
      StateWriteReorganization
      // StateDeleteReorganization means we are re-organizing whole data after delete only state.
      StateDeleteReorganization
      // StatePublic means this schema element is ok for all write and read operations.
      StatePublic    
      // StateReplicaOnly means we're waiting tiflash replica to be finished.
      StateReplicaOnly
      // StateGlobalTxnOnly means we can only use global txn for operator on this schema element
      StateGlobalTxnOnly


Explaie the design in enough detail that: it is reasonably clear how the feature would be implemented, corner cases are dissected by example, how the feature is used, etc.

It's better to describe the pseudo-code of the key algorithm, API interfaces, the UML graph, what components are needed to be changed in this section.

Compatibility is important, please also take into consideration, a checklist:
- Compatibility with other features, like partition table, security&privilege, collation&charset, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

## Test Design

A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.

### Functional Tests

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

### Scenario Tests

It's used to ensure this feature works as expected in some common scenarios.

### Compatibility Tests

A checklist to test compatibility:
- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Benchmark Tests

The following two parts need to be measured:
- The performance of this feature under different parameters
- The performance influence on the online workload

## Impacts & Risks

Describe the potential impacts & risks of the design on overall performance, security, k8s, and other aspects. List all the risks or unknowns by far.

Please describe impacts and risks in two sections: Impacts could be positive or negative, and intentional. Risks are usually negative, unintentional, and may or may not happen. E.g., for performance, we might expect a new feature to improve latency by 10% (expected impact), there is a risk that latency in scenarios X and Y could degrade by 50%.

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions

What parts of the design are still to be determined?
