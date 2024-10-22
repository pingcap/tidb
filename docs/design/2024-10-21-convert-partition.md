# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss), [Hangjie Mo](http://github.com/Defined2014)
- Discussion PR: https://github.com/pingcap/tidb/pull/56749
- Tracking Issue: https://github.com/pingcap/tidb/issues/56201

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

This design propose a new syntax for moving data in and out from table partitions, simpler to use than EXCHANGE PARTITION, more efficient for Global Index due to added restrictions by not allowing concurrent read/write for non-partitioned table during all steps of the DDL state changes, which would also introduce more complexity and risk to the core read/write path in TiDB.

Proposed syntax:
- `ALTER TABLE t CONVERT PARTITION p TO TABLE t2`
- `ALTER TABLE t CONVERT TABLE t2 TO <partition definition> [{WITH|WITHOUT} VALIDATION]`, example: `ALTER TABLE t CONVERT TABLE t2 TO PARTITION p VALUES LESS THAN (100)`
- `ALTER TABLE t EXCHANGE PARTITION p WITH TABLE t2 [{WITH|WITHOUT} VALIDATION | UPDATE GLOBAL INDEXES]` where `UPDATE GLOBAL INDEXES` is new and have an effect on execution time etc.

## Motivation or Background

The most common use case for EXCHANGE PARTITION is to either archive an old partition or to import new data into a partition. With EXCHANGE PARTITION both the partition and the table to be exchanged needs to exists, which creates extra steps for the user/DBA. See [blog post from MariaDB](https://mariadb.org/10-7-preview-feature-convert-partition/) which now supports [CONVERT PARTITION](https://mariadb.com/kb/en/partitioning-overview/#converting-partitions-tofrom-tables).
With CONVERT we can create the new table or partition in the same DDL, making it easier and less prune to errors.

Also with the introduction of [GLOBAL INDEX](2020-08-04-global-index.md) there are new compatibility issues, since a non-partitioned table cannot have global index, the comparison will always fail, thus blocking EXCHANGE PARTITION from being supported. So to add support for GLOBAL INDEX in EXCHANGE PARTITION, we need to enhance EXCHANGE PARTITION to allow handling of GLOBAL INDEXES, similar to how ALTER TABLE t PARTITION BY ... UPDATE INDEXES (...) works. Also supporting this, would need new restrictions, like not allowing WITHOUT VALIDATION as well as the operation would take much longer time, since it needs to update the Global Index by removing the entries from the exchanged partition and add the entries from the exchaned non-partitioned table. Other restrictions may be needed depending on implementation, like blocking reads and writes during some schema change states.

With a new syntax, we can have new restrictions, without affecting/changing existing syntax, like how to handle Global Index etc.

MariaDB's implementation restricts the use to always drop the partition if CONVERT PARTITION p TO TABLE t2 or to always create a new partition if CONVERT TABLE t2 TO PARTITION p0 ...
TiDB should extend the CONVERT syntax to also allow to keep the partition definition by replacing it with an empty partition, for CONVERT PARTITION p TO TABLE t2 as well as keeping the data (similar to EXCHANGE PARTITION) for CONVERT TABLE t2 TO PARTITION p0...
This would allow the new syntax to handle all cases that EXCHANGE does, also for KEY/HASH partitioned tables, which MariaDB does not support. This is very useful when there are Global Indexes.

Conclusion of motivation:
- Easier usage, no need to create tables/partitions in separate steps, including error handling outside the DDLs.
- No need to change expectations and restrictions on existing EXCHANGE PARTITION.
- Support Global Index.

## Detailed Design

### Logical functionality

There are four main use cases:

#### 1) Move data from a partition out from the partitioned table to a new non-partitioned table:
`ALTER TABLE t CONVERT PARTITION p TO TABLE t2`
MariaDB implements this as 'Move the partition into a table, and drop the partition definition'.
But we TiDB could optionally also support 'Move the partition data into a table, and replace it with an empty partition, keeping the partition definition'. That would also support KEY/HASH partitioned tables, which MariaDB does not.
Proposed option `TRUNCATE PARTITION`, so the full syntax would be `ALTER TABLE t CONVERT PARTITION p0 TO TABLE t2 TRUNCATE PARTITION` meaning the partition will be converted to a new table, and then truncated instead of dropped, while without `TRUNCATE PARTITION` it would be dropped instead. We could add `DROP PARTITION` as the default, if that makes things more clear? But the default must be drop partition (without syntax) since that is what MariaDB implements.

Possible other options, specifically for Global Index:
- Skip converting them to local unique indexes in the non-partitioned table, since it will take extra time and resources for something that might not be needed? But it might also mean that one cannot directly do CONVERT TABLE TO PARTITION if that requires such unique index, see below! Note: Oracle supports `[{INCLUDING|EXCLUDING} INDEXES]` as option for also exchanging the local indexes, which we could use for including or excluding the Global Index to be converted to local indexes.

Discussion:
- @mjonss propose to support `TRUNCATE PARTITION` option to cover more use cases.
- @mjonss propose to always create "local" unique indexes for the global indexes, since it most likely need to be implemented any way, and could later add a new option for skipping. If we would support skipping creating "local" indexes from the global ones, then `{INCLUDING|EXCLUDING} GLOBAL INDEXES` is probably the most suiting option, with INCLUDING as default. Non-global indexes should always be included, since it is already stored that way.

So proposal is:
`ALTER TABLE t CONVERT PARTITION p TO TABLE t2 [TRUNCATE PARTITION]`
- A new non-partitioned table t2 will be created with the same structure as the partitioned table t, with the data from partition p.
- if `TRUNCATE PARTITION` is not given then the partition p will be dropped. MariaDB implements this.
- if `TRUNCATE PARTITION` is given then the partition p will become empty, which will also be supported for HASH/KEY partitioned tables.

#### 2) Move data from a non-partitioned table into a partition of a partitioned table:
`ALTER TABLE t CONVERT TABLE t2 TO PARTITION p ... [{WITH | WITHOUT} VALIDATION]`
MariaDB implements this as 'Create a new partition according to the given partitioning definition, and convert the table to the new partition, efficently dropping the table'.
TiDB could optionally also support 'Replace the existing partition's data with the data from the table, and drop the table' (3. below) as well as 'Swap the existing partition data with the table data' (4. below).

Possible other options, specifically for Global Index:
- Should we allow non-matching indexes and during the operation drop indexes on the non-partitioned table that does not match the partitioned table and create local indexes that only exists in the partitioned table?
  - Pro: easier to create a table that would be allowed to use for CONVERT TABLE TO PARTITION, including getting index ids matching.
  - Con: extra work/time/resources during DDL operation.
- Should we require "local" indexes on the non-partitioned table matching the partitioned tables global indexes? At least no need to match the internal index ids.
  - Pro: at least there are no duplicate entries within the non-partitioned table.
  - Con: it will just be dropped, and each row will be inserted and checked into the global indexes anyway, so not technically needed.

Discussion:
- @mjonss propose to require all indexes on the non-partitioned table t2 to match all non-global indexes of the partitioned table t.
- @mjonss propose to not require t2 having indexes that matches global indexes of t.

Note: Oracle supports `[{INCLUDING|EXCLUDING} INDEXES]` as option for also exchanging the local indexes, which we could use for including or excluding the Global Index to be converted to local indexes.

#### 3) Move data from a non-partitioned table and replace an existing partition
`ALTER TABLE t CONVERT TABLE t2 TO PARTITION p` basically the same syntax as 2), but only needing the partition name, and it will replace the data in that partition, so it also will work for KEY/HASH partitioned tables.
MariaDB does not support this.
It might be risky to support this, since it would efficiently TRUNCATE the existing partitions data and replace it with the data from t2.

Discussion:
- @mjonss propose to not support it, but rely on the following case instead + drop the "exchanged" table if the data is truly not needed.

#### 4) Swap the data between a non-partitioned table with a partition.
Logically the same as `ALTER TABLE t EXCHANGE PARTITION p WITH TABLE t2 [{WITH|WITHOUT} VALIDATION]`

But currently the `EXCHANGE PARTITION` implementation in TiDB allows for concurrent read and write to both the exchanged table and partition, making it complex to support Global Index, where there will need to be a DDL state where one session, S1, sees the new t2 (original p) and the new p (original t2), while another session, S2, might see the original t2 and the original p. Meaning that S2 still needs to double write/update the Global Index of t, when writing to t2 and the S1 also needs to double write/update the "local" index of t2 when writing to t/p, to keep both table's indexes consistent in the different views of S1 and S2. This makes the DDL extra complex and risky to implement and hard to test.
Having a way to only allow access to the data of both t/p and t2 through table t during the DDL operation would make it less complex, fewer error handling conditions and also having less performance impact, due to less indexes to keep updated at the same time.

So the question is if we should extend the syntax for `EXCHANGE PARTITION` for this new restriction and support Global Index or have a new syntax?

While thinking of new syntax extension of `EXCHANGE PARTITION` for Global Index, note that VALIDATION always will happen, since each row in t2 needs to be read and inserted in the Global Indexes, so it would not make sense to support `WITHOUT VALIDATION` if the partitioned table has any Global Indexes.

`UPDATE GLOBAL INDEXES` exists as an option to other partitioning management commands, like `ALTER TABLE t TRUNCATE PARTITION p UDPATE GLOBAL INDEXES` in Oracle, so we could use that, resulting in:
`ALTER TABLE t EXCHANGE PARTITION p WITH TABLE t2 [UPDATE GLOBAL INDEXES | {WITH | WITHOUT} VALIDATION]` with clear documentation of the operation for Global Indexes, i.e. that it would need to update the global indexes during the DDL execution and it will not be a meta-data only change.

Possible other options, specifically for Global Index:
- Should we allow non-matching indexes and during the operation drop indexes on the non-partitioned table that does not match the partitioned table and create local indexes that only exists in the partitioned table?
  - Pro: easier to create a table that would be allowed to use for CONVERT TABLE TO PARTITION, including getting index ids matching.
  - Con: extra work/time/resources during DDL operation.
- Should we require "local" unique indexes on the non-partitioned table matching the partitioned tables global indexes? At least no need to match the internal index ids.
  - Pro: at least there are no duplicate entries within the non-partitioned table.
  - Con: it will just be dropped, and each row will be inserted and checked into the global indexes anyway, so not technically needed.

Discussion:
- @mjonss propose extended syntax `ALTER TABLE t EXCHANGE PARTITION p WITH TABLE t2 [UPDATE GLOBAL INDEXES | {WITH | WITHOUT} VALIDATION]`, where the new `UPDATE GLOBAL INDEXES` option indicates that also partitioned tables with Global Indexes will be Exchanged, and that it is allowed to update the Global Indexes accordingly during the process, *with* new limitations that the exchanged table t2 will have restricted access during the operation.
- @mjonss propose that all indexes of t2 must match all indexes of t, except for global indexes. If indexes in t2 matches global indexes in t, then those indexes will be recreated for the new data in t2, if an global index in t does not have a match in t2, then it will be skipped.

Notes:
- MySQL does not have any notion of invalid or unusable indexes (i.e. not up-to-date) as Oracle has, so all existing indexes should always be up-to-date and consitent.
- Oracle supports `INCLUDING INDEXES` as option to `EXCHANGE PARTITION`
- `CREATE TABLE t2 LIKE t` in TiDB also creates the indexes with the same index ids, so by also doing `ALTER TABLE t2 REMOVE PARTITIONING` to get a non-partitioned table with the same indexes including ids works. But that is what makes the CONVERT PARTITION TO TABLE syntax much easier :)
- Currently Foreign Keys in TiDB does not support Partitioned tables, so no extra validation would be needed.

TODO:
- How to handle:
  - Updating table statistics.

Questions:
- If one creates and drops indexes of a partitioned table, the internal index ids will not be consecutive, so creating a new table to be used in EXCHANGE PARTITION will not match the partitioned tables index ids, and will not be allowed to be used. Should we also add support for `CREATE TABLE t2 FOR EXCHANGE WITH t` that Oracle supports?
  - No, `CREATE TABLE t2 LIKE t; ALTER TABLE t2 REMOVE PARTITIONING;` works.

### Physical / implementation design



TODO: Fill in the rest of the design :)
Do not forget about need for double writing, rollback, cross schema version compatibility, error handling for duplicate key, both in DDL as well as user sessions in various schema versions and states.



Explain the design in enough detail that: it is reasonably clear how the feature would be implemented, corner cases are dissected by example, how the feature is used, etc.

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
