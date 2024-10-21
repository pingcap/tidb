# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss), [Co-Author Name](http://github.com/Defined2014)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/XXX

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

This design propose a new syntax for moving data in and out from table partitions, simpler to use than EXCHANGE PARTITION.

Proposed syntax:
`ALTER TABLE t CONVERT PARTITION p TO TABLE t2`
and
`ALTER TABLE t CONVERT TABLE t2 TO PARTITION p VALUES LESS THAN (100)` or more generic `ALTER TABLE t CONVERT TABLE t2 TO <partition definition>`

## Motivation or Background

The most common use case for EXCHANGE PARTITION is to either archive an old partition or to import new data into a partition. With EXCHANGE PARTITION both the partition and the table to be exchanged needs to exists, which creates extra steps for the user/DBA. See [blog post from MariaDB](https://mariadb.org/10-7-preview-feature-convert-partition/) which now supports [CONVERT PARTITION](https://mariadb.com/kb/en/partitioning-overview/#converting-partitions-tofrom-tables)
With CONVERT we can create the new table or partition in the same DDL, making it easier and less prune to errors.

Currently if one creates and drops indexes of a partitioned table, the internal index ids will not be consecutive, so creating a new table to be used in EXCHANGE PARTITION will not match the partitioned tables index ids, and will not be allowed to be used.

Also with the introduction of [GLOBAL INDEX](2020-08-04-global-index.md) there are new compatibility issues, since a non-partitioned table cannot have global index, the comparison will always fail, thus blocking EXCHANGE PARTITION from being supported. So to add support for GLOBAL INDEX in EXCHANGE PARTITION, we need to enhance EXCHANGE PARTITION to allow handling of GLOBAL INDEXES, similar to how ALTER TABLE t PARTITION BY ... UPDATE INDEXES (...) works. Also supporting this, would need new restrictions, like not allowing WITHOUT VALIDATION as well as the operation would take much longer time, since it needs to update the Global Index by removing the entries from the exchanged partition and add the entries from the exchaned non-partitioned table. Other restrictions may be needed depending on implementation, like blocking reads and writes during some schema change states.

With a new syntax, we can have new restrictions, without affecting/changing existing syntax, like how to handle Global Index etc.

MariaDB's implementation restricts the use to always drop the partition if CONVERT PARTITION p TO TABLE t2 or to always create a new partition if CONVERT TABLE t2 TO PARTITION p0 ...
TiDB should extend the CONVERT syntax to also allow to keep the partition definition by replacing it with an empty partition, for CONVERT PARTITION p TO TABLE t2 as well as keeping the data (similar to EXCHANGE PARTITION) for CONVERT TABLE t2 TO PARTITION p0...
This would allow the new syntax to handle all cases that EXCHANGE does, also for KEY/HASH partitioned tables, which MariaDB does not support. This is very useful when there are Global Indexes.

Conclusion of motivition:
- Easier usage, no need to create tables/partitions in separate steps, including error handling outside the DDLs.
- No need to change expectations and restrictions on existing EXCHANGE PARTITION.
- Support Global Index.

## Detailed Design

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
