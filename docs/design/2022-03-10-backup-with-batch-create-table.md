# TiDB Design Documents

- Author(s): [fengou1](http://github.com/your-github-id)
- Discussion PR: https://github.com/pingcap/tidb/pull/27036
- Tracking Issue: https://github.com/pingcap/tidb/issues/28763

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

The proposal of this design is to speed up the BR restore.

## Motivation or Background

The cluster has 6 TiB data, 30k tables, and 11 TiKVs. When I use BR to backup and restore the cluster, The speed is particularly slow. After investigation, BR can only create 2 tables per second, the entire store speed takes nearly 4h, and the execution time spent creating tables is close to 4h. It can be seen that the execution speed of ddl is the bottleneck in the this scenario.

## Detailed Design

Backup and Restore for massive tables is exteremely slow, the bottleneck of creating table is to waiting the schema version changed. Each table create ddl cause a schema version change. 60000 tables has almost 60000 times schema change, it is very cost of restore massive table

Currently, BR uses an internal interface named CreateTableWithInfo to creating table, which creating table and wait the schema changing one-by-one, omitting the sync of the ddl job between BR and leader, the procedure of creating one table would be like this:

for _, t := range tables {
  RunInTxn(func(txn) {
    m := meta.New(txn)
    schemaVesrion := m.CreateTable(t)
    m.UpdateSchema(schemaVersion)
  })
  waitSchemaToSync() // <- This would notify and then 
  // waiting for all other TiDB nodes are synced with the latest schema version. 
}

the new design will introduce a new batch create table api
waitSchemaToSync() // <- only one time of waiting. 

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
