<!--
This is a template for TiDB's change proposal process, documented [here](./README.md).
-->

# Proposal: Support Spilling Unparalleled HashAgg

- Author(s): [@wshwsh12](https://github.com/wshwsh12)
- Discussion PR: Need Update
- Tracking Issue: Need Update

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

This proposal describes the implementation of Unparalleled HashAgg that can spill intermediate data when memory usage is higher than memory quota.
Spilling for paralleled HashAgg will be supported later.

## Motivation or Background

Currently, the calculation logic of the aggregate executors in the TIDB is divided into two types, parallel and unparallel. However, when SQL memory usage exceeds the memory limit, neither implementation can use external memory to control memory usage and can only kill the SQL that is executing. In order to enable SQL to execute normally in the case of insufficient memory, we introduce the spilling algorithm for the unparallel aggreagte executor.

## Detailed Design

In aggregate processing, memory increases when tuples are inserted into the hash table. So we can use the following algorithm to control the memroy incresaing:

1. When the memory usage is higher than the mem-quota, switch the HashAgg executor to spill-mode.
2. When HashAgg is in spill-mode, keep the tuple in the hashMap no longer growing.
  a. If the processing key exists in the Map, aggreagte the result.
  b. If the processing key doesn't exist in the Map, spill the data to disk.
3. After all data have been processed, output the aggreagte result in the Map, clear the Map. Then read the spilling data from disk, repeat the Step1-Step3 until all data have been aggregated.

## Test Design

### Functional Tests

* Querying using aggregate functions should give correct result.

### Scenario Tests

* In a scenario where aggreagte executor use too many memory, this feature helps recude memory usage and run the sql successfully.

### Compatibility Tests

* N/A

### Benchmark Tests

* The feature shouldn't cause any obvious performance regression (< 2%) on non-spilling scenario.

## Impacts & Risks

* Memory will still grow without increasing the number of new tuples in HashMap for distinct aggregate function.

## Investigation & Alternatives

## Future Work
1. Support friendly spilling implement for distinct aggregate function.
2. Support spilling for paralleled HashAgg.
