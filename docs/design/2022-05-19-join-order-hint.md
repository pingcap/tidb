# Join Order Hint

- Author(s): [qw4990](https://github.com/qw4990), [Reminiscent](https://github.com/Reminiscent)
- Last updated: 20222-05-19
- Tracking Issue: https://github.com/pingcap/tidb/issues/29932

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
  * [Functional Tests](#functional-tests)
  * [Scenario Tests](#scenario-tests)
  * [Compatibility Tests](#compatibility-tests)
  * [Benchmark Tests](#benchmark-tests)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

In a few scenarios, optimizer doesn't find the optimal plan, so that people would intervene and control behavior manually. With the hints, some of SQL performance issues can be resolved quickly.

## Motivation or Background

For now, there is no effective way to control join order. TiDB only supports one annotation writing method: `SELECT /*! STRAIGHT_JOIN */ col1 FROM t1, t2 WHERE ....` There are two problems with this writing method at present:

1. It is not in the form of hint and cannot be used to create bindings. Users need to modify the query to take effect;
2. It will fix the join order of all tables. But in some cases, it is only necessary to fix the join order of a few tables. Others are left to the optimizer to choose;

## Detailed Design

### Implementation Overall

Mainly referring to Oracle, two new hints are introduced:

1. `/*+ straight_join() */`: The semantics are the same as `STRAIGHT_JOIN` but in the form of a hint. After using this hint, the optimizer will not adjust the join order, but use the order in which the tables appear in the query as join order;
2. `/*+ leading(t1, t2, ..., tn) */`: After use, the optimizer will first construct a left-biased tree in the order of table `t1`, `t2`, ... , `tn`, and then use this left-biased tree as Initial node, join with other nodes;

Note: The `straight_join()` hint is relatively simple, and the design of `leading()` hint is mainly described here.

### Implementation Details

1. Save the hint information to the join operator according to the table name when building the join operator.
2. Extract hint information when extracting join group from logical plan tree in join reorder optimization.
3. Construct the join operator according to the table in the leading hint.
4. Add the constructed join operator to the join group as the first table in the greedy join reorder algorithm.
5. Do the greedy join reorder algorithm.

### Limitations:

1. Can not represent the bushy tree.
2. The build/ probe side of the join operator can not be fixed.

## Test Design

### Functional Tests

* Use the join order hint when using other types of hints.
* Use join order hint under different join operators.
* Use join order hint for different table names.
* Use join order hint with SQL plan management.
* Use the join order hint for complex queries.
* Join order hint is used in combination with other features, including partition table, TiFlash engine.

### Scenario Tests

* Some oncall test cases

### Compatibility Tests

* N/A

### Benchmark Tests

* N/A

## Investigation & Alternatives

* [Oracle](https://docs.oracle.com/cd/B12037_01/server.101/b10752/hintsref.htm) support the join order hint.

## Unresolved Questions

* Use join order hint with outer join.
* Combination of join order hint and join operator hint.
* Support the join order hint for the DP join reorder algorithm.
