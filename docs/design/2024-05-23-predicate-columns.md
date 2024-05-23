# Analyze Predicate Columns

- Author(s): [Rustin Liu](http://github.com/hi-rustin)
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of **Contents**

- [Analyze Predicate Columns](#analyze-predicate-columns)
  - [Table of **Contents**](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
  - [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
    - [Compatibility Tests](#compatibility-tests)
    - [Performance Tests](#performance-tests)
  - [Impacts \& Risks](#impacts--risks)
  - [Investigation \& Alternatives](#investigation--alternatives)
    - [CRDB](#crdb)
    - [Redshift](#redshift)
  - [Unresolved Questions](#unresolved-questions)

## Introduction

TBD

## Motivation or Background

The ANALYZE statement would collect the statistics of all columns currently. If the table is big and wide, executing `ANALYZE` would consume lots of time, memory, and CPU. See <https://github.com/pingcap/tidb/issues/27358> for details.****
However, only the statistics of some columns are used in creating query plans, while the statistics of others are not. Predicate columns are those columns whose statistics are used in query plans, usually in where conditions, join conditions, and so on. If ANALYZE only collects statistics for predicate columns and indexed columns (statistics of indexed columns are important for index selection), the cost of ANALYZE can be reduced.

## Detailed Design

## Test Design

### Functional Tests

### Compatibility Tests

### Performance Tests

## Impacts & Risks

## Investigation & Alternatives

### CRDB

### Redshift

## Unresolved Questions
