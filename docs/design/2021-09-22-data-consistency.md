# Proposal: Reduce Data inconsistencies

- Author(s): [Li Su](http://github.com/lysu), [Ziqian Qin](http://github.com/ekexium)
- Tracking Issue: https://github.com/pingcap/tidb/issues/26833

## Table of Contents

- [Table of Contents](#table-of-contents)

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)

- [Test Design](#test-design)

    - [Regression Tests](#regression-tests)
    - [System Tests](#system-tests)
    - [Performance Tests](#performance-tests)

- [Impacts & Risks](#impacts--risks)

## Introduction

The document proposes an enhancement that reduces data inconsistency issues in TiDB. The feature also includes an
enhancement in the logging and error handling of data inconsistency issues to help diagnosis.

## Motivation or Background

TiDB occasionally encounters data index inconsistencies, i.e., row data and indices of the same record are
inconsistent (in their numbers or content). Inconsistent data indices in production environments can have a very bad
impact on users, including but not limited to

- Read errors: errors are reported when reading or writing inconsistent data
- Data error: data can be read but the result is incorrect
- Data Loss: Data written cannot be read

It is very difficult for support engineers to locate data index inconsistencies when they occur.

- Multiple possible causes of data errors
- Inconsistency phenomenon cannot easily imply its cause
- Difficulty in collecting information available for investigation on the cause

In short, the serious impact of the problem and the difficulty of troubleshooting is the main reason why we need to
invest in improvement at present.

## Detailed Design

There are two methods to reduce data corruption from happening and spreading. And we have separate system variables to
enable them.

`tidb_enable_mutation_checker`, takes value of `ON` or `OFF`.

`tidb_txn_assertion_level`, takes value of `OFF`, `FAST`, or `STRICT`. Details will be discussed later.

### Assertion

Based on the operator characteristics and the information that the operator has already checked, some assertions can be
made with regard to the data being mutated. The assertions are pushed down to TiKV and are executed before writing. DML
operators (e.g. UpdateExec, InsertExec) are encapsulated in tables to make KV modifications to memDB:

- Put KV: Add or update KV pairs
- Del KV: delete KV pair

While the data is modified, the KV pair modified to memDB is additionally set with 4 new Assertion Flags (occupying a
total of 2 bits):

- None(00): There is currently no Assertion for KV, but subsequent modifications in the transaction can overwrite this
  value [default Flag]
- Unknown(11): KV pair cannot be asserted, and subsequent modifications within the transaction cannot be overwritten
- MustExist(10): Key must exist in storage, subsequent modifications within the transaction cannot be overwritten
- MustNotExist(01): Key must not exist in the storage, subsequent modifications within the transaction cannot be
  overwritten

The flags in MemDB, like the KV data, are first set to the current stage of the statement. After that, if the statement
reports an error and the statement rolls back, the flags will be invalidated. If the statement is executed successfully
and is submitted, the flags will take effect in the transaction. The assertion information is sent to TiKV in the
prewrite stage of 2PC. TiKV performs additional checks based on the assertions before modifying data. If the assertions
fail, TiKV rejects the request.

For pessimistic transactions, prewrite requests may not read the previous value of the key. In this case, we let the
pessimistic lock requests return the existence information of the key and cache it in the client(TiDB) side. Before
committing the transaction, the client(TiDB) side will use the information to check assertions, so that doesn't have to
be the overhead of reads in TiKV. Some keys in pessimistic transactions are not pessimistically locked, so we have 2
strategies to deal with this situation: the `FAST` mode just ignores the keys, and the `STRICT` mode will read all keys
in TiKV.

### Check Mutations

The basic idea is to ensure that the transaction will not write inconsistent data when the data before the transaction
is consistent.

For possible errors in the executor: Assume that the set of row values before and after the transaction are V1 and V2
respectively, and the data indices are consistent before the transaction is executed. The condition of data consistency
after the transaction's commitment is V1-V2 = { Del_{index}.value }, V2-V1 = { Put_{index}.value }.

## Test Design

The main quality objectives of this project include correctness, effectiveness, efficiency, ease of use, and
compatibility. The test part involves two parts: ensuring correctness (including compatibility) and measuring
effectiveness.

Correctness: There will be no false positives when the data is correct

Effectiveness:

1. Reject transactions with inconsistent mutations
2. Report errors as soon as possible for existing problematic data to prevent the spread of errors

### Regression tests

Regression tests are useful to check correctness since all tests should not report data inconsistency errors.

### System tests

Specific system test cases need to be constructed. Cases should at least involve the following parts

- Encoding related
    - Collation
    - Row format
    - Charset
    - Time zone
- Table structure
    - Concurrent DDL
    - Clustered index
    - Prefix index
    - Expression index

### Performance Tests

The enhancement should have little impact on performance, including latency, throughput, memory and CPU consumption.

Standard sysbench and TPC-C tests are needed.

## Impacts & Risks

According to the performance tests, the following impacts and risks are expected:

In the `STRICT` assertion mode, pessimistic transactions results in a notable increase in the scheduler CPU usage and
the number of kvdb get and seek operations.

The mutation checker can increase the TiDB CPU usage. When the CPU becomes the bottleneck of TiDB, introducing the
enhancement may decrease the throughput by 1.5%, and increase P95 latency by 2.4%.


