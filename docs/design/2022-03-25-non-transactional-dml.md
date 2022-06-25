# Non-transactional statements

- Author(s): [Ziqian Qin](http://github.com/ekexium)
- Tracking issue: https://github.com/pingcap/tidb/issues/33485

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

A new syntax that transforms a given DML into a serial execution of mutually exclusive collectively exhaustive statements. 

For example, `BATCH ON a LIMIT 1000 DELETE FROM t` can be transformed into `DELETE FROM t WHERE a BETWEEN 1 AND 1000` and `DELETE FROM t WHERE a BETWEEN 1001 AND 2000`, assuming the min and max of `a` are respectively 1 and 2000.

## Motivation or Background

Users need to do a bulk delete or update using a single statement, while currently TiDB cannot satisfy the requirement because of the performance issue, transaction size limit, and compatibility issues with tools.

A new syntax is proposed to work around the problem. A non-transactional DML contains a DML and information that are used to "split" the statement, thus it is equivalent to a sequence of DMLs are not transactional since it does not provide atomicity and probably isolation.

Different from the deprecated batch-DML, a non-transactional DML splits SQLs so every split SQL is a normal statement and does not risk data-index consistency.

## Detailed Design

### User Interface

#### Syntax
The syntax: 
- `BATCH ON <column_name> LIMIT <batch_size> <DML>` is the complete form
- `BATCH LIMIT <batch_size> <DML>` is the shorter form. The shard column will be automatically selected from a PK column if possible. But it may return an error and require the user to specify the shard column.

In the first step only `DELETE` is going to be supported, but `UPDATE` and `INSERT INTO SELECT` are also worth considering.

For performance consideration, the shard column must be indexed. 

There can be dry run syntax to show the actual SQLs that will be executed. Query plans are not returned since there is no elegant way to contain both a SQL and a plan in a result set. 
- `BATCH ON <column_name> LIMIT <batch_size> DRY RUN QUERY <DML>` outputs the `SELECT` statement that will be executed.
- `BATCH ON <column_name> LIMIT <batch_size> DRY RUN <DML>` outputs how the statement will be split. It only outputs the first and the last split statement.

#### Output

Users get feedback from SQL return values, logs and process infos.

If all split statement succeed, the non-transactional DML returns a summary: the number of split statements and a success message. 
If the non-transactional DML is aborted, e.g. by `KILL TIDB`, it returns the error of context cancellation, and output details of failed jobs in logs if there is any.

The `info` field in process info describes not only the current SQL that is executing, but also the progress of all jobs.
Logs, slow logs and statement summaries will also contain the progress in the SQL text. 
For example, a split delete statement in slow queries can be like ``/* job 11/41 */DELETE FROM `test`.`t` WHERE `id` BETWEEN xxx AND yyy;``,
where 11 and 41 are the current job ID and the number of total jobs respectively.

Each split statement logs its detail in the INFO level as well.

### Session

Different from most of the statements, the non-transactional DML is handled at the session level. 

Non-transactional statements are treated as `SimplePlan`s and we won't compile them. In `TiDBContext.ExecuteStmt`, instead of `Session.ExecuteStmt` for most types of statements, `HandleNontransactionalDML` will be called. In `HandleNontransactionalDML`, we will split the DML into multiple statements and execute them one by one using `Session.ExecuteStatement`.

### How the split works

To find the split keys, a `SELECT` is used to read the shard column specified by the user. For example, for `BATCH ON a LIMIT 1000 DELETE FROM t WHERE b < 1000`, the select statement would look like: `SELECT a FROM t WHERE b < 1000 ORDER BY ISNULL(a,0,1), a`. It assures that NULL values are put in the first places. The result set could be large, but we don't need all of them, so results are batched until the size of the batch is greater than or equal to the specified `batchSize`. The interaction of any two batches must be none, since we use `BETWEEN` clauses to split the statement. Only the first and last elements of each batch are kept, thus forming a job. A job specifies a range in the specified column.

Theoretically, jobs can be executed in parallel, but that would require multiple sessions to execute them. There are two kinds of sessions in TiDB: user sessions that must be bound to client connections in a 1-on-1 manner, and internal sessions. Both kinds of sessions are inappropriate for the split statements, so in favor of maintenance, only the current user session will be used to execute the jobs, which is serial. In this way, the main benefit of a non-transactional DML is to overcome the transaction size limit, instead of performance superiority.

Each split statement is generated by embedding the split range in the where clause of the given DML using a `BETWEEN` operator. An exception is a `NULL` boundary where the `BETWEEN` clause is replaced by a `IS NULL` or `x IS NULL OR x < a` clause.
For example, the original statement is `BATCH ON a LIMIT 1000 DELETE FROM t WHERE b < 1000`, 
a job `{start:1, end:1000}` generates a split SQL `DELETE FROM t WHERE (a BETWEEN 1 AND 1000) AND b < 1000`.

### Error handling

A non-transactional statement obviously cannot roll back, when one of the split statement fails, a system variable `tidb_nontransactional_ignore_error` controls what happens next:
- If `tidb_nontransactional_ignore_error=0`, the non-transactional DML cancels all following jobs and returns an error immediately.
- Otherwise, the non-transactional DML continues until finishing all jobs and returns the details and error messages of all failed jobs.

If the statement is aborted by the user, it should report its progress and return the errors it has collected.

There is one exception, however. If the first job failed, we abort the whole process and return the error. There are 2 reasons for this behavior:

1. If for any reason all the jobs cannot succeed, failing in the first place is the best choice. For example, privileges are missing.
2. It's equivalent to rolling back the whole statement.

### Constraints

To make the semantics of non-transactional statements simple enough and to avoid misuse, 
there are several constraints on the DML.
Take non-transactional `DELETE` for example, only the `WHERE` clause in the delete statement is supported. 
`ORDER BY` or `LIMIT` clauses are ignored and will not take effect. An error will be returned if the DML contains them.

There may be other constraints or incompatibilities. They will be demonstrated in the official documentation of the feature. 

## Test Design

### Functional Tests

- non-transactional delete can delete everything it should delete, and don't delete anything it should not
    - multiple shard column types: int, varchar(with or without new collation), timestamp, double, decimal
    - different delete statements:
        - syntax: `DELETE FROM t`, `DELETE t FROM`
        - table aliases
        - column alias
        - where clause: no `WHERE`, simple `WHERE`, complex `WHERE` that contains `SELECT`
- errors are correctly propagated
    - inject error in the first job, it should return
    - inject errors in non-initial jobs, all errors should be collected and returned, remaining jobs should finish.
    - if appropriate privilege is not granted, the statement should fail

The tests can be done in both deterministic unit tests, and randomly generated tests that use the same set of data and check the results of a simple delete and that of a non-transactional delete.

### Scenario Tests

Simulate a simple bulk delete scenario.

- There are a large amount of data that cannot be deleted using a single delete
- Users dry run and check the query plan before executing the command
- The user aborted the command and can retry the remaining part based on the information in the error (or log).
- The result should be equivalent to a single delete statement if no error has happened.

### Compatibility Tests

Theoretically the feature won't break any compatibility in peripheral tools. 
But we will perform simple tests to see if they work with non-transactional DMLs:

- BR backup and restore
- TiCDC synchronization
- tidb-binlog synchronization

### Benchmark Tests

Benchmark the delete performance compared with a single normal delete statement:
1. BATCH ON a unique index
2. BATCH ON _tidb_rowid or an int PK
3. BATCH ON a clustered index

## Impacts & Risks

If any feature that influences sort and ordering (e.g. collation) is unstable, then the split process may be wrong and the delete statement may miss some rows or delete too many rows.

## Investigation & Alternatives

CockroachDB doesn't provide a similar mechanism, instead they have an official doc teaching users how to write scripts to manually split SQLs.

An alternative solution is to improve the ability of large transactions. But tools like CDC don't support large transactions well, so it's not feasible for now.

## Unresolved Questions

The user interface, including the syntax, the error messages, and the dry run results, may be improved.