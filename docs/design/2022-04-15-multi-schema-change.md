# Proposal: Support Multi-Schema Change

- Author(s): [tangenta](https://github.com/tangenta)
- Tracking Issue: https://github.com/pingcap/tidb/issues/14766

## Abstract

This proposes an implementation of applying multiple schema changes in one `ALTER TABLE` statement.

## Background

Multi-Schema Change is one of MySQL's extended features to the SQL standard. It allows the users to atomically make multiple schema changes in one statement, including column and index `ADD`, `ALTER`, `DROP`, and `CHANGE`, as well as table option changes. For example:

```sql
CREATE TABLE t (a INT, c INT);
ALTER TABLE t ADD COLUMN b INT, 
 MODIFY COLUMN c CHAR(5),
 ADD INDEX idx(a),
 ADD PRIMARY KEY (c),
 COMMENT = "comments for t";
```

Currently, TiDB only supports one schema change per SQL statement and limited multi-schema changes for some rare cases.

When users attempt to migrate data from MySQL-like databases, they may expend additional effort to rewrite a multi-schema change DDL to several single-schema change DDLs. For users who rely on ORM frameworks such as [Flyway](https://flywaydb.org/) to automatically construct SQLs, rewriting SQL could be tedious and the scripts are difficult to maintain.

Above all, the lack of this capability can be a blocking issue for those who wish to use TiDB.

### Goal

- Support MySQL-compatible Multi-Schema Change that used commonly, including `ADD/DROP COLUMN`, `ADD/DROP INDEX`, `MODIFY COLUMN`, `RENAME COLUMN`, etc.

### Non-Goals

- Support TiDB-specific Multi-Schema Change like `ADD TIFLASH REPLICA`, `ADD PARTITION`, `ALTER PARTITION`, etc.
- Resolve the 'schema is changed' error when DDL and DML are executed concurrently.
- Be 100% compatible with MySQL. MySQL may reorder the execution of schema changes, which makes the behavior counter-intuitive sometimes.
- Improve the performance of DDL, although it introduces a way to improve the performance of long-running jobs(see 'Future Work' part).

## Proposal

### Data Structure

The implementation is based on the [online DDL architecture](https://github.com/pingcap/tidb/blob/e0c461a84cf4ad55c7b51c3f9db7f7b9ba51bb62/docs/design/2018-10-08-online-DDL.md). Similar to the existing [Job](https://github.com/pingcap/tidb/blob/6bd54bea8a9ec25c8d65fcf1157c5ee7a141ab0b/parser/model/ddl.go/#L262) structure, we introduce a new structure "SubJob":

- "Job": A job is generally the internal representation of one DDL statement.
- "SubJob": A sub-job is a representation of one DDL schema change. A job may contain zero(when multi-schema change is not applicable) or more sub-jobs.

```go
// Job represents a DDL action.
type Job struct {
    Type       ActionType    `json:"type"`
    State      JobState      `json:"state"`
    // ...
    MultiSchemaInfo *MultiSchemaInfo `json:"multi_schema_info"`
}

// MultiSchemaInfo contains information for multi-schema change.
type MultiSchemaInfo struct {
   // ...
   SubJobs  []SubJob `json:"sub_jobs"`
}

// SubJob represents one schema change in a multi-schema change DDL.
type SubJob struct {
   Type    ActionType      `json:"type"`
   State   JobState        `json:"state"`
   // ...
}
```

The field `ActionType` stands for the type of DDL. For example, `ADD COLUMN` is mapped to `ActionAddColumn`; `MODIFY COLUMN` is mapped to `ActionModifyColumn`.

The Multi-Schema Change DDL jobs have the type `ActionMultiSchemaChange`. In the current worker model, there is a dedicated code path (`onMultiSchemaChange()`) to run these jobs. Only Multi-Schema Change jobs can have sub-jobs.

For example, the DDL statement

```SQL
ALTER TABLE t ADD COLUMN b INT, MODIFY COLUMN a CHAR(10);
```

can be modeled as a job like

```go
job := &Job {
    Type: ActionMultiSchemaChange,
    MultiSchemaInfo: &MultiSchemaInfo {
        SubJobs: []*SubJob {
            &SubJob {
                Type: ActionAddColumn,
                Args: ...
            },
            &SubJob {
                Type: ActionModifyColumn,
                Args: ...
            },
        }
    } 
}
```

In this way, we pack multiple schema changes into one job. Like any other job, it enqueue the DDL job queue to the persistent storage and waits for an appropriate worker to pick it up and process it.

### Job/Sub-job Execution

As shown in the code above, there is a field `State` in both `Job` and `SubJob`. All the possible states and the changes are listed here:

```
           ┌-->---    Done     ->------------------┐
           |                                       |
None -> Running -> Rollingback -> RollbackDone -> Synced
           |                                       |
           └-->--- Cancelling  -> Cancelled    -->-┘
```

We can divided these states into four types:

| States          | Normal            | Abnormal                    |
|-----------------|-------------------|-----------------------------|
| **Uncompleted** | `None`, `Running` | `Rollingback`, `Cancelling` |
| **Completed**   | `Done`            | `RollbackDone`, `Cancelled` |

Since a `Job` is executed by a DDL worker, the sub-jobs are executed in a single thread. The general principal to select a sub-job is as follows:

- For the normal state, the first uncompleted sub-job is selected in ascending order, i.e., the sub-job with the smaller order number is executed first.
- For the abnormal state, the first uncompleted sub-job is selected in descending order, i.e., the sub-job with the larger order number is executed first.

When one of the sub-job becomes abnormal, the parent job and all the other sub-jobs are changed to an abnormal state.

### Schema Object State Management

To ensure atomic execution of Multi-Schema Change, we need to carefully manage the states of the changing schema objects. Let's take the above SQL as an example:

```SQL
ALTER TABLE t ADD COLUMN b INT, MODIFY COLUMN a CHAR(10);
```

If the second sub-job `MODIFY COLUMN a CHAR (10)` fails for some reason(e.g., a row has more than ten characters), the first sub-job should be able to roll back its changes (roll back the added column `b`).

This requirement means that we cannot simply publish the schema object when a sub-job is finished. Instead, it should remain in a state invisible to users, waiting for the other sub-jobs to complete, eventually publishing all at once when it is confirmed that all sub-jobs have succeeded. This method is similar to 2PC: the "commit" cannot be started until the "prewrites" are completed.

Here is the table of schema states that can occur in different DDLs. Note that the "Revertible States" means that the changes are invisible to the users.

| DDL (Schema Change)     | Revertible States                          | Non-revertible States                       |
|-------------------------|--------------------------------------------|---------------------------------------------|
| Add Column              | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |
| Add Index               | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |
| Drop Column             | Public                                     | Write-Only, Delete-Only, Delete-Reorg, None |
| Drop Index              | Public                                     | Write-Only, Delete-Only, Delete-Reorg, None |
| Non-reorg Modify Column | Public (before meta change)                | Public (after meta change)                  |
| Reorg Modify Column     | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |

To achieve this behavior, we introduce a flag named "non-revertible" in the sub-job. This flag is set when a schema object has reached the last revertible state. A sub-job with this flag is considered temporary completed, so that the worker can select the next sub-job. When all sub-jobs are non-revertible, all associated schema objects change to the next state in one transaction. After that, the sub-jobs are executed serially to do the rest.

On the other hand, if there is an error returned by any sub-job before all of them become non-revertible, the entire job is placed in to a `Rollingback` state. For the executed sub-jobs, we set them to `Cancelling`; for the unexecuted sub-jobs, we set them to `Cancelled`.

Finally, we consider the extreme case: an error occurs while all the sub-jobs are non-revertible. There are two kinds of errors in general, the logical error(such as the violation of unique constraint, out-of-range data) and the physical error(such as unavailablity of the network, unusability of the storage). In this situation, the error is guaranteed to be a physical one: we tend to assume that it can be resolved in a trivial way, e.g., by retrying. This behavior is consistent with the current DDL implementation. Take `DROP COLUMN` as an example, once the column enters the "Write-Only" state, there is no way to abort this job.

## Compatibility

### Upgrade Compatibility

When a TiDB cluster performs a rolling upgrade, there are 2 cases:

1. DDL owner is the new version of TiDB. When users execute a Multi-Schema Change statement on an old version of TiDB, they receive an "unsupported multi-schema change" error message.
2. DDL owner is the old version of TiDB. When users execute a Multi-Schema Change statement on a new version of TiDB, they receive the "invalid ddl job type" error message.

In both cases, the Multi-Schema Change statement cannot be executed. Therefore, users should avoid executing Multi-Schema Change during a rolling upgrade.

### MySQL Compatibility

MySQL may reorder some schema changes:

```SQL
CREATE TABLE t (b INT, a INT, INDEX i(b));
ALTER TABLE t DROP COLUMN b, RENAME COLUMN a TO b, ADD INDEX i(b), DROP INDEX i; -- success!

SHOW CREATE TABLE t;
```

```console
+-------+-------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                  |
+-------+-------------------------------------------------------------------------------------------------------------------------------+
| t     | CREATE TABLE `t` (
 `b` int DEFAULT NULL,
 KEY `i` (`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci |
+-------+-------------------------------------------------------------------------------------------------------------------------------+
```

MySQL's execution order is as follows:

1. `DROP INDEX i`
2. `DROP COLUMN b`
3. `RENAME a TO b`
4. `ADD INDEX i(b)`

Since this behavior is a bit counter-intuitive, we decided not to be fully compatible with MySQL.

```SQL
ALTER TABLE t DROP COLUMN b, RENAME COLUMN a TO b, ADD INDEX i(b), DROP INDEX i;
ERROR 1060 (42S21): Duplicate column name 'b'
```

TiDB validates the schema changes against a snapshot schema structure retrieved before the execution, regardless of the previous changes in the same DDL statement. This may affect some common use cases. For example, `ALTER TABLE t ADD COLUMN a, ADD INDEX i1(a);` is not supported. However, it is not difficult to support such statements: removing the specific validation is enough. 

## Future Work

In the future, this implementation can be utilized to develop other features or achieve some optimizations:

- The table-level data reorganization like `ALTER TABLE CONVERT TO CHARSET` can be implemented by splitting a job into multiple sub-jobs.

- In the scenario of adding multiple indexes, we can improve the performance by reducing the read cost: merge multiple "ADD INDEX" sub-jobs into one sub-job, and read the data once instead of multiple times.

Furthermore, to help users understand how multi-schema change works, it is a good choice to support "EXPLAIN DDL", which can display some useful information like execution order, sub-jobs, and others.

## Alternative Proposals

- The 'job group' way. Unlike the 'SubJob', we can use a job group to represent multiple schema changes. However, this exposes the internal details of the job groups to the DDL job queue, making it more difficult to maintain.

- The existing way, which is a case-by-case implementation (for example, [#15540](https://github.com/pingcap/tidb/pull/15540)). Although some cases can be supported, it is not extensible.

- Another solution is to abandon atomization and use multiple DDL jobs to accomplish the task. This is much less complex and requires little effort. However, it can lead to confusion in the visible intermediate state.
