# Proposal: Support Multi-Schema Change

- Author(s): [tangenta](https://github.com/tangenta)
- Tracking Issue: https://github.com/pingcap/tidb/issues/14766

## Abstract

This proposes an implementation of Multi-Schema Change.

## Background

Multi-Schema Change is one of MySQL's extended features to the SQL standard. It allows the users to atomically make multiple schema changes in one SQL statement, including column and index `ADD`, `ALTER`, `DROP`, and `CHANGE`, as well as table option changes. For example:

```sql
CREATE TABLE t (a INT, c INT);
ALTER TABLE t ADD COLUMN b INT AUTO_INCREMENT PRIMARY KEY, 
 MODIFY COLUMN c CHAR(5),
 ADD INDEX idx(a),
 AUTO_INCREMENT = 1000;
```

Currently, TiDB only supports one schema change per SQL statement and multi-schema changes in rare cases.

When users try to migrate data from MySQL-like databases, they have to spend extra effort to rewrite a multi-schema change DDL to several single-schema change DDLs. For the users who rely on ORM frameworks like flyway to construct SQLs automatically, rewriting SQL is not even possible.

Above all, Multi-Schema Change can be a blocking issue for those who want to use TiDB.

## Proposal

The implementation is based on the [online DDL architecture](https://github.com/pingcap/tidb/blob/e0c461a84cf4ad55c7b51c3f9db7f7b9ba51bb62/docs/design/2018-10-08-online-DDL.md). Similar to the DDL job, we introduce a new structure "sub-job", which represents a single schema change. As its name suggests, a job can contain zero or more sub-jobs.

The Multi-Schema Change DDL jobs have the type `ActionMultiSchemaChange`. In the current worker model, there is a dedicated code path (`onMultiSchemaChange()`) to run these jobs. Only Multi-Schema Change jobs can have sub-jobs.

For example, the DDL

```SQL
ALTER TABLE t ADD COLUMN b INT, MODIFY COLUMN a CHAR(255);
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

In this way, we pack multiple schema changes into one job. Just like any other jobs, it enters the DDL job queue stored in TiKV and waits for a suitable worker to pick it up.

In normal cases, the worker executes the sub-jobs one by one serially as if they are plain jobs. However, the thing gets complex in abnormal cases.

To ensure the atomicity of a Multi-Schema Change execution, we need to maintain the changing schema object states carefully. Take the above SQL as an example, if the second sub-job `MODIFY COLUMN a CHAR(255)` failed for some reason, the first sub-job should be able to rollback its changes(rollback the added column `b`).

This requirement means we cannot simply public the schema object when a sub-job is finished. Instead, it should stay in a state which is still unnoticeable to users, and wait for the other sub-jobs, finally public all at once when all the sub-jobs are confirmed to be successful. This method is kind of like 2PC: the "commit" can only be started when "prewrites" are complete.

Here is the table of states that should stay for different DDLs. Note the "Next State" means the changes are noticeable to the users.

| DDL (Schema Change)     | Unnoticeable State | Next State |
|-------------------------|--------------------|------------|
| Add Column              | Write-Reorg        | Public     |
| Add Index               | Write-Reorg        | Public     |
| Drop Column             | Public             | Write-Only |
| Drop Index              | Public             | Write-Only |
| Non-reorg Modify Column | Public             | Public     |
| Reorg Modify Column     | Write-Reorg        | Public     |

To achieve this behavior, we introduce a flag in the sub-job named "non-revertible". This flag is set when a schema object has stepped to the last unnoticeable state. When all sub-jobs become non-revertible, all the related schema objects step to the next state in one transaction. After that, the sub-jobs are executed serially to do the rest.

On the other hand, if there is an error returned by any sub-job before all of them become non-revertible, the whole job is switched to a `rollingback` state. For the executed sub-jobs, we set them to `cancelling`; for the sub-job that have not been executed, we set them to `cancelled`.

Finally, we consider the extreme case: an error occurs while all the sub-jobs are non-revertible. In this situation, we tend to believe that the error can be solved in a trivial way like retrying. This behavior is compatible with the current DDL implementation. Take `DROP COLUMN` as an example, once the column steps to the "Write-Only" state, there is no way to cancel this job.

## Compatibility

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

TiDB validates the schema changes against a snapshot schema structure retrieved before the execution, regardless of the previous changes in the same DDL SQL statement.

## Alternative Proposals

An alternative solution is to give up the atomicity and uses multiple DDL jobs to complete the task. This is much less complex and requires little effort. However, it may lead to a visible intermediate state confusion.
