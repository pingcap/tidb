# Proposal: Support Multi-Schema Change

- Author(s): [tangenta](https://github.com/tangenta)
- Tracking Issue: https://github.com/pingcap/tidb/issues/14766

## Abstract

This proposes an implementation of applying multiple schema changes in one `ALTER TABLE` SQL statement.

## Background

Multi-Schema Change is one of MySQL's extended features to the SQL standard. It allows the users to atomically make multiple schema changes in one SQL statement, including column and index `ADD`, `ALTER`, `DROP`, and `CHANGE`, as well as table option changes. For example:

```sql
CREATE TABLE t (a INT, c INT);
ALTER TABLE t ADD COLUMN b INT AUTO_INCREMENT PRIMARY KEY, 
 MODIFY COLUMN c CHAR(5),
 ADD INDEX idx(a),
 AUTO_INCREMENT = 1000;
```

Currently, TiDB only supports one schema change per SQL statement and limited multi-schema changes for some rare cases.

When users try to migrate data from MySQL-like databases, they have to spend extra effort to rewrite a multi-schema change DDL to several single-schema change DDLs. For the users who rely on ORM frameworks like [Flyway](https://flywaydb.org/) to construct SQLs automatically, rewriting SQL could be tedious and the scripts are hard to maintain.

Above all, the lack of this capability can be a blocking issue for those who want to use TiDB.

## Proposal

The implementation is based on the [online DDL architecture](https://github.com/pingcap/tidb/blob/e0c461a84cf4ad55c7b51c3f9db7f7b9ba51bb62/docs/design/2018-10-08-online-DDL.md). Similar to the existing [Job](https://github.com/pingcap/tidb/blob/6bd54bea8a9ec25c8d65fcf1157c5ee7a141ab0b/parser/model/ddl.go/#L262) structure, we introduce a new structure "SubJob", which represents one single schema change. As its name suggests, a job can contain zero or more sub-jobs.

The Multi-Schema Change DDL jobs have the type `ActionMultiSchemaChange`. In the current worker model, there is a dedicated code path (`onMultiSchemaChange()`) to run these jobs. Only Multi-Schema Change jobs can have sub-jobs.

For example, the DDL statement

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

In this way, we pack multiple schema changes into one job. Just like any other jobs, it enters the DDL job queue stored in the storage, waits for a suitable worker to pick it up and handle it.

In normal cases, the worker executes the sub-jobs one by one serially as if they are plain jobs. However, the thing gets complex in abnormal cases.

To ensure the atomicity of a Multi-Schema Change execution, we need to maintain the changing schema object states carefully. Take the above SQL as an example, if the second sub-job `MODIFY COLUMN a CHAR(255)` failed for some reason, the first sub-job should be able to rollback its changes(rollback the added column `b`).

This requirement means we cannot simply public the schema object when a sub-job is finished. Instead, it should stay in a state which is still unnoticeable to users, and wait for the other sub-jobs, finally public all at once when all the sub-jobs are confirmed to be successful. This method is kind of like 2PC: the "commit" can only be started when "prewrites" are complete.

Here is the table of schema states that may occur in different DDLs. Note the "Revertible States" means the changes are unnoticeable to the users.

| DDL (Schema Change)     | Revertible States                          | Non-revertible States                       |
|-------------------------|--------------------------------------------|---------------------------------------------|
| Add Column              | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |
| Add Index               | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |
| Drop Column             | Public                                     | Write-Only, Delete-Only, Delete-Reorg, None |
| Drop Index              | Public                                     | Write-Only, Delete-Only, Delete-Reorg, None |
| Non-reorg Modify Column | Public (before meta change)                | Public (after meta change)                  |
| Reorg Modify Column     | None, Delete-Only, Write-Only, Write-Reorg | Public                                      |

To achieve this behavior, we introduce a flag in the sub-job named "non-revertible". This flag is set when a schema object has stepped to the last revertible state. When all sub-jobs become non-revertible, all the related schema objects step to the next state in one transaction. After that, the sub-jobs are executed serially to do the rest.

On the other hand, if there is an error returned by any sub-job before all of them become non-revertible, the whole job is switched to a `rollingback` state. For the executed sub-jobs, we set them to `cancelling`; for the sub-job that have not been executed, we set them to `cancelled`.

Finally, we consider the extreme case: an error occurs while all the sub-jobs are non-revertible. In this situation, we tend to believe that the error can be solved in a trivial way like retrying. This behavior is compatible with the current DDL implementation. Take `DROP COLUMN` as an example, once the column steps to the "Write-Only" state, there is no way to cancel this job.

## Compatibility

### Upgrade Compatibility

When a TiDB cluster performs a rolling upgrade, there are 2 cases:

1. DDL owner is the new version of TiDB. When users execute a Multi-Schema Change statement on an old version of TiDB, they receive an "Unsupported Multi-Schema Change" error message.
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

TiDB validates the schema changes against a snapshot schema structure retrieved before the execution, regardless of the previous changes in the same DDL SQL statement.

## Alternative Proposals

An alternative solution is to give up the atomicity and uses multiple DDL jobs to complete the task. This is much less complex and requires little effort. However, it may lead to a visible intermediate state confusion.
