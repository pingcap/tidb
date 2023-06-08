# Pause/Resume DDL jobs

- Author: [D3Hunter](https://github.com/D3Hunter)
- Tracking Issue: [#42930](https://github.com/pingcap/tidb/issues/42930)

## Background and Benefits

In order to reduce the complexity of deploying, configuring, and maintaining
Lightning, and to enhance the data import experience (as many configuration
options are not well-supported through the web UI). Especially when doing
parallel import for large mount of data, it's easy to mis-config and cause
the whole import fail.

So we plan to introduce a new SQL statement called "IMPORT INTO" to integrate
the functionality of the Lightning local backend. With this command, users can
accomplish data import tasks that previously required the use of the Lightning
local backend separately, thereby lowering the barrier for importing data.

## Goal

Add a new IMPORT INTO statement to support importing data into TiDB using lightning
local backend. This statement will be run under [Distributed Execution Framework](./2023-04-11-dist-task.md).

SQL syntax:
```sql
IMPORT INTO tbl_name [(col_name_or_user_var [, col_name_or_user_var] ...)]
[SET col_name=expr [, col_name=expr] ...]
FROM 'file_or_url'
[FORMAT string]
[WITH option=val [, option=val]...]
```

Supported data formats: CSV, SQL, PARQUET.

`WITH` options which are only allowed for CSV format:
- `character_set ='<string>'` to specify the character set of the file.
- `fields_terminated_by ='<string>'` to specify the field delimiter.
- `fields_enclosed_by ='<char>'` to specify the field enclosure character.
- `fields_escaped_by ='<char>'` to specify the escape character.
- `fields_defined_null_by ='<string>'` to specify the string that represents NULL.
- `lines_terminated_by ='<string>'` to specify the line terminator.
- `skip_rows =<number>` to specify the number of rows to skip at the beginning of the file.

Other `WITH` options that's supported for all formats:
- `detached` to run in detached mode.
- `disk_quota`: see lightning `disk-quota`.
- `checksum_table`: see lightning `checksum`.
- `max_write_speed`: control how fast we write to TiKV.
- `disable_tikv_import_mode`: to avoid switching TiKV into import mode.
- `thread`: to control import concurrency.

We also add two statements to show or cancel the import job:

```sql
SHOW IMPORT JOBS;
    
SHOW IMPORT JOB <job-id>;
    
CANCEL IMPORT JOB <job-id>;
```

## Architecture

IMPORT INTO statement will be run under [Distributed Execution Framework](./2023-04-11-dist-task.md),
for how task is dispatched and scheduled, please refer that doc.

## Detail Design

Workflow of IMPORT INTO statement:
1. Add the statement in parser
2. Add builder in planner
3. Add builder in executor
4. In executor, we check parameters and do some preparation work.
    - If we're importing files from TiDB server disk, or tidb_enable_dist_task is false,
      we'll run only on the instance that we're connecting to.
   - If we're importing files from TiDB server disk, all data files should be on the
      instance that we're connecting to.
5. Create a job in `tidb_import_jobs`, and submit to the Distributed Execution Framework.
6. Wait for the task to finish if we're not in detached mode.

Design of each interface which is required to run on the Distributed Execution Framework:
- FlowHandle: the dispatcher will call this on each step of the execution.
    - on `init` step, the task is divided into multiple sub-tasks, each sub-task
      represents an Engine in TiDB Lightning. and then enter `import` step
    - when `import` step finished, we enter `validating` step to verify checksum,
    - when `validating` step finished, then enter `add-index` step.
    - when `add-index` step finished, the job is done.
- During import, we might need to switch TiKV into import mode and register import task into PD,
  in a periodical manner,
  so we add an OnTicker to the framework, when the task is NOT FINISHED and NO ERROR, we call it.
- Scheduler: for the `import` step, the scheduler will init environment, such as create a local backend,
  schedule the sub-tasks to run, each subtask is divided
  into multiple minimal tasks, each minimal task represents a Chunk in TiDB Lightning.
  when all minimal tasks are finished, we ingest the data into TiKV.
  for `validating` and `add-index` step, we don't need to split into minimal tasks,
  we run them directly.
- Subtask executor: the executor will execute the minimal tasks.

Considering further extension of IMPORT INTO statement, we store each job as a row in
`tidb_import_jobs` table.

`tidb_import_jobs` table structure:
```sql
CREATE TABLE IF NOT EXISTS mysql.tidb_import_jobs (
    id bigint(64) NOT NULL AUTO_INCREMENT,
    create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    start_time TIMESTAMP(6) NULL DEFAULT NULL,
    update_time TIMESTAMP(6) NULL DEFAULT NULL,
    end_time TIMESTAMP(6) NULL DEFAULT NULL,
    table_schema VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    table_id bigint(64) NOT NULL,
    created_by VARCHAR(300) NOT NULL,
    parameters text NOT NULL,
    source_file_size bigint(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    step VARCHAR(64) NOT NULL,
    summary text DEFAULT NULL,
    error_message TEXT DEFAULT NULL,
    PRIMARY KEY (id),
    KEY (created_by)
);
```

Most of the fields are self-explanatory, `parameters` is a JSON string that stores
the parameter user passes in the statement, `summary` is a JSON string that stores
summary info of the job execution, such as how many rows are imported.

Status represents the status of the job, the status transition is as follows:
```
	┌───────┐     ┌───────┐    ┌────────┐
	│pending├────►│running├───►│finished│
	└────┬──┘     └────┬──┘    └────────┘
	     │             │       ┌──────┐
	     │             ├──────►│failed│
	     │             │       └──────┘
	     │             │       ┌─────────┐
	     └─────────────┴──────►│cancelled│
	                           └─────────┘
```

Step means the current step of the job, the step transition is as follows:
When the job is in pending or finished, there's no step, we uses empty string to represent it.
```
None(empty string) -> importing -> validating -> add-index -> None(empty string)
```

### Privilege Requirement

User need to have `INSERT`/`UPDATE`/`INSERT`/`DELETE`/`ALTER` privilege to the target table
to run this IMPORT INTO statement.

If we're importing files from TiDB server disk, we need `FILE` privilege too.

If SEM is enabled, we don't allow importing files from TiDB server disk.

For `SHOW IMPORT JOB` or `CANCEL IMPORT JOB`, user can only show or cancel jobs that's created by
the user, or if the user have `SUPER` privilege, they can show or cancel all jobs.

## Future Work

None.
