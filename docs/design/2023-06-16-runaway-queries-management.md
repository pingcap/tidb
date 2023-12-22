# Runaway Queries Management

- Author: [Connor1996](https://github.com/Connor1996)
- Tracking Issue:
  - <https://github.com/pingcap/tidb/issues/43691>

## Motivation

Run-away queries are queries that consume more resources beyond user expectation. This could be caused by improper SQL statement, suboptimal plan.
Runaway query can impact overall performance if they are not managed properly. We need to manage run-away queries effectively. Long-running operations should be identified and aborted.
Currently, we already have the deadline mechanism pushed down to the TiKV layer that one coprocessor request would not execute in TiKV more than 60s by default. But a runaway query may not cost too much time on one single coprocessor request, thus the deadline mechanism can't help avoid run-away queries.  In the meantime, deadlines can't be too small, otherwise, normal requests can be quickly aborted.

## How to identify run-away queries?

Runaway queries can adversely impact overall performance if they are not managed properly. Resource manager can take action when a query exceeds more than a specified amount of elapsed time. The elapsed time indicates the time of being processed, which excludes the waiting time.
Differentiating run-away queries from queries that really need to perform a full table/index scan is hard. There is no absolute rule. So we just let users define the rule to identify run-away queries. They can twist it on their own needs. The criteria are only the execution time, at least at present. Maybe add more dimension later.
TiKV would send back the scan detail in coprocessor responses. If the total elapsed time of the query exceeds the threshold, then it would be recognized as a run-away query(statement).

## User Interface

### Create/Alter runaway rule

```sql
CREATE/ALTER RESOURCE GROUP rg1
  RU_PER_SEC = 100000 [ PRIORITY = (HIGH|MEDIUM|LOW) ] [BURSTABLE]
  [ QUERY_LIMIT = (
         EXEC_ELAPSED = <#>,
         ACTION = (DRYRUN|COOLDOWN|KILL),
         [ WATCH = (EXACT|SIMILAR) DURATION <#> ])
  ];
```

Runaway rules are stored in resource group meta.

### Show runaway rule

```sql
mysql> SELECT * FROM information_schema.resource_groups;
+---------+------------+----------+-----------+-----------------------------------------------------------+                   
| NAME    | RU_PER_SEC | PRIORITY | BURSTABLE |                       QUERY_LIMIT                         |      
+---------+------------+----------+-----------+-----------------------------------------------------------+                   
| default | 1000000    | MEDIUM   | YES       | NULL                                                      |
| rg1     | 2000       | HIGH     | YES       | EXEC_ELAPSED=10s, ACTION=KILL, WATCH=EXACT, DURATION=10m  |
| rg2     | 1000       | HIGH     | YES       | EXEC_ELAPSED=10s, ACTION=KILL                             |
+---------+------------+----------+-----------+-----------------------------------------------------------+
```

### Log

These fields are included for each record:  

- TIME: sample time
- RULE: "EXEC_ELAPSED"
- ACTION: "DRYRUN"/"COOLDOWN"/"KILL"
- MATCH: "RULE": identified by criteria, "WATCH": identified by matching queries
- SQL_TEXT: text of the runaway sql
- SQL_DIGEST: plan digest of the runaway sql

#### Option1: persist in log file

Runaway query records are stored in a dedicated, auto rotated, run-away log file, quite like slow log file. And the records can be also viewed by the admin table `mysql.TIDB_RUNAWAY_QUERIES` which is a mapping to the run-away log file.

#### Option2: persist in kv data (chosen)

Print runaway query records in tidb log file, and also persist the records in KV data by the admin table `mysql.TIDB_RUNAWAY_QUERIES`.

## How to handle run-away queries?

As you can see from the SQL interface, the actions are of three types:

- DRYRUN:
Only detect and record but do nothing

- COOLDOWN:
Once regarded as a run-away query, the later coprocessor requests will be deprioritized by setting requests context with the lowest priority. On TiKV side, the priority of the request would override the setting of the resource group. Note, we can't deprioritize for the already executing coprocessor requests for simplicity. As coprocessor paging is enabled by default, it won't affect too much. As for batch_coprocessor which doesn't support paging, it would be a problem. But it's enabled by default, so let's ignore it currently.
The override priority is passed in the resource control context of requests.

    ```protobuf
    message ResourceControlContext {
        ...
        // Override the priority of resource group for the request.
        uint64 override_priority = 3;
    }
    ```

- KILL:
The query is aborted with error killed as runaway query. The deadline of the request should be set with the minimum between default copr request deadline and the rest duration to reach runaway threshold to abort the TiKV requests as soon as possible.

## How to quarantine run-away queries?

Cancelling a runaway query is helpful to prevent wasting system resources, but if that problem query is run repeatedly, it could still result in a considerable amount of wasted resources. SQL quarantine solves this problem by quarantining cancelled SQL statements, so they can't be run multiple times.

If `WATCH` clause is set, we will try to match the signature of the queries that are already marked as "runaway" in the coming N seconds.

- `WATCH EXACT`: match statements by exact SQL text.
- `WATCH SIMILAR`: match the statements by normalized SQL.
- `DURATION <#>`:  When the very first SQL query is treated as "runaway", we will mark matches SQL as "runaway" in the coming N seconds.

For the later quarantined query, reject with the error `quarantined plan used` and logs in `mysql.TIDB_RUNAWAY_QUERIES` as well.

Meanwhile, we need to provide an admin table `mysql.TIDB_RUNAWAY_QUARANTINED_WATCH` to let users know which watches are now active for quarantining queries.

These fields are included for each record:  

- START_TIME: The first time hit the watch rule
- END_TIME: When the quarantine watch would finish
- RULE: "EXACT"/"SIMILAR"
- SQL_TEXT: text of the runaway sql
- SQL_DIGEST: plan digest of the runaway sql
