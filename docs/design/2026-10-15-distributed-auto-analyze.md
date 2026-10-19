# Distributed Auto Analyze

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/72039

> [!WARNING]
> LLM disclosure: AI wrote this document from my RFC and prototype. I will review the final text before this PR leaves draft.

## Table of Contents

* [Introduction](#introduction)
* [Motivation](#motivation)
* [Detailed Design](#detailed-design)
    * [Current Implementation](#current-implementation)
    * [Run Auto Analyze Statements on DXF](#run-auto-analyze-statements-on-dxf)
        * [Workflow](#workflow)
        * [The Switch](#the-switch)
        * [Dispatch on the Stats Owner](#dispatch-on-the-stats-owner)
        * [Task, Scheduler, and Executor](#task-scheduler-and-executor)
        * [Concurrency and Slots](#concurrency-and-slots)
        * [Timeout and Cancellation](#timeout-and-cancellation)
    * [Run on a DXF Service Node](#run-on-a-dxf-service-node)
        * [Load the Analyzed Table on Demand](#load-the-analyzed-table-on-demand)
        * [Stats Handle Without a Domain](#stats-handle-without-a-domain)
        * [Settings of the User Keyspace](#settings-of-the-user-keyspace)
    * [Compatibility and Rollout](#compatibility-and-rollout)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
    * [Impacts](#impacts)
    * [Risks](#risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)

## Introduction

This document shows a design that runs auto-analyze statements on the Distributed eXecution Framework (DXF). Today, only the stats owner runs these statements. The stats owner continues to build the analysis jobs and to set their priority, and it submits each statement to DXF as a task. DXF runs the task on one of its nodes and sends back the result.

On the classic kernel, the task runs on a TiDB node of the cluster. On the next-gen kernel, DXF runs only on the service nodes of the `SYSTEM` keyspace, so the statement of a user keyspace runs there. For this, the info schema of a cross-keyspace runtime can load the analyzed table on demand. Also, `ANALYZE` can run in a session that has no domain.

## Motivation

Many users tell us that auto-analyze is too slow for the changes to their data. Some users changed the `ANALYZE` parameters to collect statistics faster. But for large clusters with many tables, a change of the parameters does not solve the problem.

The main cause is that auto-analyze runs only on the stats owner. A higher `tidb_auto_analyze_concurrency` does not change this, because all statements still run on that one TiDB node. Thus the resources of that node limit the throughput of all the cluster, and the node itself can get too much load.

To avoid this limit, some users wrote scripts that send `ANALYZE` statements to different TiDB nodes. When their clusters grow, these scripts become more difficult to maintain. We want TiDB to send auto-analyze statements to its nodes itself, with no manual work.

On the next-gen kernel, the same change moves auto-analyze from the TiDB nodes of a user keyspace to the shared DXF service nodes. These nodes are for this type of background work.

## Detailed Design

### Current Implementation

The stats owner keeps a [priority queue](./2024-09-06-maintain-priority-queue-in-memory.md) of analysis jobs. Its [worker](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/refresher/worker.go#L95-L109) runs a maximum of `tidb_auto_analyze_concurrency` jobs at the same time. Each job makes one or more `ANALYZE` statements for its table. All statements go to [`exec.AutoAnalyze`](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/exec/exec.go#L58-L87), which runs them in a session of the stats handle on the stats owner.

This design changes only the node that runs the statement. The stats owner sends the statement to a different node, which runs it and sends back the result. The priority queue, the job types, and the statements do not change. **We use [DXF](./2023-04-11-dist-task.md) to send and run the statements**, because DXF has the necessary functions. It puts work on the nodes by their free slots. It moves the work of a lost node to a different node, and it cancels work. On the next-gen kernel, DXF also runs on the service nodes.

### Run Auto Analyze Statements on DXF

#### Workflow

The [workflow](https://editor.plantuml.com/uml/RLFBJiCm4BpdArQzGANo0psW5hMLa2f0AaY0Ujd4MsBgs25hjuIlnuycQGMdsElPcTbPPSOM6-igbTN-AmjPexOmsbYq3EQegTdgQyVKm9vQhh6WyGYGuJ4yNVOiNbV0HKd2AMeYQDFT_WBfcmfdJM3tv0TP46WZJkJBqsCcq6AEJ31wERUmrUnVyOEIIZfdMVG4DxV9t1IUJ0q8doQ7NMCgi2L1tKZJIDl2boD7GyU9OmehvRYCK8uH25BMQvXTh7atwhohcsyA6vTNqW9gc7ldSusg_GbuteS9FHLLfEq45dTmlvWaedXYPIoFivugy7RpJti5a64TMhN0BW-iaBUfDUl0burhoNQm35B7I-EqbHLTiI_iJDnMXLkDMa3kf18-hZHswKk-RUTfGryN_8LOdzdQe1YqF7dac0Grc3_Crs_loqaIOJnGH6ew9jGbuv3dIchPBI9DV1xHiEFdCgNiKKL19FYVdVR-UbK_lZKnqm8zQpOZBS8_y0i0) is as follows:

![](https://img.plantuml.biz/plantuml/png/RLFBJiCm4BpdArQzGANo0psW5hMLa2f0AaY0Ujd4MsBgs25hjuIlnuycQGMdsElPcTbPPSOM6-igbTN-AmjPexOmsbYq3EQegTdgQyVKm9vQhh6WyGYGuJ4yNVOiNbV0HKd2AMeYQDFT_WBfcmfdJM3tv0TP46WZJkJBqsCcq6AEJ31wERUmrUnVyOEIIZfdMVG4DxV9t1IUJ0q8doQ7NMCgi2L1tKZJIDl2boD7GyU9OmehvRYCK8uH25BMQvXTh7atwhohcsyA6vTNqW9gc7ldSusg_GbuteS9FHLLfEq45dTmlvWaedXYPIoFivugy7RpJti5a64TMhN0BW-iaBUfDUl0burhoNQm35B7I-EqbHLTiI_iJDnMXLkDMa3kf18-hZHswKk-RUTfGryN_8LOdzdQe1YqF7dac0Grc3_Crs_loqaIOJnGH6ew9jGbuv3dIchPBI9DV1xHiEFdCgNiKKL19FYVdVR-UbK_lZKnqm8zQpOZBS8_y0i0)

1. The worker on the stats owner takes a job from the priority queue and makes its `ANALYZE` statement, as before.
2. On the next-gen kernel, the stats owner flushes the stats deltas of the table on the TiDB nodes of its keyspace.
3. The stats owner does not run the statement locally. It submits an `AutoAnalyze` task that contains the statement, the IDs of the table and its database, and the slots that the statement uses.
4. The DXF scheduler makes the only subtask of the task. It puts the subtask on a node that has enough free slots.
5. The task executor runs the statement. On a service node, the executor first lists the table in the info schema of the runtime of the user keyspace. Then it builds a stats handle without a cache for that keyspace.
6. The statement loads the current statistics of the table and analyzes the table. Then it saves the new statistics into the user keyspace, as a local `ANALYZE` does.
7. When the subtask is complete, the task succeeds and the stats owner stops the wait.
8. The stats owner loads the new statistics into its cache. A local `ANALYZE` does this as its last step.

On the classic kernel, the flow is the same, but without step 2. In step 5, the statement runs on a TiDB node of the same cluster, with the domain of that node.

#### The Switch

We add a new `GLOBAL` variable, `tidb_auto_analyze_dist_mode`:

| Value | Meaning |
| --- | --- |
| `OFF` (default) | Auto-analyze runs all statements on the stats owner, as before. |
| `DXF` | The stats owner runs each auto-analyze statement as a DXF task. |

**With `OFF`, auto-analyze operates as before**: the stats owner runs all statements locally. The mode has no effect on manual `ANALYZE`.

#### Dispatch on the Stats Owner

The only change on the side of the queue is a hook in `exec.AutoAnalyze`. When the mode is `DXF`, the hook sends the statement to a dispatcher, and the stats owner does not run the statement locally. The job types and their statements do not change.

The task meta keeps the statement as a template and its parameters. It also adds the data that the executor cannot find itself:

```go
type TaskMeta struct {
	// SQL is the analyze statement template, such as "analyze table %n.%n".
	SQL string `json:"sql"`
	// Params are the identifiers to fill into SQL.
	Params []string `json:"params"`
	// NeedVersionRewriteWarn tells the node that runs the statement to log that
	// it rewrites version 1 statistics.
	NeedVersionRewriteWarn bool `json:"need_version_rewrite_warn,omitempty"`
	// DBID and TableID identify the table the statement analyzes.
	DBID    int64 `json:"db_id"`
	TableID int64 `json:"table_id"`
}
```

The keyspace of the task is the keyspace of the store of the stats owner. Thus the dispatcher knows the keyspace and does not look for it. The task key is `<keyspace>/AutoAnalyze/<UUID>`.

The dispatcher then does these steps:

1. It tracks itself as an auto-analyze process of the stats owner, and it shows the statement as its current query. Thus the controls of auto-analyze continue to operate, as [Timeout and Cancellation](#timeout-and-cancellation) shows.
2. On the next-gen kernel, it runs `FLUSH STATS_DELTA db.tbl CLUSTER` on the stats owner. Before `ANALYZE` reads the row count, it flushes the deltas of the table on all TiDB nodes. If it does not, TiDB can apply an old delta later and count the same rows two times. A service node cannot connect to the TiDB nodes of a user keyspace, so the stats owner flushes their deltas before it submits the task. If the flush fails, the dispatch fails, and the queue analyzes the table again later.
3. It submits the task with the slots that the statement uses, as [Concurrency and Slots](#concurrency-and-slots) shows. Then it waits until the task is complete.
4. After the task succeeds, it loads the new statistics of the table and its partitions into the cache of the stats owner. **The priority queue must have this step:** the queue removes a finished job from the jobs that run, because it expects the new statistics in the cache. If they are not in the cache, the queue can see the old modify count and analyze the table again.

If DXF is not ready on the node, the dispatch fails, and the queue analyzes the table again later. The dispatcher does not run the statement locally. On the next-gen kernel, that moves the work back to the TiDB nodes of the user keyspace.

#### Task, Scheduler, and Executor

In DXF, the scheduler divides a task into subtasks, and the task executors on the nodes run the subtasks. For this feature, each task has one step and only one subtask. The subtask contains the task meta with no change:

- **Scheduler:** makes the only subtask. It returns no eligible instances, so the framework puts the subtask on the nodes of the target scope by their free slots. The scheduler does not retry an error: if the task fails, the priority queue analyzes the table again later.
- **Task executor:** runs the statement in a session of the stats handle. The subtask is idempotent, because a second `ANALYZE` only writes the statistics again. Thus DXF can run the subtask again on a different node if its node stops.

A statement analyzes one table, and it is the smallest unit of work. Thus a task never contains the statements of different tables.

We use a task type only for auto-analyze, `AutoAnalyze`, and not a general task that runs any SQL statement. A general SQL task must have two functions: it loads a user table on a service node, and it runs a statement without a domain. These functions are in the info schema and the session layer, as [Run on a DXF Service Node](#run-on-a-dxf-service-node) shows. Thus a future SQL task can use them without this task type.

#### Concurrency and Slots

`tidb_auto_analyze_concurrency` continues to limit auto-analyze for all the cluster. **The worker on the stats owner keeps its slot while the dispatcher waits for the task.** Thus the variable limits the number of statements that run at the same time, on all nodes.

On the node, a DXF task must stay in the resources that the framework gives to it. The framework counts these resources in [slots](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L42-L61). But an `ANALYZE` statement sets its parallelism with three variables. For an internal `ANALYZE`, TiDB reads two of them from the global variables, and not from the session variables with the same function. Refer to [`ExecStmt.Exec`](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/executor/adapter.go#L730-L759):

| Variable | Controls |
| --- | --- |
| `tidb_auto_build_stats_concurrency` | The tables or partitions that TiDB analyzes at the same time. |
| `tidb_build_sampling_stats_concurrency` | The workers that merge the samples of each table or partition. |
| `tidb_sysproc_scan_concurrency` | The coprocessor requests that each table or partition sends at the same time. With `0`, TiDB sets the value from the number of TiKV stores, with a minimum of 4. |

The dispatcher asks for **the largest of the three values as the slots of the task**. The slots are not more than the CPU count of a DXF node. A scan concurrency of `0` counts as 4.

Then the task executor keeps the statement in the slots that it gets. While the statement runs, the executor decreases the session value of `tidb_build_sampling_stats_concurrency`. It also wraps the accessor of global variables of the session, so that the two global variables are not more than the slots. A scan concurrency of `0` becomes the smaller value of 4 and the slots. When the statement ends, the executor sets the two settings back, so that a session from a pool keeps its own settings.

#### Timeout and Cancellation

Auto-analyze has these controls: `tidb_max_auto_analyze_time`, `KILL`, and the shutdown of the stats owner. All of them kill the auto-analyze processes of the stats owner. The auto-analyze time window only stops new jobs, so a statement that runs continues until it ends, as a local statement does. The dispatcher tracks itself as such a process and shows the statement as its current query. Thus **each of these controls kills the dispatcher, and then the dispatcher cancels the task**. The [cancellation flow](https://editor.plantuml.com/uml/ZLDDYzim4BthLmmvjP0DxNKFPKExXRAXfJZG3mnXR4s8Y3moqhYRzDTt92TnqkwX5riUltdlpOSUac2KeVEcrxThNOyiCAi49K5uPeerlx8kzIZjZkBh6M22BpbkB0esc0Xc7xv_1C6qrqVZgK3M-NJBcL5qe7QG40lcyVHn2rj-Ngv-_7oiMPq9TSHIm9LqOanHXhjt8_yzL4FJEG5aM2hJaj4VVrFnCa7BuHw-eGAt8OBiJeZ5Ol4FwibvJnQQ8pnzMgtcDOkppQR3mmPLO8EZmaPSHtDGihGRn6gVZ0-X1_f5yGY9si0M1itreA8kGTHGj6GDmDybl4TkoK-kYAr1BvS09ANnyApsK_6_ZKIGV92aR8K4xgO2rkSqLMqBjTOp1mvmwIT4P4DU9vSrhi83PyhpMAxRJ2FsGZXQlSMUppSbQIU43W9XEueDpGkPURPZwmkeZw6bb2uvPuEQbP6bsaciaj3F0NqAu5Z1r2TDGwbP5xRTGs1_B3pLrnNiytZZoqMk9tzA4_h_Rs-P6GSX2Be0rmkN046NntL3f-NK_FRDQJlqnrPeNCPioZmeHxw6vWy) is as follows:

![](https://img.plantuml.biz/plantuml/png/ZLDDYzim4BthLmmvjP0DxNKFPKExXRAXfJZG3mnXR4s8Y3moqhYRzDTt92TnqkwX5riUltdlpOSUac2KeVEcrxThNOyiCAi49K5uPeerlx8kzIZjZkBh6M22BpbkB0esc0Xc7xv_1C6qrqVZgK3M-NJBcL5qe7QG40lcyVHn2rj-Ngv-_7oiMPq9TSHIm9LqOanHXhjt8_yzL4FJEG5aM2hJaj4VVrFnCa7BuHw-eGAt8OBiJeZ5Ol4FwibvJnQQ8pnzMgtcDOkppQR3mmPLO8EZmaPSHtDGihGRn6gVZ0-X1_f5yGY9si0M1itreA8kGTHGj6GDmDybl4TkoK-kYAr1BvS09ANnyApsK_6_ZKIGV92aR8K4xgO2rkSqLMqBjTOp1mvmwIT4P4DU9vSrhi83PyhpMAxRJ2FsGZXQlSMUppSbQIU43W9XEueDpGkPURPZwmkeZw6bb2uvPuEQbP6bsaciaj3F0NqAu5Z1r2TDGwbP5xRTGs1_B3pLrnNiytZZoqMk9tzA4_h_Rs-P6GSX2Be0rmkN046NntL3f-NK_FRDQJlqnrPeNCPioZmeHxw6vWy)

1. The dispatcher sees the kill and cancels the task. If the cancellation fails, the dispatcher tries again each second until DXF records the cancellation.
2. DXF cancels the context of the subtask on the node that runs it.
3. On that node, the cancellation of the context kills the process of the statement. Some steps of `ANALYZE` examine only the SQL killer, and not the context. An example is the merge of the global statistics of a partitioned table. Thus the kill is necessary to stop these steps.
4. The dispatcher waits a maximum of 10 seconds for the task to stop. Thus a task that the node cannot read does not block a shutdown.

`tidb_max_auto_analyze_time` measures the time from the start of the dispatch. Thus it also includes the time that the task waits for free slots. We do not add a different timeout.

If a node that runs a subtask stops because of a failure, DXF moves the subtask to a different node and runs it again. If the statement fails, the task fails. The dispatcher returns the error to the job, and the priority queue analyzes the table again, as it does after a local failure.

### Run on a DXF Service Node

On the next-gen kernel, a service node is part of the `SYSTEM` keyspace and runs the tasks of all user keyspaces. It runs the statement of a user keyspace through a cross-keyspace runtime. The runtime has a store, a session pool, and an info schema of that keyspace. The [nodes and keyspaces](https://editor.plantuml.com/uml/NLFVQzim47xtNt5aBniaML_689JGh4dfGqUXTjd6N8PiNs8H-MHqSh9iz7_lIKxSj2z1kVj-t7sIhzWhvxl69BpLr2gd6YXjqrf2yga_60I7fLUqCNW68LiXOmjVBiwAJj4s5BzU9Cb0Wj4ZeuCj7hXL9OxWVmAH3gDCBx_5OquVPGpFOFU4xjC85CDzE0xGUr-Bo43W20b_1L4fhmh5GL0L1daCGKoproMFSvd15ih0JZaTsv6w54xo_6RCz5UQhRw_6_HLU_dp1hpYhVo8IkyVJW8w4maeMNUdIuo3zYhm6YcC5fst2rBcy0zxGVoBfTHSJdljQr3GPr4hgen4KtJQU5XR1o8GUzY8K9pWDcBUEpnqv7M3ODSWiK5tdlyi9qPcRGbQQyqODAqjS5bZeuu4GHnOJFfD6SGZwXY95AjZDTeVZSJ_EOGPhmmcJczgFvdtjpM3cxl7z1RIR96bVvQhksoHqtGwXMifPwk74wcIgm-S82waj2iQxQDjo2ZfwuDqlxVWP0BG4XPtHSGT6mFm5CKCVbYtbGHpYl0-0avE_KdN2ZgyYY2AgZfQ1zUmFwFtcZRXFR7QOUmIxizUsibw-lk3m-gfQwT30fUbDTRDf3o_LaJMoujHPNtvETRdoHLI5Rx05m) of this flow are as follows:

![](https://img.plantuml.biz/plantuml/png/NLFVQzim47xtNt5aBniaML_689JGh4dfGqUXTjd6N8PiNs8H-MHqSh9iz7_lIKxSj2z1kVj-t7sIhzWhvxl69BpLr2gd6YXjqrf2yga_60I7fLUqCNW68LiXOmjVBiwAJj4s5BzU9Cb0Wj4ZeuCj7hXL9OxWVmAH3gDCBx_5OquVPGpFOFU4xjC85CDzE0xGUr-Bo43W20b_1L4fhmh5GL0L1daCGKoproMFSvd15ih0JZaTsv6w54xo_6RCz5UQhRw_6_HLU_dp1hpYhVo8IkyVJW8w4maeMNUdIuo3zYhm6YcC5fst2rBcy0zxGVoBfTHSJdljQr3GPr4hgen4KtJQU5XR1o8GUzY8K9pWDcBUEpnqv7M3ODSWiK5tdlyi9qPcRGbQQyqODAqjS5bZeuu4GHnOJFfD6SGZwXY95AjZDTeVZSJ_EOGPhmmcJczgFvdtjpM3cxl7z1RIR96bVvQhksoHqtGwXMifPwk74wcIgm-S82waj2iQxQDjo2ZfwuDqlxVWP0BG4XPtHSGT6mFm5CKCVbYtbGHpYl0-0avE_KdN2ZgyYY2AgZfQ1zUmFwFtcZRXFR7QOUmIxizUsibw-lk3m-gfQwT30fUbDTRDf3o_LaJMoujHPNtvETRdoHLI5Rx05m)

Two parts of a normal TiDB node are not on the service node:

1. The info schema of the runtime [loads only the system tables](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/infoschema/issyncer/loader.go#L422-L431) of the user keyspace. A node that loads all user tables of all its keyspaces is slow and uses too much memory. Also, a task must not see the tables that it does not use.
2. A session of the runtime has no domain. Thus it has no stats handle, no sysvar cache, and no historical-stats worker.

#### Load the Analyzed Table on Demand

The task meta contains the IDs of the table and its database. The info schema of the runtime loads that table, and only that table, when the task lists it. **This function is in the info schema, not in the task.** Thus each task that runs SQL on a user table can use it.

- `InfoCache` gets an optional list of tables that it can load on demand. Each runtime has this list from its start.
- A holder of the runtime lists a table with `AllowUserTable(dbID, tableID)`, and this call reads the table from the latest meta. The holder releases the table when its work is complete. The runtime gives this call through an optional interface, `sqlsvrapi.UserTableAllower`. If two holders list a table, the table stays in the list until both holders release it.
- Sessions read the info schema through `GetLatestWithOnDemandTables`. If the list is not empty, this function returns a wrapper. When a lookup by name, by table ID, or by partition ID finds no table, the wrapper uses the listed tables. It does the same for the lookup of their database by ID. A function that lists all schemas, or all tables of a schema, does not see the listed tables.
- The schema syncer and the builder continue to use `GetLatest`. Thus they never see a table that the cache does not load.
- A listed table does not change until all of its holders release it. Thus the statement uses one version of the table, as a normal statement uses one info schema. The next statement sees the schema changes that occur between the two statements. The list loads only public tables, without ID allocators, and it reads a cached table as a normal table.

On a normal TiDB node, the list is not used, and `GetLatestWithOnDemandTables` returns the same info schema as `GetLatest`.

#### Stats Handle Without a Domain

A full stats handle keeps the statistics cache of a keyspace and runs its background workers. `ANALYZE` does not use these parts. Thus **`ANALYZE` uses only a narrow interface, `AnalyzeStatsHandle`**, and not the full handle. The interface contains the operations that the statement does:

- It reads the locked tables and the predicate columns, and it writes the results, the analyze jobs, and the historical statistics. It does these operations through internal sessions of the keyspace.
- It merges the global statistics of a partitioned table.
- It reads the current statistics of a table. These statistics set the sample rate, and they show if the statement must write old version 1 statistics again.

The executor and the planner get this interface from `domain.GetAnalyzeStatsHandle(sctx)`. This function returns the handle of the domain, as before, and this handle implements the interface. For a session without a domain, the function returns the handle that is bound to the session. The storage, history, and global-statistics components of the stats handle also use only the methods that they call.

On a service node, the task executor binds an `AnalyzeHandle` of the user keyspace to the statement session. This handle uses these components on the sessions of the runtime. It has no cache, and it starts no background worker:

- It reads the statistics of a table from the storage each time the statement asks for them. Thus the sample rate comes from the row count in `mysql.stats_meta` of the user keyspace, as with a cache.
- It does not load the new statistics into a cache. The stats owner does this after the task succeeds.
- A session without a domain reads its global variables from the table of its keyspace. Each time the handle takes a session, the session reads approximately 12 of these variables again. Thus the sessions of one statement keep the values that they read first.
- When the statement ends, the executor waits until no worker uses a session. Then it removes the sessions from the runtime. A killed statement returns before its save workers stop. Thus the executor must not close a session while a save worker uses it in a transaction.

On the classic kernel, the executor uses the stats handle of the domain of its node, as a local auto-analyze does.

#### Settings of the User Keyspace

Many global variables have a copy in process-level variables. These copies hold the values of the keyspace of the node. On a service node, this keyspace is `SYSTEM`. Thus a statement of a user keyspace must read the settings of its own keyspace:

- The statement reads `tidb_analyze_column_options`, `tidb_persist_analyze_options`, and the default numbers of buckets and TopN from the global variables of the user keyspace.
- A session without a domain reads its global variables from the table of its keyspace. If a read fails, the session returns an error and does not use the default value.
- Internal SQL in the statement runs in the sessions of the runtime. An example is the query that reads the row count of a small table.
- The statement does not run `FLUSH STATS_DELTA ... CLUSTER` before it analyzes. On a service node, this command goes to the TiDB nodes of the `SYSTEM` keyspace, and not to the user keyspace. The stats owner flushes the deltas before the dispatch.
- If historical statistics are on, the statement dumps them itself, because the node has no historical-stats worker of the user keyspace.
- The cache of approximate row counts from PD also uses the keyspace in its key. Tables of two keyspaces can have the same IDs and names.

### Compatibility and Rollout

`tidb_auto_analyze_dist_mode` is `OFF` in new clusters and in upgraded clusters. Thus an upgrade never starts the feature. An old stats owner does not know the variable and continues to run all statements locally.

**Set the variable to `DXF` only after you upgrade all nodes.** On the next-gen kernel, this includes the DXF service nodes. If you set the variable before the upgrade is complete, a task that goes to an old node fails. An old DXF owner [fails a task of an unknown type](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/scheduler/scheduler_manager.go#L277-L282), and an old task executor [fails its subtask](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/taskexecutor/manager.go#L341-L347). The statement does not run, TiDB writes no statistics, and the priority queue analyzes the table again later.

## Test Design

### Functional Tests

- **Dispatch:** a statement runs as a task, and TiDB records it as an auto-analyze job. When the dispatcher returns, the cache of the stats owner contains the new statistics, so the priority queue does not add the table again.
- **Cancellation:** a kill of the dispatcher cancels the task and stops the statement on the node that runs it. This also stops the merge of global statistics. The process that waits holds no transaction start timestamp.
- **Slots:** the slots agree with the effective settings, also with a scan concurrency of `0`. The statement reads no value above its slots. After the statement, a session from a pool keeps its own settings.
- **On-demand tables:** TiDB finds a listed table by name, by table ID, and by partition ID. Tables that are not listed stay invisible. A listed table keeps the version that TiDB loaded until all holders release it, and the schema syncer and the builder never see it. Concurrent calls that list and release tables pass the race detector.
- **Service node:** with a `SYSTEM` domain and a runtime of a user keyspace, `ANALYZE` writes statistics only into the user keyspace. The result is the same as the result of the same statement in the user keyspace. Do this test for plain, partitioned, and cached tables, and after a schema change. Also do it with historical statistics, and with the sample rate and the analyze settings of the user keyspace.

### Scenario Tests

- **Classic:** a cluster with three TiDB nodes analyzes groups of tables through DXF. The groups contain partitioned tables.
- **Next-gen:** a cluster with one service node and two TiDB nodes in a user keyspace analyzes groups of tables on the service node. The groups contain partitioned tables with global statistics and a table after a schema change.
- **Cancellation:** with a low `tidb_max_auto_analyze_time`, TiDB cancels tasks while they wait for slots and while their statements run. The statements stop, TiDB marks the job of each statement that runs as failed, and it analyzes the tables again later.
- **Settings:** a user keyspace has a large build concurrency or an adaptive scan concurrency. Its tasks are not larger than the CPU count of the service node.

### Compatibility Tests

- **Mode `OFF`:** the stats owner runs all statements locally. On a normal TiDB node, `GetLatestWithOnDemandTables` returns the same info schema as `GetLatest`, and `domain.GetAnalyzeStatsHandle` returns the handle of the domain. Thus all tests of auto-analyze, the analyze executor, and the planner pass with no change.
- **Upgrade:** an upgraded cluster starts with the mode `OFF`. If the mode is `DXF` before the upgrade of an old node, the task fails. TiDB writes no statistics and analyzes the table again later.

### Benchmark Tests

Compare the duration of auto-analyze for a cluster with many tables, with the mode `OFF` and with the mode `DXF`. Use different values of `tidb_auto_analyze_concurrency` and different numbers of DXF nodes. Record the fixed overhead of each statement, the CPU and memory of the stats owner and the DXF nodes, and the effect on foreground queries. Use the result to select the default of `tidb_auto_analyze_concurrency`.

## Impacts & Risks

### Impacts

- Auto-analyze does not run only on the stats owner. Thus its throughput increases with the number of DXF nodes. On the next-gen kernel, auto-analyze moves from the TiDB nodes of the user keyspace to the service nodes.
- Each statement has the fixed overhead of a DXF task. This overhead is most important for small tables.
- While the stats owner waits, it keeps a worker slot and one session for each statement that runs.

### Risks

- By default, a maximum of 16 DXF tasks run at the same time. Other task types also use this limit, such as `ADD INDEX` and `IMPORT INTO`. On the next-gen kernel, all user keyspaces also use it.
- Each statement adds a row to the DXF task history. TiDB removes the subtask history after 14 days, but it keeps the task history.
- The stats owner can stop because of a failure while a task runs. Then the task continues until it is complete, and no node waits for it. In this time, the new stats owner can analyze the same table again.
- On a service node, some process-level settings of the `SYSTEM` keyspace apply to the statements of all keyspaces. These settings are `tidb_mem_quota_analyze` and `tidb_max_auto_analyze_time`. The timeout of the `SYSTEM` keyspace applies in addition to the timeout of the user keyspace. They also include the default numbers of TopN and buckets, which decide if `ANALYZE` prunes TopN and buckets.
- The TiDB nodes can collect stats deltas after the flush on the stats owner and before the statement starts. The statement reads the row count without these deltas.
- In the slots of a task, a scan concurrency of `0` does not increase with the number of TiKV stores.

## Investigation & Alternatives

- **Poll a system table:** the stats owner writes the statements into `mysql.analyze_jobs`, and the other nodes poll the table. This method must have its own scheduler, failover, and resource control. DXF has all of these functions.
- **A generic SQL task:** the [first prototype](https://github.com/pingcap/tidb/pull/60969) put any SQL statement into a task. A statement alone does not tell the framework which tables to load or how many slots it uses. Thus this design uses a task type only for auto-analyze. It keeps the parts that other tasks can use in the info schema and the session layer.
- **Load the info schema of all of the user keyspace:** this is slow and uses much memory on a node that serves many keyspaces. Also, a task can then see tables that it does not use. A [prototype](https://github.com/pingcap/tidb/pull/65618) that loads any table on demand has the same problems. Thus this design loads only the tables that a task lists.
- **Build a full stats handle on the service node:** the cache of this handle is empty at the start. Thus each statement must first load the statistics of its table into a cache, and then discard the cache. Also, each statement must manage the background workers and the life cycle of the handle. The narrow interface keeps only the parts that `ANALYZE` uses.
- **Put the table into the temporary-table slot of the session:** each task that runs SQL on a user table must do this itself. When the info schema loads the listed tables, the SQL layer does not know about this function.
- **Run more than one statement in one task:** this avoids the limit of 16 tasks. But a statement is the smallest unit of work. Also, unrelated tables in one task make the control of failures and cancellation less precise.
- **Select the node in the scheduler:** if the scheduler sends tasks to nodes by their IDs, the framework does not put tasks by slots. Also, the subtasks that run move when the nodes change. Thus the framework puts the subtask by its slots.

## Unresolved Questions

- What is the correct default of `tidb_auto_analyze_concurrency` when statements run on many nodes?
- How must the limit of 16 concurrent DXF tasks change for the auto-analyze of many user keyspaces? DXF plans to increase it in steps.
- Must the slots of a statement be the largest of its three concurrency settings, as now, or nearer to their product?
- What must a new stats owner do with the tasks of the old stats owner? For example, it can cancel them when it starts.
- Must the process-level settings of the `SYSTEM` keyspace continue to limit the statements of all keyspaces on a service node?

## Future Possibility

TiDB can send manual `ANALYZE` statements to DXF in the same way.
