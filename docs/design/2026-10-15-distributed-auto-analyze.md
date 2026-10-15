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
    * [Rolling Upgrade](#rolling-upgrade)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)

## Introduction

This document proposes running auto-analyze statements on the Distributed eXecution Framework (DXF), instead of only on the stats owner. The stats owner keeps building and prioritizing the analysis jobs, and submits each statement as a DXF task. DXF runs the task on one of its nodes and reports the result back.

On the classic kernel, the task runs on any TiDB node of the cluster. On the next-gen kernel, DXF runs only on the service nodes of the `SYSTEM` keyspace, so the statement of a user keyspace runs there. To support this, the info schema of a cross-keyspace runtime can load the analyzed table on demand, and `ANALYZE` can run in a session that has no domain.

## Motivation

Users have often reported that auto-analyze cannot keep up with their data changes. Some of them tuned the `ANALYZE` parameters to collect statistics faster, but for large clusters with many tables, tuning alone cannot solve the problem.

The main reason is that auto-analyze runs only on the stats owner. Even with a higher `tidb_auto_analyze_concurrency`, every statement still runs on that single TiDB node, so the node's resources cap the throughput of the whole cluster, and the node itself can become overloaded.

To work around this limitation, some users wrote their own scripts to distribute `ANALYZE` statements across TiDB nodes. As their clusters grow, these scripts become harder and harder to maintain. We therefore want TiDB to distribute auto-analyze statements across its nodes by itself, without any manual maintenance.

On the next-gen kernel, the same change moves auto-analyze off the TiDB nodes of a user keyspace and onto the shared DXF service nodes, which are built to run this kind of background work.

## Detailed Design

### Current Implementation

The stats owner maintains a [priority queue](./2024-09-06-maintain-priority-queue-in-memory.md) of analysis jobs. Its [worker](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/refresher/worker.go#L95-L109) runs up to `tidb_auto_analyze_concurrency` jobs at the same time, and each job builds one or more `ANALYZE` statements for its table. Every statement eventually goes through [`exec.AutoAnalyze`](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/exec/exec.go#L58-L87), which executes it in a session of the stats handle on the stats owner.

This design changes only where the statement runs: the stats owner delivers it to another node, which executes it and reports the result. The priority queue, the job types, and the statements stay the same. **We use [DXF](./2023-04-11-dist-task.md) to deliver and run the statements**, because it already places work on the nodes by their free slots, moves the work of a lost node to another node, cancels running work, and, on the next-gen kernel, runs on the service nodes.

### Run Auto Analyze Statements on DXF

#### Workflow

The [workflow](https://editor.plantuml.com/uml/RLFBJiCm4BpdArQzGANo0psW5hMLa2f0AaY0Ujd4MsBgs25hjuIlnuycQGMdsElPcTbPPSOM6-igbTN-AmjPexOmsbYq3EQegTdgQyVKm9vQhh6WyGYGuJ4yNVOiNbV0HKd2AMeYQDFT_WBfcmfdJM3tv0TP46WZJkJBqsCcq6AEJ31wERUmrUnVyOEIIZfdMVG4DxV9t1IUJ0q8doQ7NMCgi2L1tKZJIDl2boD7GyU9OmehvRYCK8uH25BMQvXTh7atwhohcsyA6vTNqW9gc7ldSusg_GbuteS9FHLLfEq45dTmlvWaedXYPIoFivugy7RpJti5a64TMhN0BW-iaBUfDUl0burhoNQm35B7I-EqbHLTiI_iJDnMXLkDMa3kf18-hZHswKk-RUTfGryN_8LOdzdQe1YqF7dac0Grc3_Crs_loqaIOJnGH6ew9jGbuv3dIchPBI9DV1xHiEFdCgNiKKL19FYVdVR-UbK_lZKnqm8zQpOZBS8_y0i0) is as follows:

![](https://img.plantuml.biz/plantuml/png/RLFBJiCm4BpdArQzGANo0psW5hMLa2f0AaY0Ujd4MsBgs25hjuIlnuycQGMdsElPcTbPPSOM6-igbTN-AmjPexOmsbYq3EQegTdgQyVKm9vQhh6WyGYGuJ4yNVOiNbV0HKd2AMeYQDFT_WBfcmfdJM3tv0TP46WZJkJBqsCcq6AEJ31wERUmrUnVyOEIIZfdMVG4DxV9t1IUJ0q8doQ7NMCgi2L1tKZJIDl2boD7GyU9OmehvRYCK8uH25BMQvXTh7atwhohcsyA6vTNqW9gc7ldSusg_GbuteS9FHLLfEq45dTmlvWaedXYPIoFivugy7RpJti5a64TMhN0BW-iaBUfDUl0burhoNQm35B7I-EqbHLTiI_iJDnMXLkDMa3kf18-hZHswKk-RUTfGryN_8LOdzdQe1YqF7dac0Grc3_Crs_loqaIOJnGH6ew9jGbuv3dIchPBI9DV1xHiEFdCgNiKKL19FYVdVR-UbK_lZKnqm8zQpOZBS8_y0i0)

1. The worker on the stats owner pops a job from the priority queue and builds its `ANALYZE` statement, as before.
2. On the next-gen kernel, the stats owner flushes the pending stats deltas of the table on the TiDB nodes of its keyspace.
3. Instead of running the statement locally, the stats owner submits an `AutoAnalyze` task that carries the statement, the IDs of the table and its database, and the slots the statement needs.
4. The DXF scheduler creates the only subtask of the task and places it on a node with enough free slots.
5. The task executor runs the statement. On a service node, it first lists the table in the info schema of the user keyspace's runtime and builds a stats handle for that keyspace.
6. The statement loads the current statistics of the table, analyzes it, and saves the new statistics into the user keyspace, as a local `ANALYZE` does.
7. Once the subtask finishes, the task succeeds and the stats owner stops waiting.
8. The stats owner loads the new statistics into its own cache, which a local `ANALYZE` would have done as its last step.

On the classic kernel, the flow is the same without step 2, and in step 5 the statement runs on a TiDB node of the same cluster with that node's domain.

#### The Switch

We add a new `GLOBAL` variable, `tidb_auto_analyze_dist_mode`:

| Value | Meaning |
| --- | --- |
| `OFF` (default) | Auto-analyze runs every statement on the stats owner, as before. |
| `DXF` | The stats owner runs every auto-analyze statement as a DXF task. |

**With `OFF`, auto-analyze behaves as before**: the stats owner runs every statement locally. Manual `ANALYZE` is not affected in either mode.

#### Dispatch on the Stats Owner

The only change on the queue side is a hook in `exec.AutoAnalyze`: when the mode is `DXF`, the statement is handed to a dispatcher instead of being executed locally. The job types and the statements they build stay unchanged.

The task meta keeps the statement as its template and parameters, and adds the information the executor cannot derive by itself:

```go
type TaskMeta struct {
	// SQL is the analyze statement template, such as "analyze table %n.%n".
	SQL string `json:"sql"`
	// Params are the identifiers to fill into SQL.
	Params []string `json:"params"`
	// StatsVer is the statistics version to analyze with.
	StatsVer int `json:"stats_ver"`
	// NeedVersionRewriteWarn tells the node that runs the statement to log that
	// it rewrites version 1 statistics.
	NeedVersionRewriteWarn bool `json:"need_version_rewrite_warn,omitempty"`
	// DBID and TableID identify the table the statement analyzes.
	DBID    int64 `json:"db_id"`
	TableID int64 `json:"table_id"`
}
```

The keyspace of the task is the keyspace of the stats owner's store, so the dispatcher knows it without any lookup. The task key is `<keyspace>/AutoAnalyze/<UUID>`.

The dispatcher then:

1. Tracks itself as an auto-analyze process of the stats owner, and shows the statement as its running query. This is what keeps the existing controls working, as described in [Timeout and Cancellation](#timeout-and-cancellation).
2. On the next-gen kernel, runs `FLUSH STATS_DELTA db.tbl CLUSTER` on the stats owner. `ANALYZE` flushes the deltas of the table on every TiDB node before it reads the row count, otherwise an old delta can be applied later and count the same rows twice. A service node cannot reach the TiDB nodes of a user keyspace, so the stats owner flushes their deltas before submitting the task. A failed flush fails the dispatch, and the queue retries the table later.
3. Submits the task with the slots the statement needs, as described in [Concurrency and Slots](#concurrency-and-slots), and waits until the task is done.
4. After the task succeeds, loads the new statistics of the table and its partitions into the stats owner's cache. **The priority queue relies on this:** it removes a finished job from the running jobs on the assumption that the cache already holds the new statistics. Otherwise, it could see the old modify count and analyze the table again.

If DXF is not ready on the node, or the statement cannot be described by a task meta, the dispatcher runs the statement locally.

#### Task, Scheduler, and Executor

In DXF, a task is split into subtasks by the scheduler, and the subtasks are run by the task executors on the nodes. For this feature, each task has one step and exactly one subtask, which carries the task meta as it is:

- **Scheduler:** generates the only subtask, and returns no eligible instances, so the framework places the subtask on the nodes of the target scope by their free slots. Errors are not retryable: the priority queue analyzes the table again later if the task fails.
- **Task executor:** runs the statement in a session of the stats handle. The subtask is idempotent, because running `ANALYZE` again only refreshes the statistics again, so DXF can rerun it on another node if its node goes away.

A statement analyzes one table and is already the smallest unit of work, so a task never combines the statements of different tables.

We use a task type dedicated to auto-analyze, `AutoAnalyze`, rather than a generic task that runs any SQL statement. The parts that a generic SQL task would need, loading a user table on a service node and running a statement without a domain, live in the info schema and the session layer, as described in [Run on a DXF Service Node](#run-on-a-dxf-service-node), so a future SQL task can reuse them without going through this task type.

#### Concurrency and Slots

`tidb_auto_analyze_concurrency` still limits auto-analyze for the whole cluster. **The worker on the stats owner holds its slot while the dispatcher waits for the task**, so the variable limits how many statements run at the same time, wherever they run.

On the node, a DXF task must stay within the resources it gets from the framework, which are counted in [slots](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L42-L61). An `ANALYZE` statement, on the other hand, decides its parallelism by three variables. For an internal `ANALYZE`, TiDB takes two of them from global variables instead of the session ones of the same purpose, see [`ExecStmt.Exec`](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/executor/adapter.go#L730-L759):

| Variable | Controls |
| --- | --- |
| `tidb_auto_build_stats_concurrency` | The tables or partitions analyzed at the same time. |
| `tidb_build_sampling_stats_concurrency` | The workers that merge the samples of each of them. |
| `tidb_sysproc_scan_concurrency` | The coprocessor requests each of them sends at the same time. `0` lets TiDB choose by the number of TiKV stores, at least 4. |

The dispatcher asks for **the largest of the three as the slots of the task, but no more than the CPU count of a DXF node**. A scan concurrency of `0` counts as 4.

The task executor then keeps the statement within the slots it gets. For the duration of the statement, it lowers the session value of `tidb_build_sampling_stats_concurrency`, and wraps the session's accessor of global variables, so that the two global variables read no higher than the slots. A scan concurrency of `0` reads as the smaller of 4 and the slots. Both are undone when the statement ends, so a pooled session keeps its own settings.

#### Timeout and Cancellation

Auto-analyze already has its controls: `tidb_max_auto_analyze_time`, the auto-analyze time window, `KILL`, and the shutdown of the stats owner. They all work by killing the auto-analyze processes of the stats owner. Since the dispatcher tracks itself as such a process and shows the statement as its running query, **every one of these controls kills the dispatcher, and the dispatcher cancels the task**:

1. The dispatcher sees the kill and cancels the task, and retries every second until the cancellation is recorded.
2. DXF cancels the context of the subtask on the node that runs it.
3. The cancellation of the context is delivered as a kill of the statement's own process on that node. Some steps of `ANALYZE`, such as merging the global statistics of a partitioned table, only check the SQL killer, not the context, so the kill is needed to stop them.
4. The dispatcher waits for the task to stop for at most 10 seconds, so a shutdown is never blocked by a task that can no longer be read.

`tidb_max_auto_analyze_time` measures the time from the start of the dispatch, so it also covers the time the task waits for free slots. We do not add another timeout.

If a node that runs a subtask crashes, DXF moves the subtask to another node and reruns it. If the statement fails, the task fails, the dispatcher returns the error to the job, and the priority queue retries the table as it does after a local failure.

### Run on a DXF Service Node

On the next-gen kernel, a service node belongs to the `SYSTEM` keyspace and serves the tasks of every user keyspace. It runs the statement of a user keyspace through a cross-keyspace runtime, which has a store, a session pool, and an info schema of that keyspace. Two parts of a normal TiDB node are missing there:

1. The info schema of the runtime [loads only the system tables](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/infoschema/issyncer/loader.go#L422-L431) of the user keyspace. Loading every user table of every keyspace that a node serves would be slow and would take too much memory, and a task should not see the tables it does not need.
2. A session of the runtime has no domain, so it has no stats handle, no sysvar cache, and no historical-stats worker.

#### Load the Analyzed Table on Demand

The task meta carries the IDs of the table and its database, and the info schema of the runtime loads that table, and only that table, when it is asked for. **This lives in the info schema, not in the task**, so any task that runs SQL on a user table can use it.

- `InfoCache` gets an optional list of tables that may be loaded on demand. The runtime enables it when it is created.
- A holder of the runtime lists a table with `AllowUserTable(dbID, tableID)` and releases it when it is done. The runtime exposes this as an optional interface, `sqlsvrapi.UserTableAllower`. A table listed twice stays listed until both release it.
- Sessions read the info schema through `GetLatestWithOnDemandTables`. When the list is not empty, it returns a wrapper that falls back to the listed tables for the lookups by name, by table ID, and by partition ID, and for the lookups of their database by name and ID. Whatever enumerates the schemas, or the tables of a schema, does not see them.
- The schema syncer and the builder keep using `GetLatest`, so they never see a table that the cache does not load.
- A listed table is read from the latest meta once, when it is first asked for, and stays the same until it is released. The statement therefore works with one version of the table, like the info schema a normal statement holds, and the next statement sees the schema changes made in between. Only public tables are loaded, without ID allocators, and a cached table is read as a normal table.

On a normal TiDB node, the list is never enabled and `GetLatestWithOnDemandTables` returns the same info schema as `GetLatest`.

#### Stats Handle Without a Domain

`ANALYZE` takes the stats handle from the domain of its session in many places. We replace these lookups with `domain.GetStatsHandle(sctx)`, which returns the domain's handle as before, or, for a session without a domain, a handle bound to the session.

On a service node, the task executor builds a stats handle of the user keyspace for each statement:

- The handle works on sessions taken from the runtime, and binds itself to each of them. It starts no background worker.
- Its cache starts empty, and a statement takes a table that is not in the cache as one without statistics, so the executor first loads the current statistics of the table and its partitions. Otherwise, the statement would choose its sample rate from a pseudo row count.
- The handle reads a dozen global variables every time it takes a session, and a session without a domain reads them from the table of its keyspace. The executor therefore keeps the values each statement reads first, so the statement works with one set of settings.
- When the statement ends, the executor waits until no worker uses a session of the handle, then closes the handle and makes the sessions leave the runtime. A killed statement returns before its save workers do, so a session must not be closed while one of them is still in a transaction.

On the classic kernel, the executor uses the stats handle of its node's domain, as a local auto-analyze does.

#### Settings of the User Keyspace

Many global variables are mirrored in process-level variables, which hold the values of the node's own keyspace. On a service node that is the `SYSTEM` keyspace, so a statement of a user keyspace must read the settings of its keyspace instead:

- `tidb_analyze_column_options`, `tidb_persist_analyze_options`, and the default numbers of buckets and TopN are read from the global variables of the user keyspace.
- A session without a domain reads its global variables from the table of its keyspace, and reports a failed read as an error instead of falling back to the default value.
- Internal SQL nested in the statement, such as reading the row count of a small table, runs in the runtime's own sessions.
- The statement skips the `FLUSH STATS_DELTA ... CLUSTER` before analyzing: it would reach the TiDB nodes of the `SYSTEM` keyspace instead of the user keyspace. The stats owner flushes the deltas before dispatching.
- With historical statistics enabled, the statement dumps them itself, because there is no historical-stats worker of the user keyspace on the node.
- The cache of approximate row counts from PD is keyed by keyspace as well, because tables of two keyspaces can share IDs and names.

### Rolling Upgrade

`tidb_auto_analyze_dist_mode` is `OFF` in both new and upgraded clusters, so an upgrade never turns the feature on. An old stats owner does not know the variable and keeps running every statement locally.

**Set the variable to `DXF` only after every node is upgraded**, including the DXF service nodes on the next-gen kernel. If it is set earlier, a task that reaches an old node fails: an old DXF owner [fails a task of an unknown type](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/scheduler/scheduler_manager.go#L277-L282), and an old task executor [fails its subtask](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/taskexecutor/manager.go#L341-L347). The statement does not run, no statistics are written, and the priority queue analyzes the table again later.

## Test Design

The tests cover what can go wrong when the statement leaves the stats owner:

- **Result:** a dispatched statement is recorded as an auto-analyze job, and its statistics are in the stats owner's cache when the dispatcher returns, so the priority queue does not queue the table again.
- **Cancellation:** a kill and `tidb_max_auto_analyze_time` cancel the task both while it waits for slots and while its statement runs, including the merge of global statistics. The job of a running statement is marked failed, the table is analyzed again later, and the waiting process holds no transaction start timestamp.
- **Slots:** the slots follow the effective settings, including a scan concurrency of `0`. The statement reads no setting above its slots, and a pooled session keeps its own settings afterwards.
- **On-demand tables:** a listed table is found by name, table ID, and partition ID, and unlisted tables stay invisible. A listed table keeps the version it was loaded with until it is released, and the schema syncer and the builder never see it. Concurrent listing and releasing pass the race detector.
- **Service node:** `ANALYZE` of a user keyspace on a service node writes statistics only into that keyspace, and gives the same result as the same statement in the keyspace: for plain, partitioned, and cached tables, after a schema change, and with historical statistics. It uses the settings of the user keyspace, not those of `SYSTEM`.
- **Rolling upgrade:** with the mode set to `DXF` before an old node is upgraded, the task fails and the table is analyzed again later.
- **Throughput:** the duration of auto-analyze for many tables with the mode `OFF` and `DXF`, across values of `tidb_auto_analyze_concurrency` and numbers of DXF nodes, and the fixed overhead per statement. The result is used to choose the default of `tidb_auto_analyze_concurrency`.

## Impacts & Risks

- Each statement pays the fixed overhead of scheduling a DXF task, which matters most for small tables.
- The stats owner still holds a worker slot and one session for each running statement while it waits.
- At most 16 DXF tasks run at the same time by default, and this limit is shared with other task types, such as `ADD INDEX` and `IMPORT INTO`, and on the next-gen kernel with every user keyspace.
- Every statement adds a row to the DXF task history. Subtask history is removed after 14 days, but task history is kept.
- If the stats owner crashes while a task runs, the task runs to the end without anyone waiting for it, and the new stats owner may analyze the same table again in the meantime.
- On a service node, some process-level settings of the `SYSTEM` keyspace still apply to the statements of every keyspace: `tidb_mem_quota_analyze`, `tidb_max_auto_analyze_time` and the auto-analyze time window on top of the user keyspace's own, and the default TopN and bucket numbers that decide whether `ANALYZE` prunes TopN and buckets.
- The stats deltas that the TiDB nodes collect between the flush on the stats owner and the start of the statement are not flushed before the statement reads the row count.
- Under the slots of a task, a scan concurrency of `0` no longer grows with the number of TiKV stores.

## Investigation & Alternatives

- **Poll a system table:** the stats owner writes the statements into `mysql.analyze_jobs` and the other nodes poll it. This needs its own scheduling, failover, and resource control, all of which DXF already has.
- **A generic SQL task:** the [first prototype](https://github.com/pingcap/tidb/pull/60969) put any SQL statement into a task. A statement alone does not tell the framework which tables to load or how many slots it needs, so this design uses a task type dedicated to auto-analyze, and keeps the reusable parts in the info schema and the session layer.
- **Load the info schema of the whole user keyspace:** it is slow and takes a lot of memory on a node that serves many keyspaces, and lets a task see tables it does not need. A [prototype](https://github.com/pingcap/tidb/pull/65618) that loads any table on demand has the same concerns, so this design loads only the tables a task lists.
- **Put the table into the session's temporary-table slot:** every task that runs SQL on a user table would have to do this by itself. Loading the listed tables in the info schema keeps the SQL layer unaware of it.
- **Run several statements in one task:** this would work around the limit of 16 tasks, but a statement is already the smallest unit of work, and combining unrelated tables in one task makes failure handling and cancellation coarser.
- **Pick the node in the scheduler:** spreading the tasks by their IDs bypasses the slot-aware placement of the framework and moves running subtasks when the nodes change. The framework places the subtask by its slots instead.

## Unresolved Questions

- What should the default of `tidb_auto_analyze_concurrency` be once statements run on many nodes?
- How should the limit of 16 concurrent DXF tasks evolve to fit auto-analyze of many user keyspaces? DXF plans to raise it step by step.
- Should the slots of a statement be the largest of its three concurrency settings, as now, or closer to their product?
- How should a new stats owner handle the tasks of the previous one, for example by cancelling them when it takes over?
- Should the process-level settings of the `SYSTEM` keyspace keep bounding the statements of every keyspace on a service node?

## Future Possibility

Manual `ANALYZE` could be distributed in the same way.
