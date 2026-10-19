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
        * [Auto Analyze](#auto-analyze)
        * [DXF](#dxf)
    * [Run Auto Analyze Statements on DXF](#run-auto-analyze-statements-on-dxf)
        * [Workflow](#workflow)
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

This document shows a design that runs auto-analyze statements on the Distributed eXecution Framework (DXF). Today, only the stats owner runs these statements. With this design, the stats owner continues to select the tables and to make the statements. It submits each statement to DXF as a task, and DXF runs the task on one of its nodes.

On the classic kernel, the task runs on a TiDB node of the cluster. On the next-gen kernel, the task runs on a DXF service node of the `SYSTEM` keyspace.

## Motivation

Many users tell us that their statistics become old. Their data changes faster than auto-analyze can analyze the tables. For large clusters with many tables, a change of the `ANALYZE` parameters does not solve this problem.

The main cause is that auto-analyze runs only on the stats owner. A higher `tidb_auto_analyze_concurrency` does not change this, because all statements still run on that one TiDB node. Thus the resources of that node limit the throughput of all the cluster, and the node can get too much load.

Some users wrote scripts that send `ANALYZE` statements to different TiDB nodes. When the clusters grow, these scripts become more difficult to maintain. TiDB must send the statements to its nodes itself.

On the next-gen kernel, there is also a second reason. Auto-analyze runs on the TiDB nodes of the user keyspace, and these nodes also run the queries of the user. Thus auto-analyze takes CPU and memory from the online workload. On the shared DXF service nodes, auto-analyze does not affect this workload.

## Detailed Design

### Current Implementation

#### Auto Analyze

The stats owner keeps a [priority queue](./2024-09-06-maintain-priority-queue-in-memory.md) of analysis jobs. Its [worker](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/refresher/worker.go#L95-L109) runs a maximum of `tidb_auto_analyze_concurrency` jobs at the same time. Each job makes one or more `ANALYZE` statements for its table. [`exec.AutoAnalyze`](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/statistics/handle/autoanalyze/exec/exec.go#L58-L87) runs each statement in a session on the stats owner.

#### DXF

The [Distributed eXecution Framework (DXF)](./2023-04-11-dist-task.md) runs background tasks, such as `ADD INDEX` and `IMPORT INTO`, on many TiDB nodes. [Its parts](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L21-L38) are as follows:

- **Task:** a unit of work of one task type. Its task meta tells the type what to do. A task has one or more steps that run in order. Each step has subtasks, and different nodes can run them at the same time.
- **Scheduler:** runs on the DXF owner, one for each task. It makes the subtasks of each step, and the framework puts them on the nodes.
- **Task executor:** runs the subtasks of a task on one node.
- **Slots:** DXF divides the resources of each node into [slots](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L42-L61), one slot for each CPU core. A task uses a fixed number of slots on each node that runs its subtasks. The scheduler prefers the nodes that have enough free slots. A node starts a subtask only when it has enough free slots, so a task can wait for slots.
- **Service scope:** each node has a service scope, `tidb_service_scope`. A task runs only on the nodes of its target scope. If the target scope is empty and some nodes have the scope `background`, the task runs on those nodes.
- **Failover and cancellation:** if a node stops, the framework [moves its subtasks](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L164-L167) to a different node. When TiDB cancels a task, the framework stops its subtasks.

The [diagram](https://editor.plantuml.com/uml/ZPD1JuD048Nl-oiczQ99gxPfZ6cQnbBqfWVnO28ULfXI0ioItI7Q6F-xkulIsdBmniwRz_5c1suCIyrDLGfJv5HBBIj8L5KhGkA8joM2neGbPINkjP1AqM0DqykzefPKkEBLfH2T2GPFqXGndVAsnY6mFK25B8Sm1LCgDiEOR8GC6KoYQfp1cqoAJAk6qhC1I0FE_mSOFj-1UYVK0_WKi8iBWoZPODgKg46jWJVeN-SXLX5VVoWHswZZLdIF3hKlDsy-wq_7k5-TjEh4iTsTs6dpCF3N0ykOZYQAwGhM6h4TtPkNXqEuUG4_C6bOQTzokxJyGtpGYvyUueD_u8CU_AeNFzxXlNLrv0ixtscAQzcKF0IiQjwUnNHz62uKmiSPwJpRy6ZXrWGpiBTRvIpSOPJQJJl1hMW6zx90lrZJBi28QDprB6t3OqCYcdIbuBVaxESl9vcMsrUddIzWPRLveagbPwwyU50CIAh9Dct2-OKN5xtciCUSqyytj_kQEyODKkh-icy) shows how DXF puts the two subtasks of a task with 4 slots:

![](https://img.plantuml.biz/plantuml/png/ZPD1JuD048Nl-oiczQ99gxPfZ6cQnbBqfWVnO28ULfXI0ioItI7Q6F-xkulIsdBmniwRz_5c1suCIyrDLGfJv5HBBIj8L5KhGkA8joM2neGbPINkjP1AqM0DqykzefPKkEBLfH2T2GPFqXGndVAsnY6mFK25B8Sm1LCgDiEOR8GC6KoYQfp1cqoAJAk6qhC1I0FE_mSOFj-1UYVK0_WKi8iBWoZPODgKg46jWJVeN-SXLX5VVoWHswZZLdIF3hKlDsy-wq_7k5-TjEh4iTsTs6dpCF3N0ykOZYQAwGhM6h4TtPkNXqEuUG4_C6bOQTzokxJyGtpGYvyUueD_u8CU_AeNFzxXlNLrv0ixtscAQzcKF0IiQjwUnNHz62uKmiSPwJpRy6ZXrWGpiBTRvIpSOPJQJJl1hMW6zx90lrZJBi28QDprB6t3OqCYcdIbuBVaxESl9vcMsrUddIzWPRLveagbPwwyU50CIAh9Dct2-OKN5xtciCUSqyytj_kQEyODKkh-icy)

On the next-gen kernel, the service nodes of the `SYSTEM` keyspace run the DXF tasks of all user keyspaces.

### Run Auto Analyze Statements on DXF

This design changes only the node that runs the statement. **Each statement becomes a DXF task of a new type, `AutoAnalyze`.**

#### Workflow

The [workflow](https://editor.plantuml.com/uml/RLDDJ-Cm4BtxLunw0QJk7yW1jQjIQLL4LxJg0kBYn5F5uDZPZ0sR_VMCRP8sWbCI-ttCUtOMv6NdGsD4oqzTwLPQ3xETbvx0lLlidkrb8EpW5NjgPOLNCv04sxWrvMnzpR2zNlq4wnJIzyIuF-Mj7jP0LOqg6EmIPZTyVGNYFwo2Tt4grdxJ5IQph7tpkIcKzBAKX33RwytXqtTp42AD3J-ky_m5_7OjI7Xn9Hmxrm0dWBRJhjE-Xxy10vuOSV02rYPGdN2KAb9eFAkxOrfZLuCYLP0OcqC1VrWClp1EkcFO0dQXRBG7QM79CPPMclv_5ANN69VPsA3rSs3vNwjvTajlP9odAp5AHTsXZ69q01tFr9eUA9HH5Se-KyK0dX9lDVbJBj0sVNJ1UjtW1V72qQMZQ-IpbLP16RHHN6XEMlCIquui6td3FPYOvOxkKQepeoWLPqqoysW8oxlbxUFJpJpxa7pBbLfypwY9w5cbQstrS60vz7bBfukmhtFHZAegH4KYJdQE66SSFJc_Tscfagmf5cXL_9C-00) is as follows:

![](https://img.plantuml.biz/plantuml/png/RLDDJ-Cm4BtxLunw0QJk7yW1jQjIQLL4LxJg0kBYn5F5uDZPZ0sR_VMCRP8sWbCI-ttCUtOMv6NdGsD4oqzTwLPQ3xETbvx0lLlidkrb8EpW5NjgPOLNCv04sxWrvMnzpR2zNlq4wnJIzyIuF-Mj7jP0LOqg6EmIPZTyVGNYFwo2Tt4grdxJ5IQph7tpkIcKzBAKX33RwytXqtTp42AD3J-ky_m5_7OjI7Xn9Hmxrm0dWBRJhjE-Xxy10vuOSV02rYPGdN2KAb9eFAkxOrfZLuCYLP0OcqC1VrWClp1EkcFO0dQXRBG7QM79CPPMclv_5ANN69VPsA3rSs3vNwjvTajlP9odAp5AHTsXZ69q01tFr9eUA9HH5Se-KyK0dX9lDVbJBj0sVNJ1UjtW1V72qQMZQ-IpbLP16RHHN6XEMlCIquui6td3FPYOvOxkKQepeoWLPqqoysW8oxlbxUFJpJpxa7pBbLfypwY9w5cbQstrS60vz7bBfukmhtFHZAegH4KYJdQE66SSFJc_Tscfagmf5cXL_9C-00)

1. The worker on the stats owner takes a job from the priority queue and makes its `ANALYZE` statement.
2. On the next-gen kernel, the stats owner flushes the stats deltas of the table on the TiDB nodes of its keyspace.
3. The stats owner submits an `AutoAnalyze` task. The task contains the statement, the IDs of the table and its database, and the number of slots that the statement uses.
4. The DXF scheduler makes one subtask, and the framework puts it on a node by the free slots.
5. The task executor runs the statement. On a service node, the executor first lists the table in the info schema of the user keyspace. Then it makes a stats handle for that keyspace.
6. The statement analyzes the table and saves the new statistics into the user keyspace.
7. The task succeeds, and the stats owner loads the new statistics into its cache. The other TiDB nodes load the new statistics at their next stats update, as after a local `ANALYZE`.

On the classic kernel, step 2 does not occur, and the statement runs on a TiDB node of the same cluster.

#### Dispatch on the Stats Owner

A new `GLOBAL` variable, `tidb_enable_dist_auto_analyze`, controls this feature. With `ON`, the default in new clusters, a hook in `exec.AutoAnalyze` sends each statement to a dispatcher. With `OFF`, the stats owner runs all statements as before. **The variable has no effect on manual `ANALYZE`**.

The task belongs to the keyspace of the stats owner.

The dispatcher does these steps:

1. It tracks itself as an auto-analyze process of the stats owner, and it shows the statement as its current query. Thus the controls of auto-analyze continue to operate. Refer to [Timeout and Cancellation](#timeout-and-cancellation).
2. On the next-gen kernel, it runs `FLUSH STATS_DELTA db.tbl CLUSTER`. `ANALYZE` must flush the deltas of the table on all TiDB nodes before it reads the row count. If not, TiDB can apply an old delta later and count the same rows two times. A service node cannot connect to the TiDB nodes of a user keyspace, so the stats owner does the flush.
3. It submits the task and waits until the task is complete.
4. After the task succeeds, it loads the new statistics of the table into the cache of the stats owner. **The priority queue must have this step.** The queue removes a finished job because it expects the new statistics in the cache. If they are not in the cache, the queue can use the old modify count and analyze the table again.

If the flush fails or DXF is not ready, the dispatch fails, and the queue analyzes the table again later.

#### Task, Scheduler, and Executor

Each `AutoAnalyze` task has one step and one subtask.

- **Scheduler:** makes the subtask and selects no node. The scheduler does not retry an error, because the priority queue analyzes the table again later.
- **Task executor:** runs the statement. The subtask is idempotent, because a second `ANALYZE` only writes the statistics again. Thus DXF can run the subtask again on a different node if its node stops.

#### Concurrency and Slots

`tidb_auto_analyze_concurrency` continues to limit auto-analyze for all the cluster. **A job counts against this limit while the dispatcher waits for the task.** Thus the variable limits the number of statements that run at the same time on all nodes.

A DXF task must stay in the slots that the framework gives to it. The dispatcher asks for **`tidb_auto_build_stats_concurrency` as the slots of the task**. This variable sets the tables or partitions that the statement analyzes at the same time. The scan and the sampling run in TiKV, and they are most of the work. Thus the slots do not count the other concurrency settings of the statement.

A node cannot run a task that asks for more slots than its CPU count. Thus the slots are not more than the CPU count of a DXF node. If `tidb_auto_build_stats_concurrency` is larger, the statement uses the slots as its build concurrency.

#### Timeout and Cancellation

`tidb_max_auto_analyze_time`, the end of the auto-analyze time window, `KILL`, and the shutdown of the stats owner kill the auto-analyze processes of the stats owner. The dispatcher also tracks itself as an auto-analyze process. Thus **each of these controls kills the dispatcher, and the dispatcher cancels the task**:

1. The dispatcher cancels the task and tries again until DXF records the cancellation. If the task still waits for slots, no statement runs.
2. DXF cancels the context of the subtask on its node.
3. The task executor changes this cancellation into a kill of the statement. Some steps of `ANALYZE` examine only the SQL killer, for example the merge of global statistics.
4. The dispatcher waits a maximum of 10 seconds for the task to stop. Thus a shutdown does not wait forever when the node can no longer read the state of the task.

`tidb_max_auto_analyze_time` measures the time from the start of the dispatch. Thus it includes the time that the task waits for slots.

If a node stops during a subtask, DXF runs the subtask again on a different node. If the statement fails, the task fails, and the priority queue analyzes the table again later.

### Run on a DXF Service Node

On the classic kernel, the subtask runs on a normal TiDB node of the same cluster. That node has all the parts that `ANALYZE` uses. Thus the statement runs there in the same way as on the stats owner.

On the next-gen kernel, the subtask runs on a service node of the `SYSTEM` keyspace. The domain of this node belongs to the `SYSTEM` keyspace, not to the keyspace of the table. The node can get to a user keyspace only through a [cross-keyspace runtime](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/domain/sqlsvrapi/server.go#L63-L66). The runtime has a store, a session pool, and an info schema of that keyspace. But it does not have all the parts that `ANALYZE` uses:

| `ANALYZE` uses | Normal TiDB node | Runtime on a service node | What this design does |
| --- | --- | --- | --- |
| The table in the info schema | All tables | [Only the system tables](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/infoschema/issyncer/loader.go#L422-L431) | [Loads the table on demand](#load-the-analyzed-table-on-demand) |
| A stats handle | The handle of the domain | None, because the sessions have no domain | [Makes a stats handle without a domain](#stats-handle-without-a-domain) |
| The global variables of the keyspace | The values of its keyspace | The process copy holds the values of the `SYSTEM` keyspace | [Reads the settings from the user keyspace](#settings-of-the-user-keyspace) |

#### Load the Analyzed Table on Demand

The info schema cache of the runtime keeps a new list: the user tables that its tasks use now. **The list is in the info schema cache, not in the task.** Thus each task that runs SQL on a user table can use it.

The runtime gets a new method, `AllowUserTable(dbID, tableID)`. The task executor adds the table of the task to the list with it, and removes the table when the statement ends. This call reads the table from the latest meta.

- Sessions read the info schema through a new method, `GetLatestWithOnDemandTables`. When a lookup by name, by table ID, or by partition ID finds no table, this info schema looks in the list.
- The functions that list all tables do not show the listed tables. Only a lookup finds them.
- The schema syncer does not see the listed tables. It makes each new info schema from `GetLatest`. If `GetLatest` had the listed tables, they would stay in all later versions, also after their release.
- A listed table does not change until all holders release it. Thus a statement uses the version from its start, as `ANALYZE` does on a normal TiDB node. All sessions of the runtime share the list, so a table can have only one version in it. If the listed version is not the version in the latest meta, `AllowUserTable` fails. The task fails, and the priority queue analyzes the table again later.
- A listed table has no ID allocator, so it is read-only. A cached table must have a session before TiDB can use its cache. The list has no session, so TiDB reads the table from TiKV as a normal table.

On a normal TiDB node, there is no list, and `GetLatestWithOnDemandTables` returns the same info schema as `GetLatest`.

#### Stats Handle Without a Domain

A full stats handle keeps the statistics cache of a keyspace and runs background workers. `ANALYZE` does not use these parts. Thus **`ANALYZE` uses only a new narrow interface, `AnalyzeStatsHandle`**. The handle of the domain implements this interface, so a normal TiDB node does not change.

On a service node, the task executor makes an `AnalyzeHandle`, a new type, for the user keyspace and binds it to the statement session. This handle uses the store of the keyspace through the sessions of the runtime. It has no cache and no background worker:

- It reads the statistics of a table from the store. Thus the sample rate comes from the row count of the user keyspace.
- It does not load the new statistics into a cache. The stats owner does this after the task.

#### Settings of the User Keyspace

Many global variables have a copy in the process. On a service node, this copy holds the value of the `SYSTEM` keyspace. Thus the statement reads these settings from the user keyspace:

- `tidb_analyze_column_options`, `tidb_persist_analyze_options`, and the default numbers of buckets and TopN.
- All other global variables of its session. If a read fails, the statement fails and does not use a default value.

Also:

- The internal SQL of the statement runs in the sessions of the runtime.
- The cache of approximate row counts from PD uses the keyspace in its key, because tables of two keyspaces can have the same IDs.

### Compatibility and Rollout

`tidb_enable_dist_auto_analyze` is `ON` in new clusters and `OFF` in upgraded clusters. Thus an upgrade does not start the feature. An old stats owner does not know the variable and runs all statements locally.

**In an upgraded cluster, set the variable to `ON` only after you upgrade all nodes**, also the DXF service nodes. An old node does not know the task type and fails the task. Then the statement does not run, and the priority queue analyzes the table again later.

On the next-gen kernel, a new user keyspace also starts with `ON`. A new user keyspace starts only after the `SYSTEM` keyspace has the new version. While some service nodes are old, a task can fail on them, and the priority queue analyzes the table again later.

## Test Design

### Functional Tests

- **Dispatch:** a statement runs as a task, and TiDB records it as an auto-analyze job. When the dispatcher returns, the cache of the stats owner contains the new statistics. On the next-gen kernel, the stats owner flushes the deltas of the table before the task. The row count is the same as with a local `ANALYZE`.
- **Cancellation:** a kill of the dispatcher cancels the task and stops the statement, also during the merge of global statistics. The end of the auto-analyze time window also cancels a task that waits or runs. The process that waits holds no transaction start timestamp.
- **Slots:** the slots are `tidb_auto_build_stats_concurrency`, and not more than the CPU count of a node, also when the variable is larger. The statement stays in its slots.
- **On-demand tables:** TiDB finds a listed table by name, by table ID, and by partition ID, and finds no other user table. A listed table keeps its version until all holders release it. A second `AllowUserTable` call fails when the listed version is not the version in the meta.
- **Service node:** with a `SYSTEM` domain and a runtime of a user keyspace, `ANALYZE` writes statistics only into the user keyspace. The result is the same as in the user keyspace for plain, partitioned, and cached tables, and after a schema change. The test also uses the sample rate and the analyze settings of the user keyspace. On one service node, tables with the same ID in two user keyspaces get their own sample rates.

### Scenario Tests

- **Classic:** a cluster with three TiDB nodes analyzes groups of tables through DXF. The groups contain partitioned tables. The number of tasks that run at the same time is not more than `tidb_auto_analyze_concurrency`.
- **Next-gen:** a cluster with one service node and two TiDB nodes in a user keyspace analyzes groups of tables on the service node. The groups contain partitioned tables and a table after a schema change.
- **Cancellation:** with a low `tidb_max_auto_analyze_time`, TiDB cancels tasks while they wait for slots and while their statements run. TiDB marks the jobs of the statements that run as failed, and analyzes the tables again later.
- **Failover:** a DXF node stops while it runs a subtask. DXF runs the subtask again on a different node, and the statistics are correct.

### Compatibility Tests

- **Variable `OFF`:** the stats owner runs all statements locally, as before.
- **Manual `ANALYZE`:** with the variable `ON`, a manual `ANALYZE` runs locally.
- **Upgrade:** a new cluster starts with the variable `ON`, and an upgraded cluster starts with `OFF`. If the variable is `ON` before the upgrade of an old node, the task fails, and TiDB analyzes the table again later.

### Benchmark Tests

Compare the duration of auto-analyze for a cluster with many tables, with the variable `OFF` and with the variable `ON`. Use different values of `tidb_auto_analyze_concurrency` and different numbers of DXF nodes. Record the fixed overhead of each statement, the CPU and memory of the stats owner and the DXF nodes, and the effect on foreground queries. Use the result to select the default of `tidb_auto_analyze_concurrency`.

## Impacts & Risks

### Impacts

- Auto-analyze does not run only on the stats owner. Thus its throughput increases with the number of DXF nodes. On the next-gen kernel, auto-analyze moves from the TiDB nodes of the user keyspace to the service nodes.
- Each statement has the fixed overhead of a DXF task. This overhead is most important for small tables.

### Risks

- By default, a maximum of 16 DXF tasks run at the same time. Other task types, such as `ADD INDEX` and `IMPORT INTO`, use the same limit. On the next-gen kernel, all user keyspaces also share it.
- Each statement adds a row to the DXF task history. TiDB removes the subtask history after 14 days, but it keeps the task history.
- On a service node, `tidb_mem_quota_analyze` and `tidb_max_auto_analyze_time` of the `SYSTEM` keyspace apply to the statements of all keyspaces. The default numbers of TopN and buckets of the `SYSTEM` keyspace also decide if `ANALYZE` prunes TopN and buckets.

## Investigation & Alternatives

- **Poll a system table:** the stats owner writes the statements into a table, and the other nodes poll the table. This method must have its own scheduler, failover, and resource control, and DXF has these functions. On the next-gen kernel, only the TiDB nodes of the user keyspace can read its table. Thus the work cannot move to the service nodes.
- **A generic SQL task:** the [first prototype](https://github.com/pingcap/tidb/pull/60969) put any SQL statement into a task. A statement alone does not tell the framework which tables to load or how many slots it uses. Thus this design uses a task type only for auto-analyze. It keeps the parts that other tasks can use in the info schema and the session layer.
- **Load the info schema of all of the user keyspace:** this is slow and uses much memory on a node that serves many keyspaces. Also, a task can then see tables that it does not use. A [prototype](https://github.com/pingcap/tidb/pull/65618) that loads any table on demand has the same problems. Thus this design loads only the tables that a task lists.
- **Build a full stats handle on the service node:** the cache of this handle is empty at the start. Thus each statement must load the statistics of its table into a cache, and then discard the cache. Also, each statement must manage the background workers and the life cycle of the handle.
- **Load the database of the table through the schema syncer:** the syncer then keeps the table at the same version as on a normal node. But its filter selects databases, not tables, so it loads all the tables of the database. Also, each change of the list makes a full reload of the schema of the runtime necessary, and all cross-keyspace work uses this schema.
- **Run more than one statement in one task:** this avoids the limit of 16 tasks. But a statement is the smallest unit of work. Also, unrelated tables in one task make the control of failures and cancellation less precise.

## Unresolved Questions

- What is the correct default of `tidb_auto_analyze_concurrency` when statements run on many nodes?
- How must the limit of 16 concurrent DXF tasks change for the auto-analyze of many user keyspaces? DXF plans to increase it in steps.
- What must a new stats owner do with the tasks of the old stats owner? For example, it can cancel them when it starts.
- Must the process-level settings of the `SYSTEM` keyspace continue to limit the statements of all keyspaces on a service node?

## Future Possibility

TiDB can send manual `ANALYZE` statements to DXF in the same way.
