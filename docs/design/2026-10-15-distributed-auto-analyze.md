# Distributed Auto Analyze

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/72039

> [!WARNING]
> LLM disclosure: AI wrote this document from my RFC and prototype. I reviewed the final text.

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
        * [Change of the Stats Owner](#change-of-the-stats-owner)
    * [Run on a DXF Service Node](#run-on-a-dxf-service-node)
        * [Info Schema of the Analyzed Table](#info-schema-of-the-analyzed-table)
        * [Stats Handle Without a Domain](#stats-handle-without-a-domain)
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

Many users tell us that their statistics become old. Their data changes faster than auto-analyze can analyze the tables. **For large clusters with many tables, a change of the `ANALYZE` parameters does not solve this problem.**

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
- **Scope:** each node has a service scope, `tidb_service_scope`. Each task has a target scope. A task runs only on the nodes whose service scope is its target scope. If the target scope is empty, the task runs on the nodes with the service scope `background`, or on the nodes with an empty service scope if no node has `background`.
- **Failover and cancellation:** if a node stops, the framework [moves its subtasks](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/dxf/framework/doc.go#L164-L167) to a different node. When TiDB cancels a task, the framework stops its subtasks.

On the next-gen kernel, the service nodes of the `SYSTEM` keyspace run the DXF tasks of all user keyspaces.

### Run Auto Analyze Statements on DXF

This design changes only the node that runs the statement. **Each auto-analyze statement becomes a DXF task of a new type, `AutoAnalyze`.** The size of the table does not change this. Refer to [Investigation & Alternatives](#investigation--alternatives) for the row limit that this design does not use.

#### Workflow

The [workflow](https://editor.plantuml.com/uml/VPFBRjim44Nt-eg1LmtW2EWRqAA82oT0uJGfQiDD2creQbopfaYLGyPnlxv3wc4B0RgIH7BkdNiukY8ld0-rpXf-AgaQOJnCbbvu0dimw2OW2ExZs_Z8lT-XWvMQVmPZAwJIs2tm6kpnI8sGc0hZ_hXk_d0397TO1Ts9B_klbmVn6MNmrfNcbTI2ICa2nCbqMXg3p_xo5vguJkYUbCIqTP6ahxlwh19UR0GXJ5PgiUvQMwopB2M3oqzjn09Uv_3DDYpsssvWwsoTCZLEMQVy4Vu439ZPniDTPynvdK4DrkXZ1Z3enIOBU9F3ZGwqIogKg5QeFTjtjBWjZLoNCgMonRg07so6BseoDDMfuG5R0MzpM8PDhJm80pCEF3D27_z6TTepkoY2DHe_1NRvCf-sjlnsaGqoKRFdLS0xvX0yg3W6d0meRA8OVwIs8kBI79HlastnWEGX45P0sdhw7wBUP6puFeUlOeynWJ9RcsQa5X5JyXzObRpf1ZGf5LyyZuOrF0x2L50dcOxSZbSuQL_J3sT2UTv8o_r33jy38xoRtJx-l2x0eQZEhQCyYQVsQWmUaeaY7bewKnjVpYgssj6Z82LYHUVKJxFtCK-7noVF1YfsU6kxZiRkpCpRj2u5IsTNd2_-rVy0) is as follows:

![](https://img.plantuml.biz/plantuml/png/VPFBRjim44Nt-eg1LmtW2EWRqAA82oT0uJGfQiDD2creQbopfaYLGyPnlxv3wc4B0RgIH7BkdNiukY8ld0-rpXf-AgaQOJnCbbvu0dimw2OW2ExZs_Z8lT-XWvMQVmPZAwJIs2tm6kpnI8sGc0hZ_hXk_d0397TO1Ts9B_klbmVn6MNmrfNcbTI2ICa2nCbqMXg3p_xo5vguJkYUbCIqTP6ahxlwh19UR0GXJ5PgiUvQMwopB2M3oqzjn09Uv_3DDYpsssvWwsoTCZLEMQVy4Vu439ZPniDTPynvdK4DrkXZ1Z3enIOBU9F3ZGwqIogKg5QeFTjtjBWjZLoNCgMonRg07so6BseoDDMfuG5R0MzpM8PDhJm80pCEF3D27_z6TTepkoY2DHe_1NRvCf-sjlnsaGqoKRFdLS0xvX0yg3W6d0meRA8OVwIs8kBI79HlastnWEGX45P0sdhw7wBUP6puFeUlOeynWJ9RcsQa5X5JyXzObRpf1ZGf5LyyZuOrF0x2L50dcOxSZbSuQL_J3sT2UTv8o_r33jy38xoRtJx-l2x0eQZEhQCyYQVsQWmUaeaY7bewKnjVpYgssj6Z82LYHUVKJxFtCK-7noVF1YfsU6kxZiRkpCpRj2u5IsTNd2_-rVy0)

1. The worker on the stats owner takes a job from the priority queue and makes its `ANALYZE` statement.
2. On the next-gen kernel, the stats owner flushes the stats deltas of the table on the TiDB nodes of its keyspace.
3. The stats owner submits an `AutoAnalyze` task for its keyspace. The task contains the statement and the IDs of the table and its database.
4. The DXF scheduler makes one subtask, and the framework puts it on the node with the fewest used slots.
5. On the next-gen kernel, the task executor first makes an info schema that contains only the table, for the statement session. Then it makes a stats handle for that keyspace.
6. The task executor runs the statement. The statement analyzes the table and saves the new statistics.
7. The task succeeds, and the stats owner loads the new statistics into its cache. The other TiDB nodes load the new statistics at their next stats update, as after a local `ANALYZE`.

**On the classic kernel, steps 2 and 5 do not occur, and the statement runs on a TiDB node of the same cluster.**

#### Dispatch on the Stats Owner

A new `GLOBAL` variable, `tidb_enable_dist_auto_analyze`, controls this feature. `ON` is the default in new clusters. With `ON`, the stats owner runs each auto-analyze statement as a DXF task. With `OFF`, it runs all statements itself, as before. **The variable has no effect on manual `ANALYZE`**.

In the pseudocode, `runAutoAnalyze` is the new code in `exec.AutoAnalyze`. The part that submits the task and waits for it is the dispatcher. A kill of the dispatcher ends `runAutoAnalyze` with an error.

```go
runAutoAnalyze(statement, table):
    if tidb_enable_dist_auto_analyze is OFF:
        run the statement on the stats owner          // as before
        return

    track this session as an auto-analyze process     // 1
    flush the stats deltas of the table               // 2, next-gen kernel only

    // an earlier stats owner can leave a task for this statement. Wait for it, so
    // that the statement does not run two times. Find it by task key and statement.
    task = the AutoAnalyze task with the same statement that waits or runs    // 3
    if there is such a task:
        wait(task)
        if the task succeeded:
            load the new statistics of the table      // 5
            return

    task = submit(statement)                          // 4
    wait(task)
    load the new statistics of the table              // 5

wait(task):
    wait until the task is complete or a control kills this session
    if killed and this node shuts down:
        let the task continue
    else if killed:
        cancel the task, and wait until it stops
```

The numbers refer to these notes:

1. The dispatcher tracks itself as an auto-analyze process of the stats owner, and it shows the statement as its current query. Thus the controls in [Timeout and Cancellation](#timeout-and-cancellation) can kill it.
2. On the next-gen kernel, the dispatcher runs `FLUSH STATS_DELTA db.tbl CLUSTER`. `ANALYZE` must flush the deltas of the table on all TiDB nodes before it reads the row count. If not, TiDB can apply an old delta later and count the same rows two times. **A service node cannot connect to the TiDB nodes of a user keyspace, so the stats owner does the flush.**
3. A task with the same statement that waits or runs comes from an earlier stats owner. Refer to [Change of the Stats Owner](#change-of-the-stats-owner).
4. The key of the task is `<keyspace>/AutoAnalyze/<table ID>/<UUID>`. The table ID lets a dispatcher find the tasks of a table. The UUID is necessary because DXF does not accept the key of an earlier task.
5. After the task succeeds, the dispatcher loads the new statistics of the table into the cache of the stats owner. **The priority queue must have this step.** The queue removes a finished job because it expects the new statistics in the cache. If they are not in the cache, the queue can use the old modify count and analyze the table again.

If the flush fails or DXF is not ready, the dispatch fails, and the queue analyzes the table again later.

#### Task, Scheduler, and Executor

Each `AutoAnalyze` task has one step and one subtask.

- **Target scope:** on the classic kernel, the target scope of the task is always empty. Other task types use the service scope of the node that submits them. For auto-analyze, this node is the stats owner, which TiDB elects, and its service scope must not select the nodes of the tasks. On the next-gen kernel, the target scope is `dxf_service`, as for all task types.
- **Scheduler:** makes the subtask and orders the nodes of the target scope. The framework puts the subtask on the first node. The scheduler does not retry an error, because the priority queue analyzes the table again later.
- **Task executor:** runs the statement. The subtask is idempotent, because a second `ANALYZE` only writes the statistics again. Thus DXF can run the subtask again on a different node if its node stops.

On the classic kernel, all nodes run tasks by default, also the stats owner and the nodes that serve queries. This design adds no new setting to change this, because the service scope `background` already does it: if some nodes have this scope, the tasks run only on them. Other DXF tasks with an empty target scope also move to these nodes. Use two or more such nodes, because one node gets all the statements.

The `AutoAnalyze` scheduler orders the nodes by their used slots. Tasks that start at the same time read the same used slots, and all of them would select the same node. Thus the scheduler also counts the tasks that it put on a node and that the used slots do not show yet:

```text
placed = {}    // task -> node, for the tasks that have no subtask in the used slots yet

orderNodes(task):
    nodes = the nodes of the target scope
    used  = the used slots of each node, from the subtasks that wait or run
    for each (other task, node) in placed:
        used[node] += 1                   // each task asks for 1 slot
    sort nodes by used, ascending         // equal nodes rotate by the task ID
    placed[task] = nodes[0]
    return nodes                          // the framework uses nodes[0]
```

#### Concurrency and Slots

`tidb_auto_analyze_concurrency` still limits the number of jobs that the stats owner runs at the same time. **A job that uses DXF also counts, because it ends only when its task ends.** Thus the variable limits the number of `ANALYZE` statements that run at the same time on all nodes.

**The DXF task limit and the free slots of the target nodes also limit the tasks that run at the same time.** If `tidb_auto_analyze_concurrency` is larger than these limits, the extra jobs only wait for DXF, and the throughput does not increase.

**Each task asks for 1 slot.** Most of the work of `ANALYZE`, the scan and the sampling, runs in TiKV. More slots for each task would let fewer tasks run on a node.

#### Timeout and Cancellation

The table shows what each event does to a task:

| Event | Effect on the task |
| --- | --- |
| `tidb_max_auto_analyze_time`, the end of the auto-analyze time window, or `KILL` on the stats owner | The dispatcher cancels the task, and DXF stops the statement. A task that still waits for slots does not run. |
| Shutdown of the stats owner | The dispatcher stops. A task on a different node continues, and the new stats owner waits for it. |
| Crash of the stats owner | The task continues. The new stats owner waits for it. |
| Stop of the node that runs the statement | DXF can run the subtask again on a different node. If the shutdown kills the statement first, the task fails. |
| The statement or the task fails | The priority queue analyzes the table again later. |

`tidb_max_auto_analyze_time` measures the time from the start of the dispatch. Thus it includes the time that the task waits for slots.

#### Change of the Stats Owner

A shutdown of the stats owner also kills the dispatcher. But the task can run on a different node, and the shutdown must not stop it. Thus **when its node shuts down, the dispatcher stops and does not cancel the task**. The task continues on its node.

The new stats owner continues such a task. Before the dispatcher submits a statement, it looks for a task with the same statement that waits or runs. It finds the tasks of the table by the table ID in the task key, and compares their statements. If it finds one, it waits for that task, with the same controls as for its own task:

- If the task succeeds, the dispatcher loads the new statistics, and the job is complete.
- If the task fails, the dispatcher submits its statement.

Thus the new stats owner does not run the same statement a second time, also after a crash of the old stats owner. The dispatcher does not wait for a task with a different statement.

The [diagram](https://editor.plantuml.com/uml/PL91QiCm4Bph5Klk-K309PJGKsYXDRI7N1HfWqLioNYb-FiToN4JqPEatTcPsR7t4lKKqz2h4QSpRjG-qkQZjuHE50gpvsb3MWYr6lJEypyGQZNeyFDM6ZZhHkiEh-I3POgTZYS_9I-uugdbkd2XgLIsidt9yplwIkV1nOAWbYvX8bqSyC2W31TqpZqO5D1vA3Fiw3FvGYpfN2Q1QvKsqTqmkQWq3RL0X7fjaYxXRl6WfbcTuDWLj7KowcYw38nXb9FNtWBDWhqY6UqDzv320zGtm1O_BPdWe_E9PHNsTPGqw2ilQOWU-6b7sCYnhu4SGxZUSrXNkqT0iulT7yF9FoXot3HhbyKcoh4lSUMyU8QzacubymrqaK4vVx7jscYVOfLa3BCLPRaEjsP_uEuAfNmCsXQtQn9EyAk8shEt-U_y1G00) shows a shutdown of the stats owner while task T runs on a different node:

![](https://img.plantuml.biz/plantuml/png/PL91QiCm4Bph5Klk-K309PJGKsYXDRI7N1HfWqLioNYb-FiToN4JqPEatTcPsR7t4lKKqz2h4QSpRjG-qkQZjuHE50gpvsb3MWYr6lJEypyGQZNeyFDM6ZZhHkiEh-I3POgTZYS_9I-uugdbkd2XgLIsidt9yplwIkV1nOAWbYvX8bqSyC2W31TqpZqO5D1vA3Fiw3FvGYpfN2Q1QvKsqTqmkQWq3RL0X7fjaYxXRl6WfbcTuDWLj7KowcYw38nXb9FNtWBDWhqY6UqDzv320zGtm1O_BPdWe_E9PHNsTPGqw2ilQOWU-6b7sCYnhu4SGxZUSrXNkqT0iulT7yF9FoXot3HhbyKcoh4lSUMyU8QzacubymrqaK4vVx7jscYVOfLa3BCLPRaEjsP_uEuAfNmCsXQtQn9EyAk8shEt-U_y1G00)

### Run on a DXF Service Node

On the classic kernel, the subtask runs on a normal TiDB node of the same cluster. That node has all the parts that `ANALYZE` uses. Thus the statement runs there in the same way as on the stats owner.

On the next-gen kernel, the subtask runs on a service node of the `SYSTEM` keyspace. The domain of this node belongs to the `SYSTEM` keyspace, not to the keyspace of the table. The node can get to a user keyspace only through a [cross-keyspace runtime](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/domain/sqlsvrapi/server.go#L63-L66). The runtime has a store, a session pool, and an info schema of that keyspace. But it does not have all the parts that `ANALYZE` uses:

| `ANALYZE` uses | Normal TiDB node | Runtime on a service node | What this design does |
| --- | --- | --- | --- |
| The table in the info schema | All tables | [Only the system tables](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/infoschema/issyncer/loader.go#L422-L431) | [Makes an info schema of only this table for the statement](#info-schema-of-the-analyzed-table) |
| A stats handle | The handle of the domain | None, because the sessions have no domain | [Makes a stats handle without a domain](#stats-handle-without-a-domain) |

The [diagram](https://editor.plantuml.com/uml/VLHBRzf04BxxLup8DKZ5RIySghWonP5Q3XX5JUiU5dl0AzkpQ7S9Sglyz-w3XuqJk7c-dVaUC-91QIRrlga3LN7QCSaQo4Mp4uIaKztM219ppMXRO-S9YG8Lxk3ZXqvHCgfiyTFd8B0FOBGGic4rh7ZyrRMCW2bOmBy0BYGmYXRHz-TVCmT64uET6M4KYuPn0h41NI9KsAeTo_sSUE86zKOzqaQ0oajis3GZLjUWsRf6vHeUqydr-rGphQ1aLDHe6d9cMi5yPRHcURMLOay578IiKFePQJAOCQ_5sfXyOP8xhepUKZj_ieslmUifc_ZdDr0eNtYEbspIczc4mNjcJm6PRMfi36hC2AfRLsGcYFOl5ZwB-umaiW8sKZGERb2pOpvN195lJ4xna02NqBqHV4p52qlwIrlkIVC6JwBsncO_ZELgkBeKbUA2b8qVUCUQepJigbN6st6XODtvMQdBzUPAfbUUCnfgcZ-5Dqule_GvNSr-d7jS7DfwA_mrXd0tSp-xucW2uq8SQFp5NXrClUfksHoIATjzSQLvhhgOkHMBeTQSjYgm4yQ_IruKI7yatvRQzmTsnW2mXnP4OOS_lF1Vob5oHP-SeMLg89kNtyL1ScslkfT_TvH9hYyt7Aeo9s8PXcB3mBOFWDZMtt1dR_G1gR3_MFy1) shows where `ANALYZE` gets these parts on a normal TiDB node and on a service node:

![](https://img.plantuml.biz/plantuml/png/VLHBRzf04BxxLup8DKZ5RIySghWonP5Q3XX5JUiU5dl0AzkpQ7S9Sglyz-w3XuqJk7c-dVaUC-91QIRrlga3LN7QCSaQo4Mp4uIaKztM219ppMXRO-S9YG8Lxk3ZXqvHCgfiyTFd8B0FOBGGic4rh7ZyrRMCW2bOmBy0BYGmYXRHz-TVCmT64uET6M4KYuPn0h41NI9KsAeTo_sSUE86zKOzqaQ0oajis3GZLjUWsRf6vHeUqydr-rGphQ1aLDHe6d9cMi5yPRHcURMLOay578IiKFePQJAOCQ_5sfXyOP8xhepUKZj_ieslmUifc_ZdDr0eNtYEbspIczc4mNjcJm6PRMfi36hC2AfRLsGcYFOl5ZwB-umaiW8sKZGERb2pOpvN195lJ4xna02NqBqHV4p52qlwIrlkIVC6JwBsncO_ZELgkBeKbUA2b8qVUCUQepJigbN6st6XODtvMQdBzUPAfbUUCnfgcZ-5Dqule_GvNSr-d7jS7DfwA_mrXd0tSp-xucW2uq8SQFp5NXrClUfksHoIATjzSQLvhhgOkHMBeTQSjYgm4yQ_IruKI7yatvRQzmTsnW2mXnP4OOS_lF1Vob5oHP-SeMLg89kNtyL1ScslkfT_TvH9hYyt7Aeo9s8PXcB3mBOFWDZMtt1dR_G1gR3_MFy1)

#### Info Schema of the Analyzed Table

On a service node, the info schema of the runtime has only the system tables. **Thus the task executor makes an info schema that contains only the analyzed table, and only the statement session uses it.** `IMPORT INTO ... FROM SELECT` already runs its query on a service node [in this way](https://github.com/pingcap/tidb/blob/d05c6d9de10bc9a77f8d4923a1ab9f7cba642383/pkg/executor/importer/query_source.go#L109-L148), and this design uses the same code to make the info schema.

```go
// The task executor on a service node, in a session of the runtime:
read the table definition by its ID from the meta of the keyspace
if the table does not exist {
    return an error // the priority queue analyzes the table again later
}
give an info schema with only this table to this session
run the statement in this session
```

- The statement uses the version of the table from its start, as `ANALYZE` does on a normal TiDB node. Each statement has its own info schema, so the tasks share no state.
- The other sessions of the statement use only system tables, and the info schema of the runtime has them. **Thus the code that `ANALYZE` runs in another session must not look up the analyzed table.**

On a normal TiDB node, the statement uses the info schema of the domain, as before.

#### Stats Handle Without a Domain

A full stats handle keeps the statistics cache of a keyspace and runs background workers. `ANALYZE` does not use these parts. Thus **`ANALYZE` uses only a new narrow interface, `AnalyzeStatsHandle`**. The handle of the domain implements this interface, so a normal TiDB node does not change.

On a service node, the task executor makes an `AnalyzeHandle`, a new type, for the user keyspace and binds it to the statement session. This handle uses the store of the keyspace through the sessions of the runtime. It has no cache and no background worker:

- It reads the statistics of a table from the store. Thus the sample rate comes from the row count of the user keyspace.
- It does not load the new statistics into a cache. The stats owner does this after the task.

### Compatibility and Rollout

`tidb_enable_dist_auto_analyze` is `ON` in new clusters and `OFF` in upgraded clusters. Thus an upgrade does not start the feature. An old stats owner does not know the variable and runs all statements locally.

**In an upgraded cluster, set the variable to `ON` only after you upgrade all nodes**, also the DXF service nodes. An old node does not know the task type and fails the task. Then the statement does not run, and the priority queue analyzes the table again later.

On the next-gen kernel, a new user keyspace also starts with `ON`. A new user keyspace starts only after the `SYSTEM` keyspace has the new version. While some service nodes are old, a task can fail on them, and the priority queue analyzes the table again later.

## Test Design

### Functional Tests

- **All statements:** the statements of a small table, of some partitions of a partitioned table, and of a table without statistics run as tasks.
- **Dispatch:** a statement runs as a task, and TiDB records it as an auto-analyze job. When the dispatcher returns, the cache of the stats owner contains the new statistics. On the next-gen kernel, the stats owner flushes the deltas of the table before the task. The row count is the same as with a local `ANALYZE`.
- **Cancellation:** a kill of the dispatcher cancels the task and stops the statement, also during the merge of global statistics. The end of the auto-analyze time window also cancels a task that waits or runs. A shutdown of the stats owner does not cancel the task. The process that waits holds no transaction start timestamp.
- **Slots:** each task asks for 1 slot, and the statement uses `tidb_auto_build_stats_concurrency` as on the stats owner.
- **Placement:** tasks that start at the same time go to different nodes. With a service scope on the stats owner, the target scope of the task is empty.
- **Change of the stats owner:** a dispatcher waits for a task with the same statement that waits or runs, and submits no second task. It does not wait for a task with a different statement.
- **Info schema of the table:** the statement finds the analyzed table and no other user table. The task fails when the table does not exist.
- **Service node:** with a `SYSTEM` domain and a runtime of a user keyspace, `ANALYZE` writes statistics only into the user keyspace. The result is the same as in the user keyspace for plain, partitioned, and cached tables, for a partitioned table with a global index, for generated columns and expression indexes, for statements that analyze one index, and after a schema change. The test also uses the sample rate and the analyze settings of the user keyspace. On one service node, tables with the same ID in two user keyspaces get their own sample rates.

### Scenario Tests

- **Classic:** a cluster with three TiDB nodes analyzes groups of small and large tables. The groups contain partitioned tables. All statements use DXF, and their tasks run on all three nodes. The number of tasks that run at the same time is not more than `tidb_auto_analyze_concurrency`.
- **Next-gen:** a cluster with one service node and two TiDB nodes in a user keyspace analyzes groups of tables on the service node. The groups contain partitioned tables and a table after a schema change.
- **Cancellation:** with a low `tidb_max_auto_analyze_time`, TiDB cancels tasks while they wait for slots and while their statements run. TiDB marks the jobs of the statements that run as failed, and analyzes the tables again later.
- **Failover:** a DXF node stops while it runs a subtask. DXF runs the subtask again on a different node, or the priority queue analyzes the table again later, and the statistics are correct.
- **Change of the stats owner:** the stats owner stops while its tasks run on other nodes. The tasks continue, the new stats owner waits for them, and TiDB analyzes no table two times.

### Compatibility Tests

- **Variable `OFF`:** the stats owner runs all statements locally, as before.
- **Manual `ANALYZE`:** with the variable `ON`, a manual `ANALYZE` runs locally.
- **Upgrade:** a new cluster starts with the variable `ON`, and an upgraded cluster starts with `OFF`. If the variable is `ON` before the upgrade of an old node, the task fails, and TiDB analyzes the table again later.

### Benchmark Tests

Compare the duration of auto-analyze for a cluster with many tables, with the variable `OFF` and with the variable `ON`. Use different values of `tidb_auto_analyze_concurrency`, different numbers of DXF nodes, different DXF task limits, and small and large tables. Also use a high rate of small tables that need analysis, with and without a backlog of large tables. Record the fixed overhead of each statement, the delay from a change to new statistics, the CPU and memory of the stats owner and the DXF nodes, and the effect on foreground queries. Use the result to find how `tidb_auto_analyze_concurrency` must relate to the DXF task limit and to the slots of the DXF nodes.

## Impacts & Risks

### Impacts

- Auto-analyze statements do not run only on the stats owner, so the stats owner has less load. Their throughput increases with the number of DXF nodes only if two or more nodes run tasks, `tidb_auto_analyze_concurrency` gives work to all of them, and the DXF task limit is not smaller than the slots of these nodes.
- On the next-gen kernel, all auto-analyze statements move from the TiDB nodes of the user keyspace to the service nodes.
- On the classic kernel, the stats owner also runs tasks, unless some nodes have the service scope `background`.
- Each task has a fixed cost. For a small table, this cost can be larger than the work of the statement. When the priority queue has a backlog, the wait in the queue is much longer than this cost.

### Risks

- By default, a maximum of 16 DXF tasks run at the same time. Other task types, such as `ADD INDEX` and `IMPORT INTO`, use the same limit. On the next-gen kernel, all user keyspaces also share it. While a task waits, its job holds a place in `tidb_auto_analyze_concurrency`, and smaller tables can wait behind it.
- Each task adds a row to the DXF task history, and a cluster with many small tables makes many tasks. TiDB removes the subtask history after 14 days, but it keeps the task history.
- The tasks of small tables use slots and places in the DXF task limit. When many small tables need analysis at the same time, fewer slots are free for the tasks of large tables.
- On the next-gen kernel, the stats owner flushes the stats deltas before it submits the task, and the task can then wait for a node. TiDB can count the changes of this wait time two times, until the next `ANALYZE` of the table.
- On a service node, `tidb_mem_quota_analyze` and `tidb_max_auto_analyze_time` of the `SYSTEM` keyspace apply to the statements of all keyspaces.
## Investigation & Alternatives

- **A row limit:** the stats owner runs the statements of small tables itself, and sends only the statements of large tables to DXF, because each task has a fixed cost. But when many small tables need analysis, all their statements run on one node. They take CPU from the queries on that node, they run slower than on DXF, and they hold places in `tidb_auto_analyze_concurrency`. Thus this design sends all statements to DXF.
- **Poll a system table:** the stats owner writes the statements into a table, and the other nodes poll the table. This method must have its own scheduler, failover, and resource control, and DXF has these functions. On the next-gen kernel, only the TiDB nodes of the user keyspace can read its table. Thus the work cannot move to the service nodes.
- **A generic SQL task:** the [first prototype](https://github.com/pingcap/tidb/pull/60969) put any SQL statement into a task. A statement alone does not tell the framework which tables to load or how many slots it uses. Thus this design uses a task type only for auto-analyze. It keeps the parts that other tasks can use in the info schema and the session layer.
- **Load the info schema of all of the user keyspace:** this is slow and uses much memory on a node that serves many keyspaces. Also, a task can then see tables that it does not use. A [prototype](https://github.com/pingcap/tidb/pull/65618) that loads any table on demand has the same problems. Thus this design reads only the table of the task.
- **A list of user tables in the info schema cache of the runtime:** the task executor adds the table to a list in the cache, and all sessions of the runtime find it. But all tasks share the list, and a table can have only one version in it. It also changes how all sessions of the runtime read the info schema. Thus this design gives the info schema only to the statement session.
- **Run more than one statement in one task:** this avoids the limit of 16 tasks. But a statement is the smallest unit of work. Also, unrelated tables in one task make the control of failures and cancellation less precise.

## Unresolved Questions

- What is the correct default of `tidb_auto_analyze_concurrency`, when the DXF task limit and the number of DXF nodes can change?
- Does auto-analyze need its own DXF task limit? Now it shares the limit of 16 tasks with other task types and, on the next-gen kernel, with all user keyspaces. DXF plans to increase this limit in steps.
- Do the process-level settings of the `SYSTEM` keyspace continue to limit the statements of all keyspaces on a service node, or does each keyspace get its own limits?
- How do we stop DXF from interrupting statements that run? On the next-gen kernel, the scheduler does not check the free slots, so a node can get more subtasks than slots. Then a task executor cancels a task of a lower rank, and for tasks of the same priority, the newer task has the lower rank. The balancer also moves running subtasks away from a node that does not have enough slots. The interrupted statement runs again from the start, and TiDB records a failed auto-analyze job for it. This wastes work, and it makes a DXF task limit above the slots expensive. A possible fix: a running subtask gives its slot only to a task of a higher priority, and the balancer moves only the subtasks that do not run.

## Future Possibility

TiDB can send manual `ANALYZE` statements to DXF in the same way.
