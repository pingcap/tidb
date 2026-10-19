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
        * [Row Limit](#row-limit)
        * [Dispatch on the Stats Owner](#dispatch-on-the-stats-owner)
        * [Task, Scheduler, and Executor](#task-scheduler-and-executor)
        * [Concurrency and Slots](#concurrency-and-slots)
        * [Timeout and Cancellation](#timeout-and-cancellation)
        * [Change of the Stats Owner](#change-of-the-stats-owner)
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

This document shows a design that runs auto-analyze statements on the Distributed eXecution Framework (DXF). Today, only the stats owner runs these statements. With this design, the stats owner continues to select the tables and to make the statements. It submits the statement of a large table to DXF as a task, and DXF runs the task on one of its nodes. The stats owner continues to run the statements of small tables itself.

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

For example, a task with 4 slots, an empty target scope, and two subtasks runs on these nodes:

| Node | Scope | Free slots | Runs a subtask |
| --- | --- | --- | --- |
| A | `background` | 6 | Yes |
| B | `background` | 4 | Yes |
| C | `background` | 1 | No. The task needs 4 slots. |
| D | Empty | 8 | No. Other nodes have the scope `background`. |

On the next-gen kernel, the service nodes of the `SYSTEM` keyspace run the DXF tasks of all user keyspaces.

### Run Auto Analyze Statements on DXF

This design changes only the node that runs the statement. **Each statement of a table with 1,000,000 rows or more becomes a DXF task of a new type, `AutoAnalyze`.** The stats owner runs the statements of smaller tables itself. Refer to [Row Limit](#row-limit).

#### Workflow

The [workflow](https://editor.plantuml.com/uml/XLJBRjim4BphAnO-DO0ZE7q2EXHnu0G8OYH5RQGF-49BwverHQfSCexpzLrI3vlDeGU_oDsPsPcLBya9wtojieO_PIaReHsC5auu0hFNQ4SW21x2jxJamMtHmbBEha2R2ccbpGRu37Pue4QK61l3VTet-tu3L6wnyge3N_I_NXVYCvRU6RlIRqebY6HPW3YIZbTQuxCx_uKwd1FQ9rbYl3gBqDTTVrO99zQ244PBUVVOIRjxpB8u6Pn_RaSiu2A7BwPXiDzc3HjhwZXJOwMnqXtWZqUFcL0EbdpCc0fXos0Rt3ECsmeD5-F9P19UOCsUCe218Z0MyDLhcDvFvpz-NeFH4PwETe-3j3LkZCKC5GjEUL9mC1RgK0bW6WVtdHSSWTMe65qT-2fHCBigu6qEDyhJzeIxGkKOleimyiNEQFKW_HjxWA-wk11rTJhea6W1xt9O-7Kj7R0rKyvYgeKwl0GEsd6i0GXhr6uCp7Gx6tV3tixE67402gXzc0Myvv2y0nbCunc1_3h0zNQ6_5TwBrqxOSY677Z22aWPH_-pgoTAIJ_aC9UCCnW4igMJUcFYIjUYDz1wxMJD8eIke1Ox3DiPBUCDgRYLyqqUbi73K-xMz8_vwSeKO55KHn4j1OcdDXUD-qZ4GcL92LwQIrYhtZRoPObOKMhuyPdub7TRcDR-izTpq-bADR0zpiJpKZ1u5iQyvBVmd_CN) is as follows:

![](https://img.plantuml.biz/plantuml/png/XLJBRjim4BphAnO-DO0ZE7q2EXHnu0G8OYH5RQGF-49BwverHQfSCexpzLrI3vlDeGU_oDsPsPcLBya9wtojieO_PIaReHsC5auu0hFNQ4SW21x2jxJamMtHmbBEha2R2ccbpGRu37Pue4QK61l3VTet-tu3L6wnyge3N_I_NXVYCvRU6RlIRqebY6HPW3YIZbTQuxCx_uKwd1FQ9rbYl3gBqDTTVrO99zQ244PBUVVOIRjxpB8u6Pn_RaSiu2A7BwPXiDzc3HjhwZXJOwMnqXtWZqUFcL0EbdpCc0fXos0Rt3ECsmeD5-F9P19UOCsUCe218Z0MyDLhcDvFvpz-NeFH4PwETe-3j3LkZCKC5GjEUL9mC1RgK0bW6WVtdHSSWTMe65qT-2fHCBigu6qEDyhJzeIxGkKOleimyiNEQFKW_HjxWA-wk11rTJhea6W1xt9O-7Kj7R0rKyvYgeKwl0GEsd6i0GXhr6uCp7Gx6tV3tixE67402gXzc0Myvv2y0nbCunc1_3h0zNQ6_5TwBrqxOSY677Z22aWPH_-pgoTAIJ_aC9UCCnW4igMJUcFYIjUYDz1wxMJD8eIke1Ox3DiPBUCDgRYLyqqUbi73K-xMz8_vwSeKO55KHn4j1OcdDXUD-qZ4GcL92LwQIrYhtZRoPObOKMhuyPdub7TRcDR-izTpq-bADR0zpiJpKZ1u5iQyvBVmd_CN)

1. The worker on the stats owner takes a job from the priority queue and makes its `ANALYZE` statement. The steps below are for a table at or above the [row limit](#row-limit).
2. On the next-gen kernel, the stats owner flushes the stats deltas of the table on the TiDB nodes of its keyspace.
3. The stats owner submits an `AutoAnalyze` task. The task contains the statement and the IDs of the table and its database.
4. The DXF scheduler makes one subtask, and the framework puts it on the node with the fewest used slots.
5. On the next-gen kernel, the task executor first lists the table in the info schema of the user keyspace. Then it makes a stats handle for that keyspace.
6. The task executor runs the statement. The statement analyzes the table and saves the new statistics.
7. The task succeeds, and the stats owner loads the new statistics into its cache. The other TiDB nodes load the new statistics at their next stats update, as after a local `ANALYZE`.

On the classic kernel, steps 2 and 5 do not occur, and the statement runs on a TiDB node of the same cluster.

#### Row Limit

A DXF task has a fixed cost. The stats owner submits the task, the framework schedules it, and the stats owner reads its result. For a small table, this cost is larger than the work of the statement. Thus **the stats owner sends a statement to DXF only if its table has 1,000,000 rows or more**.

- The limit is a constant, and it is low. Thus the large tables, which take most of the time of auto-analyze, use DXF.
- The row count comes from the statistics cache of the stats owner. For a partitioned table, it is the row count of all partitions.
- If the stats owner has no statistics of the table, it runs the statement itself.

#### Dispatch on the Stats Owner

A new `GLOBAL` variable, `tidb_enable_dist_auto_analyze`, controls this feature. `ON` is the default in new clusters. With `ON`, a hook in `exec.AutoAnalyze` sends each statement of a table at or above the row limit to a dispatcher. With `OFF`, the stats owner runs all statements as before. **The variable has no effect on manual `ANALYZE`**.

The task belongs to the keyspace of the stats owner.

The pseudocode shows the hook and the dispatcher. A kill of the dispatcher ends `runAutoAnalyze` with an error.

```text
runAutoAnalyze(statement, table):
    if tidb_enable_dist_auto_analyze is OFF or rows(table) < 1,000,000:
        run the statement on the stats owner          // as before
        return

    track this session as an auto-analyze process     // 1
    flush the stats deltas of the table               // 2, next-gen kernel only

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
        cancel the task, and wait until it stops, 10 seconds at most
```

The numbers refer to these notes:

1. The dispatcher tracks itself as an auto-analyze process of the stats owner, and it shows the statement as its current query. Thus the controls of auto-analyze continue to operate. Refer to [Timeout and Cancellation](#timeout-and-cancellation).
2. On the next-gen kernel, the dispatcher runs `FLUSH STATS_DELTA db.tbl CLUSTER`. `ANALYZE` must flush the deltas of the table on all TiDB nodes before it reads the row count. If not, TiDB can apply an old delta later and count the same rows two times. A service node cannot connect to the TiDB nodes of a user keyspace, so the stats owner does the flush.
3. A task with the same statement that waits or runs comes from an earlier stats owner. Refer to [Change of the Stats Owner](#change-of-the-stats-owner).
4. The key of the task is `<keyspace>/AutoAnalyze/<table ID>/<UUID>`. The table ID lets a dispatcher find the tasks of a table. The UUID is necessary because DXF does not accept the key of an earlier task.
5. After the task succeeds, the dispatcher loads the new statistics of the table into the cache of the stats owner. **The priority queue must have this step.** The queue removes a finished job because it expects the new statistics in the cache. If they are not in the cache, the queue can use the old modify count and analyze the table again.

If the flush fails or DXF is not ready, the dispatch fails, and the queue analyzes the table again later.

#### Task, Scheduler, and Executor

Each `AutoAnalyze` task has one step and one subtask.

- **Target scope:** on the classic kernel, the target scope of the task is always empty. It does not follow the `tidb_service_scope` of the stats owner, so a scope on the stats owner does not move the tasks.
- **Scheduler:** makes the subtask and orders the nodes of the target scope. The framework puts the subtask on the first node. The scheduler does not retry an error, because the priority queue analyzes the table again later.
- **Task executor:** runs the statement. The subtask is idempotent, because a second `ANALYZE` only writes the statistics again. Thus DXF can run the subtask again on a different node if its node stops.

The scheduler orders the nodes by their used slots. Tasks that start at the same time read the same used slots, and all of them would select the same node. Thus the scheduler also counts the tasks that it put on a node and that the used slots do not show yet:

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

The scheduler removes a task from `placed` at its first tick after the subtask exists, and when the task or the scheduler stops.

#### Concurrency and Slots

`tidb_auto_analyze_concurrency` continues to limit auto-analyze for all the cluster. **A job counts against this limit while the dispatcher waits for the task**, and while the stats owner runs a statement itself. Thus the variable limits the number of statements that run at the same time on all nodes.

**Each task asks for 1 slot.** The scan and the sampling run in TiKV, and they are most of the work. The statement uses `tidb_auto_build_stats_concurrency` as on the stats owner: this variable sets the tables or partitions that the statement analyzes at the same time. More slots for a larger value would only decrease the number of tasks that a node runs.

By default, all TiDB nodes of a classic cluster run tasks, also the stats owner. To keep the tasks off the nodes that serve queries, set `tidb_service_scope` to `background` on the nodes that must run the tasks. Then DXF uses only those nodes. Use two nodes or more: with one node, all the statements of large tables run on that node.

#### Timeout and Cancellation

The table shows what each event does to a task:

| Event | Effect on the task |
| --- | --- |
| `tidb_max_auto_analyze_time`, the end of the auto-analyze time window, or `KILL` on the stats owner | The dispatcher cancels the task. |
| Shutdown of the stats owner | The dispatcher stops. A task on a different node continues, and the new stats owner waits for it. |
| Crash of the stats owner | The task continues. The new stats owner waits for it. |
| Stop of the node that runs the statement | DXF can run the subtask again on a different node. If the shutdown kills the statement first, the task fails. |
| The statement or the task fails | The priority queue analyzes the table again later. |

`tidb_max_auto_analyze_time`, the end of the auto-analyze time window, and `KILL` kill the auto-analyze processes of the stats owner. The dispatcher also tracks itself as an auto-analyze process. Thus **each of these controls kills the dispatcher, and the dispatcher cancels the task**:

1. The dispatcher cancels the task and tries again until DXF records the cancellation. If the task still waits for slots, no statement runs.
2. DXF cancels the context of the subtask on its node.
3. The task executor changes this cancellation into a kill of the statement. Some steps of `ANALYZE` examine only the SQL killer, for example the merge of global statistics.
4. The dispatcher waits a maximum of 10 seconds for the task to stop. Thus the dispatcher does not wait forever when the node cannot read the state of the task.

`tidb_max_auto_analyze_time` measures the time from the start of the dispatch. Thus it includes the time that the task waits for slots.

#### Change of the Stats Owner

A shutdown of the stats owner also kills the dispatcher. But the task can run on a different node, and the shutdown must not stop it. Thus **when its node shuts down, the dispatcher stops and does not cancel the task**. The task continues on its node.

The new stats owner continues such a task. Before the dispatcher submits a statement, it looks for a task with the same statement that waits or runs. It finds the tasks of the table by the table ID in the task key, and compares their statements. If it finds one, it waits for that task, with the same controls as for its own task:

- If the task succeeds, the dispatcher loads the new statistics, and the job is complete.
- If the task fails, the dispatcher submits its statement.

Thus the new stats owner does not run the same statement a second time, also after a crash of the old stats owner. The dispatcher does not wait for a task with a different statement. In static prune mode, each partition has its own job, and these jobs run at the same time.

The [diagram](https://editor.plantuml.com/uml/PL91QiCm4Bph5Klk-K309PJGKsYXDRI7N1HfWqLioNYb-FiToN4JqPEatTcPsR7t4lKKqz2h4QSpRjG-qkQZjuHE50gpvsb3MWYr6lJEypyGQZNeyFDM6ZZhHkiEh-I3POgTZYS_9I-uugdbkd2XgLIsidt9yplwIkV1nOAWbYvX8bqSyC2W31TqpZqO5D1vA3Fiw3FvGYpfN2Q1QvKsqTqmkQWq3RL0X7fjaYxXRl6WfbcTuDWLj7KowcYw38nXb9FNtWBDWhqY6UqDzv320zGtm1O_BPdWe_E9PHNsTPGqw2ilQOWU-6b7sCYnhu4SGxZUSrXNkqT0iulT7yF9FoXot3HhbyKcoh4lSUMyU8QzacubymrqaK4vVx7jscYVOfLa3BCLPRaEjsP_uEuAfNmCsXQtQn9EyAk8shEt-U_y1G00) shows a shutdown of the stats owner while task T runs on a different node:

![](https://img.plantuml.biz/plantuml/png/PL91QiCm4Bph5Klk-K309PJGKsYXDRI7N1HfWqLioNYb-FiToN4JqPEatTcPsR7t4lKKqz2h4QSpRjG-qkQZjuHE50gpvsb3MWYr6lJEypyGQZNeyFDM6ZZhHkiEh-I3POgTZYS_9I-uugdbkd2XgLIsidt9yplwIkV1nOAWbYvX8bqSyC2W31TqpZqO5D1vA3Fiw3FvGYpfN2Q1QvKsqTqmkQWq3RL0X7fjaYxXRl6WfbcTuDWLj7KowcYw38nXb9FNtWBDWhqY6UqDzv320zGtm1O_BPdWe_E9PHNsTPGqw2ilQOWU-6b7sCYnhu4SGxZUSrXNkqT0iulT7yF9FoXot3HhbyKcoh4lSUMyU8QzacubymrqaK4vVx7jscYVOfLa3BCLPRaEjsP_uEuAfNmCsXQtQn9EyAk8shEt-U_y1G00)

### Run on a DXF Service Node

On the classic kernel, the subtask runs on a normal TiDB node of the same cluster. That node has all the parts that `ANALYZE` uses. Thus the statement runs there in the same way as on the stats owner.

On the next-gen kernel, the subtask runs on a service node of the `SYSTEM` keyspace. The domain of this node belongs to the `SYSTEM` keyspace, not to the keyspace of the table. The node can get to a user keyspace only through a [cross-keyspace runtime](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/domain/sqlsvrapi/server.go#L63-L66). The runtime has a store, a session pool, and an info schema of that keyspace. But it does not have all the parts that `ANALYZE` uses:

| `ANALYZE` uses | Normal TiDB node | Runtime on a service node | What this design does |
| --- | --- | --- | --- |
| The table in the info schema | All tables | [Only the system tables](https://github.com/pingcap/tidb/blob/ab37692e9ebef44a736cd7243deef6020067b743/pkg/infoschema/issyncer/loader.go#L422-L431) | [Loads the table on demand](#load-the-analyzed-table-on-demand) |
| A stats handle | The handle of the domain | None, because the sessions have no domain | [Makes a stats handle without a domain](#stats-handle-without-a-domain) |
| The global variables of the keyspace | The values of its keyspace | The process copy holds the values of the `SYSTEM` keyspace | [Reads the settings from the user keyspace](#settings-of-the-user-keyspace) |

The [diagram](https://editor.plantuml.com/uml/VLH1Rjim4Bpp5Glts1YesVRYGn45Skq0hG-H4JIjSg35jKL8N1eadS0jylUIKXnBaM7TjCkPsHckTEsyi7wlQ-OgHJjXXOR2w9qX99_vGurWiV22jZLsZf2Hw707dpzrYbPG5OjVlZ8M3y1eQQmMDQnKUjD0HY0SBE4VWvC8Z99bykFXzwnf9fFGUrE4KMgqK0Hc0xv4gF3WTg9eUT99GzIZkgMD0LUKgCKq9r7NuCMwHjS0RhF9-_EP5zv1AKZM60252508RpcjHL5jhTcJX6TZAxGjHxOOSCnhimucduHLZLPEvwQTtqVW2tivPfF--WuExPCgy9HDTZ4Rti_cRazUQJo6imziVSsxrSM4eB4RRBk3ywXVeyhf8nWQot0WUE_5nni7_Vb2o8U_k3WcwD0vPSWTXxBjZ5SvaMdrheBRrqXRLmjsCSv0Qb58s5YZ8sLEGwFpUtvXRNBA7hBLxESRPXgdyJ4msMuIXm-pvedNaqnWBCqpZR_5ZO5fcq6t79OWbCENuvNpgd3TNhZds4Fl5MqTYmpZFwMI4kdHgctfMpoB78D6N1AMy8u-F-cVoidYdJploFE-F1-gXCG3mnbnpY9yq4XZ_SoqSI6kaMJyU_m7) shows where `ANALYZE` gets these parts on a normal TiDB node and on a service node:

![](https://img.plantuml.biz/plantuml/png/VLH1Rjim4Bpp5Glts1YesVRYGn45Skq0hG-H4JIjSg35jKL8N1eadS0jylUIKXnBaM7TjCkPsHckTEsyi7wlQ-OgHJjXXOR2w9qX99_vGurWiV22jZLsZf2Hw707dpzrYbPG5OjVlZ8M3y1eQQmMDQnKUjD0HY0SBE4VWvC8Z99bykFXzwnf9fFGUrE4KMgqK0Hc0xv4gF3WTg9eUT99GzIZkgMD0LUKgCKq9r7NuCMwHjS0RhF9-_EP5zv1AKZM60252508RpcjHL5jhTcJX6TZAxGjHxOOSCnhimucduHLZLPEvwQTtqVW2tivPfF--WuExPCgy9HDTZ4Rti_cRazUQJo6imziVSsxrSM4eB4RRBk3ywXVeyhf8nWQot0WUE_5nni7_Vb2o8U_k3WcwD0vPSWTXxBjZ5SvaMdrheBRrqXRLmjsCSv0Qb58s5YZ8sLEGwFpUtvXRNBA7hBLxESRPXgdyJ4msMuIXm-pvedNaqnWBCqpZR_5ZO5fcq6t79OWbCENuvNpgd3TNhZds4Fl5MqTYmpZFwMI4kdHgctfMpoB78D6N1AMy8u-F-cVoidYdJploFE-F1-gXCG3mnbnpY9yq4XZ_SoqSI6kaMJyU_m7)

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

- **Row limit:** the stats owner runs the statement of a table below the row limit, or of a table without statistics, and submits no task.
- **Dispatch:** a statement of a table at or above the row limit runs as a task, and TiDB records it as an auto-analyze job. When the dispatcher returns, the cache of the stats owner contains the new statistics. On the next-gen kernel, the stats owner flushes the deltas of the table before the task. The row count is the same as with a local `ANALYZE`.
- **Cancellation:** a kill of the dispatcher cancels the task and stops the statement, also during the merge of global statistics. The end of the auto-analyze time window also cancels a task that waits or runs. A shutdown of the stats owner does not cancel the task. The process that waits holds no transaction start timestamp.
- **Slots:** each task asks for 1 slot, and the statement uses `tidb_auto_build_stats_concurrency` as on the stats owner.
- **Placement:** tasks that start at the same time go to different nodes. With a service scope on the stats owner, the target scope of the task is empty.
- **Change of the stats owner:** a dispatcher waits for a task with the same statement that waits or runs, and submits no second task. It does not wait for a task with a different statement.
- **On-demand tables:** TiDB finds a listed table by name, by table ID, and by partition ID, and finds no other user table. A listed table keeps its version until all holders release it. A second `AllowUserTable` call fails when the listed version is not the version in the meta.
- **Service node:** with a `SYSTEM` domain and a runtime of a user keyspace, `ANALYZE` writes statistics only into the user keyspace. The result is the same as in the user keyspace for plain, partitioned, and cached tables, and after a schema change. The test also uses the sample rate and the analyze settings of the user keyspace. On one service node, tables with the same ID in two user keyspaces get their own sample rates.

### Scenario Tests

- **Classic:** a cluster with three TiDB nodes analyzes groups of tables below and above the row limit. The groups contain partitioned tables. Only the tables at or above the limit use DXF, and their tasks run on all three nodes. The number of tasks that run at the same time is not more than `tidb_auto_analyze_concurrency`.
- **Next-gen:** a cluster with one service node and two TiDB nodes in a user keyspace analyzes groups of tables on the service node. The groups contain partitioned tables and a table after a schema change.
- **Cancellation:** with a low `tidb_max_auto_analyze_time`, TiDB cancels tasks while they wait for slots and while their statements run. TiDB marks the jobs of the statements that run as failed, and analyzes the tables again later.
- **Failover:** a DXF node stops while it runs a subtask. DXF runs the subtask again on a different node, or the priority queue analyzes the table again later, and the statistics are correct.
- **Change of the stats owner:** the stats owner stops while its tasks run on other nodes. The tasks continue, the new stats owner waits for them, and TiDB analyzes no table two times.

### Compatibility Tests

- **Variable `OFF`:** the stats owner runs all statements locally, as before.
- **Manual `ANALYZE`:** with the variable `ON`, a manual `ANALYZE` runs locally.
- **Upgrade:** a new cluster starts with the variable `ON`, and an upgraded cluster starts with `OFF`. If the variable is `ON` before the upgrade of an old node, the task fails, and TiDB analyzes the table again later.

### Benchmark Tests

Compare the duration of auto-analyze for a cluster with many tables, with the variable `OFF` and with the variable `ON`. Use different values of `tidb_auto_analyze_concurrency`, different numbers of DXF nodes, and tables below and above the row limit. Record the fixed overhead of each statement, the CPU and memory of the stats owner and the DXF nodes, and the effect on foreground queries. Use the result to select the default of `tidb_auto_analyze_concurrency`.

## Impacts & Risks

### Impacts

- The statements of large tables do not run only on the stats owner, so the stats owner has less load. Their throughput increases with the number of DXF nodes only if two or more nodes run tasks and `tidb_auto_analyze_concurrency` gives work to all of them.
- On the next-gen kernel, the statements of large tables move from the TiDB nodes of the user keyspace to the service nodes.
- The statements of tables below the row limit continue to run on the stats owner, on the classic kernel and on the next-gen kernel.
- On the classic kernel, the stats owner also runs tasks, unless some nodes have the scope `background`.
- Each task has a fixed cost. The row limit keeps the small tables, for which this cost is too large, off DXF.

### Risks

- By default, a maximum of 16 DXF tasks run at the same time. Other task types, such as `ADD INDEX` and `IMPORT INTO`, use the same limit. On the next-gen kernel, all user keyspaces also share it. While a task waits, its job holds a place in `tidb_auto_analyze_concurrency`, and smaller tables can wait behind it.
- On the classic kernel, only the nodes with the empty scope or the scope `background` run the tasks. If all nodes have a different scope, a task waits until `tidb_max_auto_analyze_time` stops it.
- Each task adds a row to the DXF task history. TiDB removes the subtask history after 14 days, but it keeps the task history.
- A task asks for 1 slot, but its statement can use more than one CPU core when `tidb_auto_build_stats_concurrency` is large. The framework does not count this load when it puts other tasks on the node.
- After a change of the stats owner, a task has no dispatcher until the new stats owner selects its table again. During this time, the end of the time window and a `KILL` on the stats owner do not stop the task. `tidb_max_auto_analyze_time` continues to apply on the node that runs the statement.
- On the next-gen kernel, the stats owner flushes the stats deltas before it submits the task, and the task can then wait for a node. TiDB can count the changes of this wait time two times, until the next `ANALYZE` of the table.
- On a service node, `tidb_mem_quota_analyze` and `tidb_max_auto_analyze_time` of the `SYSTEM` keyspace apply to the statements of all keyspaces. The default numbers of TopN and buckets of the `SYSTEM` keyspace also decide if `ANALYZE` prunes TopN and buckets.

## Investigation & Alternatives

- **Poll a system table:** the stats owner writes the statements into a table, and the other nodes poll the table. This method must have its own scheduler, failover, and resource control, and DXF has these functions. On the next-gen kernel, only the TiDB nodes of the user keyspace can read its table. Thus the work cannot move to the service nodes.
- **A generic SQL task:** the [first prototype](https://github.com/pingcap/tidb/pull/60969) put any SQL statement into a task. A statement alone does not tell the framework which tables to load or how many slots it uses. Thus this design uses a task type only for auto-analyze. It keeps the parts that other tasks can use in the info schema and the session layer.
- **Load the info schema of all of the user keyspace:** this is slow and uses much memory on a node that serves many keyspaces. Also, a task can then see tables that it does not use. A [prototype](https://github.com/pingcap/tidb/pull/65618) that loads any table on demand has the same problems. Thus this design loads only the tables that a task lists.
- **Build a full stats handle on the service node:** the cache of this handle is empty at the start. Thus each statement must load the statistics of its table into a cache, and then discard the cache. Also, each statement must manage the background workers and the life cycle of the handle.
- **Load the database of the table through the schema syncer:** the syncer then keeps the table at the same version as on a normal node. But its filter selects databases, not tables, so it loads all the tables of the database. Also, each change of the list makes a full reload of the schema of the runtime necessary, and all cross-keyspace work uses this schema.
- **Send the statements of all tables to DXF:** the fixed cost of a task is larger than the work of a small table. The limit of 16 tasks then also applies to all tables. Thus small tables stay on the stats owner.
- **Use `tidb_auto_build_stats_concurrency` as the slots of a task:** a larger value then decreases the number of tasks that a node runs. But most of the work is in TiKV. Thus each task asks for 1 slot.
- **Run more than one statement in one task:** this avoids the limit of 16 tasks. But a statement is the smallest unit of work. Also, unrelated tables in one task make the control of failures and cancellation less precise.

## Unresolved Questions

- What is the correct default of `tidb_auto_analyze_concurrency` when statements run on many nodes?
- How must the limit of 16 concurrent DXF tasks change for the auto-analyze of many user keyspaces? DXF plans to increase it in steps.
- Must a new stats owner cancel the tasks of the old stats owner that it does not select again? An example is a task that continues after the end of the time window.
- Must the process-level settings of the `SYSTEM` keyspace continue to limit the statements of all keyspaces on a service node?

## Future Possibility

TiDB can send manual `ANALYZE` statements to DXF in the same way.
