# Integrate `ANALYZE` into Background Task Resource Control

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/69362
- Tracking Issue: https://github.com/pingcap/tidb/issues/69472

## Table of Contents

* [Introduction](#introduction)
* [Motivation](#motivation)
* [Detailed Design](#detailed-design)
    * [Current Implementation](#current-implementation)
    * [Introducing a New Task Type for Stats Maintenance](#introducing-a-new-task-type-for-stats-maintenance)
    * [Limit `ANALYZE` Resource Usage by Default](#limit-analyze-resource-usage-by-default)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)
* [FAQ](#faq)

## Introduction

This document proposes introducing a new internal task type for stats maintenance work other than `ANALYZE`, so that `ANALYZE` requests can be integrated into Resource Control separately. It also proposes enabling background throttling for `ANALYZE` by default without setting a fixed `UTILIZATION_LIMIT`.

## Motivation

Over the past year, we have repeatedly encountered issues where `ANALYZE` statistics collection consumes excessive resources, especially on the TiKV side. Across multiple user environments, we have observed that `ANALYZE` requests can affect foreground user queries, causing them to wait for a long time and eventually leading to slow queries.

Additionally, in the current TiDB Resource Control implementation, all statistics-related requests share the same `stats` task type. When users try to use Resource Control to limit the resources used by `ANALYZE`, all statistics-related requests are affected, including sync-load stats requests. This may prevent statistics from being loaded in time and can even increase query latency if a foreground query is blocked by statistics loading.

Therefore, this document introduces a new internal task type, `StatsForegroundPriority`, for stats maintenance work other than `ANALYZE`. Under this proposal, the existing `stats` task type continues to represent `ANALYZE` requests, including both automatic and manually triggered `ANALYZE`.

## Detailed Design

### Current Implementation

Before diving into the new design, let's first take a quick look at the current implementation and how background tasks are scheduled.

Resource Control is TiDB's mechanism for isolating and throttling workload resource usage through resource groups. Foreground SQL (user workload) is normally charged to and controlled by its assigned resource group. `BACKGROUND` is a setting on the `default` resource group that marks selected task types, such as `br`, `ddl`, and `stats`, as background work; see the [TiDB documentation source](https://github.com/pingcap/docs/blob/42da4252914248472710bc8f9d3bb0546015093e/tidb-resource-control-background-tasks.md#background-parameters). When TiKV receives a request, it matches the request source task type against `TASK_TYPES`, and matching requests are attached to the background limiter. All matched background tasks share one background limiter in TiKV.

TiKV currently adjusts the background limiter based on CPU pressure. The related configuration settings are:

- `bg-cpu-throttle-threshold`: the CPU utilization percentage where background throttling starts. The default value is `60.0`.
- `fg-cpu-throttle-threshold`: the CPU utilization percentage where background tasks are throttled to the minimum floor and foreground protection can start. The default value is `70.0`.
- `UTILIZATION_LIMIT`: an optional user-provided CPU utilization cap for task types matched by the `BACKGROUND` resource group setting. The default value is `0`, which means unset.

The first two settings are defined in the TiKV [Resource Control config](https://github.com/tikv/tikv/blob/f8cd7fabb5ec3c2bbf855aa6935e63daf726d84e/components/resource_control/src/config.rs#L15-L22). When `UTILIZATION_LIMIT` is set, TiKV still [caps it](https://github.com/tikv/tikv/blob/f8cd7fabb5ec3c2bbf855aa6935e63daf726d84e/components/resource_control/src/worker.rs#L187-L205) to `fg-cpu-throttle-threshold`, so a higher value cannot bypass foreground protection. When `UTILIZATION_LIMIT` remains `0`, TiKV treats it as unset and effectively uses `fg-cpu-throttle-threshold` as the target cap.

The background [Resource Control flow](https://editor.plantuml.com/uml/TLHTRzim37pNho3oqWvOjpvjG84CTJuMP4qRBt4sjCkGAZRjQ5KaLwBHfhz-jECaDeQzIVBnUEIHllOa7HLRBKYHHkZ9-2bpjZ09pD3RmiK8VMl8MGrVjNsv0WuWBuICoJfOU7GYPmOLgrmQWaWDUgs7SD2wTY9ry-D8FU9C-QqqCaFN0UbXDhfjmuExO7B_CAm-1aRgtHks0TyBInA2v4_X2NvQzrGpxOzjx7mZ7IRD6YTggwYEw8tgcn1bMN3nfQhc2e99D9p1R3YVBcRz8OncqqK8Zmccij3qk3Ize7zJFjuDkkTHCBzPHVVXSDuzgpfrzawKB2UsK3hFVJx6W39ab-QRNKUx77ttLwUAw_nUcWKfhTuaAMigQTrBE2-CHpfnXeChcBCJ2EjJLOmWPJ8lma69uQPosi9lm1qiBO4v57aY2GL_Fy9c65kNmAaCYDbzgOo7MXVBYiQJyKCGZQTtgyduTXmnxxQ2LEAEscg_M22ogDf9nZuNw6bfwiEgbjGj6u6EOrRx4Ql3Zz82_mGfpvpUkmJh7RGfMeKCNydnZ0993YFkx4b_wkpsFw776U1qH3BU77105dmrFalJw7IjHMXhSyZHAPkeM6gz4r1FOCcwFvNDbSd9WvnFld2uD9APghteXmkW3Rzlh4uyPygwdoxVGvfrFr-s61iwvvolvVwwI3AakapURhwKwbFMw9WIwGNHm3g3CcsQDH8AzgdPzaOM_WVuOekLmeymdi9pkNVYY82Bfk43r787AlrN5YWF-1eZ9fW8OImXE9Lj26pQMC10QzXAYZUyhGsAqak_jq5OqohwC1JAL9tyK9O29RsuvXF_t_u2) is as follows:

![](https://img.plantuml.biz/plantuml/png/TLHTRzim37pNho3oqWvOjpvjG84CTJuMP4qRBt4sjCkGAZRjQ5KaLwBHfhz-jECaDeQzIVBnUEIHllOa7HLRBKYHHkZ9-2bpjZ09pD3RmiK8VMl8MGrVjNsv0WuWBuICoJfOU7GYPmOLgrmQWaWDUgs7SD2wTY9ry-D8FU9C-QqqCaFN0UbXDhfjmuExO7B_CAm-1aRgtHks0TyBInA2v4_X2NvQzrGpxOzjx7mZ7IRD6YTggwYEw8tgcn1bMN3nfQhc2e99D9p1R3YVBcRz8OncqqK8Zmccij3qk3Ize7zJFjuDkkTHCBzPHVVXSDuzgpfrzawKB2UsK3hFVJx6W39ab-QRNKUx77ttLwUAw_nUcWKfhTuaAMigQTrBE2-CHpfnXeChcBCJ2EjJLOmWPJ8lma69uQPosi9lm1qiBO4v57aY2GL_Fy9c65kNmAaCYDbzgOo7MXVBYiQJyKCGZQTtgyduTXmnxxQ2LEAEscg_M22ogDf9nZuNw6bfwiEgbjGj6u6EOrRx4Ql3Zz82_mGfpvpUkmJh7RGfMeKCNydnZ0993YFkx4b_wkpsFw776U1qH3BU77105dmrFalJw7IjHMXhSyZHAPkeM6gz4r1FOCcwFvNDbSd9WvnFld2uD9APghteXmkW3Rzlh4uyPygwdoxVGvfrFr-s61iwvvolvVwwI3AakapURhwKwbFMw9WIwGNHm3g3CcsQDH8AzgdPzaOM_WVuOekLmeymdi9pkNVYY82Bfk43r787AlrN5YWF-1eZ9fW8OImXE9Lj26pQMC10QzXAYZUyhGsAqak_jq5OqohwC1JAL9tyK9O29RsuvXF_t_u2)

1. The user updates the default resource group with `BACKGROUND=(TASK_TYPES='stats')`, and TiDB stores this background configuration through PD.

2. TiKV receives the resource group update and records that requests whose task type is `stats` should be treated as background work.
3. When TiDB sends a manual or automatic `ANALYZE` request, the request source is marked as `internal_stats`, so TiKV can extract `stats` as the task type.
4. TiKV matches `stats` against `TASK_TYPES` and attaches the request to the shared background limiter.
5. A TiKV quota adjustment worker periodically checks CPU pressure and adjusts the shared limiter. With the default TiKV thresholds, the background budget starts decreasing when CPU utilization exceeds `bg-cpu-throttle-threshold` (`60.0`) and reaches the minimum floor at `fg-cpu-throttle-threshold` (`70.0`).

Once TiKV receives these requests, they are executed as Rust [futures](https://github.com/tikv/tikv/blob/642bdf8c631079f8ffe96a20af56e09dd5088f40/components/resource_control/src/future.rs#L97-L100) and scheduled by TiKV's [`Read Pool`](https://github.com/tikv/tikv/blob/642bdf8c631079f8ffe96a20af56e09dd5088f40/src/read_pool.rs#L208-L220). To control how these futures are scheduled, TiKV wraps background requests with [`LimitedFuture`](https://github.com/tikv/tikv/blob/642bdf8c631079f8ffe96a20af56e09dd5088f40/components/resource_control/src/future.rs#L57-L78), which allows it to track the CPU time and IO bytes consumed by each task. After each poll, each wrapped background future consumes quota from the globally shared background limiter; if the quota is exceeded, TiKV delays the next poll through an async timer. The overall [workflow](https://editor.plantuml.com/uml/VLHHRzem47xthpZr2QHsi4fxWHOLDY5DOnjhsPug8PCSYOLZP_Pvb4txxxiuXf49BHAHyNtt-Rllpddm91s5IWjIf15MwiCtcCbiLpWRJ0vFMObvqCeKiBKE64rUY3jF1uqJuV5xG9FXEoKB9olu9NmfSxkFSAazZiMIwLMcQcbeAVEyYTo-6OkPVJd-VJqSYu_elInmgbLX84D3wCRY4NrjZSSKdA6s-wbpTW_a0bF_5lLVSZxxH7ZRn8mbFDySDfjtWwIyathrY3A7fRNw2gIFYsvyeIf5cCy31OSHivXt4LCZzU4dmXaaRNLnJwLqc3-RJR7MaTIrTEswYrsfAeNZIWXjRGt1aD9j8u2aty5M6ULBzABH3JSJrZQ2schTwul5_BWH-mnlbrzX0Ey_iuFSO_P594WIsoQ2vPwAOqatYM699ZevWCQkWpWZpwJEWfPiE6fPUyoRWvwDv07YipO7OdK9tHRAATpPK64lgyEQkBnhK7OTaQow1PrEtaj5wpom0rBJKIeeduG0DDoCRGnWdLp1Q9Gq8W_XAH_kxs78y3WQJXKbWTqshGH-lWHBiY7rxAy6D3OKvQiO-cG1NzWneD9PedWRSgxqq8JPbKXuMKOoxLyXaqiwqWnqnDWVw6u2EzegMPNODhqAdY8TR90l9iOeSTImqjZ42zecxI2DMl6zIfdUKT4rNn3Vx_-_26Zpp_goGy6n-1CF_EOFoHy0) is as follows:

![](https://img.plantuml.biz/plantuml/png/VLHHRzem47xthpZr2QHsi4fxWHOLDY5DOnjhsPug8PCSYOLZP_Pvb4txxxiuXf49BHAHyNtt-Rllpddm91s5IWjIf15MwiCtcCbiLpWRJ0vFMObvqCeKiBKE64rUY3jF1uqJuV5xG9FXEoKB9olu9NmfSxkFSAazZiMIwLMcQcbeAVEyYTo-6OkPVJd-VJqSYu_elInmgbLX84D3wCRY4NrjZSSKdA6s-wbpTW_a0bF_5lLVSZxxH7ZRn8mbFDySDfjtWwIyathrY3A7fRNw2gIFYsvyeIf5cCy31OSHivXt4LCZzU4dmXaaRNLnJwLqc3-RJR7MaTIrTEswYrsfAeNZIWXjRGt1aD9j8u2aty5M6ULBzABH3JSJrZQ2schTwul5_BWH-mnlbrzX0Ey_iuFSO_P594WIsoQ2vPwAOqatYM699ZevWCQkWpWZpwJEWfPiE6fPUyoRWvwDv07YipO7OdK9tHRAATpPK64lgyEQkBnhK7OTaQow1PrEtaj5wpom0rBJKIeeduG0DDoCRGnWdLp1Q9Gq8W_XAH_kxs78y3WQJXKbWTqshGH-lWHBiY7rxAy6D3OKvQiO-cG1NzWneD9PedWRSgxqq8JPbKXuMKOoxLyXaqiwqWnqnDWVw6u2EzegMPNODhqAdY8TR90l9iOeSTImqjZ42zecxI2DMl6zIfdUKT4rNn3Vx_-_26Zpp_goGy6n-1CF_EOFoHy0)

1. The Read Pool polls a background `stats` task through `LimitedFuture`, instead of polling the inner future directly.

2. `LimitedFuture` polls the inner future once, then measures the CPU time and IO bytes consumed during that poll.

3. The measured CPU/IO usage is charged to the globally shared background limiter; if the task is still within budget, it returns the inner future's poll result without adding extra delay.

4. If the background quota is exceeded, `LimitedFuture` registers an async timer and returns `Poll::Pending`, so the next poll is delayed until the timer wakes the task.

**With the current default settings, `TASK_TYPES` is empty and `UTILIZATION_LIMIT` is set to `0`. This means that no background work is controlled by this mechanism until `TASK_TYPES` includes a matching task type. Once a task type is marked as background work, leaving `UTILIZATION_LIMIT` unset allows TiKV to use its CPU-threshold-based throttling policy.**

### Introducing a New Task Type for Stats Maintenance

The current `stats` task type is too coarse-grained for this change. A TiDB cluster has a `default` resource group by default, and users can add `stats` to `BACKGROUND.TASK_TYPES` to throttle stats requests:

```sql
ALTER RESOURCE GROUP `default` BACKGROUND=(TASK_TYPES='br,ddl,stats');
```

However, TiDB currently uses the same `stats` task type for both `ANALYZE` requests and sync/async-load queries:

```go
// For sync and async load
StatsCtx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
func ExecRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	...
	return ExecRowsWithCtx(StatsCtx, sctx, sql, args...)
}
// For analyze request
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars any,
	...
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = kv.InternalTxnStats
	resp := client.Send(ctx, kvReq, vars, &kv.ClientSendOption{})
	...
	return result, nil
}
```

This shared classification is a problem because we should not throttle sync-load requests. Although sync load belongs to the stats module, it is not regular background work. It is on the critical path of query optimization, so it must finish in time. Otherwise, the user query may have to wait until the sync-load request [times out](https://github.com/pingcap/docs/blob/42da4252914248472710bc8f9d3bb0546015093e/system-variables.md#tidb_stats_load_sync_wait-new-in-v540) before query optimization can continue.

We can introduce a new task type, `StatsForegroundPriority`, for internal stats maintenance work other than `ANALYZE` stats collection.

This keeps the behavior backward compatible. In most cases, users who enable this feature to limit stats resource usage are trying to control `ANALYZE` requests. Therefore, instead of introducing a new task type dedicated to `ANALYZE`, we can keep using the existing `stats` task type for `ANALYZE` and move other stats maintenance work to the new `StatsForegroundPriority` task type. **`StatsForegroundPriority` should not be allowed to be throttled as background work, because these tasks are critical to user query optimization.** To enforce this, Resource Control should not expose `StatsForegroundPriority` as a configurable `TASK_TYPES` value.

In the future, if we add other heavy stats maintenance work that is not on the critical path of user queries, it can use the same `stats` task type as `ANALYZE`. This keeps the model simple and avoids additional confusion or learning cost for users.

Based on the TiDB code example above, we need to update `StatsCtx` to use the new `StatsForegroundPriority` type.

```go
// For sync and async load
StatsCtx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnStatsForegroundPriority)
func ExecRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	...
	return ExecRowsWithCtx(StatsCtx, sctx, sql, args...)
}
```

**`ANALYZE` requests remain unchanged and continue to use the existing `stats` task type.**

### Limit `ANALYZE` Resource Usage by Default

Currently, `ANALYZE` resource usage is not limited by default. However, we should generally enforce an upper bound for it. `ANALYZE` is not latency-sensitive, so delaying an `ANALYZE` request for a short period of time is acceptable. This is especially reasonable for auto-analyze, whose default statement timeout is 12 hours.

Even for manually triggered `ANALYZE` statements, foreground user queries should always take priority. Statistics are collected only to improve query optimization and better serve user queries. Therefore, statistics collection should not negatively impact user query execution.

Based on this reasoning, this design enables background resource limiting for `ANALYZE` by default by adding `stats` to the default background `TASK_TYPES`, while keeping `UTILIZATION_LIMIT` unset. We should rely on TiKV's `bg-cpu-throttle-threshold` and `fg-cpu-throttle-threshold` instead of setting a fixed utilization limit from TiDB.

With the current TiKV defaults, background throttling starts when CPU utilization exceeds `60.0%` and linearly tightens until `70.0%`, where the background CPU budget reaches its minimum floor. This is a better fit for `ANALYZE` than a fixed low `UTILIZATION_LIMIT`: it allows `ANALYZE` to use available resources when the node is not busy, but quickly reduces its budget when foreground workload pressure rises.

The following [threshold flow](https://editor.plantuml.com/uml/ZLB1JiCm3BtdAtpi3lk15SG44a8QI0WDxcdhjDRIk4eSfF7rEDa4azY1InGxp-yzszaciL7ox8sGU8I7rtVOJn7Jn7v8u3Z2lUqFO-GS1jZRT4Z6r1gpUK6RSaJOq-wZ5cOXJrzMY4Dh33beIlRc5hfntIzvBLDaKPLoEIdShK0c3D0SCQqCm6Q7k9GKkEmzTB_INHv6bBPWBWn9CGIz3P0JWlsqv6sdRIWzvzXZQWrwVMa25_eVI-3-wqZrdYcWZVweh4FkiTgxwaonJpHE6Eiq5cMiyvdHXWHUe-jbbmwLuksayWSAmbLwfa1pdi5vvdYMVUI8P_2Y7M5VTb3d8lQ0cYEE9CUg5WPovdJg9danMmtq1tm3) summarizes the core idea:

![](https://img.plantuml.biz/plantuml/png/ZLB1JiCm3BtdAtpi3lk15SG44a8QI0WDxcdhjDRIk4eSfF7rEDa4azY1InGxp-yzszaciL7ox8sGU8I7rtVOJn7Jn7v8u3Z2lUqFO-GS1jZRT4Z6r1gpUK6RSaJOq-wZ5cOXJrzMY4Dh33beIlRc5hfntIzvBLDaKPLoEIdShK0c3D0SCQqCm6Q7k9GKkEmzTB_INHv6bBPWBWn9CGIz3P0JWlsqv6sdRIWzvzXZQWrwVMa25_eVI-3-wqZrdYcWZVweh4FkiTgxwaonJpHE6Eiq5cMiyvdHXWHUe-jbbmwLuksayWSAmbLwfa1pdi5vvdYMVUI8P_2Y7M5VTb3d8lQ0cYEE9CUg5WPovdJg9danMmtq1tm3)

That said, we still need more testing to understand the typical CPU usage pattern during table analysis. **If the default behavior needs adjustment, it should be tuned through the TiKV CPU throttle thresholds rather than by setting a low `UTILIZATION_LIMIT` for `ANALYZE`.**

## Test Design

### Functional Tests

- Ensure that the new task type does not affect the existing background throttling behavior.
- Ensure that `ANALYZE` requests are throttled by default in newly created clusters when `stats` is added to the default background `TASK_TYPES`.

### Scenario Tests

The scenario tests should evaluate how background throttling affects tables of different sizes. We need to understand its impact on relatively small tables versus large tables, and use the results to determine appropriate default limits.

The scenario tests should include normal foreground queries, because the main goal is to verify that throttling protects user traffic while `ANALYZE` is running. To keep the result stable, the foreground workload should be fixed and reproducible: it should use a controlled query set, fixed concurrency, and a stable CPU usage target. We should first run the foreground workload alone to establish the baseline CPU usage and query latency, then run the same foreground workload together with `ANALYZE` and compare the difference.

The two primary metrics we should focus on are:

- TiKV CPU usage
- `ANALYZE` duration

### Compatibility Tests

- The main risk is upgrade compatibility. We should verify that existing settings are preserved after an upgrade and that background throttling continues to work as expected.
- For upgraded clusters, if background throttling was not previously configured, the new default background `TASK_TYPES` change should not be applied. The default should only take effect for newly created clusters.

### Benchmark Tests

We should reuse the same workload from the scenario test and evaluate it with different CPU-throttle threshold settings, such as the default `60.0`/`70.0`, a more aggressive pair, and a more conservative pair. This helps us understand whether the TiKV defaults are appropriate for `ANALYZE` and whether threshold tuning is needed.

## Impacts & Risks

The impact of this change is limited to `ANALYZE` requests. Since `ANALYZE` is background work and throttling helps protect foreground user queries, most users should benefit from this change. We also preserve backward compatibility. Therefore, the overall risk is manageable.

## Investigation & Alternatives

None

## Unresolved Questions

- Are the current TiKV default CPU throttle thresholds sufficient for `ANALYZE`, or do they need TiKV-side tuning?

## Future Possibility

Another possible future improvement is to make `ANALYZE` more cooperative with the scheduler. `ANALYZE` contains a large amount of computation, and once the data scan succeeds, some phases may perform substantial synchronous computation before yielding control back to the async runtime. In that shape, background throttling can only delay later polls and has fewer opportunities to intervene while CPU-heavy work is already running.

If testing shows this to be a bottleneck, we can consider adding explicit yield points or quota checkpoints in CPU-intensive `ANALYZE` phases. This would allow `ANALYZE` to return control to the scheduler more frequently, making it easier for Resource Control to pace the task. This is complementary to the task-type separation and default background throttling proposed in this document, and is not required for the initial change.

## FAQ

- Why is `PriorityLow` not sufficient if `ANALYZE` requests are already assigned low priority?

  Low priority only affects scheduling before the `ANALYZE` task is picked up by the read pool, or when it is re-queued after an `await`. Once the task is actually running, it is not preempted or rate-limited based on that priority. As a result, if `ANALYZE` performs heavy scan or CPU work within a single poll, it can still occupy read-pool workers and consume I/O aggressively.

  Background throttling works differently. It measures the resources consumed by `ANALYZE`, such as CPU time, read bandwidth, and IOPS, and deliberately delays the task when it exceeds the background quota.
