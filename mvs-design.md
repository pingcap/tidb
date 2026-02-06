# MVS Design (pkg/mvs)

## 1. Scope

This document describes the current design of `pkg/mvs` in TiDB, based on the implementation in:

- `pkg/mvs/tasks.go`
- `pkg/mvs/task_handler.go`
- `pkg/mvs/task_executor.go`
- `pkg/mvs/server_maintainer.go`
- `pkg/mvs/consistenthash.go`
- `pkg/mvs/priority_queue.go`
- `pkg/mvs/utils.go`

The module is a background scheduler for Materialized View (MV) refresh and MV log (MVLog) purge jobs.

## 2. Design Goals

- Distribute MV tasks across TiDB nodes by consistent hashing.
- Keep per-node scheduling state in memory with low overhead.
- React quickly to DDL changes while still doing periodic reconciliation.
- Isolate execution with a bounded worker pool and timeout control.
- Expose clear extension points for actual refresh/purge business logic.

## 3. High-Level Architecture

### 3.1 Entry point

- `RegisterMVS` (`pkg/mvs/tasks.go`) is called from session bootstrap (`pkg/session/session.go`).
- It creates a singleton `MVService`, registers a DDL notifier handler, then starts service loop.

### 3.2 Main components

- `MVService`
    - Owns scheduler loop, in-memory queues, retry state, and task submission.
- `ServerConsistentHash`
    - Tracks active TiDB nodes and maps task IDs to owner nodes.
- `TaskExecutor`
    - Bounded async executor with dynamic concurrency and timeout logic.
- `Notifier`
    - Multi-producer single-consumer wakeup primitive.
- `MVRefreshHandler` / `MVLogPurgeHandler`
    - Injected business logic interfaces for real refresh/purge behavior.

## 4. Core Data Structures

### 4.1 Service state (`MVService`)

- `lastRefresh atomic.Int64`
    - Last successful (or failure-throttled) metadata fetch timestamp.
- `running atomic.Bool`
    - Service lifecycle state.
- `sch *ServerConsistentHash`
    - Ownership resolution by consistent hash.
- `executor *TaskExecutor`
    - Background execution pool.
- `mvDDLEventNotifier Notifier`, `ddlDirty atomic.Bool`
    - Wakeup and "force fetch" signaling.
- `taskHandler` (`RWMutex`, `refresh`, `purge`)
    - Handler registry for MV and MVLog operations.
- `mvRefreshMu` / `mvLogPurgeMu`
    - Each has:
        - `pending map[id]item`: authoritative in-memory task index.
        - `prio PriorityQueue`: min-heap by next execution time.

### 4.2 Task models

- `mv`
    - `ID`, `refreshInterval`, `nextRefresh`, `retryCount`.
- `mvLog`
    - `ID`, `purgeInterval`, `nextPurge`, `retryCount`.

## 5. Ownership and Distribution

`ServerConsistentHash` flow:

1. `Init()` resolves local server ID via `infosync.GetServerInfo()`.
2. Initial `Refresh()` loads all servers and builds hash ring.
3. Runtime `Refresh(ctx)` updates ring only when server membership changes.
4. `Available(taskID)` returns true if `ToServerID(taskID) == localServerID`.

The scheduler only keeps tasks owned by current node in memory.

## 6. Scheduling Lifecycle

### 6.1 Start

- `Start()`:
    - Guarded by `running.Swap(true)`.
    - Initializes server hash.
    - Marks `ddlDirty=true` and wakes notifier.
    - Starts `scheduleLoop()` goroutine.

### 6.2 Event and timer driven loop

`scheduleLoop()` uses a timer and notifier channel:

1. Wait on timer or DDL notifier.
2. On DDL event:

- `clear()` notifier.
- `forceFetch = ddlDirty.Swap(false)`.

3. Exit if `running=false`.
2. Fetch metadata if:

- `forceFetch == true`, or
- `shouldFetch(now)` (default interval 30s).

5. Pop due tasks from both priority queues.
2. Submit due tasks to executor.
3. Compute next wake time:

- min(next fetch time, next due task time).

### 6.3 Stop

- `Close()`:
    - Sets `running=false`.
    - Wakes notifier so loop can exit.
    - Closes executor and sets pointer to nil.

## 7. Metadata Fetch and Reconciliation

### 7.1 Sources

- MVLog purge metadata:
    - `mysql.tidb_mlog_purge` JOIN `mysql.tidb_mlogs`.
- MV refresh metadata:
    - `mysql.tidb_mview_refresh` JOIN `mysql.tidb_mviews`.

### 7.2 Pipeline

1. Fetch rows via `ExecRCRestrictedSQL`.
2. Build `newPending` for tasks owned by current node.
3. Reconcile with in-memory `pending` map:

- Existing task:
    - Update interval.
    - If not retrying (`retryCount == 0`) and next time changed, update heap position.
- New task:
    - Insert into map and heap.
- Removed task:
    - Remove from map and heap.

4. Update metrics counters.

### 7.3 Fetch timestamp semantics

- On full fetch success: `lastRefresh = now`.
- On fetch failure in loop: `lastRefresh = now` too, intentionally throttling retries.

## 8. Execution and Retry

### 8.1 Handler abstraction

`MVService` does not implement MV logic directly. It calls injected handlers:

- `RefreshMV(ctx, mvID) -> (relatedMVLogIDs, nextRefresh, err)`
- `PurgeMVLog(ctx, mvLogID) -> (nextPurge, err)`

Default handler is `noopMVTaskHandler` returning:

- `ErrMVRefreshHandlerNotRegistered`
- `ErrMVLogPurgeHandlerNotRegistered`

### 8.2 Submission flow

- For each due MV:
    - Submit `mv-refresh/<id>`.
    - On success:
        - `retryCount = 0`, reschedule to handler-provided `nextRefresh`.
    - On failure:
        - `retryCount++`, reschedule to `now + retryDelay(retryCount)`.
- For each due MVLog:
    - Same pattern with purge variant.
- Each completion wakes notifier to recalculate next schedule time.

### 8.3 Retry backoff

- Base: 5s
- Exponential doubling
- Cap: 5m

## 9. TaskExecutor Design

### 9.1 Behavior

- FIFO queue protected by mutex + condition variable.
- Worker count can scale up/down dynamically via `UpdateConfig`.
- Timeout handling:
    - If timeout <= 0: run task inline in worker.
    - Else run task in child goroutine and `select` on done vs timer.
    - On timeout: worker slot is released, task continues in background.

### 9.2 Metrics

- Submitted, running, waiting, completed, failed, timeout, rejected.

### 9.3 Close semantics

- `Close()` marks executor closed, wakes waiting workers, waits for:
    - all tasks (`tasksWG`)
    - all workers (`workers.wg`)

## 10. Utility Components

- `Notifier`
    - Buffered channel size 1 + atomic `awake` bit.
    - Coalesces multiple wakeups.
- `PriorityQueue`
    - Generic min-heap with O(log n) push/pop/update/remove.
- `ConsistentHash`
    - CRC32-based ring with virtual nodes.

## 11. Current Known Constraints and Risks

1. Handler registration is not wired in production path yet.
   - `RegisterMVS` only creates and starts `MVService`.
   - Without injected handler, tasks fail and enter retry flow.

2. No strict validation for handler-returned next schedule timestamps.
   - Zero or stale timestamps can cause immediate re-trigger loops.

3. Retry fields are mutable task state shared by scheduling and worker paths.
   - Current code uses locks for queue operations but does not fully serialize all reads/writes of `retryCount`.

4. `TaskExecutor` timeout does not cancel task body.
   - Timed-out task keeps running in background.

5. `Close()` sets `executor=nil`.
   - Service lifecycle ordering must ensure no concurrent submit path touches nil pointer.

## 12. Test Coverage Snapshot

- `pkg/mvs/mvs_test.go`
    - TaskExecutor concurrency, dynamic config, timeout behavior, close rejection.
- `pkg/mvs/utils_test.go`
    - ConsistentHash behavior and priority queue correctness.
- `pkg/mvs/task_handler_test.go`
    - Default handler errors and injected handler dispatch.

No dedicated end-to-end scheduler integration test exists yet for:

- full fetch -> queue -> execute -> reschedule loop
- DDL-triggered force fetch path
- close/race edge cases

## 13. Suggested Integration Contract for Future Modules

When plugging in real MV logic, handler implementations should:

1. Return deterministic next schedule times.
2. Distinguish retryable and non-retryable errors.
3. Keep operations idempotent.
4. Respect `context.Context` for timeout/cancel integration.
5. Emit business metrics and structured logs for traceability.

## 14. Summary

`pkg/mvs` already provides a reusable scheduling framework:

- distributed ownership,
- in-memory priority scheduling,
- bounded asynchronous execution,
- and pluggable business handlers.

The main remaining work is business logic integration and hardening around lifecycle, retries, and observability.
