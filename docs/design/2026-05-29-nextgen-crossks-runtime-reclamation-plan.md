# NextGen Cross-KS Runtime Reclamation

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it. Repository policy in `AGENTS.md` is stricter for TiDB validation and must also be followed.

## Purpose / Big Picture

SYSTEM keyspace currently creates cross-keyspace runtimes lazily when DXF needs to run user-keyspace work such as IMPORT or distributed DDL backfill. Those runtimes hold a user-keyspace store, session pool, schema syncer, etcd client, and background loops. Without an explicit lifecycle, they can remain alive after the DXF task that needed them has finished.

After this change, DXF can run SYSTEM-to-user-keyspace tasks as before, but the cross-keyspace runtime is held only while the DXF scheduler or task executor needs it. When all holders release their runtime handles and the runtime has been idle for the configured timeout, a periodic sweeper closes and removes it. This can be observed through crossKS logs, low-cardinality metrics, and tests that show the runtime remains alive while active and is evicted only after release plus idle timeout.

A runtime handle is an acquired ownership handle, not a time-bounded lease. It has no fixed duration, no TTL, and no auto-expiration while active. The idle timeout starts only after the last active runtime handle has been released.

## Progress

- [x] (2026-06-02 Asia/Shanghai) Converted the prior design summary into this `PLANS.md`-style ExecPlan.
- [x] (2026-06-02 Asia/Shanghai) Implemented crossKS runtime handle API and active bookkeeper tracking in commit `571a893d58eb` with tests, `make bazel_prepare`, and `make lint`. Raw getter removal from `sqlsvrapi.Server` is intentionally deferred to the caller-migration milestone so each subtask commit remains buildable.
- [x] (2026-06-02 Asia/Shanghai) Added crossKS idle sweeper lifecycle tied to `Domain` lifecycle with active-handle eviction tests, raw-getter coexistence guard, `make bazel_prepare`, targeted failpoint-aware tests, and `make lint`.
- [ ] Move SYSTEM-to-user acquire/release ownership to DXF scheduler manager and task executor manager boundaries.
- [ ] Migrate IMPORT and DDL backfill code to consume DXF-provided runtime/store/session pool values.
- [ ] Add unit and integration coverage for handle bookkeeping, DXF boundary ownership, migration guards, and idle eviction.
- [ ] Run required TiDB validation and record evidence in this plan.

## Surprises & Discoveries

- Observation: Current SYSTEM-to-user raw getter calls are concentrated in DXF, IMPORT, DDL backfill, and one session construction path.
  Evidence: `rg -n "GetKSStore|GetKSSessPool" pkg/session/session.go pkg/ddl/backfilling_dist_scheduler.go pkg/ddl/backfilling_dist_executor.go pkg/dxf/importinto/scheduler.go pkg/dxf/importinto/task_executor.go pkg/dxf/framework/scheduler/scheduler_manager.go` currently reports raw getter calls in those files.
- Observation: DXF scheduler manager already has a single scheduler startup/exit boundary in `pkg/dxf/framework/scheduler/scheduler_manager.go:startScheduler`, and task executor manager already has a single executor startup/exit boundary in `pkg/dxf/framework/taskexecutor/manager.go:startTaskExecutor`.
  Evidence: Both functions create the scheduler/executor, start the goroutine, and have an existing exit `defer` where release can be centralized.
- Observation: Packages touched by this plan use failpoints.
  Evidence: `pkg/domain/crossks`, `pkg/dxf/framework/scheduler`, `pkg/dxf/framework/taskexecutor`, `pkg/dxf/importinto`, and `pkg/ddl` have `failpoint` or `testfailpoint` usages or Bazel failpoint dependencies, so targeted tests must use the failpoint-aware test runner.
- Observation: Removing `GetKSStore` and `GetKSSessPool` from `sqlsvrapi.Server` cannot be isolated from DXF, IMPORT, and DDL caller migration.
  Evidence: `rg -n "GetKSStore|GetKSSessPool" pkg/dxf pkg/ddl pkg/session` reports non-domain call sites through `sessionctx.Context.GetSQLServer()`, so removing the interface methods before migration would break compilation.
- Observation: The temporary raw getter path has no release boundary, so idle sweeping cannot safely reclaim runtimes touched by `GetOrCreate` while raw getters remain.
  Evidence: A nextgen regression test failed before the guard because a runtime acquired and released through a handle, then touched through `GetOrCreate`, was evicted by `sweepIdleRuntimes` using the old `lastReleaseAt`.

## Decision Log

- Decision: Use `KSRuntimeHandle` terminology instead of `lease`.
  Rationale: "Lease" suggests a fixed duration or TTL, which is not intended. The handle remains active until `Release()` or domain shutdown.
  Date/Author: 2026-06-02 / User and Codex.
- Decision: Acquire/release only at DXF scheduler and task executor manager boundaries.
  Rationale: DXF is the only current SYSTEM-to-user crossKS runtime user, and centralizing ownership avoids many acquire/release sites in IMPORT and DDL backfill logic.
  Date/Author: 2026-06-02 / User.
- Decision: Require a non-empty bookkeeper name and reject duplicate active acquisition by the same bookkeeper for the same target keyspace.
  Rationale: The bookkeeper identifies ownership in logs and catches accidental double bookkeeping without relying on high-cardinality metrics.
  Date/Author: 2026-06-02 / User and Codex.
- Decision: Use bookkeeper names `DXF/scheduler/<taskID>` and `DXF/executor/<taskID>` for current DXF owners.
  Rationale: The names are deterministic, human-readable, and unique for the two independent DXF holders of a task.
  Date/Author: 2026-06-02 / User.
- Decision: Do not use full bookkeeper names, task IDs, or target keyspace names as Prometheus labels in v1.
  Rationale: Those labels are high-cardinality. Full bookkeeper identity belongs in structured logs and tests.
  Date/Author: 2026-06-02 / User and Codex.
- Decision: Remove `GetKSStore` and `GetKSSessPool` from `sqlsvrapi.Server`.
  Rationale: There is no compatibility requirement for the SQL server interface, and leaving raw getters exposed makes it easy to bypass runtime handle ownership.
  Date/Author: 2026-06-02 / User.
- Decision: Keep `GetKSStore` and `GetKSSessPool` temporarily on `sqlsvrapi.Server` during the first handle-bookkeeping milestone, then remove them in the DXF/IMPORT/DDL migration milestone.
  Rationale: Existing DXF, IMPORT, and DDL call sites still compile through `sessionctx.Context.GetSQLServer()`. Adding `AcquireKSRuntime` first while deferring raw getter removal keeps the branch buildable after each subtask commit.
  Date/Author: 2026-06-02 / Codex.
- Decision: Mark runtimes touched by temporary raw getters as ineligible for idle sweeping until raw getter migration removes that untracked path.
  Rationale: Raw users do not acquire a runtime handle and therefore have no release event. Conservatively skipping raw-touched entries preserves compatibility during staged migration; final reclamation for normal task flows is restored after DXF/IMPORT/DDL move to handle-owned runtime values and raw getter exposure is removed.
  Date/Author: 2026-06-02 / Codex.

## Outcomes & Retrospective

Milestone 1 completed in commit `571a893d58eb` (`domain/crossks: add runtime handle bookkeeping`). `pkg/domain/sqlsvrapi/server.go` now defines `KSRuntime`, `KSRuntimeHandle`, and `AcquireKSRuntime`; `pkg/domain/domain.go` delegates acquisition to the crossKS manager; and `pkg/domain/crossks/cross_ks.go` tracks active bookkeepers plus `lastReleaseAt` per runtime. Targeted failpoint-aware crossKS tests, nextgen-tagged crossKS tests, `make bazel_prepare`, `make lint`, and diff whitespace checks passed during the subtask. Remaining gap: raw SQL server getter removal is deferred until the caller migration milestone.

Milestone 2 added `crossKSRuntimeIdleTimeout`, `crossKSRuntimeSweepInterval`, `Manager.RunGCLoop`, and a single-sweep helper that removes only released handle-owned runtimes whose idle age exceeds the timeout. `Domain.Start` now runs the crossKS GC loop under the Domain wait group, so `Domain.Close` cancels it before closing remaining runtimes. Resource close happens after removal from the manager map and outside the manager lock. Tests cover active non-eviction, idle eviction, close-outside-lock, re-acquire after eviction, raw-getter coexistence, shutdown close behavior, and GC loop cancellation. Remaining gap: runtimes touched through temporary raw getters are deliberately not idle-reclaimed until later milestones remove raw getter use and exposure.

## Context and Orientation

TiDB next-generation keyspace mode separates SYSTEM keyspace from user keyspaces. A keyspace is an isolation namespace for storage and metadata. SYSTEM keyspace owns shared service work; user keyspaces own tenant/user data.

The crossKS manager lives in `pkg/domain/crossks/cross_ks.go`. It now maps target keyspace name to a runtime entry that owns a `*SessionManager`, an active bookkeeper set, and `lastReleaseAt`. A `SessionManager` owns the target keyspace store, session pool, etcd client, schema version syncer, info schema cache, info schema syncer, and background loops. `Manager.GetOrCreate` still lazily creates a `SessionManager` for compatibility; `Manager.Acquire` creates an acquired runtime handle; `Manager.CloseKS` and `Manager.Close` close managers.

The SQL server interface lives in `pkg/domain/sqlsvrapi/server.go`. It now exposes `AcquireKSRuntime(targetKS string, bookkeeper string) (KSRuntimeHandle, error)` and still temporarily exposes raw getters `GetKSSessPool(targetKS string)` and `GetKSStore(targetKS string)` until DXF, IMPORT, and DDL callers are migrated. Those raw methods let callers obtain resources without an ownership handle and must be removed in the migration milestone. The concrete implementation is `pkg/domain/domain.go`, where `Domain.AcquireKSRuntime`, `Domain.GetKSStore`, `Domain.GetKSInfoCache`, and `Domain.GetKSSessPool` call the crossKS manager.

DXF is TiDB's distributed task framework under `pkg/dxf/framework`. It runs task schedulers on owner nodes and task executors on executor nodes. In next-generation mode, SYSTEM keyspace DXF may run tasks whose `task.Keyspace` is a user keyspace. The scheduler boundary is `pkg/dxf/framework/scheduler/scheduler_manager.go:startScheduler`. The task executor boundary is `pkg/dxf/framework/taskexecutor/manager.go:startTaskExecutor`.

IMPORT task-specific code lives under `pkg/dxf/importinto`. DDL distributed backfill code lives in `pkg/ddl/backfilling_dist_scheduler.go` and `pkg/ddl/backfilling_dist_executor.go`. Because this work touches DDL code, any implementer must first read `docs/agents/ddl/README.md`; distributed backfill background is in `docs/agents/ddl/03-reorg-backfill.md`.

A bookkeeper is a required string that identifies who acquired a runtime handle. For current DXF users, valid names are `DXF/scheduler/<taskID>` and `DXF/executor/<taskID>`. A bookkeeper may acquire a target keyspace runtime only once while still active. After `Release()`, the same name may acquire again.

## Plan of Work

Milestone 1 changes the API and crossKS manager state. Add runtime view interfaces, implement `AcquireKSRuntime`, replace anonymous refcounting with active bookkeeper tracking, and keep raw getters temporarily in `sqlsvrapi.Server` until their callers are migrated. At the end of this milestone, a caller can acquire a `KSRuntimeHandle`, use `Store()` and `SessPool()`, release it idempotently, and observe duplicate active acquisition failing.

Milestone 2 adds idle reclamation. Add a periodic sweeper tied to the `Domain` lifecycle. The sweeper evicts only entries with no active bookkeepers whose last release time is older than the idle timeout. Close runtime resources outside the manager lock.

Milestone 3 centralizes DXF ownership. Update scheduler and task executor manager startup paths to acquire handles for cross-keyspace tasks and release in existing exit defers and failure paths. Extend scheduler and task executor params to carry a non-releasing `KSRuntime` view, or derived task store/session pool values, so task-specific code does not acquire directly.

Milestone 4 migrates callers. Remove raw getter use from IMPORT and DDL backfill task-specific code. Those packages must consume the runtime, store, or session pool passed through DXF params.

Milestone 5 validates behavior. Add manager, DXF boundary, migration guard, and end-to-end cross-keyspace tests. Run `make bazel_prepare` because this plan changes Go imports/interfaces and likely adds top-level Go tests. Run targeted failpoint-aware tests and Ready validation, including `make lint` before claiming implementation completion.

## Concrete Steps

Work from repository root `/Users/jujiajia/code/pingcap/tidb`.

1. Read required context before editing:

       sed -n '1,260p' PLANS.md
       sed -n '1,180p' docs/agents/ddl/README.md
       sed -n '1,140p' docs/agents/ddl/03-reorg-backfill.md
       sed -n '1,220p' pkg/domain/sqlsvrapi/server.go
       sed -n '1,330p' pkg/domain/crossks/cross_ks.go
       sed -n '850,890p' pkg/domain/domain.go
       sed -n '335,395p' pkg/dxf/framework/scheduler/scheduler_manager.go
       sed -n '300,370p' pkg/dxf/framework/taskexecutor/manager.go

   Expected result: the implementer can identify the current raw getter interface, current crossKS manager map, Domain implementation, and the two DXF startup/exit boundaries.

2. Write failing crossKS manager tests before implementation. Prefer adding an internal test file `pkg/domain/crossks/cross_ks_internal_test.go` with `package crossks` for state-oriented cases, and update `pkg/domain/crossks/cross_ks_test.go` for public behavior if needed. Cover empty bookkeeper, duplicate active acquire, different active bookkeepers, idempotent release, re-acquire after release, no eviction while active, eviction after idle, and close/sweeper/acquire races.

   Run:

       ./tools/check/failpoint-go-test.sh pkg/domain/crossks -run 'Test.*RuntimeHandle|Test.*Bookkeeper|Test.*Evict' -count=1

   Expected before implementation: failures showing missing `Acquire`/`KSRuntimeHandle` behavior or missing assertions.

3. Update `pkg/domain/sqlsvrapi/server.go`. Define runtime interfaces in this package so callers can depend on a small API. Keep raw getter methods on `Server` only until the later DXF/IMPORT/DDL caller migration removes their last interface users:

       type KSRuntime interface {
           Store() kv.Storage
           SessPool() util.DestroyableSessionPool
       }

       type KSRuntimeHandle interface {
           KSRuntime
           Release()
       }

       type Server interface {
           AcquireKSRuntime(targetKS string, bookkeeper string) (KSRuntimeHandle, error)
           // Temporary until the caller migration milestone:
           GetKSSessPool(targetKS string) (util.DestroyableSessionPool, error)
           GetKSStore(targetKS string) (store kv.Storage, err error)
           GetDDLOwnerMgr() owner.Manager
       }

4. Update `pkg/domain/crossks/cross_ks.go`. Replace `sessMgrs map[string]*SessionManager` with per-target runtime entries. The entry must contain the `*SessionManager`, an active bookkeeper set, `lastReleaseAt`, and any closing/eviction state needed to prevent races. Keep public test helpers such as `GetAllKeyspace` and `Get` semantically compatible by returning keyspace names and `*SessionManager`.

   Add an acquired handle type in `pkg/domain/crossks` with `Store()`, `SessPool()`, and `Release()` methods. `Release()` must use `sync.Once` or equivalent so repeated calls are no-ops. Do not make release close resources synchronously.

   Add manager acquire/release methods. The acquire method should reject classic kernel, same-keyspace access, empty bookkeeper, and duplicate active bookkeeper for the same target keyspace. It should lazily create the runtime as current `GetOrCreate` does, then add the bookkeeper. Release removes the bookkeeper and sets `lastReleaseAt` only when the active set becomes empty.

5. Update `pkg/domain/domain.go`. Add:

       func (do *Domain) AcquireKSRuntime(targetKS string, bookkeeper string) (sqlsvrapi.KSRuntimeHandle, error)

   This method delegates to the crossKS manager and traces returned errors. Keep `GetKSInfoCache` for `createCrossKSSession`. Keep any raw `Domain` helper that remains necessary for internal construction or tests, but ensure raw getters are no longer part of `sqlsvrapi.Server`.

   Update `loadSysKSInfoSchema` and `pkg/session/session.go:createCrossKSSession` only if compilation requires it. If they can keep using concrete `Domain` helpers, do not broaden SQL server interface exposure to satisfy them.

6. Add idle sweeping in `pkg/domain/crossks/cross_ks.go` and hook it to `Domain` lifecycle. Use code constants or package-level values for v1 defaults:

       crossKSRuntimeIdleTimeout = 30 * time.Minute
       crossKSRuntimeSweepInterval = time.Minute

   Add a blocking loop method such as `RunGCLoop(ctx context.Context)` that periodically calls a single-sweep helper. In `pkg/domain/domain.go`, start it after `do.ctx` exists using the Domain wait group:

       do.wg.Run(func() {
           do.crossKSSessMgr.RunGCLoop(do.ctx)
       }, "crossKSSessMgrGCLoop")

   The exact placement should be near other Domain background loops after `do.ctx` is initialized. Domain shutdown already calls `do.crossKSSessMgr.Close()`, which must close all entries regardless of idle timeout.

7. Add low-cardinality observability. Use structured logs in `pkg/domain/crossks/cross_ks.go` for create, acquire, duplicate acquire, release, evict, and close failure. Include `targetKS`, `bookkeeper`, event, and active owner count in logs. Add metrics in the existing metrics structure only if they can stay low-cardinality; do not use target keyspace, task ID, or full bookkeeper as Prometheus labels.

8. Update `pkg/dxf/framework/scheduler/interface.go`. Add a non-releasing runtime field to `Param`, for example:

       TaskRuntime sqlsvrapi.KSRuntime

   Keep `TaskStore kv.Storage` because existing schedulers already use it. `NewParamForTest` should set only `TaskStore` unless tests need a runtime.

9. Update `pkg/dxf/framework/scheduler/scheduler_manager.go:startScheduler`. Replace the raw `GetKSStore` path. When `task.Keyspace != sm.store.GetKeyspace()`, acquire through `sm.taskMgr.WithNewSession`:

       bookkeeper := fmt.Sprintf("DXF/scheduler/%d", task.ID)
       runtimeHandle, err := se.GetSQLServer().AcquireKSRuntime(task.Keyspace, bookkeeper)

   Store `runtimeHandle.Store()` in `taskStore`, pass `runtimeHandle` as `TaskRuntime`, and release it in all paths after acquisition: scheduler factory/init failure before goroutine startup and the existing scheduler exit defer. Do not release from task-specific schedulers.

10. Update `pkg/dxf/framework/taskexecutor/task_executor.go` or the current file defining `taskexecutor.Param`. Add:

        TaskRuntime sqlsvrapi.KSRuntime

    Keep `Store kv.Storage` for current task executor code.

11. Update `pkg/dxf/framework/taskexecutor/manager.go:startTaskExecutor`. When `task.Keyspace != m.store.GetKeyspace()`, acquire through `m.taskTable.WithNewSession` using bookkeeper `DXF/executor/<taskID>`. Pass `runtimeHandle.Store()` as `Store` and `runtimeHandle` as `TaskRuntime`. Release the handle on all paths after acquisition: missing task executor factory, executor init failure, and the existing executor exit defer.

12. Migrate IMPORT-specific code. In `pkg/dxf/importinto/scheduler.go`, replace `se.GetSQLServer().GetKSSessPool(taskKS)` in `getTaskMgrForAccessingImportJob` with the scheduler param runtime/session pool. The scheduler should use `storage.NewTaskManager(param.TaskRuntime.SessPool())` for cross-keyspace task job access. In `pkg/dxf/importinto/task_executor.go`, remove `se.GetSQLServer().GetKSStore(task.Keyspace)` and use `e.store`, which should already be the task keyspace store from task executor params.

13. Migrate DDL backfill code. In `pkg/ddl/backfilling_dist_scheduler.go:getUserStoreAndTable`, use the scheduler `TaskStore` or supplied runtime/store instead of raw SQL server getters. In `pkg/ddl/backfilling_dist_executor.go:newBackfillStepExecutor`, use the task executor param/runtime-derived store and session pool instead of `svr.GetKSStore` and `svr.GetKSSessPool`. If the DDL executor struct does not currently retain the param runtime/session pool, add fields at construction time in `newBackfillDistExecutor`.

14. Add DXF boundary tests. In `pkg/dxf/framework/scheduler`, cover cross-keyspace scheduler acquire/release, init-failure release, and acquisition-failure behavior. In `pkg/dxf/framework/taskexecutor`, cover cross-keyspace executor acquire/release, missing-factory or init-failure release, and param wiring. Use mock/fake `sqlsvrapi.Server` and task table/session helpers where possible; avoid creating full IMPORT or DDL flows for these unit tests.

15. Add migration guard tests or checks. A lightweight Go test can scan relevant source files, or a documented validation command can be used. The acceptance command is:

        rg -n "GetKSStore|GetKSSessPool|AcquireKSRuntime" pkg/dxf pkg/ddl pkg/session

    Expected after implementation: `AcquireKSRuntime` appears only in DXF scheduler/task executor manager boundary code plus allowed Domain/session internals; IMPORT and DDL backfill task-specific files do not call acquire or raw getters.

16. Run `make bazel_prepare` after Go interface/import/test changes:

        make bazel_prepare

    Expected result: command succeeds and any generated Bazel metadata changes are reviewed and included.

17. Run targeted failpoint-aware tests:

        ./tools/check/failpoint-go-test.sh pkg/domain/crossks -run 'Test.*RuntimeHandle|Test.*Bookkeeper|Test.*Evict' -count=1
        ./tools/check/failpoint-go-test.sh pkg/dxf/framework/scheduler -run 'Test.*CrossKS|Test.*RuntimeHandle|Test.*Scheduler' -count=1
        ./tools/check/failpoint-go-test.sh pkg/dxf/framework/taskexecutor -run 'Test.*CrossKS|Test.*RuntimeHandle|Test.*Executor' -count=1
        ./tools/check/failpoint-go-test.sh pkg/dxf/importinto -run 'Test.*Nextgen|Test.*Import|Test.*CrossKS' -count=1
        ./tools/check/failpoint-go-test.sh pkg/ddl -run 'Test.*Backfill.*' -count=1

    Narrow the `-run` patterns to the actual test names added or impacted. Expected result: all targeted tests pass. If a broad `pkg/ddl` run is too slow, keep to the smallest exact test names that cover changed distributed backfill behavior and record the reason.

18. Run Ready validation before declaring the implementation complete:

        make lint

    Expected result: lint passes. Also rerun the migration guard `rg` command and record its output summary.

## Validation and Acceptance

The implementation is acceptable when all of the following are true.

Raw crossKS resource access is no longer exposed through `sqlsvrapi.Server`. `pkg/domain/sqlsvrapi/server.go` contains `AcquireKSRuntime(targetKS string, bookkeeper string) (KSRuntimeHandle, error)` and `GetDDLOwnerMgr()`, but not `GetKSStore` or `GetKSSessPool`.

The crossKS manager rejects empty bookkeeper names and duplicate active acquisition for the same target keyspace and bookkeeper. Different bookkeepers can hold the same runtime concurrently. `Release()` is idempotent. The same bookkeeper can acquire again after release.

The runtime is not evicted while either `DXF/scheduler/<taskID>` or `DXF/executor/<taskID>` is active. After both release and the idle timeout has elapsed, the sweeper closes and removes the runtime. Re-acquiring after eviction recreates the runtime.

DXF scheduler and task executor managers are the only current acquire/release sites for SYSTEM-to-user crossKS runtime handles. IMPORT and DDL backfill task-specific code consumes DXF-provided runtime/store/session pool values and does not acquire/release directly.

SYSTEM can still execute user-keyspace IMPORT and DDL distributed backfill flows. Existing crossKS restrictions remain: cross-keyspace sessions access only allowed system tables, and cross-keyspace SQL DDL remains prohibited.

Required evidence must be recorded in `Artifacts and Notes`: `make bazel_prepare`, targeted failpoint-aware tests, migration guard output, and `make lint`.

## Idempotence and Recovery

It is safe to rerun `make bazel_prepare`; review generated Bazel metadata after each run and keep only changes caused by this work.

It is safe to rerun failpoint-aware tests. The helper script enables failpoints, runs the test, and disables failpoints during cleanup. If a test is interrupted, run:

    make failpoint-disable

or use the repository failpoint cleanup command documented in `docs/agents/testing-flow.md` before retrying.

Runtime handle `Release()` must be idempotent so cleanup defers can safely call it on failure paths. If an implementation attempt leaks handles, inspect active bookkeeper logs and manager test failures before changing task-specific code; acquire/release should remain centralized at DXF manager boundaries.

When the caller migration milestone removes `GetKSStore` and `GetKSSessPool` from `sqlsvrapi.Server`, do not restore them to hide compile failures. Instead, update callers to use DXF params or concrete Domain internals as described above. Raw getters must not be reachable through `sessionctx.Context.GetSQLServer()` at final acceptance.

## Artifacts and Notes

Milestone 1 evidence, commit `571a893d58eb`:

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -run 'Test.*RuntimeHandle|Test.*Bookkeeper' -count=1
    # red before implementation: failed with missing runtime handle/bookkeeper symbols
    # green after implementation: ok github.com/pingcap/tidb/pkg/domain/crossks 1.587s

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -tags=intest,deadlock,nextgen -run 'Test.*RuntimeHandle|Test.*Bookkeeper' -count=1
    # ok github.com/pingcap/tidb/pkg/domain/crossks 16.515s

    make bazel_prepare
    # success; reviewed generated pkg/domain/crossks/BUILD.bazel changes

    make lint
    # success

    git diff --check
    git diff --cached --check
    # success

    Spec compliance subagent review
    # approved; verified adjusted milestone requirements against commit 571a893d58eb

    Code quality subagent review
    # approved; noted only a minor direct same-keyspace Acquire test gap, with behavior covered by shared validation

Parent-session verification after review:

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -run 'Test.*RuntimeHandle|Test.*Bookkeeper' -count=1
    # ok github.com/pingcap/tidb/pkg/domain/crossks 1.760s; failpoint refcount returned to 0

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -tags=intest,deadlock,nextgen -run 'Test.*RuntimeHandle|Test.*Bookkeeper' -count=1
    # ok github.com/pingcap/tidb/pkg/domain/crossks 16.850s; failpoint refcount returned to 0

    make lint
    # success

Milestone 2 evidence:

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -tags=intest,deadlock,nextgen -run 'Test.*RuntimeHandle|Test.*Bookkeeper|Test.*Evict|Test.*GCLoop' -count=1
    # red before raw-getter guard: TestEvictRuntimeSkipsRawTouchedEntry failed because the sweeper evicted a raw-touched runtime with stale lastReleaseAt

    make bazel_prepare
    # success; reviewed generated pkg/domain/crossks/BUILD.bazel shard_count change

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -run 'Test.*RuntimeHandle|Test.*Bookkeeper|Test.*Evict|Test.*GCLoop' -count=1
    # ok github.com/pingcap/tidb/pkg/domain/crossks 1.827s; failpoint refcount returned to 0

    ./tools/check/failpoint-go-test.sh pkg/domain/crossks -tags=intest,deadlock,nextgen -run 'Test.*RuntimeHandle|Test.*Bookkeeper|Test.*Evict|Test.*GCLoop' -count=1
    # ok github.com/pingcap/tidb/pkg/domain/crossks 16.673s; failpoint refcount returned to 0

    make lint
    # success

    git diff --check
    # success

    Spec compliance subagent re-review
    # approved; verified the idle sweeper subtask scope

    Code quality subagent re-review
    # approved; verified raw-getter coexistence, lock safety, lifecycle hook placement, and test coverage

## Interfaces and Dependencies

In `pkg/domain/sqlsvrapi/server.go`, define these interfaces:

    type KSRuntime interface {
        Store() kv.Storage
        SessPool() util.DestroyableSessionPool
    }

    type KSRuntimeHandle interface {
        KSRuntime
        Release()
    }

    type Server interface {
        AcquireKSRuntime(targetKS string, bookkeeper string) (KSRuntimeHandle, error)
        // Temporary until the caller migration milestone:
        GetKSSessPool(targetKS string) (util.DestroyableSessionPool, error)
        GetKSStore(targetKS string) (store kv.Storage, err error)
        GetDDLOwnerMgr() owner.Manager
    }

In `pkg/domain/domain.go`, `*Domain` must implement `sqlsvrapi.Server` with `AcquireKSRuntime`. `Domain.Close()` must continue to close all crossKS runtimes through `do.crossKSSessMgr.Close()`.

In `pkg/domain/crossks/cross_ks.go`, `Manager` must expose acquire/release handle behavior to `Domain`. The handle type must satisfy `sqlsvrapi.KSRuntimeHandle` by methods, but the crossKS package should not need to import `sqlsvrapi` if that creates unnecessary coupling.

In `pkg/dxf/framework/scheduler/interface.go`, `Param` must carry the task keyspace runtime or enough derived values to avoid task-specific acquire. The preferred field is:

    TaskRuntime sqlsvrapi.KSRuntime

In `pkg/dxf/framework/taskexecutor/task_executor.go`, `Param` must carry the same non-releasing runtime view for task executors:

    TaskRuntime sqlsvrapi.KSRuntime

DXF boundary bookkeeper names are exactly:

    DXF/scheduler/<taskID>
    DXF/executor/<taskID>

Prometheus metrics must not use `<taskID>`, target keyspace, or full bookkeeper names as labels. Structured logs may include the full bookkeeper and target keyspace.

## Revision Note

This file was converted from a compact design summary into a self-contained ExecPlan. The revision adds living-plan sections required by `PLANS.md`, records the decisions made in discussion, names exact code boundaries and interfaces, and provides concrete implementation and validation steps.
