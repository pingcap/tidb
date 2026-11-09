# The distributed execution framework (DXF), ported from Go

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

Go TiDB runs its long, splittable jobs through the distributed execution
framework, called DXF (Go `pkg/dxf/framework`). A job (a "task") is stored as
a row of `mysql.tidb_global_task`; one node, the "scheduler" owner, splits it
into steps and each step into "subtasks" (rows of
`mysql.tidb_background_subtask`); every node runs a "task executor" that
claims subtasks and executes them; finished tasks move to
`mysql.tidb_global_task_history`. Fast index backfill (Go's ingest path for
`ADD INDEX`) and `IMPORT INTO` are both DXF task types.

The Rust rewrite has only DXF's value types (`rust/crates/tidb-dxf`, from Go
`proto` and `schstatus`) and its operator crate (`rust/crates/tidb-dxf-operator`).
Nothing stores, schedules or executes a task, so register rows O10 (no DXF),
D02 (no ingest backfill), D11 (no disk-full auto pause, whose producer is the
DXF add-index task) and E05 (`IMPORT INTO` refused) stay unresolved. After
this plan, a Rust node runs Go's DXF: a submitted task is scheduled, split,
executed by the nodes' task executors and archived, through Go's tables and
Go's SQL, so the task types that depend on it can be ported next.

Observable outcome: on a Rust node, Go's `example` task type (Go
`pkg/dxf/example`, a three-step test application) submitted through the
framework's handle runs to `succeed`, and `SELECT state FROM
mysql.tidb_global_task_history` shows it; the same submission against a Go
tidb-server shows the same rows.

## Progress

- [x] Milestone 1: `storage` (Go `TaskManager`) over the domain's DXF session pool.
  - [x] 2026-11-09: crate `rust/crates/tidb-dxf-storage` with Go's column
    lists, sentinel errors, the session-context trait, `WithNewSession`,
    `WithNewTxn`, `ExecuteSQLWithNewSession` and `converter.go`.
  - [x] `nodes.go`, `task_table.go` (all of it, with `TestSerializeErr` and
    `TestSplitSubtasks`), `task_state.go`, `subtask_state.go`, `history.go`
    (`TestClassifyTaskError`, `TestIsCancelledErr`).
  - [x] Go `pkg/objstore/recording`, `dxfmetric`, `metering` (classic
    kernel; next-gen `Meter` is O15's) and `taskexecutor/execute`.
  - [x] The domain's `dxfSessionPool` (capacity 100, the system-session
    factory) and the cluster node's storage session context
    (`rust/crates/tidb-session/src/domain/cluster_session_node/dxf_sessions.rs`).
  - [x] Go `testutil`'s `table_util.go` and `task_util.go`
    (`rust/crates/tidb-dxf-testutil`).
  - [x] Go's store tests (`table_test.go`, `task_state_test.go`): 29 run on
    a unistore node in
    `rust/crates/tidb-session/src/domain/cluster_session_node/tests/unistore_cop/dxf_storage.rs`.
    Not transcreated: `TestWithNewTxnRollbackOnCanceledCtx` and the
    cancelled-context half of `TestCancelAndExecIdChanged` (see Surprises).
  - [ ] `dxfutil` moves to milestone 3: it needs Go's `sqlsvrapi.Runtime`
    (on a classic kernel, the node's own runtime; other keyspaces are O15).
- [x] Milestone 2: `handle` (`rust/crates/tidb-dxf-handle`) and `planner`
  (`rust/crates/tidb-dxf-planner`), with Go's `TestRunWithRetry`,
  `TestGetTargetScope`, `TestGetDefaultRegionSplitConfig`,
  `TestCalculateRequiredNodes`, `TestGetNeededNodes`, `TestPhysicalPlan`,
  and, on a unistore node, `TestScheduleFlag` and `TestPlanner`. Go's
  `vardef.ServiceScope` now exists (`tidb_vardef::SERVICE_SCOPE`, with its
  `SET GLOBAL` hook and the server flag); Go's `context` package moved to
  `rust/crates/tidb-gocontext`. Waiting on their callers: `GetCloudStorageURI`
  and `NewObjStore*` (Go `pkg/objstore`), `GetScheduleTuneFactors` (a meta
  transaction through the session), `SendRowAndSizeMeterData`, and the
  status port's `GetBusyNodes` / `GetScheduleStatus` (O15). `TestHandle`
  needs the scheduler (milestone 4).
- [ ] Milestone 3: `dxfutil` and `scheduler` (manager, nodes, slots,
  balancer, state transitions; autoscaler where it applies to a classic
  kernel).
  - [x] Go `pkg/domain/sqlsvrapi` (`rust/crates/tidb-domain-sqlsvrapi`),
    the domain as its server and current-keyspace runtime
    (`tidb_domain_core::domain::sqlsvr`, with Go `crossks`'s classic
    refusals), and `dxfutil` (`rust/crates/tidb-dxf-dxfutil`) with Go's
    `TestAcquireTaskRuntime` and `TestCheckTaskRuntime`.
  - [x] `scheduler` (`rust/crates/tidb-dxf-scheduler`): the TaskManager
    interface, slots, nodes, state transforms, autoscaler calculations,
    Extension/Param/factories/cleaners, BaseScheduler, the Manager and the
    balancer; Go `pkg/ingestor/errdef` (`rust/crates/tidb-ingestor-errdef`)
    and client_golang's `DeletePartialMatch` (`tidb_metrics`). Go's slot,
    node, state-transform, autoscaler, `scheduler_nokit_test.go` and
    `balancer_test.go` tests pass over gomock-style mocks (`src/mock.rs`).
  - [ ] `scheduler_manager_nokit_test.go`; the testkit tests
    (`scheduler_test.go`, `scheduler_manager_test.go`) need the executor and
    the domain composition (milestones 4 and 5).
- [ ] Milestone 4: `taskexecutor` (manager, slots, executor; `execute` is done).
- [ ] Milestone 5: composition in the domain (Go `InitDistTaskLoop`, `distTaskFrameworkLoop`) and the `example` task type end to end.
- [ ] Milestone 6: `metering` and `dxfmetric` where a classic kernel uses them.

## Surprises & Discoveries

- 2026-11-09: Go's packages live under `pkg/dxf`, not `pkg/disttask`
  (only `pkg/util/disttask` keeps the old name). The framework is about 9,200
  lines of Go without tests: scheduler 2,763, storage 2,508, taskexecutor
  1,592 plus `execute` 340, proto 897 (already ported), handle 738,
  metering 610, dxfmetric 278, planner 174, schstatus 173 (already ported),
  dxfutil 92. `importinto` (4,594 plus `conflictedkv` 1,211) is E05's, not
  this plan's.
- The system tables already exist in the Rust bootstrap
  (`rust/crates/tidb-metadef/src/bootstrap_tables.rs`).
- 2026-11-09: every Go `TaskManager` method takes a `ctx`, which carries
  cancellation and the internal source type. The Rust internal-SQL path
  reads only a request source from its context and has no cancellation, so
  the storage methods take no context (only `WithNewTxn` tags its statements
  `DistTask`, as Go does). Go's cancelled-context tests wait on that SQL-layer
  capability; the scheduler and executor ports must decide how to stop
  in-flight SQL when they get there.
- A SQL error must keep its terror identity: Go stores the task error's RFC
  code (`taskErrorCode`) and matches `[kv:1062]` text (`isDataError`).
  `StorageError::Terror` carries it, and `serializeErr` writes it like Go
  (bytes pinned by an overlay test in Go's own package).
- Go's zero `time.Time` is 0001-01-01, not the Unix epoch; NULL times read
  back as `tidb_dxf::go_zero_time()`.

## Decision Log

- 2026-11-09: Go's dependency order is deeper than the milestone list first
  assumed: `storage` uses `execute.SubtaskSummary`, `execute` uses
  `metering` and `objstore/recording`, and `metering` uses `dxfmetric`. The
  order is therefore `proto` (done), `objstore/recording`, `dxfmetric`,
  `metering`, `execute`, then the rest of `storage`, `handle`, `planner`,
  `taskexecutor`, `scheduler`. The `storage` methods already ported use none
  of these and stay; milestone 6's metering moves ahead of milestone 1's
  remainder.

- 2026-11-09: `scheduler` comes before `taskexecutor`: Go's taskexecutor
  imports the scheduler package (its SQL retry settings), not the other way.
- 2026-11-09: Go code that mutates through shared pointers must keep that
  aliasing: the balancer rewrites the subtasks' own exec IDs, which its
  deferred `updateUsedNodes` then counts (Go's balancer case 4 caught a
  clone-based port). Go tests whose gomock `DoAndReturn` mutates the
  scheduler's own task answer the refresh's `GetTaskByID` instead.
- Go's `sqlsvrapi` pool hands out `pools.Resource`; the Rust traits are
  generic over the session view their user declares. A runtime answers its
  store's keyspace itself, since the narrowed Rust `kv.Storage` has none;
  the domain's runtime hands DXF its DXF pool, over the same session
  factory as Go's `sysSessionPool`.
- 2026-11-09: O10 is ported before M01 (MPP fragments) and X01 (pushdown
  authority) because one framework unblocks D02, D11 and E05 (user's choice
  over the full register, by gap kind: user-visible capability first).
- 2026-11-09: this plan replaces `docs/system-table-sql-execplan.md` as the
  one active ExecPlan; Y01's remaining work stays in its register row (the
  replaced plan's detail is in git at commit `d64db1f96bc`).
- Port one whole Go package per milestone, in Go's dependency order, keeping
  Go's names and structure (repository rule: `AGENTS.md`, "Go TiDB is the
  source of truth"). A package's Go tests are transcreated with it; a
  failing transcreated test is the work list.
- 2026-11-09: the domain opens `dxfSessionPool` over the same session
  factory as its advanced pool; a domain system session hands itself over
  as storage's session context (`DomainSysSession::into_dxf_session`)
  rather than making storage's trait a supertrait, whose `commit_txn` and
  `affected_rows` would collide with the domain's.
- Go's `failpoint.Inject` / `InjectCall` sites the storage tests drive are
  `fail::fail_point!`s behind the crate's `failpoints` feature.
- The storage layer runs Go's SQL text on a pooled internal session, as Go's
  `TaskManager` does through `dxfSessionPool`; no raw record writes (the
  very mismatch Y01 records elsewhere).

## Outcomes & Retrospective

Nothing yet.

## Context and Orientation

Go owners, all under `pkg/dxf/framework`:

- `proto`: task, subtask, step, node and modification types. Ported in
  `rust/crates/tidb-dxf/src` (`task.rs`, `subtask.rs`, `step.rs`, `node.rs`,
  `modify.rs`, `task_type.rs`); `schstatus` in `schstatus.rs`.
- `dxfutil`: small helpers (`util.go`).
- `storage`: `TaskManager` (75 methods), the only code that reads or writes
  the DXF tables, through `ExecRestrictedSQL`-style statements on a session
  from a pool (`task_table.go`, `task_state.go`, `subtask_state.go`,
  `history.go`, `nodes.go`, `converter.go` row-to-struct).
- `handle`: the API task types use to submit a task and wait for it
  (`SubmitTask`, `WaitTask`, `CancelTask`).
- `planner`: turns a task's step into subtask metas (`planner.go`, `plan.go`).
- `scheduler`: on the owner node, `Manager` watches tasks and runs one
  `Scheduler` per task through its state transitions (`state_transform.go`),
  places subtasks on nodes (`nodes.go`, `slots.go`, `balancer.go`).
- `taskexecutor`: on every node, `Manager` claims subtasks within its slots
  and runs a `TaskExecutor`, which calls the task type's step executor
  (`execute/interface.go`).
- Composition: Go `pkg/domain/domain.go` `InitDistTaskLoop` builds the
  `TaskManager` over `do.dxfSessionPool` and `distTaskFrameworkLoop` starts
  the executor manager and, on the owner, the scheduler manager.
- `pkg/dxf/example`: a minimal task type (three steps) used as the
  end-to-end proof.

Terms: a "task" is one job row; a "step" is a phase of a task; a "subtask"
is one unit of a step assigned to one node; "slots" are a node's CPU budget
that subtasks reserve; the "owner" is the node elected to run schedulers.

## Plan of Work

Milestone 1 ports `storage` (then `dxfutil`, which needs `storage` and the
keyspace runtime, row O15) into a new crate `rust/crates/tidb-dxf-storage`
(Go package name kept in the module docs). Go's `TaskManager` holds only a
`util.SessionPool`; every method runs Go's SQL text through
`WithNewSession` (a pooled session, its `TxnEntrySizeLimit` raised to the
global value for the call) or `WithNewTxn` (`begin` as SQL, then the
session's `CommitTxn` / `RollbackTxn`), with `sqlexec.ExecSQL`
(`ExecuteInternal`, then drain the record set). The Rust shape already
exists in this tree: `tidb_syssession::Pool<C>` and its sessions, whose
context gives Go's `SQLExecutor` (`rust/crates/tidb-sqlexec/src/lib.rs`
`execute_internal`). The crate declares the `sessionctx.Context` subset it
uses as a trait extending `tidb_syssession::SessionContext` (commit, the
`TxnEntrySizeLimit` session variable), as TTL does with its
`SysSessionContext` (`rust/crates/tidb-ttl-ttlworker/src/lib.rs`), and the
cluster node implements it next to TTL's
(`rust/crates/tidb-session/src/domain/cluster_session_node/ttl_sessions.rs`).
Go's `storage` tests (`task_table_test.go` and the rest) are transcreated
against a unistore node.

Milestone 2 ports `handle` and `planner` next to it. Milestone 3 ports
`taskexecutor` with `execute`; milestone 4 ports `scheduler`. Each keeps
Go's goroutine structure as threads, as the executor ports in this tree do.

Milestone 5 composes them in the domain where Go's `InitDistTaskLoop` does,
behind `tidb_enable_dist_task`, ports `pkg/dxf/example`, and proves a task
end to end. Milestone 6 ports metering where a classic kernel uses it.

## Concrete Steps

From `rust/`, per milestone: `cargo check -p <crate> --tests`, then
`cargo nextest run -p <crate>`; before a commit, the suites of every crate
whose behavior changed; before a push, `cargo build --locked -p
tidb-server`. Build with `CARGO_INCREMENTAL=0`.

## Validation and Acceptance

Milestone 5's acceptance: on a Rust unistore node with
`tidb_enable_dist_task = ON`, submitting the `example` task through the
handle ends with one `mysql.tidb_global_task_history` row in state
`succeed` and its subtasks in `mysql.tidb_background_subtask_history`; the
same submission on a Go tidb-server gives the same states and step counts.
Each milestone's transcreated Go tests pass.

## Idempotence and Recovery

Every milestone adds new crates or new code behind the framework's switch;
a half-done milestone leaves existing behavior unchanged. The DXF tables
are ordinary system tables; a test cluster is rebuilt from scratch.

## Interfaces and Dependencies

The storage layer depends on an internal-session pool (the domain's
`dxfSessionPool` counterpart) able to run SQL and transactions; the
scheduler depends on owner election (the DDL owner manager in this tree is
the model) and on the node list (`mysql.dist_framework_meta`). Task types
register their scheduler and executor factories as Go's `RegisterSchedulerFactory`
and `RegisterTaskType` do.
