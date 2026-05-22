# Auto-pause DXF add-index when TiKV disk is full

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

When a distributed add-index DDL writes or ingests index data into TiKV and TiKV reports disk full, TiDB should stop turning the whole DDL into a failed job. Instead, TiDB should pause the DDL job with a durable reason that says it was paused by the system because TiKV disk was full. Operators can expand TiKV capacity and then resume the same job. If the expansion is still insufficient, the resumed job may hit disk full again and auto-pause again.

The visible outcome is that `ALTER TABLE ... ADD INDEX` or `ADD PRIMARY KEY` using DXF backfill returns an actionable DDL error, the persisted DDL job is paused with a durable pause reason, and an end user can resume this specific system-paused job.

## Progress

- [x] (2026-05-22 Asia/Shanghai) Read repository DDL guidance and relevant add-index / reorg docs.
- [x] (2026-05-22 Asia/Shanghai) Captured user decisions for first-version scope.
- [x] (2026-05-22 Asia/Shanghai) Add typed TiKV disk-full propagation for local ingest space checks and failpoint paths.
- [x] (2026-05-22 Asia/Shanghai) Add DXF task metadata and scheduler transition from failed disk-full subtask to paused task.
- [x] (2026-05-22 Asia/Shanghai) Add durable DDL job pause reason and resume semantics for system disk-full pauses.
- [x] (2026-05-22 Asia/Shanghai) Wire DXF add-index task submission and waiting code to auto-pause the owning DDL job.
- [x] (2026-05-22 Asia/Shanghai) Add targeted scheduler, storage, DDL, ingest, and RealTiKV add-index failpoint tests.
- [x] (2026-05-22 Asia/Shanghai) Run WIP validation, Bazel prepare, lint, and compile-level RealTiKV validation.

## Surprises & Discoveries

- Observation: `handle.WaitTaskDoneOrPaused` currently returns `nil` when a task is paused, regardless of why it was paused.
  Evidence: `pkg/dxf/framework/handle/handle.go` returns `nil` for `proto.TaskStatePaused`.
- Observation: DXF local-sort add-index performs TiKV local ingest during `BackfillStepReadIndex`, while global-sort uses a later write-and-ingest step.
  Evidence: `pkg/ddl/backfilling_dist_scheduler.go` maps `BackfillStepReadIndex` directly to done when `GlobalSort` is false.
- Observation: `make bazel_prepare` is required for this patch because it adds top-level Go test functions and import dependencies.
  Evidence: `git diff -U0 -- '*.go'` shows new `TestResumeSystemPausedDDLJobWithKVDiskFullReason`, `TestSchedulerAutoPauseOnKVDiskFull`, `TestPauseTaskOnError`, and `TestAddIndexDistAutoPauseOnKVDiskFull`.
- Observation: the local RealTiKV environment currently cannot run the new RealTiKV test end-to-end.
  Evidence: the TiUP playground PD reported readiness, but TiDB bootstrap failed before the test logic with an unimplemented `QueryRegion` RPC against the local playground version.

## Decision Log

- Decision: Use `model.Job.PauseReason` with type `tikv_disk_full` as the source of truth for allowing an end user to resume a system-paused job.
  Rationale: The job error text is for observability and may change; resume authorization should use structured persisted metadata.
  Date/Author: 2026-05-22 / Codex with user confirmation.
- Decision: Opt in only DXF add-index backfill tasks via a new DXF task extra parameter.
  Rationale: TiKV disk full should not silently change failure semantics for unrelated DXF task types such as `IMPORT INTO`.
  Date/Author: 2026-05-22 / Codex with user confirmation.
- Decision: Do not estimate remaining bytes before resume in this version.
  Rationale: The first closed loop accepts repeated auto-pause if capacity is still insufficient after expansion.
  Date/Author: 2026-05-22 / Codex with user confirmation.

## Outcomes & Retrospective

Implementation is complete for the first-version scope: DXF add-index backfill opts into disk-full auto-pause, TiKV disk-full is recognized through typed `errdef.ErrKVDiskFull`, the DXF scheduler converts the opted-in task to `pausing`, and the DDL job records durable pause metadata in `model.Job.PauseReason`.

The design keeps the blast radius small by putting the behavior behind a DXF task extra parameter rather than changing all DXF task failures. User resume is still normally constrained by the original pauser, with a narrow exception for system-paused `tikv_disk_full` jobs.

## Context and Orientation

TiDB DDL is job-based. A SQL DDL statement becomes a persisted `model.Job` in `mysql.tidb_ddl_job`. The DDL owner executes the job and can survive owner failover because the job state is durable. Add-index and add-primary-key DDLs may use DXF, the distributed execution framework, for backfill work. DXF persists a global task in `mysql.tidb_global_task` and subtasks in `mysql.tidb_background_subtask`.

The existing pause mechanism has two layers. The DDL job can be paused through `pkg/ddl/ddl.go`, and the associated DXF task can be paused through `pkg/dxf/framework/storage.TaskManager.PauseTask`. For this feature, DXF first detects that an opted-in task failed because a subtask hit typed `errdef.ErrKVDiskFull`. The scheduler converts that task to pausing/paused rather than reverting/failed. Then the add-index DDL worker observes the paused task, marks the owning DDL job as system-paused with reason `tikv_disk_full`, and returns a clear DDL error to the original SQL session.

## Plan of Work

First, make disk-full errors typed. In `pkg/ingestor/ingestctrl/local.go`, `checkDiskAvail` should return `errdef.ErrKVDiskFull` with the existing human-readable message. In `pkg/ingestor/ingestctrl/job_worker.go`, the `WriteToTiKVNotEnoughDiskSpace` failpoint should return the same typed error. Add a small helper in `pkg/ingestor/errdef/errors.go` so callers can recognize wrapped disk-full errors consistently.

Second, extend DXF task extra params in `pkg/dxf/framework/proto/task.go` with `PauseOnKVDiskFull`. Add a task manager method that atomically moves a running task to `pausing` with a stored task error and converts failed subtasks for that task step into paused subtasks. Update `pkg/dxf/framework/scheduler/scheduler.go` so opted-in tasks whose failed subtasks all report `ErrKVDiskFull` enter pausing instead of reverting.

Third, add DDL metadata. In `pkg/meta/model/job.go`, add optional `PauseReason` metadata with type and message, plus helpers to set, clear, and test it. In `pkg/ddl/ddl.go`, allow an end-user resume only for system-paused jobs whose reason type is `tikv_disk_full`, and clear the pause reason and error on resume. Add a system helper that pauses a job with this reason without incrementing `ErrorCount`.

Fourth, wire add-index. In `pkg/ddl/index.go`, submit DXF backfill tasks with `PauseOnKVDiskFull`. Replace the waiting path with one that can inspect the paused DXF task. If the task is paused because of `ErrKVDiskFull`, call the DDL helper to set the system pause reason and return a new DDL error that tells the SQL caller the job was paused.

Fifth, test. Add a DXF scheduler unit test for the new `PauseOnKVDiskFull` transition. Add a DDL-level failpoint test that proves a DXF add-index disk-full condition pauses the DDL job with reason and allows user resume according to the decided semantics.

## Concrete Steps

Run commands from `/Users/xiaoli/coding/tidb`.

During implementation, inspect changed files with:

    git status --short
    git diff --stat

Because this change adds or changes Go imports and likely adds a test function, run:

    make bazel_prepare

For WIP validation, run targeted failpoint-enabled tests for touched failpoint packages. Exact test names will be added after tests are written.

For Ready validation before final status, run the targeted tests plus:

    make lint

## Validation and Acceptance

Acceptance criteria:

1. A DXF add-index or add-primary-key backfill task created by DDL has `PauseOnKVDiskFull` enabled.
2. If an opted-in DXF task has failed subtasks whose errors are typed `errdef.ErrKVDiskFull`, the scheduler transitions the task to `pausing`, then existing pausing logic reaches `paused`.
3. When the DDL add-index worker observes that paused DXF task, the DDL job is persisted as paused by system with `PauseReason.Type == "tikv_disk_full"` and no `ErrorCount` increment.
4. `ADMIN RESUME DDL JOBS` by an end user is accepted for this system-paused disk-full job and clears the running job's pause reason.
5. Non-opted-in DXF tasks and non-disk-full subtask failures keep existing failure/revert behavior.

## Idempotence and Recovery

All edits are ordinary source changes and can be rerun through tests. The new DXF transition is idempotent because multiple scheduler ticks can only update a task from its current state to `pausing`; after it is `pausing` or `paused`, the normal scheduler state machine handles it. If implementation proves an assumption wrong, update this plan before changing direction.

## Artifacts and Notes

Validation commands run so far:

    GOPATH=$HOME/go GOSUMDB=sum.golang.org make mockgen
    GOPATH=$HOME/go GOSUMDB=sum.golang.org GO111MODULE=on tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/dxf/framework/scheduler Scheduler,CleanUpRoutine,TaskManager > pkg/dxf/framework/mock/scheduler_mock.go
    gofmt -w pkg/ddl/db_test.go pkg/ddl/ddl.go pkg/ddl/executor.go pkg/ddl/index.go pkg/ddl/job_scheduler.go pkg/dxf/framework/handle/handle.go pkg/dxf/framework/proto/task.go pkg/dxf/framework/scheduler/interface.go pkg/dxf/framework/scheduler/scheduler.go pkg/dxf/framework/scheduler/scheduler_nokit_test.go pkg/dxf/framework/storage/task_state.go pkg/dxf/framework/storage/task_state_test.go pkg/ingestor/errdef/errors.go pkg/ingestor/ingestctrl/job_worker.go pkg/ingestor/ingestctrl/local.go pkg/ingestor/ingestctrl/local_test.go pkg/meta/model/job.go tests/realtikvtest/addindextest1/disttask_test.go
    git diff --check
    GOPATH=$HOME/go GOSUMDB=sum.golang.org ./tools/check/failpoint-go-test.sh pkg/dxf/framework/scheduler -run TestSchedulerAutoPauseOnKVDiskFull -count=1
    GOPATH=$HOME/go GOSUMDB=sum.golang.org ./tools/check/failpoint-go-test.sh pkg/dxf/framework/storage -run TestPauseTaskOnError -count=1
    GOPATH=$HOME/go GOSUMDB=sum.golang.org ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -run TestCheckDiskAvail -count=1
    GOPATH=$HOME/go GOSUMDB=sum.golang.org ./tools/check/failpoint-go-test.sh pkg/ddl -run TestResumeSystemPausedDDLJobWithKVDiskFullReason -count=1
    GOPATH=$HOME/go GOSUMDB=sum.golang.org make bazel_prepare
    GOPATH=$HOME/go GOSUMDB=sum.golang.org make lint
    GOPATH=$HOME/go GOSUMDB=sum.golang.org go test -c -tags=intest,deadlock ./tests/realtikvtest/addindextest1 -o /tmp/addindextest1.test

The RealTiKV test `TestAddIndexDistAutoPauseOnKVDiskFull` was added, but local end-to-end execution is blocked by the local TiUP playground/PD RPC compatibility issue described above.

## Interfaces and Dependencies

New expected interfaces:

In `pkg/meta/model/job.go`:

    type JobPauseReason struct {
        Type string `json:"type"`
        Message string `json:"message,omitempty"`
    }

    const JobPauseReasonKVDiskFull = "tikv_disk_full"

In `pkg/dxf/framework/proto/task.go`:

    type ExtraParams struct {
        PauseOnKVDiskFull bool `json:"pause_on_kv_disk_full,omitempty"`
    }

In `pkg/dxf/framework/scheduler/interface.go`, the scheduler task manager gains a method to pause a task because of a stored error.
