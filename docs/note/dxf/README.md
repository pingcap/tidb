# DXF Notes

This note is a navigation map for DXF (Distributed eXecution Framework).
Use it to quickly find:

- the core framework implementation under `pkg/dxf/`
- business integrations that use DXF but live outside `pkg/dxf/`

## Scope

- Core framework: `pkg/dxf/framework/`
- IMPORT INTO DXF app: `pkg/dxf/importinto/`
- DDL distributed backfill DXF app: `pkg/ddl/`
- SQL/user-facing import entry: `pkg/executor/` and `pkg/executor/importer/`
- Runtime bootstrap and ownership loops: `pkg/session/` and `pkg/domain/`

## Reading Rule

- If a package has package-level docs (`doc.go`), read that first.
  - For DXF framework, start with `pkg/dxf/framework/doc.go`.

## Core DXF Framework Map

- `pkg/dxf/framework/proto/`: task type/step constants, task-subtask models,
  and state machine enums.
- `pkg/dxf/framework/handle/`: task submission/control APIs (submit, wait,
  pause/resume, cancel, modify).
- `pkg/dxf/framework/storage/`: task manager and dist-task table persistence.
- `pkg/dxf/framework/scheduler/`: owner-side scheduler manager and task
  scheduler extension interfaces.
- `pkg/dxf/framework/taskexecutor/`: node-side executor manager and task
  executor extension interfaces.
- `pkg/dxf/framework/planner/`: logical-plan to physical-plan/task creation
  helpers used by DXF apps.
- `pkg/dxf/operator/`: operator/pipeline utilities reused by DXF apps.

## High-Signal Entry Files

- Runtime bootstrap and loops:
  - `pkg/session/session.go`: registers IMPORT INTO DXF scheduler/executor in
    bootstrap (`proto.ImportInto`).
  - `pkg/ddl/ddl.go`: registers DDL backfill DXF scheduler/executor and cleanup
    hooks (`proto.Backfill`).
  - `pkg/domain/domain.go`: starts executor manager on all nodes and starts/stops
    scheduler manager based on DDL owner role.

- IMPORT INTO integration:
  - `pkg/dxf/importinto/job.go`: DXF task submission + job/task lifecycle bridge.
  - `pkg/dxf/importinto/planner.go`: IMPORT INTO logical/physical planning by DXF
    step.
  - `pkg/dxf/importinto/scheduler.go`: IMPORT INTO scheduler extension.
  - `pkg/dxf/importinto/task_executor.go` and
    `pkg/dxf/importinto/subtask_executor.go`: executor + step executors.
  - `pkg/executor/import_into.go` and `pkg/executor/importer/import.go`: SQL
    executor/planning and user-facing behavior around IMPORT INTO.

- DDL backfill integration:
  - `pkg/ddl/index.go`: add-index flow that prepares/uses distributed backfill.
  - `pkg/ddl/backfilling_dist_scheduler.go`: backfill scheduler extension.
  - `pkg/ddl/backfilling_dist_executor.go`: backfill task executor extension.
  - `pkg/ddl/backfilling_clean_s3.go`: backfill cleanup hook implementation.

- Framework internals:
  - `pkg/dxf/framework/proto/task.go`: task rank/order (`priority`, `create_time`,
    `id`), slot-related task fields, and runtime slot calculation.
  - `pkg/dxf/framework/proto/step.go`: task-type step definitions and step-order
    contracts (includes compatibility notes on step constants).
  - `pkg/dxf/framework/scheduler/scheduler_manager.go`: owner-side task admission,
    slot reservation, cleanup/historical transfer loops.
  - `pkg/dxf/framework/taskexecutor/manager.go`: node-side executor start/stop,
    preemption handling, and meta recovery loop.
  - `pkg/dxf/framework/scheduler/slots.go` and
    `pkg/dxf/framework/taskexecutor/slot.go`: slot/stripe reservation logic and
    preemption decisions.

- Learning/edge-case references:
  - `docs/note/import-into/README.md`: IMPORT INTO conflict-resolution and
    cleanup troubleshooting map.
  - `pkg/dxf/example/`: minimal DXF app skeleton (scheduler + executor extension
    wiring) useful when adding a new task type.

## Runtime Flow (Cheatsheet)

1. Business logic builds task metadata/logical plan and submits a DXF task.
2. Framework persists task/subtask state through `framework/storage`.
3. `Domain` starts executor-manager loop on every TiDB node.
4. Scheduler-manager loop runs only while the local node is DDL owner.
5. Scheduler extension advances steps and dispatches subtasks.
6. Executor extension executes subtasks and reports state/summary.

## New Task Type Checklist

- Define/extend task type and step enums in `pkg/dxf/framework/proto/` first.
  - Keep step values backward-compatible (`step.go` explicitly forbids changing
    existing constant values).
- Implement scheduler extension (`scheduler.Extension`) and task executor extension
  (`taskexecutor.Extension`) for the new task type.
- Register all required factories:
  - owner side: `scheduler.RegisterSchedulerFactory(...)`
  - node side: `taskexecutor.RegisterTaskType(...)`
  - optional cleanup: `scheduler.RegisterSchedulerCleanUpFactory(...)`
- Keep `GetNextStep` deterministic from task base state (avoid relying on mutable
  task meta there), and keep subtask generation stable when using batch switch APIs.

## Invariants and Pitfalls

- Task rank drives both scheduling and preemption: higher rank means smaller
  `(priority, create_time, id)` tuple.
- `RequiredSlots` is the reservation baseline, while runtime execution may use a
  lower slot count through `ExtraParams.MaxRuntimeSlots` + `TargetSteps`.
- Empty target scope prefers `"background"` nodes when present; otherwise it falls
  back to empty-scope nodes.
- Scheduler manager processes normal runnable states with slot allocation, but at
  `MaxConcurrentTask` limit it switches to no-resource states only (for fast handling
  of pausing/cancelling/reverting/modifying tasks).
- Task executor can exit after a period with no runnable subtasks (about 10s),
  then be restarted by manager loops; this is expected behavior for resource reuse.

## Debug Focus Areas

- Task state transitions and scheduling-state handlers:
  `pkg/dxf/framework/scheduler/state_transform.go` and
  `pkg/dxf/framework/scheduler/scheduler.go`.
- Slot reservation / preemption decisions:
  `pkg/dxf/framework/scheduler/slots.go`,
  `pkg/dxf/framework/taskexecutor/slot.go`, and
  `pkg/dxf/framework/taskexecutor/manager.go`.
- Keyspace/scope routing (classic vs nextgen service behavior):
  `pkg/domain/domain.go`, `pkg/dxf/framework/handle/handle.go`, and
  `pkg/dxf/framework/storage/task_table.go`.

## Problem-Oriented Read Order

- "How does task state/lifecycle work?"
  - `framework/proto` -> `framework/storage` -> `framework/scheduler` ->
    `framework/taskexecutor`.

- "Where is submit/pause/resume/cancel implemented?"
  - `framework/handle` first, then follow call sites in `pkg/dxf/importinto/`
    and `pkg/ddl/`.

- "How does IMPORT INTO use DXF?"
  - `pkg/dxf/importinto/` first, then `pkg/executor/import_into.go` and
    `pkg/executor/importer/`.

- "How does DDL backfill use DXF?"
  - `pkg/ddl/index.go` -> `pkg/ddl/backfilling_dist_scheduler.go` ->
    `pkg/ddl/backfilling_dist_executor.go`, and cross-check
    `docs/agents/ddl/README.md`.

## Navigation Queries

- Find DXF registration points:
  `rg --line-number --glob '*.go' 'RegisterSchedulerFactory|RegisterSchedulerCleanUpFactory|RegisterTaskType' pkg/session pkg/ddl pkg/dxf`
- Find framework control APIs and main call sites:
  `rg --line-number --glob '*.go' 'SubmitTask|WaitTask|CancelTask|PauseTask|ResumeTask|ModifyTaskByID' pkg/dxf pkg/executor pkg/ddl`
- Find IMPORT INTO integration points:
  `rg --line-number --glob '*.go' 'proto.ImportInto|importinto' pkg/dxf pkg/executor pkg/session`
- Find DDL distributed backfill integration points:
  `rg --line-number --glob '*.go' 'proto.Backfill|backfill' pkg/ddl`

## Test Surfaces

- Framework tests: `pkg/dxf/framework/integrationtests/` and sibling framework
  package tests.
- IMPORT INTO DXF tests: `pkg/dxf/importinto/`.
- DDL backfill DXF tests: `pkg/ddl/backfilling_*_test.go` and related
  distributed-backfill tests under `pkg/ddl/`.
