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

## Runtime Flow (Cheatsheet)

1. Business logic builds task metadata/logical plan and submits a DXF task.
2. Framework persists task/subtask state through `framework/storage`.
3. `Domain` starts executor-manager loop on every TiDB node.
4. Scheduler-manager loop runs only while the local node is DDL owner.
5. Scheduler extension advances steps and dispatches subtasks.
6. Executor extension executes subtasks and reports state/summary.

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

## Fast Search Commands

```bash
# DXF registration points.
rg --line-number --glob '*.go' \
  'RegisterSchedulerFactory|RegisterSchedulerCleanUpFactory|RegisterTaskType' \
  pkg/session pkg/ddl pkg/dxf

# Framework control APIs and their call sites.
rg --line-number --glob '*.go' \
  'SubmitTask|WaitTask|CancelTask|PauseTask|ResumeTask|ModifyTaskByID' \
  pkg/dxf pkg/executor pkg/ddl

# IMPORT INTO + DXF integration points.
rg --line-number --glob '*.go' 'proto.ImportInto|importinto' \
  pkg/dxf pkg/executor pkg/session

# DDL distributed backfill + DXF integration points.
rg --line-number --glob '*.go' 'proto.Backfill|backfill' pkg/ddl
```

## Test Surfaces

- Framework tests: `pkg/dxf/framework/integrationtests/` and sibling framework
  package tests.
- IMPORT INTO DXF tests: `pkg/dxf/importinto/`.
- DDL backfill DXF tests: `pkg/ddl/backfilling_*_test.go` and related
  distributed-backfill tests under `pkg/ddl/`.
