# IMPORT INTO Notes

This note is a navigation and troubleshooting map for IMPORT INTO in DXF.
It focuses on conflict-resolution and cleanup behavior that is easy to miss
when only reading one package at a time.

## Scope

- IMPORT INTO job/task lifecycle (`mysql.tidb_import_jobs` + DXF task).
- Global-sort conflict detection, conflict collection, and conflict resolution.
- Post-process checksum and cleanup behavior.

## High-Signal Entry Files

- Job submit/runtime bridge:
  - `pkg/dxf/importinto/job.go`
- Scheduler and step transitions:
  - `pkg/dxf/importinto/scheduler.go`
  - `pkg/dxf/framework/proto/step.go`
- Step planning and conflict-spec generation:
  - `pkg/dxf/importinto/planner.go`
- Step executors:
  - `pkg/dxf/importinto/task_executor.go`
  - `pkg/dxf/importinto/collect_conflicts.go`
  - `pkg/dxf/importinto/conflict_resolution.go`
  - `pkg/dxf/importinto/subtask_executor.go`
- Cleanup and metadata redaction:
  - `pkg/dxf/importinto/clean_up.go`
- Conflict-KV semantics and examples:
  - `pkg/dxf/importinto/conflictedkv/doc.go`

## Step Flow Cheatsheet

- Local sort path:
  - `StepInit` -> `ImportStepImport` -> `ImportStepPostProcess` -> `StepDone`
- Global sort path:
  - `StepInit` -> `ImportStepEncodeAndSort` -> `ImportStepMergeSort` (optional
    subtasks) -> `ImportStepWriteAndIngest` -> `ImportStepCollectConflicts`
    (optional subtasks) -> `ImportStepConflictResolution` (optional subtasks) ->
    `ImportStepPostProcess` -> `StepDone`

## Conflict-Resolution Troubleshooting

- "Why are collect-conflicts / conflict-resolution steps skipped?"
  - `generateCollectConflictsSpecs` and `generateConflictResolutionSpecs` return
    no specs when `collectConflictInfos` finds no recorded conflicts.
  - Check whether previous step metas have `RecordedConflictKVCount > 0` in:
    - `ImportStepMeta`
    - `MergeSortStepMeta`
    - `WriteIngestStepMeta`

- "Why is checksum verification skipped in post-process?"
  - In collect-conflicts, `TooManyConflictsFromIndex` is set when the bounded
    in-memory handle set exceeds its limit.
  - Limit source: collect-conflicts uses about half of step memory for handle
    tracking (`sizeLimitOfHandlesFromIndex = resource mem / 2`).
  - Post-process checks this flag and skips final checksum verification.

- "Where are conflicted rows persisted?"
  - Collect-conflicts writes conflict-row files under:
    `conflicted-rows/<task-id>/<subtask-id>-<uuid>`
  - The prefix is intentionally outside `<task-id>/` cleanup path, so users can
    inspect conflict rows after task cleanup.

- "Where is each part executed?"
  - Conflict row/checksum collection: `collect_conflicts.go`
  - Conflict KV deletion: `conflict_resolution.go`
  - Core conflict KV semantics: `conflictedkv/doc.go`

## Cleanup Troubleshooting

- Cleanup entrypoint is `ImportCleanUp.CleanUp` in `clean_up.go`.
- Classic kernel behavior:
  - Attempts to switch table mode back to normal.
  - Ignores `infoschema.ErrTableNotExists` during cleanup.
- Global-sort artifact cleanup:
  - Deletes task-scoped files via `external.CleanUpFiles(..., <task-id>)`.
  - Does not delete `conflicted-rows/...` files by design.
- Nextgen metering:
  - On succeed state, cleanup tries to send metering data based on post-process
    subtask meta.
- Metadata redaction:
  - Cleanup path/redaction clears sensitive task fields before task transfer.

## Cross-Keyspace Job Status Notes

- Scheduler updates import job state through task-keyspace task manager
  (`getTaskMgrForAccessingImportJob`).
- On user keyspace tasks, cross-keyspace session-pool retrieval failures are
  wrapped as `errGetCrossKSSessionPool`, and treated as retryable.

## Navigation Queries

- Find step transitions and job-state updates:
  `rg --line-number --glob '*.go' 'GetNextStep|startJob|job2Step|finishJob|failJob|cancelJob' pkg/dxf/importinto`
- Find conflict planning and skip conditions:
  `rg --line-number --glob '*.go' 'generateCollectConflictsSpecs|generateConflictResolutionSpecs|RecordedConflictKVCount|collectConflictInfos' pkg/dxf/importinto`
- Find conflict executor paths:
  `rg --line-number --glob '*.go' 'CollectConflictsStepMeta|ConflictResolutionStepMeta|TooManyConflictsFromIndex|conflicted-rows' pkg/dxf/importinto`
- Find cleanup behavior:
  `rg --line-number --glob '*.go' 'ImportCleanUp|CleanUpFiles|sendMeterOnCleanUp|redactSensitiveInfo' pkg/dxf/importinto pkg/dxf/framework/scheduler`

## Test Surfaces

- `pkg/dxf/importinto/conflict_resolution_test.go`
- `pkg/dxf/importinto/collect_conflicts_test.go`
- `pkg/dxf/importinto/task_executor_test.go`
- `pkg/dxf/importinto/scheduler_test.go`
- `pkg/dxf/importinto/job_testkit_test.go`
