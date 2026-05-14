# Log observed TiKV capacity increase for DXF add-index

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, a distributed add-index job (DXF add-index) will log two size signals when the task succeeds:

- `logical_index_kv_bytes`: the logical size of encoded index KVs produced by the add-index job.
- `observed_tikv_capacity_increase_bytes`: the observed increase in TiKV used capacity while the add-index task ran.

Operators will be able to inspect TiDB logs and see both the logical KV size and the observed cluster-side capacity growth without querying new system tables or relying on `ADMIN SHOW DDL JOBS`.

## Progress

- [x] (2026-05-14 09:10 +08:00) Read repository policy, DDL docs, add-index DXF paths, and current metering/summary code paths.
- [x] (2026-05-14 09:18 +08:00) Chose the implementation strategy: persist only the initial TiKV usage snapshot in dist-task meta, compute the final observed delta at task success, and report both values through logs only.
- [x] (2026-05-14 09:42 +08:00) Implemented TiKV usage snapshot collection, task-meta persistence, read-index byte aggregation, and success logging helpers in the DDL owner path.
- [x] (2026-05-14 09:48 +08:00) Added targeted tests for TiKV usage aggregation and wired the existing RealTiKV DXF add-index test to assert the logging payload through a failpoint callback.
- [x] (2026-05-14 11:05 +08:00) Ran targeted DDL unit tests successfully, reproduced RealTiKV validation on an isolated nightly TiUP playground successfully, and recorded the current `make lint` failure as repository-wide pre-existing revive findings unrelated to this patch.

## Surprises & Discoveries

- Observation: DXF add-index already persists the logical index KV byte count indirectly through `read-index` subtask summaries by storing logical KV bytes in `execute.SubtaskSummary.Processed`.
  Evidence: `pkg/ddl/backfilling_read_index.go` updates `summary.Processed`, and `pkg/ddl/backfilling_clean_s3.go` sums it into metering data.

- Observation: Existing cleanup-based metering only runs for global-sort add-index tasks because local-sort cleanup returns early when `CloudStorageURI` is empty.
  Evidence: `pkg/ddl/backfilling_clean_s3.go` returns before metering when `len(taskMeta.CloudStorageURI) == 0`.

- Observation: The existing `GetSubtasksWithHistory` helper already provides the summary payload needed to compute `logical_index_kv_bytes`, so no new DXF storage API was required.
  Evidence: `pkg/dxf/framework/storage/task_table.go` returns both running and history subtasks with the `Summary` field populated.

## Decision Log

- Decision: Log the result from the DDL owner after the dist task succeeds instead of writing final values back into DDL history or DXF task tables.
  Rationale: The user explicitly requested log-only reporting and no new display/storage logic in MySQL system tables or `ADMIN SHOW DDL JOBS`.
  Date/Author: 2026-05-14 / Codex

- Decision: Store only the initial TiKV usage snapshot in `BackfillTaskMeta`.
  Rationale: The initial snapshot must survive pause/resume and owner transfer, but the final result can be computed and logged at success time without persisting the result itself.
  Date/Author: 2026-05-14 / Codex

- Decision: Reuse the existing logical index KV byte source from `read-index` subtask summaries instead of inventing a second aggregation path.
  Rationale: This keeps semantics aligned with current metering and avoids duplicate accounting.
  Date/Author: 2026-05-14 / Codex

## Outcomes & Retrospective

The implementation is complete and the scoped validation supports the intended behavior:

- Targeted DDL unit tests passed for TiKV store-usage aggregation and error handling.
- The existing RealTiKV DXF add-index test was extended and passed on a nightly playground, proving the success path emits the observed-capacity payload and that `logical_index_kv_bytes` matches the read-index summary.
- `make lint` does not pass in the current local tree because `revive` reports hundreds of repository-wide `var-naming` issues in unrelated files. The failure is environmental/repository-baseline noise rather than a regression from this patch.

## Context and Orientation

The relevant code lives in `pkg/ddl/` because add-index is a DDL job owned and resumed by the DDL framework.

`DXF add-index` means the distributed add-index execution path that runs as a dist task. The task key is built in `pkg/ddl/index.go`, and the dist task metadata is defined by `pkg/ddl/backfilling_dist_executor.go`.

`BackfillTaskMeta` is the JSON payload stored with the dist task. It already carries the cloned DDL job, element IDs, cloud-storage URI, and estimated row size. This is the most stable place to keep an initial TiKV usage snapshot because the dist task already persists it across retries and owner transfers.

`logical_index_kv_bytes` already exists conceptually: during `read-index`, the executor reports the logical size of generated index KVs through `execute.SubtaskSummary.Processed`. The code path starts in `pkg/ddl/backfilling_read_index.go` and is already consumed by metering logic in `pkg/ddl/backfilling_clean_s3.go`.

`observed_tikv_capacity_increase_bytes` is different. It must come from TiKV store status snapshots, not logical KV accounting. TiDB can fetch TiKV store status from PD using the PD HTTP client helpers under `pkg/store/helper/` and the PD response structs in `pkg/store/pdtypes/`.

The implementation must exclude TiFlash stores. The repo already has TiFlash label detection logic in `pkg/infoschema/tables.go`; the new code should use the same label semantics instead of inventing a new filter.

## Plan of Work

First, extend `pkg/ddl/backfilling_dist_executor.go` so `BackfillTaskMeta` can store the initial TiKV used-capacity snapshot in bytes. Keep the shape minimal: enough to compute a final delta, but not a full report payload.

Next, add helpers in `pkg/ddl/` to:

- fetch TiKV store status from PD,
- filter out TiFlash stores,
- sum used capacity in bytes across TiKV stores,
- sum `logical_index_kv_bytes` from `read-index` subtask summaries,
- compute `observed_tikv_capacity_increase_bytes` as `max(after-before, 0)`.

Then, update `pkg/ddl/index.go` where the dist task is submitted so the initial snapshot is captured before `handle.SubmitTask(...)` and embedded in `BackfillTaskMeta`.

After that, add a success-only logging hook in the DDL owner path after `handle.WaitTaskDoneOrPaused(...)` returns success. This hook should fetch the finished task from the task manager with history, read the initial snapshot from task meta, collect the final TiKV usage from PD, aggregate `logical_index_kv_bytes` from subtask summaries, and emit one structured log line.

Finally, add tests:

- a unit test in `pkg/ddl/backfilling_dist_scheduler_test.go` or another existing DDL test file for the store-usage aggregation helper using mocked PD store info,
- an end-to-end RealTiKV test by extending an existing add-index test in `tests/realtikvtest/addindextest2/global_sort_test.go`. The test should capture a failpoint callback or structured payload around the logging hook, then assert both `logical_index_kv_bytes` and a non-negative observed capacity delta are present.

## Concrete Steps

Work from repository root:

1. Edit `pkg/ddl/backfilling_dist_executor.go` to extend `BackfillTaskMeta`.
2. Edit `pkg/ddl/index.go` to capture the initial snapshot before dist-task submission and to log final observed results after successful task completion.
3. Edit or extend a DDL helper file under `pkg/ddl/` for PD store aggregation and summary collection. Prefer existing files over adding a new Go file to avoid unnecessary Bazel metadata churn.
4. Update existing tests in `pkg/ddl/backfilling_dist_scheduler_test.go` and `tests/realtikvtest/addindextest2/global_sort_test.go`.

Expected command pattern during development:

    cd /Users/xiaoli/coding/tidb
    go test -run TestBackfillingScheduler -tags=intest,deadlock ./pkg/ddl
    go test -run TestNextGenMetering -tags=intest,deadlock ./tests/realtikvtest/addindextest2/... -args -tikv-path "tikv://127.0.0.1:2379?disableGC=true"
    make lint

If a package-level `pkg/ddl` test requires failpoints, use the failpoint runner instead of plain `go test`.

## Validation and Acceptance

The change is accepted when:

1. The targeted DDL/unit-level test coverage proves the TiKV used-capacity aggregation logic correctly ignores TiFlash stores and sums TiKV `UsedSize`.
2. The RealTiKV add-index test proves a successful DXF add-index run emits a payload containing:
   - the same `logical_index_kv_bytes` value already reported by the read-index summary path, and
   - an `observed_tikv_capacity_increase_bytes` field that is present and non-negative.
3. `make lint` passes from repository root after the code changes.

## Idempotence and Recovery

The code changes are safe to rerun because:

- the initial TiKV usage snapshot is written only when submitting a new dist task,
- logging happens only after successful task completion,
- recomputing the final observed delta is read-only against PD and task history.

If a test run fails midway:

- rerun the targeted `go test` command after fixing the issue,
- for RealTiKV cleanup, stop the playground or mocked cluster according to `docs/agents/testing-flow.md`,
- if dist-task state becomes stale during local debugging, recreate the test table/database before rerunning the test.

## Artifacts and Notes

Pending implementation.

## Interfaces and Dependencies

The implementation is expected to touch these interfaces and types:

- `pkg/ddl/backfilling_dist_executor.go`
  - extend `type BackfillTaskMeta struct` with a TiKV used-capacity snapshot field.

- `pkg/ddl/index.go`
  - update `(*worker).executeDistTask(...)` to capture the initial snapshot before `handle.SubmitTask(...)`,
  - add a success-only helper invocation after the dist task finishes successfully.

- `pkg/store/helper` + `pkg/store/pdtypes`
  - use the existing PD HTTP client and `StoresInfo` / `StoreInfo` / `StoreStatus` structs to read per-store usage.

- `tests/realtikvtest/addindextest2/global_sort_test.go`
  - extend an existing add-index DXF test to assert the logged payload.
