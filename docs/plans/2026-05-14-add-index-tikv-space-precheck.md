# Add TiKV space precheck before DXF add-index submission

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, TiDB owner will reject a distributed add-index job before it submits the DXF backfill task when the predicted TiKV space growth would leave either the whole TiKV cluster or any single TiKV store below the configured safety margin. Operators will see the rejection immediately from the DDL statement instead of discovering disk pressure later while add-index is already running.

The check only runs on the first DXF task submission for add-index. Resume and owner transfer keep the existing task running path unchanged.

## Progress

- [x] (2026-05-14 14:27 +08:00) Re-read `PLANS.md`, DDL docs, and the existing DXF add-index capacity-observation implementation.
- [x] (2026-05-14 14:31 +08:00) Chose the owner-side insertion point: only `(*worker).executeDistTask` on the first task submission path, not resume and not merge-temp-index.
- [x] (2026-05-14 16:04 +08:00) Implemented TiKV cluster/store capacity snapshot helpers and wired them into task metadata without duplicating the existing PD helper path.
- [x] (2026-05-14 16:38 +08:00) Implemented `predict_tikv_index_bytes` from DDL-owned stats access and local index-row size estimation heuristics after ruling out direct planner helper reuse due to an import cycle.
- [x] (2026-05-14 19:41 +08:00) Refactored the planner row-size logic into a new leaf package `pkg/planner/util/rowsize`, converted `pkg/planner/cardinality/row_size.go` into wrappers, and switched DDL prediction to reuse that shared implementation.
- [x] (2026-05-15 12:18 +08:00) Fixed the DDL prediction path to remap `statistics.HistColl` from column IDs to `expression.Column.UniqueID` before calling the shared row-size helper, and added a regression subtest that reproduces the underestimation without that remapping.
- [x] (2026-05-18 15:18 +08:00) Upgraded the DDL predictor from key-only estimation to key-plus-value estimation, including restored-data-aware value sizing for prefix/string indexes such as `pad(32)`.
- [x] (2026-05-18 16:05 +08:00) Split prediction into `basic` and `represent` models, kept the old heuristic model for side-by-side observation, and switched the actual TiKV capacity gate to the new representative-datum/real-encoding model.
- [x] (2026-05-18 17:34 +08:00) Added the `sample` model that samples real rows from random regions on a snapshot, logs it alongside `basic` and `represent`, and switched the actual TiKV capacity gate to `sample_predicted_tikv_index_bytes`.
- [x] (2026-05-14 16:51 +08:00) Enforced the cluster-level and per-store reject conditions before `handle.SubmitTask(...)` on the first DXF task submission path.
- [x] (2026-05-14 17:09 +08:00) Extended existing DDL unit tests to cover snapshot aggregation and reject-condition behavior without adding new top-level tests.
- [x] (2026-05-14 17:37 +08:00) Ran scoped validation, then completed the repository-required `Ready` validation with `make lint`.

## Surprises & Discoveries

- Observation: DXF add-index already persists an initial TiKV usage snapshot in `BackfillTaskMeta` for post-task logging, so the precheck can reuse the same PD store collection path instead of inventing a second capacity source.
  Evidence: `pkg/ddl/index.go` already calls `collectTiKVStoreUsage(...)` before `handle.SubmitTask(...)`.

- Observation: Planner already has `GetIndexAvgRowSize(...)`, which estimates index-row encoded size from stats data and avoids a table scan.
  Evidence: `pkg/planner/cardinality/row_size.go` computes index row size from `statistics.HistColl`.

- Observation: Reusing `pkg/planner/cardinality.GetIndexAvgRowSize(...)` directly from `pkg/ddl` creates an import cycle (`pkg/ddl -> pkg/planner/cardinality -> pkg/planner/core/operator/logicalop -> pkg/planner/core/stats -> pkg/domain -> pkg/ddl`).
  Evidence: the first implementation attempt failed to compile with this cycle, so the fix was to extract the pure row-size logic into a new leaf package `pkg/planner/util/rowsize` and make `cardinality` a wrapper on top of it.

- Observation: After the extraction, DDL still fed `rowsize.GetIndexAvgRowSize(...)` with `expression.Column.UniqueID` values while reusing a raw `statistics.HistColl` keyed by column ID, which made the helper miss real column stats and fall back to pseudo-size estimation.
  Evidence: local add-index validation showed `predicted_tikv_index_bytes` much smaller than `logical_index_kv_bytes`, and the regression test demonstrates that `rowsize.GetIndexAvgRowSize(...)` returns a much smaller value until `HistColl.ID2UniqueID(indexCols)` is applied.

- Observation: Even after the `UniqueID` remapping fix, `predicted_tikv_index_bytes` still materially underestimated prefix string indexes because the shared planner helper estimates index key/scan row size, while DXF `logical_index_kv_bytes` records the actual `len(key) + len(value)` written during add-index.
  Evidence: a local `add index idx_k_pad(k, pad(32))` repro with accurate row-count stats still logged `predicted_tikv_index_bytes=188000000` versus `logical_index_kv_bytes=527737344`, which matches TiDB's version-0 index-value encoding path when restored data writes indexed values into the value payload.

- Observation: Keeping the old heuristic predictor alongside the new representative-datum predictor is useful operationally because we can now compare both against `logical_index_kv_bytes` on the same add-index run instead of replacing the old number blindly.
  Evidence: the owner-side task metadata and success log already have a stable place to persist and print both numbers, so the comparison can be collected without extra task-framework changes.

- Observation: The representative-datum predictor can still underestimate real prefix/string rows when the synthesized datum is lighter than production values, so using a tiny sample of real rows is a more direct way to capture restored-data and collation costs without further hand-tuning representative strings.
  Evidence: local add-index observation logged `represent_predicted_tikv_index_bytes=312000000` versus `basic_predicted_tikv_index_bytes=384000000` and `logical_index_kv_bytes=527737344`, which showed the sample path needed to move from synthetic values to real sampled rows.

## Decision Log

- Decision: Run the space check only when `task == nil` in `(*worker).executeDistTask(...)`, and skip resume/retry paths.
  Rationale: The requirement is to guard only the first add-index launch. Resume would need a separate “remaining predicted bytes” model, which is intentionally out of scope.
  Date/Author: 2026-05-14 / Codex

- Decision: Keep the single-store rule based on `predict_tikv_index_bytes / tikv_store_count`.
  Rationale: The user explicitly chose the average-share model under the assumption that PD will rebalance TiKV stores toward even usage.
  Date/Author: 2026-05-14 / Codex

- Decision: Reuse existing files (`pkg/ddl/index.go`, `pkg/ddl/backfilling_dist_executor.go`, `pkg/ddl/reorg_util_test.go`) instead of adding new Go files.
  Rationale: The repository requests minimal diffs and avoiding duplicate helper paths when adjacent code already exists.
  Date/Author: 2026-05-14 / Codex

- Decision: Keep the post-task observed-capacity logging backward-compatible by continuing to persist the old aggregate-used snapshot while also storing the richer initial capacity snapshot and the predicted bytes.
  Rationale: Existing tasks and existing logging already rely on the aggregate-used snapshot semantics, so the new precheck should enrich rather than silently replace that contract.
  Date/Author: 2026-05-14 / Codex

- Decision: Skip the predictive precheck when `reorgInfo.mergingTmpIdx` is true, but still keep the initial-capacity snapshot collection on a best-effort basis for success-path observability.
  Rationale: The new requirement targets the pre-submit add-index path rather than the merge-temp-index follow-up phase, while the existing observed-usage logging remains useful there.
  Date/Author: 2026-05-14 / Codex

- Decision: Introduce a third predictor, `sample_predicted_tikv_index_bytes`, that samples real rows from up to five randomly chosen regions per physical table and use that value for the actual TiKV capacity gate, while keeping `basic` and `represent` as observational metrics.
  Rationale: the user wants to compare models side by side, but the actual gate should use the real-row sample model now because it better captures prefix/string/restored-data growth without relying on synthetic representative values.
  Date/Author: 2026-05-18 / Codex

## Outcomes & Retrospective

Implementation completed in the planned files:

- `pkg/ddl/backfilling_dist_executor.go` now persists `PredictedTiKVIndexBytes` and a richer `InitialTiKVCapacity` snapshot alongside the legacy aggregate-used snapshot.
- `pkg/ddl/index.go` now:
  - collects TiKV cluster/store capacity from the existing PD helper path,
  - predicts add-index TiKV bytes from stats without scanning table data,
  - rejects the first DXF add-index submission on the cluster-level `20%` and per-store `15%` safety margins chosen for this task,
  - logs the predicted bytes together with the existing observed-capacity metrics.
- `pkg/ddl/index.go` now also remaps the stats histogram collection with `HistColl.ID2UniqueID(indexCols)` before calling the shared row-size estimator, so DDL prediction actually uses real column stats instead of pseudo fallback.
- `pkg/ddl/index.go` now estimates per-row bytes as `key + value` instead of key only, and the value side accounts for old-format/local/global index encoding differences plus restored-data growth for string/prefix indexes.
- `pkg/ddl/index.go` now keeps two predictors:
- `pkg/ddl/index.go` now keeps three predictors:
  - `basic_predicted_tikv_index_bytes`: the old heuristic key/value estimator used only for observation;
  - `represent_predicted_tikv_index_bytes`: the representative-datum estimator that calls real index key/value encoding and remains for observation;
  - `sample_predicted_tikv_index_bytes`: the new snapshot-based real-row sampler that reads a few rows from random regions and is now used for the TiKV capacity gate.
- `pkg/ddl/index.go` now reuses existing snapshot iteration, raw row decode, and `table.Index.GenIndexKVIter(...)` logic to sample real row bytes instead of maintaining a second hand-written encoding model.
- `pkg/planner/util/rowsize/rowsize.go` now hosts the shared row-size estimation helpers reused by both planner and DDL.
- `pkg/planner/cardinality/row_size.go` now delegates to `pkg/planner/util/rowsize`, preserving the existing planner API.
- `pkg/ddl/reorg_util_test.go` now extends the existing helper tests with capacity-snapshot and reject-condition coverage.
- `pkg/ddl/reorg_util_test.go` now also includes a regression subtest proving that the shared row-size estimator underestimates until the stats collection is remapped to column `UniqueID`s.
- `pkg/ddl/reorg_util_test.go` now includes a restored-data regression subtest showing that the new predictor adds non-trivial value bytes for `varchar` prefix indexes instead of staying on the old key-only model.
- `pkg/ddl/reorg_util_test.go` now also asserts that the representative-datum predictor returns a larger per-row estimate than the basic heuristic predictor for a `varchar` prefix index that needs restored data.
- `pkg/planner/cardinality/row_size_test.go` now adds wrapper-parity coverage against the extracted helper package.

Validation completed:

    cd /Users/xiaoli/coding/tidb
    GOSUMDB=sum.golang.org GOCACHE=/private/tmp/tidb-gocache ./tools/check/failpoint-go-test.sh pkg/ddl -run 'TestSumTiKVStoreUsage|TestCollectTiKVStoreUsage'
    GOSUMDB=sum.golang.org GOCACHE=/private/tmp/tidb-gocache go test -run TestAvgColLen -tags=intest,deadlock ./pkg/planner/cardinality
    GOSUMDB=sum.golang.org GOCACHE=/private/tmp/tidb-gocache go test -tags=intest,deadlock ./pkg/planner/util/rowsize
    GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go GOCACHE=/private/tmp/tidb-gocache make bazel_prepare
    GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go GOCACHE=/private/tmp/tidb-gocache make lint

Remaining local gap:

- No end-to-end RealTiKV add-index test was added or run for the owner-side rejection path, so the current local evidence is helper-level `pkg/ddl` coverage plus repository-level lint.

## Context and Orientation

`DXF add-index` is the distributed add-index execution path under `pkg/ddl/`. The DDL owner decides whether to submit a backfill task in `pkg/ddl/index.go`.

`BackfillTaskMeta` in `pkg/ddl/backfilling_dist_executor.go` is the persisted JSON payload for a DXF add-index task. It already stores row-size estimates and the initial TiKV used-capacity snapshot for post-task logging, so it is the right place to persist the predicted bytes and the richer initial capacity snapshot for observability.

`TiKV cluster capacity snapshot` in this plan means the sum of capacity and available bytes across all TiKV stores returned by PD, excluding TiFlash and TiFlash Compute stores. `Single-store capacity snapshot` means the same fields for each TiKV store in that set.

`predict_tikv_index_bytes` means an estimate of how many bytes this add-index job will add to the TiKV cluster. The implementation should prefer existing statistics and shared planner row-size estimation helpers instead of scanning table data.

The two reject conditions are:

1. Cluster-level reject when:

   `cluster_available_bytes - predict_tikv_index_bytes < cluster_total_bytes * 20%`

2. Single-store reject when any TiKV store satisfies:

   `store_available_bytes - predict_tikv_index_bytes / tikv_store_count < store_total_bytes * 15%`

The requirement explicitly says these are reject conditions, not allow conditions.

## Plan of Work

First, extend the TiKV capacity helper logic in `pkg/ddl/index.go`. The current helper only returns aggregated used bytes. Update it so the owner can reuse one PD read to obtain:

- cluster total bytes,
- cluster available bytes,
- cluster used bytes,
- store count,
- per-store totals and available bytes.

Keep the existing “observed TiKV capacity increase” logging working by continuing to expose aggregate used bytes through the new helper.

Next, extend `BackfillTaskMeta` in `pkg/ddl/backfilling_dist_executor.go` to carry:

- the richer initial TiKV capacity snapshot,
- `PredictedTiKVIndexBytes`.

Preserve the old initial-used snapshot field for compatibility with already-running tasks if needed.

Then, implement the prediction helper in `pkg/ddl/index.go`. The helper should:

- find the newly added index IDs from the current reorg info,
- build index-row columns from table metadata, including handle columns,
- obtain row counts from the existing stats handle without scanning the table,
- use shared row-size estimation logic to estimate per-row bytes,
- multiply by the estimated row count and a conservative amplification factor,
- sum across all newly added indexes.

After that, wire the precheck into `(*worker).executeDistTask(...)` only on the `task == nil` path and only when `reorgInfo.mergingTmpIdx` is false. The owner should:

- compute `predict_tikv_index_bytes`,
- collect the current TiKV capacity snapshot,
- evaluate both reject conditions,
- return a DDL error before `handle.SubmitTask(...)` if either condition fails,
- otherwise persist the prediction and snapshot into task metadata and continue.

Finally, extend existing unit tests in `pkg/ddl/reorg_util_test.go` so they verify the richer capacity snapshot and the new reject helper behavior without adding new top-level test functions. This avoids unnecessary Bazel metadata churn while still covering the new rules.

## Concrete Steps

Work from repository root:

1. Update `docs/plans/2026-05-14-add-index-tikv-space-precheck.md` as implementation progresses.
2. Edit `pkg/ddl/backfilling_dist_executor.go` to extend `BackfillTaskMeta` with predicted bytes and richer initial capacity data.
3. Edit `pkg/ddl/index.go` to:
   - collect richer TiKV capacity data,
   - predict add-index bytes from existing stats,
   - enforce the reject conditions before `handle.SubmitTask(...)`,
   - keep success-path observed-capacity logging compatible.
4. Edit `pkg/ddl/reorg_util_test.go` to update existing tests and add subtests for the new helper behavior.
5. Run targeted DDL tests from `/Users/xiaoli/coding/tidb`.

Expected validation commands:

    cd /Users/xiaoli/coding/tidb
    go test -run 'TestSumTiKVStoreUsage|TestCollectTiKVStoreUsage' -tags=intest,deadlock ./pkg/ddl
    go test -run 'TestBackfillingScheduler|TestDXFAddIndexRealtimeSummary' -tags=intest,deadlock ./pkg/ddl ./tests/realtikvtest/addindextest2/...

If the second command proves too wide or requires a RealTiKV playground, narrow to the package-local DDL test command for the first implementation pass and record the limitation in this plan.

## Validation and Acceptance

The change is accepted when:

1. The owner rejects DXF add-index before task submission when the cluster-level or single-store reject condition is met.
2. The owner continues to submit the task when both conditions pass.
3. The TiKV capacity helper tests prove that TiFlash stores are excluded and that aggregate plus per-store byte fields are computed correctly.
4. The scoped test run passes, or any unverified coverage is explicitly documented.

## Idempotence and Recovery

The precheck is safe to rerun because it is read-only against PD and the stats cache until a new task is submitted. Resume paths remain unchanged, so an interrupted validation run does not corrupt persisted DXF tasks.

If a test fails after partially editing the code:

- revert only the current patch hunk with `apply_patch`,
- rerun the targeted `go test` command,
- update this plan with the discovery before moving on.

## Artifacts and Notes

Pending implementation.

## Interfaces and Dependencies

Expected touched interfaces and types:

- `pkg/ddl/backfilling_dist_executor.go`
  - extend `type BackfillTaskMeta struct`
  - define richer TiKV capacity snapshot structs used by task metadata and helper code.

- `pkg/ddl/index.go`
  - update `(*worker).executeDistTask(...)`
  - add helper logic for TiKV capacity collection, prediction, and reject checks.

- `pkg/planner/cardinality`
  - now acts as the compatibility wrapper over the extracted row-size helper package.

- `pkg/planner/util/rowsize`
  - new leaf package that hosts the shared row-size estimation logic for planner and DDL.

- `pkg/statistics/handle.Handle`
  - reuse existing `GetPhysicalTableStats(...)` / pseudo-stats fallback through the DDL-owned stats handle.

- `pkg/ddl/reorg_util_test.go`
  - extend existing DDL helper tests with subtests for the new capacity snapshot and reject checks.
