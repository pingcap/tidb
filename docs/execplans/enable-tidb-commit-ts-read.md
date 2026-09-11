# Enable constrained `_tidb_commit_ts` reads

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB already models `_tidb_commit_ts` as a hidden unsigned integer column. After this change, users can select or filter on `_tidb_commit_ts` when the physical access path is a TiKV or TiFlash table scan, or an index lookup whose table-side TiKV scan fetches the base row. Unsupported paths must not be selected. UPDATE and DELETE references remain rejected, and a query that can only use an unsupported path, such as TABLESAMPLE, must fail.

The behavior is observable in two ways. With TiKV, inserting a row and running `SELECT _tidb_commit_ts > 0 ...` returns `1`, and EXPLAIN shows `TableReader`/`TableRangeScan` or `IndexLookUp`. With TiFlash as the only isolation-read engine, EXPLAIN succeeds, shows a TiFlash `TableFullScan`, and places a predicate on `_tidb_commit_ts` in a TiFlash task. The generated TiFlash `tipb.TableScan` includes the hidden column with ID `model.ExtraCommitTSID` (`-5`). When both engines are available, normal costing and hints remain responsible for choosing the engine; this feature does not force TiFlash. EXPLAIN must still not show Point_Get, Batch_Point_Get, IndexReader, or IndexMerge for a query that references the column.

## Progress

- [x] (2026-08-31) Located the current preprocessor ban, hidden-column schema plumbing, table-scan decoder, optimizer access-path selection, and existing integration fixture.
- [x] (2026-08-31) Compared the old `enable-commit_ts` branch with current master and selected planner-side access-path gating, strengthened with explicit IndexMerge and covering IndexReader exclusion.
- [x] (2026-08-31 15:20 +08:00) Implemented planner gating and focused preprocessing/unit coverage.
- [x] (2026-08-31 15:48 +08:00) Recorded and reviewed the targeted SQL integration result for allowed and forbidden paths; 477 cases passed.
- [x] (2026-08-31 15:50 +08:00) Ran targeted failpoint-aware unit tests and the non-recording integration verification; all passed.
- [x] (2026-08-31 15:51 +08:00) Completed the Ready gate: `make bazel_prepare`, `make lint`, formatting, `git diff --check`, and final diff review all passed.
- [x] (2026-09-11 12:45 +08:00) Revised the contract after the user clarified that TiFlash TableScan must be allowed, without forcing the optimizer to choose TiFlash.
- [x] (2026-09-11 12:46 +08:00) Captured fail-before evidence: the TiFlash-only planner regression failed with error 1815 before removing the TiFlash path guard.
- [x] (2026-09-11 12:48 +08:00) Restored normal TiKV/TiFlash path selection and added planner and TiFlash protobuf assertions.
- [x] (2026-09-11 13:09 +08:00) Completed the revised Ready validation: focused planner and protobuf tests, final `make lint`, Bazel gate inspection, formatting, `git diff --check`, and diff review all passed.
- [x] (2026-09-11) Restored the original scan-cost calculation after the user clarified that reading `_tidb_commit_ts` should continue to carry its existing scan cost; revalidated the focused behavior.

## Surprises & Discoveries

- Observation: Current master already appends `_tidb_commit_ts` to non-cluster-table logical schemas, while `pkg/util/rowcodec.(*ChunkDecoder).DecodeToChunk` accepts a commit timestamp. The feature is intentionally blocked by `pkg/planner/core/preprocess.go` rather than missing end-to-end metadata. Existing scan-cost behavior is outside this feature and remains unchanged.
  Evidence: `pkg/planner/core/logical_plan_builder.go` creates `model.ExtraCommitTSID`; `pkg/util/rowcodec/decoder.go` appends the supplied timestamp; `pkg/planner/core/preprocess.go` rejects SELECT/SET/UPDATE/DELETE references.

- Observation: The old branch prevents PointGet/BatchPointGet and TiFlash selection and rejects TABLESAMPLE, but it does not explicitly reject IndexMerge or a covering IndexReader.
  Evidence: commit `2b4e1c2adc` changes only the PointGet conversion condition, table-path store filtering, and `convertToSampleTable`.

- Observation: A query with an explicit `USE_INDEX_MERGE` hint and `_tidb_commit_ts` has no legal fallback path after the IndexMerge guard; planning returns error 1815.
  Evidence: the first targeted integration recording stopped at that EXPLAIN with `Internal : Can't find a proper physical plan for this query`. The fixture now declares this as the expected error, directly proving IndexMerge is forbidden.

- Observation: UniStore TopN contains a zero-timestamp re-decode path, but both `ORDER BY b LIMIT 1` and `ORDER BY _tidb_commit_ts LIMIT 1` remained correct without changing it because these plans do not push the timestamp-bearing TopN into that closure executor.
  Evidence: both strict non-recording integration runs passed with the proposed UniStore change temporarily removed. The speculative store change and its temporary tests were discarded to keep this patch scoped.

- Observation: One exploratory race-enabled integration run reported a startup race between gRPC logger installation and channelz logging. No changed code was present in that stack, and both final record and non-record runs passed cleanly with the race-enabled server.
  Evidence: the final `-r` and `-t` runs each completed all 477 cases successfully.

- Observation: No additional TiFlash-specific serialization branch is needed. `PhysicalTableScan.ToPB` passes its selected columns to `tables.BuildTableScanFromInfos`, and that helper serializes every column, including `_tidb_commit_ts`, into `tipb.TableScan.Columns` for TiFlash.
  Evidence: the focused `TestColumnToProto` assertion constructs the TiFlash form and verifies column ID `-5`, type `BIGINT`, and unsigned-flag semantics; the test passes.

- Observation: The old explicit TiFlash access-path guard was the reason a TiFlash-only query could not be planned.
  Evidence: after changing the existing regression expectation but before changing planner code, `TestTiFlashExtraColumnPrune` failed with planner error 1815. Removing only that guard made the same test pass for both classic and cascades optimizer runs.

## Decision Log

- Decision: Treat “table scan” as a TiKV or TiFlash base-table scan and “IndexLookup” as an index scan followed by a TiKV base-table scan. The earlier TiKV-only decision is superseded by the user clarification on 2026-09-11.
  Rationale: The target TiFlash implementation accepts `ExtraCommitTSID` in its table-scan column list. TiFlash has no secondary-index path, so enabling its table path does not broaden IndexReader or IndexMerge. This change deliberately relies on the TiFlash/TiDB deployment supporting that protocol behavior.
  Date/Author: 2026-09-11 / Codex

- Decision: Restore the ordinary TiKV/TiFlash preference and cost selection logic instead of adding a forced TiFlash preference.
  Rationale: The requirement is capability, not plan enforcement. A TiFlash-only isolation setting proves the path is available, while a mixed-engine setting must retain normal optimizer freedom.
  Date/Author: 2026-09-11 / Codex

- Decision: Do not modify `PhysicalTableScan.GetScanRowSize` or any other cost calculation as part of TiFlash commit-ts support.
  Rationale: The user clarified that fetching `_tidb_commit_ts` still requires scan I/O and should retain the existing cost treatment. Capability and serialization changes do not require a cost-model change.
  Date/Author: 2026-09-11 / Codex

- Decision: Gate access paths in `findBestTask4LogicalDataSource` instead of adding executor implementations or rejecting point-shaped SQL.
  Rationale: A primary-key equality query can safely fall back from PointGet to TableRangeScan. Central optimizer gating keeps the supported surface auditable and preserves valid SQL when an allowed plan exists.
  Date/Author: 2026-08-31 / Codex

- Decision: Explicitly exclude IndexMerge and single-read index scans when `_tidb_commit_ts` is referenced, even though column coverage normally makes an IndexReader unattractive or impossible.
  Rationale: The user requires every other read path to remain forbidden. Explicit guards prevent future coverage or costing changes from accidentally widening the contract.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

SELECT and set-operation queries can now resolve `_tidb_commit_ts`. The optimizer permits the column when each affected data source can use a TiKV or TiFlash base-table scan, or a double-read secondary-index path (`IndexLookUp`). A primary-key equality or IN predicate falls back from PointGet/BatchPointGet to TableRangeScan. A TiFlash-only query now produces a TiFlash table scan, while a query with both TiKV and TiFlash available is chosen by ordinary optimizer costing and preferences.

PointGet, BatchPointGet, single-read IndexReader, IndexMerge, and TABLESAMPLE remain excluded. A forced unsupported path fails because there is no valid physical plan; TABLESAMPLE gets a targeted error. UPDATE and DELETE keep the preprocessor error, and INSERT name resolution remains unchanged.

No cost-model code is changed. In particular, the TiFlash branch of `PhysicalTableScan.GetScanRowSize` continues to account for the columns in the scan schema, including `_tidb_commit_ts`, according to the pre-existing implementation.

The original TiKV implementation completed Ready validation on 2026-08-31. The 2026-09-11 TiFlash revision also completed Ready validation: its focused planner and protobuf tests passed, `make lint` passed, and final diff checks were clean. The Bazel gate did not require `make bazel_prepare` because this revision adds no Go file or top-level test, changes no import section, and touches no Bazel or module file. The remaining environmental gap is that no external TiFlash cluster was started locally: planner selection and protobuf serialization are verified locally, but actual timestamp values returned by a deployed TiFlash are not.

## Context and Orientation

`pkg/meta/model/table.go` reserves `ExtraCommitTSID` for `_tidb_commit_ts`. `pkg/planner/core/logical_plan_builder.go` appends that hidden column to ordinary table data sources so name resolution can find it. The original preprocessor rejection has been loosened for SELECT and set operations while remaining in place for UPDATE and DELETE. `pkg/planner/core/find_best_task.go` chooses between PointGet, BatchPointGet, table paths, index paths, IndexMerge, TiKV, TiFlash, and TABLESAMPLE. This is the narrowest place to enforce the allowed physical access paths. `pkg/planner/core/operator/physicalop/physical_table_scan.go` serializes a selected table scan to tipb, and `pkg/table/tables/tables.go` builds its column descriptors for either store.

A TableReader contains a TiKV table scan; a TiFlash root or MPP task likewise contains a physical table scan serialized to a TiFlash `tipb.TableScan`. An IndexLookUp first scans an index to obtain row handles and then runs a TiKV table-side scan to fetch full base rows. A covering IndexReader returns data only from index KVs. IndexMerge combines multiple index paths before optionally reading table rows. PointGet and BatchPointGet use direct KV get APIs instead of the coprocessor table-scan path. TABLESAMPLE uses a separate sampling executor. TiKV/TiFlash table scans and TiKV IndexLookUp are in scope.

The existing SQL fixture is `tests/integrationtest/t/planner/core/casetest/integration.test`, with expected output in the corresponding `tests/integrationtest/r/.../integration.result`. Planner unit coverage belongs in `pkg/planner/core/preprocess_test.go` and the existing casetest package.

## Plan of Work

First, loosen `pkg/planner/core/preprocess.go` so SELECT and set-operation syntax can reach logical and physical planning, while keeping UPDATE and DELETE rejected. Extend `TestValidator` to lock down the preprocessing boundary.

Second, add a small helper in `pkg/planner/core/find_best_task.go` that detects whether column pruning left `ExtraCommitTSID` in a DataSource schema. When true, prevent PointGet/BatchPointGet conversion, skip IndexMerge candidates, and skip single-read index candidates. Permit ordinary TiKV and TiFlash table scans and a double-read TiKV index candidate, which becomes IndexLookUp. Reject TABLESAMPLE explicitly with the existing planner error class so a sampling query cannot silently become an ordinary scan.

Third, extend the existing integration fixture with data and EXPLAIN/SELECT cases. Cover a point-shaped predicate falling back to TableRangeScan, a forced secondary index producing IndexLookUp, a value predicate on `_tidb_commit_ts`, a set operation, IndexMerge avoidance, and errors for TABLESAMPLE, UPDATE, and DELETE. In the planner casetest, require TiFlash-only access to produce a TiFlash table scan and require a predicate on `_tidb_commit_ts` to appear in a TiFlash task. Keep assertions stable by not prescribing the engine when both TiKV and TiFlash are available.

Fourth, verify the actual TiFlash scan serialization. Extend an existing protobuf conversion test to construct `BuildTableScanFromInfos(..., isTiFlashStore=true)` with `model.NewExtraCommitTSColInfo()` and assert that the generated `tipb.TableScan.Columns` contains ID `-5` with unsigned BIGINT metadata. Leave all cost calculations unchanged.

Finally, format changed Go code, run targeted tests according to the failpoint decision, inspect all changes, then perform the Ready profile including `make lint`. Use the Bazel prepare gate after the final diff; an import change or new top-level Go test requires `make bazel_prepare`. The 2026-09-11 revision only extends existing test functions and does not change imports, so Bazel preparation is not expected unless final diff inspection finds another trigger.

## Concrete Steps

Run all commands from `/DATA/disk3/xzx/tidb` unless a command changes directory explicitly.

Inspect the focused diff repeatedly:

    git diff -- pkg/planner/core/preprocess.go pkg/planner/core/find_best_task.go pkg/planner/core/preprocess_test.go pkg/planner/core/casetest/integration_test.go tests/integrationtest/t/planner/core/casetest/integration.test tests/integrationtest/r/planner/core/casetest/integration.result

Format Go changes:

    gofmt -w pkg/planner/core/find_best_task.go pkg/planner/core/preprocess.go pkg/planner/core/preprocess_test.go pkg/planner/core/casetest/integration_test.go

Before package tests, check failpoint use exactly as required by `docs/agents/testing-flow.md`:

    rg -n --fixed-strings -- "failpoint." pkg/planner/core
    rg -n --fixed-strings -- "testfailpoint." pkg/planner/core
    rg -n --fixed-strings -- "@com_github_pingcap_failpoint//:failpoint" pkg/planner/core/BUILD.bazel

If matches exist, use the serialized wrapper for targeted tests:

    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestValidator$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core/casetest -run '^TestTiFlashExtraColumnPrune$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestColumnToProto$' -count=1

Record and then verify the integration suite:

    pushd tests/integrationtest
    ./run-tests.sh -r planner/core/casetest/integration
    ./run-tests.sh -t planner/core/casetest/integration
    popd

At completion, run the Ready profile:

    make lint

Run `make bazel_prepare` first if the final Bazel gate finds any import-section change, added top-level test, Bazel edit, Go file add/move/remove, or module edit.

## Validation and Acceptance

Acceptance requires all of the following observable behavior:

1. `SELECT _tidb_commit_ts > 0 FROM t WHERE primary_key = ...` succeeds and EXPLAIN shows TableReader with a TiKV table range scan, not Point_Get.
2. A forced usable secondary index with a non-covering read succeeds and EXPLAIN shows IndexLookUp with a table-side TiKV scan.
3. `_tidb_commit_ts` can be used in SELECT output, WHERE, and set operations when every base access has an allowed plan.
4. An IndexMerge hint cannot produce IndexMerge for a query referencing the column; a forced IndexMerge query fails when the hint leaves no allowed fallback.
5. A TiFlash-only read plans successfully as a TiFlash TableFullScan, and a predicate using `_tidb_commit_ts` is assigned to a TiFlash task. The TiFlash `tipb.TableScan` column list contains `ExtraCommitTSID` (`-5`). TABLESAMPLE remains rejected.
6. UPDATE and DELETE references continue to return error 1815. INSERT name resolution remains unchanged.
7. Targeted planner tests, targeted integration recording, and `make lint` succeed, and the final result diff contains only intentional rows.

## Idempotence and Recovery

Source edits and Go formatting are safe to repeat. The targeted integration runner may rewrite one result file; rerun it after fixing failures and review `git diff` each time. Do not hand-edit generated Bazel metadata; if the Bazel gate triggers, rerun `make bazel_prepare`. No data-destructive commands are needed. Existing unrelated worktree changes must remain untouched.

## Artifacts and Notes

Initial repository state is clean at master commit `e0cfb27df3`. The comparison implementation is commit `2b4e1c2adc` on local/reference branch `enable-commit_ts`; it is not cherry-picked because its base differs substantially from master.

Final validation evidence:

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestValidator$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core/casetest -run '^TestTiFlashExtraColumnPrune$' -count=1
    (cd tests/integrationtest && ./run-tests.sh -r planner/core/casetest/integration)
    (cd tests/integrationtest && ./run-tests.sh -t planner/core/casetest/integration)
    make lint
    git diff --check

Both integration commands reported 477 passing cases. Both failpoint wrapper invocations enabled from refcount 0 to 1 and disabled from 1 back to 0.

Revision validation evidence (2026-09-11, completed):

    ./tools/check/failpoint-go-test.sh pkg/planner/core/casetest -run '^TestTiFlashExtraColumnPrune$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestColumnToProto$' -count=1
    make lint
    git diff --check

The planner test was first run against the revised expectation with the old TiFlash guard still present and failed with error 1815, demonstrating that the regression test detects the requested behavior. After the implementation, both focused tests passed and each failpoint wrapper returned its reference count to zero. The final planner run additionally verified that an explicit TiKV storage hint still produces a non-TiFlash TableScan. `make lint` and `git diff --check` passed. Bazel gate inspection found no trigger, so `make bazel_prepare` was not run for this revision.

## Interfaces and Dependencies

No public Go API or external dependency should be added. The implementation uses `model.ExtraCommitTSID` and `model.ExtraCommitTSName`, existing `logicalop.DataSource` schemas, existing `util.AccessPath` properties, existing `base.InvalidTask`, the existing planner internal error type, and the existing TiFlash `tipb.TableScan.Columns` protocol field. The helper remains package-private in `pkg/planner/core/find_best_task.go`.

Revision note (2026-08-31): Initial plan created after comparing the reference branch with current master; it explicitly adds IndexMerge and IndexReader guards that the reference change lacked. Updated after implementation to record the discarded UniStore investigation, final behavior, and Ready validation evidence.

Revision note (2026-09-11): Updated after the user clarified that TiFlash TableScan must be enabled and that the optimizer must not be forced to use it. The TiKV-only decision and TiFlash-only failure expectation are superseded; planner capability and TiFlash protobuf serialization are now explicit acceptance criteria.

Revision note (2026-09-11): Removed the proposed `GetScanRowSize` change after the user clarified that `_tidb_commit_ts` should retain its existing scan cost. The final TiFlash revision contains no cost-model changes.
