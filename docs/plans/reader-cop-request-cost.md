# Account for cop requests in reader cost

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

Cost Model V2 currently prices scan, network, and CPU work for TiKV readers, but most reader shapes omit the fixed cost of issuing coprocessor requests. As a result, a reader that is executed repeatedly on the probe side of `Apply` or `IndexJoin` can look much cheaper than it is. After this change, the four request-generating reader shapes (`IndexReader`, TiKV `TableReader`, `IndexLookUpReader`, and `IndexMergeReader`) include their own request cost. Existing parent cost formulas then multiply the complete child cost, so nested fanout is represented without parent-specific reader inspection.

The behavior is observable through `EXPLAIN FORMAT='cost_trace'`: TiKV reader traces contain `tidb_request_factor`, TiFlash readers do not receive a TiKV request term, and `Apply` or `IndexJoin` plans naturally amplify the reader-owned term. Regression cases derived from GitHub issues #69092 and #69392 verify plan choice at one and two fanout levels.

## Progress

- [x] (2026-08-03) Confirmed the design boundary: Reader owns request cost; parent owns multiplicity.
- [x] (2026-08-03) Located Cost Model V2 implementations in `pkg/planner/core/plan_cost_ver2.go` and the nearest CBO tests in `pkg/planner/core/casetest/cbotest`.
- [x] (2026-08-03) Added behavior-level regression tests and captured pre-fix failures in a temporary baseline worktree.
- [x] (2026-08-03) Added request cost to the four reader shapes without modifying `PhysicalApply` or `PhysicalIndexJoin` composition.
- [x] (2026-08-03) Regenerated Bazel metadata and updated the two intentional `TestIssue62438` cost-trace goldens.
- [x] (2026-08-03) Completed Ready-profile targeted validation, `make lint`, and final diff review.

## Surprises & Discoveries

- Observation: `PhysicalIndexLookUpReader` already charges the table-side double-read request as `indexRows / IndexLookupSize * 32`, but omits the initial index-side request.
  Evidence: `getPlanCostVer24PhysicalIndexLookUpReader` computes `doubleReadRequestCost` after table-side cost.

- Observation: `PhysicalIndexMergeReader.Init` uses the table plan's statistics as the reader output statistics when a table lookup exists; without a table plan it sums partial-plan row counts.
  Evidence: `pkg/planner/core/operator/physicalop/physical_indexmerge_reader.go` initializes stats from `TablePlan` or sums `PartialPlansRaw`.

- Observation: The existing `TestIssue62438` cost trace changed without changing its physical plan shape.
  Evidence: the only recorded difference is the new `cop-request(ranges(1)*tidb_request_factor(...))` term and the resulting ancestor costs in `analyze_suite_out.json` and `analyze_suite_xut.json`.

## Decision Log

- Decision: Charge requests only in the four reader cost functions and do not traverse parents or annotate probe descendants.
  Rationale: Existing `Apply` and `IndexJoin` formulas already multiply the entire probe child cost. Keeping request ownership at the Reader composes through arbitrary nesting and avoids coupling parents to physical reader shapes.
  Date/Author: 2026-08-03 / Codex and user.

- Decision: Keep the existing `tidb_index_join_double_read_penalty_cost_rate` path unchanged and set it to zero in new regressions.
  Rationale: It is an optional parent-level bias, not the base cost of requests made by a Reader. Removing it would broaden compatibility scope; using it in tests would hide whether natural amplification works.
  Date/Author: 2026-08-03 / Codex and user.

- Decision: Use `getNumberOfRanges` as the current Cost Model V2 proxy for a reader execution's initial cop request count, and keep IndexLookup's existing double-read task formula for handle lookup requests.
  Rationale: Ranges are the planner-visible unit already used for IndexJoin seek fanout. Actual region splitting and runtime batching are unavailable during optimization. Tests must include small and multi-range cases to expose over-penalization risk.
  Date/Author: 2026-08-03 / Codex.

- Decision: Put regressions in the existing `pkg/planner/core/casetest/cbotest/cbo_test.go` suite and assert public `EXPLAIN` behavior.
  Rationale: This is the nearest Cost Model V2 plan-choice suite, and behavior-level assertions survive helper refactors.
  Date/Author: 2026-08-03 / Codex.

## Outcomes & Retrospective

The implementation now prices each TiKV reader execution locally. IndexLookup preserves its existing table-side double-read term and adds only the initial index request; IndexMerge adds each partial request and a table-handle lookup term when it has a table plan. Apply and IndexJoin were not changed, and their existing probe formulas naturally multiply the complete Reader cost.

The #69392 regression proves both that the default high-fanout plan uses the TiFlash hash-join alternative and that a forced TiKV IndexHashJoin exposes the multiplied Reader request term. The #69092 regression proves a forced nested Apply contains Reader request terms under the existing parent multipliers and that the unforced plan uses the enumerated TiFlash hash-join alternative. A one-row selective counterexample still chooses IndexJoin.

Ready-profile checks passed. Remaining calibration risk is deliberate and visible: range count is a planner proxy for initial requests, and IndexMerge uses conservative upper bounds for distinct handles (sum for union, minimum partial cardinality for intersection). Runtime region boundaries and duplicate-handle elimination are unavailable to this cost layer. No RealTiKV or runtime benchmark was run because the change is confined to optimizer costing and mock-store plan selection.

## Context and Orientation

`pkg/planner/core/plan_cost_ver2.go` implements Cost Model V2. `costusage.CostVer2` carries both a numeric value and an optional trace expression. `getTaskRequestFactorVer2` returns the request factor for the current task. `getNumberOfRanges` recursively counts planner ranges under scans. A cop request is a TiDB request to TiKV's coprocessor; one logical range is only a planning proxy because TiKV regions may split it at runtime.

The relevant readers are:

- `PhysicalIndexReader`: one index-side cop execution.
- TiKV `PhysicalTableReader`: one table-side cop execution. A TiFlash MPP reader must not receive a TiKV request term.
- `PhysicalIndexLookUpReader`: an initial index execution followed by batched table handle lookups. The latter already has a request term.
- `PhysicalIndexMergeReader`: one execution for each partial path and, when `TablePlan` is present, batched table handle lookups.

`PhysicalApply` computes probe cost as child cost times outer rows. `PhysicalIndexJoin` computes probe cost from child cost times outer rows with existing batching and concurrency factors. These formulas must stay structurally unchanged.

## Plan of Work

First extend `pkg/planner/core/casetest/cbotest/cbo_test.go` with two top-level behavior tests. One test constructs covering index, table, non-covering index lookup, and index-merge queries and inspects cost traces. It also installs a virtual TiFlash replica to prove no TiKV request term is charged there. The other test constructs one-level and nested correlated fanout queries based on issues #69392 and #69092. Forced Apply/IndexJoin candidates must expose amplified request terms, while default plan choice must avoid the high-fanout TiKV probe and a selective counterexample must remain eligible for IndexJoin/Apply. Run these tests before the production change and retain the failure output as red evidence.

Then edit only Cost Model V2 in `pkg/planner/core/plan_cost_ver2.go`. Define a small request-cost helper that emits an explicit trace such as `cop-request(ranges(...)*tidb_request_factor(...))`, and a helper for the existing double-read task formula if it clarifies shared use. Add the initial request term inside each reader's concurrency division. For `PhysicalIndexLookUpReader`, add only the index-side initial request and preserve the table-side double-read cost. For `PhysicalIndexMergeReader`, add one initial request term for every partial plan. If a table plan exists, charge its lookup requests from the estimated handles feeding the table lookup, divided by `IndexLookupSize` and multiplied by the existing task-per-batch proxy; do not add an independent range request that duplicates those handle-lookup requests.

Finally run the new tests until green, inspect any cost-golden churn, regenerate Bazel metadata because top-level tests and `shard_count` changed, run the scoped Ready validation and `make lint`, and self-review that no parent-type checks or V1 behavior were introduced.

## Concrete Steps

Run all commands from `/Users/tailingxiang/go/src/github.com/pingcap/tidb`.

Before implementation, run:

    make bazel_prepare
    go test -run 'Test(ReaderCopRequestCost|LookupRequestFanoutPlanChoice)$' -tags=intest,deadlock ./pkg/planner/core/casetest/cbotest -count=1

The regression command must fail before the cost change because request terms are missing and/or the high-fanout plan remains selected. Record the failure under `Artifacts and Notes`.

After implementation, rerun the same command and expect `ok`.

For completion, run the targeted tests selected by the Ready profile:

    go test -run 'Test(ReaderCopRequestCost|LookupRequestFanoutPlanChoice|AnalyzeSuiteRegression|TiFlashCostModel|IndexJoinPreferIndexCoversMoreJoinKeyCols)$' -tags=intest,deadlock ./pkg/planner/core/casetest/cbotest -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'Test(CostModelVer2ScanRowSize|CostModelTraceVer2|IndexLookUpRowsLimit)$' -count=1
    go test -run 'Test(IndexMergePathGeneration|HintForIntersectionIndexMerge)$' -tags=intest,deadlock ./pkg/planner/core/casetest/indexmerge -count=1
    go test -run 'TestCorrelatedSubquery$' -tags=intest,deadlock ./pkg/planner/core/casetest/correlated -count=1
    make lint

## Validation and Acceptance

Acceptance requires all of the following observable results:

1. Cost traces for TiKV `IndexReader` and `TableReader` include a request-factor term and the term increases when the number of access ranges increases.
2. `IndexLookUpReader` contains an initial index request plus the existing double-read request, with no duplicated table lookup request.
3. `IndexMergeReader` contains request costs for each partial scan and for its table handle lookup when present.
4. TiFlash MPP readers and empty-range readers do not receive a TiKV request charge.
5. A forced one-level IndexJoin and a forced nested Apply show the Reader-owned request term inside the parent's multiplied probe cost.
6. Default plans for the sanitized high-fanout issue cases avoid the misleadingly cheap TiKV lookup plan, while a small selective probe case still permits it.
7. No change is made to Cost Model V1, PointGet/BatchPointGet, or parent child-type inspection.

## Idempotence and Recovery

All test and generation commands are safe to rerun. `make bazel_prepare` may rewrite generated Bazel metadata; retain only changes caused by the new top-level tests or source layout. If a cost golden changes outside the intended cases, inspect the plan and revert unjustified fixture churn with a focused patch; do not discard unrelated user files or reset the worktree.

## Artifacts and Notes

The first red slice was captured with:

    go test -run '^TestReaderCopRequestCost$' -tags=intest,deadlock ./pkg/planner/core/casetest/cbotest -count=1

The pre-fix `IndexReader` trace was:

    ((((scan(30*logrowsize(16)*tikv_scan_factor(40.7)))*1.00) + (net(30*rowsize(16)*tidb_kv_net_factor(3.96))))/1.00)*1.00

The assertion failed because the trace did not contain `cop-request(ranges(3)*tidb_request_factor`, proving the test observes the missing behavior rather than failing for setup or plan-shape reasons.

The tests-only patch was also applied to a temporary worktree at the pre-change commit. `TestLookupRequestFanoutPlanChoice` failed because the forced #69392 `IndexHashJoin` trace had no `cop-request` term, and the forced #69092 nested `Apply` trace contained zero request terms. In that baseline, the forced IndexHashJoin cost was approximately 3.72 million versus 0.345 million for MPP. With the implementation, the same forced IndexHashJoin cost is approximately 70.79 million while MPP remains approximately 0.345 million. These numeric comparisons are optimizer-model evidence, not a runtime benchmark.

The targeted green command is:

    go test -run 'Test(ReaderCopRequestCost|LookupRequestFanoutPlanChoice)$' -tags=intest,deadlock ./pkg/planner/core/casetest/cbotest -count=1

`TestAnalyzeSuiteRegression` initially exposed the expected golden change for issue #62438. Recording the suite changed only the two cascades/non-cascades entries for that query, adding the initial IndexLookUp request term while preserving the plan shape.

Ready validation passed with:

    go test -run 'Test(ReaderCopRequestCost|LookupRequestFanoutPlanChoice|AnalyzeSuiteRegression|TiFlashCostModel|IndexJoinPreferIndexCoversMoreJoinKeyCols)$' -tags=intest,deadlock ./pkg/planner/core/casetest/cbotest -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'Test(CostModelVer2ScanRowSize|CostModelTraceVer2|IndexLookUpRowsLimit)$' -count=1
    go test -run 'Test(IndexMergePathGeneration|HintForIntersectionIndexMerge)$' -tags=intest,deadlock ./pkg/planner/core/casetest/indexmerge -count=1
    go test -run 'TestCorrelatedSubquery$' -tags=intest,deadlock ./pkg/planner/core/casetest/correlated -count=1
    make lint

`make bazel_prepare` was run because two top-level Go tests were added. The intentional generated change is the CBO test target's `shard_count` increase from 19 to 21; an unrelated stale shard update in another package was excluded.

The worktree initially contained unrelated untracked paths (`.codewhale/`, `dividend-low-vol-skill/`, `docs/design/tiup-tuf.md`, `reports/`, and `research-notes/`); they are outside scope and must remain untouched.

## Interfaces and Dependencies

No exported API or dependency changes are planned. The implementation uses existing `costusage.CostVer2`, `costusage.CostVer2Factor`, `getTaskRequestFactorVer2`, `getNumberOfRanges`, `costusage.SumCostVer2`, and `costusage.DivCostVer2`. Test code uses existing `testkit`, virtual TiFlash metadata helpers, and `testify/require` already imported by the CBO suite.

Revision note (2026-08-03): initial self-contained implementation plan created after source and test-layout inspection. Updated after red/green implementation and intentional golden recording.
