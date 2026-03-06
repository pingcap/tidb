# Planner Rule AI Notes

This file records planner rule related PR experience and pitfalls. Update an existing section when the topic overlaps; append a new dated entry only for a genuinely new topic.

## 2026-01-31 - NOT NOT in Outer Join ON / Double NOT handling

Background:
- A reproduction showed `LEFT JOIN ... ON NOT NOT (t0.k0 = t2.k0)` treated as an other condition and leading to a cartesian-like join behavior.

Key takeaways:
- `PushDownNot` inside `applyPredicateSimplificationHelper` applies to WHERE predicates; outer join ON conditions are not simplified there.
- `InnerJoin/SemiJoin` fold join conditions into `tempCond` and go through `ApplyPredicateSimplificationForJoin`, so `NOT NOT` on logical ops is already handled.
- For outer join ON conditions, apply `PushDownNot` to `OtherConditions` to avoid `not(not(eq))` becoming an other condition that triggers cartesian joins.

Implementation choice:
- In `LogicalJoin.PredicatePushDown`, before outer join processing, run `PushDownNot` on `OtherConditions` only to eliminate double NOT.
- Guard the normalization with a `UnaryNot` presence check to avoid extra overhead.

Test and verification:
- Add cases to `pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_in.json`.
- Run: `go test ./pkg/planner/core/casetest/rule -run TestConstantPropagateWithCollation -record -tags=intest,deadlock`.
- Confirm `left outer join` keeps `equal:[eq(t0.k0, t2.k0)]`.

Test data pattern used:
- For predicate pushdown cases that need both plan and result validation, keep raw SQL in `predicate_pushdown_suite_in.json`.
- Use the test runner to record `EXPLAIN format='brief'` and query results in `predicate_pushdown_suite_out.json`, so the case list remains simple and readable.
Additional notes:
- `pushNotAcrossExpr` already eliminates `not(not(expr))` when `expr` is a logical operator, because `wrapWithIsTrue` returns logical ops unchanged.
- An explicit double-NOT special case is optional for logical expressions; non-logical expressions should continue to use `IsTruthWithNull` semantics.
- Plan regression (CARTESIAN + other cond) is easier to trigger than result differences on small datasets.

## 2026-02-04 - OR-constant in outer join other conditions (issue #65994)

Background:
- A rewrite introduced `LEFT JOIN ... ON (a = b OR 0)` and the OR constant prevented join key extraction, leading to a cartesian-like join behavior and wrong results.

Key takeaways:
- Outer join `OtherConditions` are not simplified by predicate pushdown, so trivial OR/AND constants must be normalized before `updateEQCond`.
- Applying `ApplyPredicateSimplificationForJoin` on `OtherConditions` is sufficient to remove `OR 0` and keep equality keys.

Implementation choice:
- In `LogicalJoin.normalizeJoinConditionsForOuterJoin`, call `ApplyPredicateSimplificationForJoin` with `propagateConstant=false` on `OtherConditions`.

Test and verification:
- Add SQL-only case to `predicate_pushdown_suite_in.json`; keep DDL in the test setup, otherwise `explain` will try to run `DROP/CREATE`.
- Record with: `go test ./pkg/planner/core/casetest/rule -run TestConstantPropagateWithCollation -tags=intest,deadlock -record`.
- Add integration test to `tests/integrationtest/t/select.test` and record via `pushd tests/integrationtest && ./run-tests.sh -r select && popd` (integration tests use `-r`, not `-record`).

## 2026-02-12 - TiFlash `HasTiflash` propagation and plan-shape regression triage

Background:
- A refactor centralized `HasTiflash` propagation and changed `PreparePossibleProperties` behavior, which triggered broad plan output changes in TiFlash-related suites.
- One real regression appeared in partition pruning: `Unknown column 'a' in expression` from `MakePartitionByFnCol` when the physical partition info was built from output names instead of real table column names.

Key takeaways:
- The primary fix should stay in the `PhysPlanPartInfo` producer path, not in ad-hoc downstream guards.
- For partition pruning, always build pruning expressions with reconstructed table column names (`ReconstructTableColNames`) rather than post-projection/output names.
- Avoid adding planner warning logs in this hot path for fallback behavior; keep the planning path deterministic and quiet unless there is actionable operator-level context.
- When moving logic into `BaseLogicalPlan.PreparePossibleProperties`, verify both `p.hasTiflash` propagation and return-value semantics against operators that previously had custom implementations.

Plan output triage checklist:
1. Classify diffs before recording:
- Plan ID only (`_5` vs `_6`) -> safe to update testdata.
- Engine/mode change (`cop[tiflash]` vs `mpp[tiflash]`) -> validate session knobs and expected default behavior.
- Structural/semantic shape change (new/removed operators or root/Mpp boundary shifts) -> treat as suspicious until proved intentional.
2. For suspicious changes, compare the same case against `master` before re-recording.
3. Keep lock-related expectations explicit:
- `FOR UPDATE` can keep `SelectLock` at root while child subtree runs on MPP; this shape can be valid.
4. Update only targeted result files after confirmation; avoid broad re-record without classification.

Suggested validation set after this class of changes:
- `go test -run TestTiflashPartitionTableScan --tags=intest ./pkg/executor/test/tiflashtest`
- `go test -run TestTiFlashCostModel --tags=intest ./pkg/planner/core/casetest/cbotest`
- `go test -run TestReadFromStorageHint --tags=intest ./pkg/planner/core/casetest/hint`
- `go test -run TestPushDownToTiFlashWithKeepOrder --tags=intest ./pkg/planner/core/casetest/pushdown`
- `go test -run TestPushDownProjectionForTiFlash --tags=intest ./pkg/planner/core/casetest/pushdown`

## 2026-02-16 - Index range dimension mismatch with appended handle column (issue #66291)

Background:
- A panic was reported under query + DDL interleaving: `runtime error: index out of range [2] with length 2`.
- The DDL worker for `ADD/DROP INDEX` on the same table is serialized, so the issue is not caused by concurrent index DDL jobs running at the same time.

Root cause:
- For non-unique index paths, planner may append a handle column to `path.IdxCols` for execution-range construction.
- In `detachCondAndBuildRangeForPath`, ranges were built from the extended column set, but row-count estimation used `indexCols` truncated to index-definition columns.
- In the partial-stats branch (`index stats invalid` + `column stats available`), cardinality estimation iterated using range dimension and read `idxCols[i]`, which could overflow when ranges had an extra handle dimension.

Why it is not deterministic:
- It only triggers when several conditions overlap:
  - optimizer chooses the affected index path,
  - estimation enters the partial-stats path,
  - range construction includes appended handle dimension,
  - query/DDL timing hits the same optimization window.

Implementation choice:
- Fix at the source invariant instead of adding defensive checks in lower-level cardinality code.
- Keep row-count estimation on index-definition columns (`indexCols`) to preserve existing plan-estimation behavior.
- In `pkg/planner/core/stats.go`, if execution ranges carry appended handle dimensions, prune each range to the first `len(indexCols)` dimensions and use the pruned ranges for estimation.
- Keep execution-range building unchanged and avoid a second `DetachCondAndBuildRangeForIndex` for estimation only.
- This removes range/column dimension mismatch while keeping estimation inputs comparable with previous behavior (same estimation column slice and row-count API).

Test and verification:
- Add regression test `TestIndexRangeEstimationWithAppendedHandleColumn` in `pkg/planner/cardinality/selectivity_test.go`.
- The test uses a non-unique index + PK handle shape and mocked stats (`missing index stats + available column stats`) and verifies `EXPLAIN` does not panic.
- Reference command:
  - `go test ./pkg/planner/cardinality -run TestIndexRangeEstimationWithAppendedHandleColumn --tags=intest -count=1`

Reusable lessons:
- Prefer fixing cross-layer shape mismatch at the producer boundary (planner/stats boundary), not by adding deep defensive guards in estimation internals.
- When execution paths append handle columns, do not blindly reuse execution ranges for stats estimation. First align dimensions with estimation columns; prefer range pruning/projection over a second range build when possible.
- For bugfixes in estimation paths, keep inputs comparable with previous baselines whenever possible (minimize semantic movement while removing the panic condition).
- For flaky panic reports under query/DDL interleaving, first separate:
  - DDL job serialization facts,
  - metadata version visibility,
  - optimizer path/branch conditions.
  This avoids misattributing the issue to impossible DDL concurrency.
- Regression tests should intentionally force the vulnerable branch (here: `index stats invalid + partial column stats`) instead of relying on random timing.
