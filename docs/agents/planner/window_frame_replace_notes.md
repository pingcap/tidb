# Window Frame Column Replacement Notes

## 2026-03-11 - Projection elimination must rewrite window frame expressions together with window sort keys

Background:
- Issue #66904 reported several `CTE + derived table + outer join + window` queries failing in planner with
  `Can't find column ... in schema`.
- The reproduced shape included both:
  - a final `ORDER BY` expression referencing a derived column, and
  - a window `RANGE` frame whose bound expressions were derived from the window `ORDER BY` items.

Root cause:
- Projection elimination rewrites downstream operators with `ReplaceExprColumns`.
- `LogicalWindow.ReplaceExprColumns` updated:
  - window function arguments,
  - `PartitionBy`,
  - `OrderBy`.
- But it did not update `Frame.Start/End.CalcFuncs` and `CompareCols`.
- For `RANGE ... CURRENT ROW`, those frame expressions can still hold the pre-elimination column identity from the child projection.
- Later physical `ResolveIndices` then sees a stale window-frame column ID and fails even though a same-named column still exists in the child schema.

Related follow-up:
- `ResolveExprAndReplace` previously rewrote scalar-function arguments in place.
- That is unsafe for planner expressions because they may already be hashed or shared across operators.
- The same issue reproduction first hit a hash-stability assertion before surfacing the original missing-column error.

Implementation choice:
- Make `ResolveExprAndReplace` copy-on-write for `ScalarFunction` so replacement does not mutate an existing expression tree in place.
- Extend `LogicalWindow.ReplaceExprColumns` to rewrite frame `CalcFuncs` and `CompareCols` together with `PartitionBy` and `OrderBy`.

Regression coverage:
- `TestWindowWithOuterJoinAndCTE` in `pkg/planner/core/casetest/windows/window_with_exist_subquery_test.go`
  reproduces the reported `CTE + outer join + window + RANGE/current-row` shape and also covers
  a nearby `CURRENT ROW ... UNBOUNDED FOLLOWING` variant so both frame-bound replacement paths stay exercised.

Validation commands:
- `make failpoint-enable`
- `go test -v -run '^TestWindowWithOuterJoinAndCTE$' -tags='intest,deadlock' -count=1 ./pkg/planner/core/casetest/windows`
- `make failpoint-disable`
- `make lint`
