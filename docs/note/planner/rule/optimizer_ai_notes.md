# Optimizer AI Notes

This file records optimizer-related PR experience and pitfalls. Append a new entry after each optimizer change.

## 2026-01-31 - NOT NOT in Outer Join ON

Background:
- A reproduction showed `LEFT JOIN ... ON NOT NOT (t0.k0 = t2.k0)` treated as an other condition and leading to a cartesian-like join behavior.

Key takeaways:
- `PushDownNot` inside `applyPredicateSimplificationHelper` only applies to `predicates` (WHERE side). It does not touch outer join ON conditions.
- `InnerJoin/SemiJoin` fold join conditions into `tempCond` and go through `ApplyPredicateSimplificationForJoin`, so `NOT NOT` is already handled there.
- Outer join ON conditions need a dedicated normalization step; avoid reclassifying or pushing conditions to children to preserve outer join semantics.

Implementation choice:
- In `LogicalJoin.PredicatePushDown`, before outer join processing, run `PushDownNot` on `OtherConditions` only to eliminate double NOT.
- Guard the normalization with a `UnaryNot` presence check to avoid extra overhead.

Test and verification:
- Add cases to `pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_in.json`.
- Run: `go test ./pkg/planner/core/casetest/rule -run TestConstantPropagateWithCollation -record -tags=intest`.
- Confirm `left outer join` keeps `equal:[eq(t0.k0, t2.k0)]`.

Test data pattern used:
- For predicate pushdown cases that need both plan and result validation, keep raw SQL in `predicate_pushdown_suite_in.json`.
- Use the test runner to record `EXPLAIN format='brief'` and query results in `predicate_pushdown_suite_out.json`, so the case list remains simple and readable.
