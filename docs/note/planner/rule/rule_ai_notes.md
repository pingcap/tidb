# Planner Rule AI Notes

This file records planner rule related PR experience and pitfalls. Append a new entry after each relevant change.

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
- Run: `go test ./pkg/planner/core/casetest/rule -run TestConstantPropagateWithCollation -record -tags=intest`.
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
- Record with: `go test ./pkg/planner/core/casetest/rule -run TestConstantPropagateWithCollation --tags=intest -record`.
- Add integration test to `tests/integrationtest/t/select.test` and record via `./run-tests.sh -r select` (integration tests use `-r`, not `-record`).
