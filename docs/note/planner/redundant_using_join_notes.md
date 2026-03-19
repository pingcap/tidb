# Redundant USING/NATURAL JOIN Notes

## 2026-03-09 - Qualified redundant-column remap must preserve projection identity and predicate type semantics

Background:
- Issue #66272 originally required remapping redundant `JOIN ... USING` / `NATURAL JOIN` columns so later planner phases do not keep an unresolvable redundant-side column.
- The root cause is that planner name resolution and executable join output use different column views:
  - `FullSchema`/`FullNames` still contain the redundant side for qualified-name lookup,
  - `Join.Schema()`/`OutputNames()` only keep the canonical visible output column.
- A qualified predicate such as `t3.id = 10` could therefore bind to the redundant side during name resolution, then survive into later optimization even though that redundant column no longer exists in `Join.Schema()`.
- Two follow-up review findings showed the first fix was too broad:
  - projection metadata for `SELECT t_right.col` could be mislabeled as the canonical visible side,
  - `WHERE/HAVING` remap could silently change predicate semantics when the redundant and visible columns had different types.

Key takeaways:
- Projection naming and predicate remapping have different correctness constraints.
- For projection metadata, keep the original redundant-side `FullNames` entry so `ResultField` table/original-table metadata still matches the selected column.
- For predicate remapping, only reuse the canonical visible column when the join is an inner join and both sides have identical `RetType`.
- Outer joins must not reuse the same remap because null-preserving side semantics are not interchangeable.
- DML must not reuse the coalesced-output mapping because `UPDATE`/`DELETE` restore the join schema to merged child outputs after `USING`/`NATURAL JOIN` coalescing.

Implementation choice:
- `coalesceCommonColumns` records `redundant column -> canonical visible output` mappings only for the `SELECT`-style coalesced join output that survives in normal query paths.
- `findColFromNaturalUsingJoin` now reads the redundant-side identity from `FullSchema`/`FullNames` instead of `ResolveRedundantColumn`.
- `LogicalJoin.ResolveRedundantColumn` now returns a mapped column only when the redundant-side and visible-side `RetType` values are equal.
- `expression_rewriter` and `havingWindowAndOrderbyExprResolver` only remap qualified redundant base-table columns for inner joins.
- `UPDATE`/`DELETE` explicitly skip redundant-column remap because the final DML schema is reset to merged child outputs; a mapping captured from the temporary coalesced output would become stale and could point to the wrong side.

Regression coverage:
- `SELECT t3.id FROM t1 JOIN t3 USING(id)` verifies result-field metadata still reports `t3`.
- Mixed-type `VARCHAR`/`INT` `USING(id)` with `WHERE t_mixed_r.id = '01a'` verifies qualified predicates keep right-side integer semantics.
- `UPDATE ... JOIN ... USING(id)` and `DELETE ... JOIN ... USING(id)` verify DML binding still follows merged-schema semantics.
- `LEFT JOIN`/`RIGHT JOIN` null-side checks verify outer joins do not incorrectly reuse inner-join remapping.

Validation commands:
- `make bazel_prepare`
- `go test ./pkg/planner/core/casetest/schema -run ^TestSchemaCannotFindColumnRegression$ -tags=intest,deadlock`
- `make lint`
