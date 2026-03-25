# Window Subquery Notes

## 2026-03-10: Deferred window rewrite with correlated and non-scalar subqueries (issue #56532)

Background:
- The original bug report was an `unknown column` failure for correlated scalar subqueries nested inside window expressions.
- Follow-up review found a second correctness issue on the same path: auxiliary-field collection rewrote every extracted `*ast.SubqueryExpr` with `asScalar=true`, even when the user SQL was an `IN`, `ANY`, or `ALL` subquery.

Root cause:
- Deferred window rewrite needs auxiliary select fields for outer columns referenced from subqueries embedded in window expressions, `WINDOW` specs, and related `ORDER BY` items.
- The initial fix extracted bare `*ast.SubqueryExpr` nodes and rewrote them through the scalar-subquery path only to collect correlated outer columns.
- For non-scalar subqueries, that path is semantically wrong:
  - scalar rewrite unconditionally adds `MaxOneRow`,
  - uncorrelated variants may be pre-evaluated with `EvalSubqueryFirstRow`,
  - valid multi-row `IN` / `ANY` / `ALL` subqueries can therefore fail with `ERROR 1242 (21000): Subquery returns more than 1 row`.

Implementation choice:
- Keep the fix on the proven root-cause path in `appendAuxiliaryFieldsForSubqueries`.
- Extract and rewrite complete subquery-bearing expressions instead of always descending to the inner `*ast.SubqueryExpr`.
- The extractor now keeps:
  - `*ast.SubqueryExpr`,
  - `*ast.ExistsSubqueryExpr`,
  - `*ast.CompareSubqueryExpr`,
  - `*ast.PatternInExpr` when `Sel != nil`.
- Rewriting the outer expression preserves the correct subquery handler (`EXISTS`, `IN`, quantified compare, or scalar) while still exposing correlated outer columns from the resulting logical plan.

Regression coverage:
- Existing window regressions cover:
  - correlated scalar subqueries inside window expressions,
  - `EXISTS` subqueries,
  - redundant `USING` / `NATURAL JOIN` outer-column lookup through `FullSchema`.
- `TestWindowSubqueryRewrite` adds direct coverage for valid multi-row non-scalar subqueries inside window expressions:
  - `count(1 in (select t2.c1 from t2)) over ()`
  - `count(1 = any (select t2.c1 from t2)) over ()`

Validation commands:
- `make failpoint-enable`
- `go test -run 'TestWindowSubqueryRewrite|TestWindowSubqueryOuterRef' -tags=intest,deadlock ./pkg/planner/core/casetest/windows`
- `make failpoint-disable`
- `make lint`

Debugging takeaway:
- When planner code needs to inspect subqueries for metadata such as correlated outer columns, avoid reusing a more specialized rewrite path than the original SQL semantics require.
- For subquery-bearing expressions, the safe default is to rewrite the smallest complete expression that still preserves the original subquery kind.
