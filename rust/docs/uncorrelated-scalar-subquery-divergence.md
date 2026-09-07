# Divergence: uncorrelated scalar subqueries are rejected

Recorded 2026-09-06 (executor-loop batch). NOT FIXED — planner-boundary
feature; implementation plan below.

## Divergence

Go evaluates an UNCORRELATED scalar subquery by optimizing and running it
during expression rewriting, folding the first row to a constant
(`expression_rewriter.go` `handleScalarSubquery` → `EvalSubqueryFirstRow`).
It works in SELECT lists, WHERE clauses, and DML statements:

```sql
select (select max(v) from s) from t;             -- Go: constant column
select a from t where a = (select max(v) from s); -- Go: works
delete from t where a = (select max(a) from t);   -- Go: works
update t set b = (select count(*) from s);        -- Go: works
```

The port fails all four with the internal text "uncorrelated scalar-subquery
evaluation is not available to the planner" — see
`crates/tidb-planner/src/plan_builder.rs` (the
`ScalarSubqueryOutcome::EvaluateSeparately` arm).

CORRELATED scalar subqueries are NOT affected: they take the Apply path and
work (`update t set b = (select max(v) from s where s.k = t.a)` is pinned in
`crates/tidb-executor/tests/update_subquery_source.rs`).

## Why it is not a one-line fix

`ScalarSubqueryOutcome::EvaluateSeparately { outer, inner }` hands the
rewriter's CALLER both plans: Go's own comment says folding needs
`DoOptimize`/`EvalSubqueryFirstRow`, which are executor-boundary operations
(execution). The planner crate cannot execute; the executor crate executes
`QueryStmt` ASTs it retains (the INSERT SELECT source pattern via
`physical_builder::execute_query`).

## Implementation plan

1. Planner: instead of erroring, record the subquery as a DEFERRED scalar
   site — keep the site's `QueryStmt` (the rewriter has the AST node) on the
   built plan, and emit a placeholder expression whose evaluation reads a
   pre-evaluated constant (the `getparam` shape or an executor-side
   constant-table entry).
2. Executor: when planning a DML/SELECT statement whose plan carries deferred
   scalar sites, plan + execute each `QueryStmt` via the existing
   `physical_builder::execute_query` machinery, then apply Go's semantics:
   zero rows → NULL, more than one row → ER_SUBQUERY_NO_1_ROW (1242, variant
   already exists), else fold the single row.
3. Cache interaction: Go folds these before the plan cache sees the plan, so
   the deferred evaluation must happen in the same pre-cache position.
4. Pins: the four statement shapes above (SELECT list, WHERE, DELETE WHERE,
   UPDATE SET), plus zero-row→NULL and multi-row→1242.

## Blocked on

Executor/planner boundary plumbing sized like a feature branch (the rewriter
region is actively shared with sibling in-flight planner work
`cached_plan_rebuilds`), so it is queued behind that sibling stream rather
than risk conflicting edits in `expression_rewriter.rs`.
