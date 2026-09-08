# FIXED: uncorrelated scalar subqueries are evaluated at plan time

Recorded 2026-09-06 (executor-loop batch) as a divergence; fixed 2026-09-09.

## Divergence (historical)

Go evaluates an UNCORRELATED scalar subquery by optimizing and running it
during expression rewriting, folding the first row to a constant
(`expression_rewriter.go` `handleScalarSubquery` → `EvalSubqueryFirstRow`).
It works in SELECT lists, WHERE clauses, and DML statements:

```sql
select (select max(v) from s) from t;             -- Go: constant column
select a from t where a = (select max(v) from s); -- Go: works
delete from t where a = (select max(a) from t);   -- Go: works
update t set b = (select count(*) from s);        -- Go: works
insert into d (a) values ((select max(a) from s)); -- Go: works (VALUES form)
```

The port failed all of them with the internal text "uncorrelated
scalar-subquery evaluation is not available to the planner".

CORRELATED scalar subqueries were never affected: they take the Apply path and
work (`update t set b = (select max(v) from s where s.k = t.a)` is pinned in
`crates/tidb-executor/tests/update_subquery_source.rs`).

## Fix

Implemented 2026-09-09 with the executor-installed hook the implementation plan
called for, but WITHOUT deferred plan sites: the planner stores an optional
`SubqueryEvaluator` on `PlanBuilder` and the executor installs a closure that
performs Go's `DoOptimize` + `EvalSubqueryFirstRow` for one child. The
evaluated first row is folded into a `Constant` carrying Go's
`ScalarQueryCol#N` id (a plain signed 1/0 for EXISTS), the optimized child is
registered as its own `ScalarSubQuery` EXPLAIN root, and a new
`MarkerKind::Constant` carries the folded expression through the existing
marker tail. The batch also fixes the Go-order conjunct rewrite in
`buildSelection`, the trailing projection's fresh output column ids, the
parallel HashAgg's group order, and the table scan's `_tidb_commit_ts` slot.

Full details, Go file:line table, regression pins, and validation commands:
`testport/receipts/scalar_subquery_plan_time_evaluation.md`.

The four statement shapes above are covered by
`crates/tidb-executor/src/driver/tests/subqueries.rs` (SELECT list, WHERE,
zero-row → NULL, multi-row → 1242) and by the existing UPDATE/DELETE
subquery-source tests. Zero rows folds to NULL because Go's `MaxOneRowExec`
appends a NULL row; more than one row raises 1242 from the same executor.

The plan-cache interaction Go has (folding happens before the cache sees the
plan, and the statement is marked un-cacheable) is not reachable here: this
port does not evaluate the subquery from a cached plan, and the hook runs in
the same pre-cache build position.
