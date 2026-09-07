# Divergence: correlated EXISTS duplicates outer rows (lost semi semantics)

## Reproduction (2026-09-06)

```sql
create table u (a int primary key);
create table s (x int);
insert into u values (1), (2);
insert into s values (10), (20), (20);
select a from u where exists (select 1 from s where x = a * 10);
-- Port: 1, 2, 2   (THREE rows; the a=2 row fans out over the two x=20 matches)
-- Go:   1, 2      (EXISTS is a boolean predicate; at most one row per outer)
```

With a UNIQUE inner column the answer is correct (`1, 2`), isolating the
failure to multiple inner matches per outer row.

## Diagnosis

`handle_exist_subquery` (`crates/tidb-planner/src/expression_rewriter.rs:1583`)
takes the decorrelated branch when `enable_correlate_subquery` is off. In
that branch `build_limit_one` is NOT applied (it is gated on
`no_decorrelate`), and the resulting join conversion loses the SEMI
qualification for the correlated case — each inner match emits the outer
row, i.e. the plan behaves as an INNER join. Go's oracle keeps
`LogicalSemiApply`/semi-join semantics regardless of correlation, so the
outer row can never multiply.

Contrast: the NOT EXISTS (anti) form and the SELECT-list EXISTS form do not
fan out, and uncorrelated EXISTS is unaffected.

## Plan (queued behind the sibling planner stream)

The fix belongs with the semi-apply/join-conversion region shared with the
in-flight `cached_plan_rebuilds` work:

1. in `build_semi_apply`, keep `semi` semantics on the decorrelated EXISTS
   path (do not let the join conversion downgrade semi -> inner when the
   inner lacks LIMIT 1);
2. or restore Go's gating so `build_limit_one` applies to the inner plan
   whenever the EXISTS is decorrelated without the SEMI_JOIN_REWRITE hint;
3. pin with `u`/`s` above (3 vs 2 rows) plus the NOT EXISTS anti case.
