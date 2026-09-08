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

## Diagnosis (2026-09-06)

`handle_exist_subquery` (`crates/tidb-planner/src/expression_rewriter.rs:1583`)
takes the decorrelated branch when `enable_correlate_subquery` is off. In
that branch `build_limit_one` is NOT applied (it is gated on
`no_decorrelate`), and the resulting join conversion loses the SEMI
qualification for the correlated case — each inner match emits the outer
row, i.e. the plan behaves as an INNER join. Go's oracle keeps
`LogicalSemiApply`/semi-join semantics regardless of correlation, so the
outer row can never multiply.

## Resolution (2026-09-09)

The filter-context reproduction above no longer fans out: the Rust planner
now plans it as a semi `HashJoin` whose probe side is
`Projection(u.a, mul(u.a, 10))`, and the driver answers `1, 2`. The
planner-side SEMI qualification was restored by the semi-join conversion
work that landed after this note.

The surviving fan-out was on the EXECUTOR side and hit the SELECT-list form,
which this note had wrongly recorded as unaffected:

```sql
-- t: (1,10),(1,20),(2,5),(3,100),(NULL,7); s: (1,1),(1,2),(2,3)
select g, exists(select 1 from s where s.k = t.g) from t order by g;
-- Port: <nil>|0, 1|1, 1|1, 1|1, 1|1, 2|1, 3|0   (7 rows)
-- Go:   <nil>|0, 1|1, 1|1, 2|1, 3|0             (5 rows)
```

`NestedLoopApplyExec` fed the joiner one inner row per call so one output
chunk could be filled incrementally, but every semi-family joiner in Go
consumes the WHOLE remaining iterator and calls `inners.ReachEnd()` on the
row that settles the outer row. The lost stop made each further matching
inner row append the outer row (or its 0/1/NULL flag) again. The apply now
advances past the remaining inner rows once `try_to_match_inners` reports
`matched` for the semi family; inner/outer joiners keep the one-row loop
because `matched` does not settle their outer row. Regression:
`driver::tests::subqueries::correlated_exists_apply_answers_once_per_outer_row`,
and the grouped `SUM(CASE WHEN EXISTS ...)` shape in
`driver::tests::subqueries::grouped_correlated_subqueries`. Receipt:
`rust/testport/receipts/executor_root_distsql_indexjoin.md`.

The NOT EXISTS (anti) and uncorrelated forms were unaffected, as recorded
here.
