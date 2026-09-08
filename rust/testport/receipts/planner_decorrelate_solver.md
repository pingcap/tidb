# `DecorrelateSolver`: the uncorrelated and simple-apply arms

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## Go behavior (the oracle)

`DecorrelateSolver.Optimize` (`pkg/planner/core/rule_decorrelate.go:229`)
walks the logical tree and rewrites a `LogicalApply` into a `LogicalJoin`
wherever the correlated inner plan can be lifted out. The arms implemented in
this batch, in Go's order:

- recompute `apply.CorCols` with
  `coreusage.ExtractCorColumnsBySchema4LogicalPlan(innerPlan, outerSchema)`;
  an empty set turns the apply into its embedded join (`:290`);
- `NoDecorrelate` falls through to the children walk;
- an inner `Selection` has every condition `Decorrelate`d against the outer
  schema, attached as join conditions, and the selection is peeled (`:301`);
- an inner `MaxOneRow` over a max-one-row child is peeled (`:311`);
- an inner `Sort` is dropped (`:578`);
- an inner `Limit` is peeled for the semi/anti family when the apply carries
  no conditions and the offset is zero (`:326`).

Go assigns `p = join` and falls through to `NoOptimize`, so the CONVERTED
join's children are still visited by the rule.

## Rust changes

`crates/tidb-planner/src/logical/rule_decorrelate.rs` (new): the rule above.
`logical/rule.rs` now returns its body instead of `None`, and
`logical/mod.rs` declares the module.

`plan_builder.rs::build_expression_subquery` now sets Go's
`FlagPredicatePushDown | FlagBuildKeyInfo | FlagDecorrelate |
FlagConstantPropagation`, which `PlanBuilder.buildApply`
(`logical_plan_builder.go:1000`) sets whenever it builds an apply. Without
this a query whose FROM has no join never had the `decorrelate` bit, so the
rule was skipped even though it is in `optRuleList`.

`driver::physical_builder` maps a physical `LeftOuterSemi` join's output
offsets through `physical_join_output_offsets`, keeping the identity mapping
so the executor's 0/1 marker column lands where `JoinExec` emits it.

## Narrowing

The left-outer-semi family (`LeftOuterSemi`/`AntiLeftOuterSemi`) is NOT
converted yet. Its marker column is emitted after the outer child's columns,
while the Rust pruner leaves the outer child's unused columns in place, so
the converted join's schema and the executor's emission disagree (Go inserts
the pruning projection during `LogicalJoin.PruneColumns`). Those applies stay
`Apply` until that projection alignment is ported. `Semi`/`AntiSemi` carry no
marker and convert.

The aggregation pull-up arm (`:343`), the projection arm (`:314`), the
aggregate group-below arm (`:404`) and `pruneRedundantApply` (`:137`) are not
ported yet.

## Regressions

- `driver::tests::subqueries::tpch_q16_non_null_not_in_is_an_anti_semi_join`:
  the uncorrelated `NOT IN` apply now becomes `HashJoin anti semi join`.
- `driver::tests::subqueries::correlated_subqueries`: the uncorrelated scalar
  apply no longer duplicates its outer row.
- `driver::tests::subqueries::evaluated_scalar_predicate_is_pushed_below_a_sibling_anti_semi_join`
  now clears its anti-semi-join assertion (the correlated `NOT EXISTS` inner
  Selection is attached to the join) and stops on the separate
  `InjectProjBelowAgg` shape assertion.
- The `*_exists_under_or_is_explainable` family keeps its applies and still
  passes.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib driver::tests::subqueries:: -- --test-threads=1
# 8 passed / 9 failed (was 6 passed / 11 failed)

cargo test -p tidb-planner --lib -- --test-threads=1
# 999 passed / 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1229 passed / 23 failed; the two tests above removed from the baseline, no additions

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --check <changed files>
git diff --check -- rust
```
