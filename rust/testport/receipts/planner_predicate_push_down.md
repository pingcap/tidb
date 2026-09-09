# `pkg/planner/core`: predicate push-down keeps a projection's leftover below it

Status: Ready for this focused fix (two coupled planner bugs found by walking
`driver::tests::subqueries::correlated_sum_predicate_pulls_above_unique_outer_join`).

## Bug 1: the projection's leftover Selection was attached above it

Go `BaseLogicalPlan.PredicatePushDown` (`base_logical_plan.go:128`):

```go
rest, newChild, err := child.PredicatePushDown(predicates)
AddSelection(p.self, newChild, rest, 0)
return nil, p.self, nil
```

The predicates the CHILD could not push are attached as a `Selection` above
the child (below the current node), and only the node's own un-pushable
predicates travel upward. `LogicalProjection.PredicatePushDown` relies on this:
`breakDownPredicates` rewrites the predicates through the projection's
expressions, so the result references the CHILD's columns and must stay below
the projection.

The Rust port's `PendingPredicates::PassThrough` returned the child's
leftovers UPWARD instead. The substituted predicate was therefore re-attached
above the wrapper projection, referencing a column the projection no longer
outputs; the second column prune then dropped the projection's expression and
left a dangling reference. `LogicalSequence.PredicatePushDown`
(`logical_sequence.go:60`) really does return the leftovers upward, so it keeps
`PassThrough`; the projection and `LogicalUnionScan` (which inherit Go's base
method) now use a new `PendingPredicates::AttachBelow`.

## Bug 2: the pulled-up aggregation kept a stale output schema

The `CanPullUpAgg` arm (`rule_decorrelate.go:441` first branch) sets the
aggregation's schema to `apply.Schema()` BEFORE the apply's own schema is
replaced. That value is `MergeSchema(outer, aggregation)` — the outer columns
followed by the aggregation's own outputs. The Rust arm read the apply's
stored schema, which column pruning can leave stale relative to the
aggregation (the scalar output column was `15` while the aggregation's output
was `14`). The arm now rebuilds it from the outer columns plus the
aggregation's own schema.

## Effect

With both fixes the decorrelated plan matches the Go `testkit` oracle through
its plan shape:

```text
Projection -> Selection(gt(cast(ps_availqty), mul(0.5, Column#14)))
  -> Projection(ps_suppkey, ps_availqty)
    -> HashAgg(group by ps_partkey, ps_suppkey, firstrow(ps_suppkey),
               firstrow(ps_availqty), sum(l_quantity))
      -> HashJoin(left outer join)
```

`correlated_sum_predicate_pulls_above_unique_outer_join` now clears every
plan-shape assertion and stops on
`IndexHashJoin(Build)` for the preserved side — the separate
`compareCandidates` skyline gap recorded in the ExecPlan.

## Regression

`logical::rule_tests::predicate_push_down_attaches_a_projection_leftover_below_the_projection`
fails before and passes after.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-planner --lib -- --test-threads=1
# 1,002 passed / 0 failed
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,235 passed / 19 failed (unchanged set; no deterministic additions)
cargo check --locked --all-targets -p tidb-expr -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
# only the pre-existing rewrite.rs:523/562/586 drift
git diff --check -- rust
```
