# `simplifyOuterJoin` converts a null-rejected outer join before attribution

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## Go behavior (the oracle)

`LogicalJoin.PredicatePushDown` (`pkg/planner/core/operator/logicalop/
logical_join.go:171`) opens with `simplifyOuterJoin(p, predicates)` (`:306`,
called at `:192`). For a left/right outer join it picks the null-supplying
(inner) child, skips every predicate that reads ONLY the preserved (outer)
child, and runs `util.IsNullRejected` (`pkg/planner/util/null_misc.go:98`,
after `expression.PushDownNot`) over the rest. The first predicate that cannot
be true when the inner side is NULL-extended flips `JoinType` to
`InnerJoin`. The per-join-type attribution below then runs the INNER arm, so
the predicate is absorbed by the join instead of remaining a Selection above
it.

## The Rust gap

`LogicalJoin::predicate_push_down_local` documented this as a narrowing:
"Blocked on `util.IsNullRejected`'s session-dependent half". The shared
receipts it needed already exist (`tidb_expr::expression::is_null_rejected`,
`tidb_expr::expr_util::push_not::push_down_not`,
`tidb_expr::expr_util::normal_form::expr_from_schema`), so
`crates/tidb-planner/src/logical/join.rs` now runs
`LogicalJoin::simplify_outer_join` at the top of `predicate_push_down_local`,
before the attribution switch, with the same outer/inner mapping and the same
outer-only skip.

`driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables`
was the repro: `SELECT COUNT(*) FROM (... LEFT JOIN ... WHERE orders.o_w_id=1)
AS T WHERE T.o_ol_cnt != T.order_line_count` kept a `Selection(ne(...))` above
a `left outer` MergeJoin and a `Selection(not(isnull(...)))` on the probe side.
With the fix the root Selection is gone and the join is inner (the analyzed
arm of that test then still fails on the MergeJoin-vs-IndexJoin choice, which
is the separate `skylinePruning` gap recorded in the ExecPlan).

## Regression

`crates/tidb-planner/src/logical/rule_tests.rs`:
`predicate_push_down_turns_a_null_rejected_left_outer_join_into_an_inner_join`
builds `Selection(gt(right_col, 7))` over a `LeftOuter` join and asserts the
resulting join type is `Inner`. Without the call the join stays `LeftOuter`
and the Selection is left above the join.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-planner --lib predicate_push_down_turns_a_null_rejected_left_outer_join_into_an_inner_join
# ok

cargo test -p tidb-planner --lib -- --test-threads=1
# 999 passed / 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1214 passed / 38 failed: the same 25 baseline failures plus the 13 known
# full-run flake `driver::catalog::statistics_request_tests::*` (16/16 in
# isolation). No baseline test regressed and none flipped to passing on its
# own; `tpcc_condition_six`'s operator-list assertion now passes and the test
# stops at the analyzed MergeJoin-vs-IndexJoin choice.

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --check crates/tidb-planner/src/logical/join.rs \
    crates/tidb-planner/src/logical/rule_tests.rs
git diff --check -- rust
```
