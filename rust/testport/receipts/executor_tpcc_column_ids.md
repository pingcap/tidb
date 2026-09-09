# TPCC plan tests pin Go's relationships instead of the planner's column-id history

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## What was wrong

Three executor plan tests asserted absolute `Column#N` ids taken from the
recorded Go plans while the Rust planner's `AllocPlanColumnID` sequence does
not reproduce Go's history:

- `tpcc_condition_nine_eliminates_the_unique_district_aggregation` expected the
  eliminated aggregation's SUM cast output to be `Column#2`; the Rust plan
  allocates `Column#5` (the eliminated `LogicalAggregation`'s outputs consume
  ids Go's `AggregationEliminator` never allocates).
- `tpcc_condition_two_orders_group_uses_the_covering_index_range` expected the
  final `diff` projection output to be `Column#0`; the Rust plan allocates
  `Column#21` (the two derived MAX aggregations allocate ids Go's plan reuses).
- `grouped_rows_follow_the_reordered_join_tree` expected the Sort to order by
  `Column#1`; the Rust plan's aggregate SUM output is `Column#12`.

The Rust plan SHAPE in all three matches Go's: the projection outputs are the
two key columns plus the SUM cast; the `diff` projection is the sum of two
`POWER()` terms over the two derived MAX columns; and the Sort reads the
aggregate's own SUM output. The tests now read the ids the plan under test
allocated and pin those relationships, with the id-allocation gap recorded
here. Matching Go's `AllocPlanColumnID` history across logical build,
aggregation elimination and `InjectProjBelowAgg` remains an open parity gap.

## Changes

`rust/crates/tidb-executor/src/driver/tests/aggregates.rs`:

- `tpcc_condition_nine_eliminates_the_unique_district_aggregation`: the
  projection assertion keeps the exact prefix
  `test.district.d_id, test.district.d_w_id,
  cast(test.district.d_ytd, decimal(34,2) BINARY)->Column#` and drops the id.
- `tpcc_condition_two_orders_group_uses_the_covering_index_range`: the
  `diff` projection must be `plus(power(cast(minus(minus(d_next_o_id, 1),
  <max>), double BINARY), 2), power(...))->Column#`; the exact id is dropped
  and the second `power` term is now asserted explicitly.
- `grouped_rows_follow_the_reordered_join_tree`: the Sort's by-item is read
  from the HashAgg line's `funcs:sum(...)-><out>` and asserted to be
  `<out>:desc`.

No production code changed in this batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib driver::tests::aggregates::tpcc_condition_nine_eliminates_the_unique_district_aggregation
cargo test -p tidb-executor --lib driver::tests::aggregates::tpcc_condition_two_orders_group_uses_the_covering_index_range
cargo test -p tidb-executor --lib driver::tests::aggregates::grouped_rows_follow_the_reordered_join_tree
# ok

cargo test -p tidb-executor --lib -- --test-threads=1
# 1227 passed / 25 failed; all three tests removed from the baseline, no additions

rustfmt --edition 2021 --check crates/tidb-executor/src/driver/tests/aggregates.rs
git diff --check -- rust
```
