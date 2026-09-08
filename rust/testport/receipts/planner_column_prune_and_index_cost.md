# `pkg/planner/core`: empty-projection pruning and common-handle index pricing

Status: Ready for this focused two-cluster batch. Both clusters were found by
walking `driver::tests::aggregates::tpcc_condition_eleven_*` against a Go
`testkit` oracle; each has its own fail-before/pass-after regression.

Go source: the port's Go base in this worktree, `b94006d3ab`
(`physical_index_scan.go`, `logical_projection.go`,
`rule/rule_column_pruning.go`). The oracle probe ran
`explain format='brief'` under `GOTOOLCHAIN=go1.25.12` with
`testkit.CreateMockStore` on the exact TPCC condition-eleven DDL and SQL.

## Cluster 1: an all-pruned projection is replaced by its child

Go `LogicalProjection.PruneColumns` (`logical_projection.go:105`) computes
`allPruned`, keeps the projection only when `allPruned` AND the child is a
`LogicalTableDual`, and otherwise runs the deletion loop unconditionally. The
projection then empties and `:139` returns the CHILD.

Rust guarded the deletion loop with `if !all_pruned`, so an all-pruned
projection kept every output column and never reported itself empty. The
observable consequence was a join-reorder restore projection that Go drops:
condition eleven planned `StreamAgg -> Projection[10,13,12,15,18,17] ->
MergeJoin`, while the Go oracle plans `StreamAgg -> MergeJoin` with no
projection at all. The dual guard was also missing, so the guard's removal
would have wrongly dropped a projection over a `LogicalTableDual`.

Rust changes:

- `logical/projection.rs`: the deletion loop always runs; `all_outputs_pruned`
  exposes Go's `allPruned` test for the dual guard.
- `logical/rewrite.rs`: the `Projection` arm implements Go's dual escape
  (one zero expression, a fresh plan column id, child untouched) via
  `Descend::Stop`.

Regression: `logical::operator_tests::projection_pruning_empties_when_no_output_is_used`
fails before and passes after.

## Cluster 2: a covering index prices the common handle once

Go `PhysicalIndexScan.InitSchema` (`physical_index_scan.go:363`) builds the
physical index schema as the index columns plus `CommonHandleCols`, and only
appends a separate handle column when that schema does not already carry one
(`setHandle`). For `idx_order(o_w_id,o_d_id,o_c_id,o_id)` over the
three-column common handle `(o_w_id,o_d_id,o_id)` that is seven columns.

`find_best_task/dispatch.rs` appended BOTH `ds.common_handle_cols` and
`ds.handle_cols`. For a common-handle table the port keeps those lists equal,
so the priced schema had ten INT slots. On a narrow table the duplicate three
slots pushed the covering `IndexRangeScan` above the clustered
`TableRangeScan`, so `SELECT o_w_id, o_d_id, count(*) FROM orders WHERE
o_w_id = 1 GROUP BY o_w_id, o_d_id` chose the table path; the Go oracle keeps
`IndexRangeScan` with `range:[1,1], keep order:true`. The wide-payload sibling
test `tpcc_condition_two_orders_group_uses_the_covering_index_range` masked
the mistake because the table row dominated the cost.

Regression:
`driver::tests::aggregates::a_narrow_covering_index_range_prices_the_common_handle_once`
fails before and passes after.

## Condition-eleven test correction

The test's synthetic-count search looked for `funcs:count(1)` inside the ROOT
customer `StreamAgg`. Go's root aggregates the cop partial count, so its
detail is `funcs:count(Column#93)->Column#41`; `count(1)` belongs to the cop
child. The search now uses `funcs:count(` and the ordering assertion
(`firstrow(c_d_id) < firstrow(c_w_id) < count`) is unchanged.

## Remaining condition-eleven blocker

With both clusters the UNANALYZED plan matches the Go oracle exactly (two
MergeJoins, three RangeScans, no Projection). The ANALYZED arm still chooses
`IndexHashJoin(IndexJoin(...))` where Go keeps two MergeJoins, which is the
`compareCandidates` metric-by-metric skyline comparison documented in
`docs/go-physical-plan-parity-execplan.md`.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-planner --lib -- --test-threads=1
# 1,000 passed / 0 failed (the new projection regression included)
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,233 passed / 21 failed (the new narrow-index regression is included; the
# 13 statistics-request transport tests passed in this run and flake in others)
cargo test -p tidb-executor --lib -- --test-threads=1 a_narrow_covering_index_range
cargo test -p tidb-planner --lib -- --test-threads=1 projection_pruning_empties
cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
# only the pre-existing rewrite.rs:523/562/586 drift
git diff --check -- rust
```
