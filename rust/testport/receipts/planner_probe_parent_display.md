# IndexJoin inner plans display the outer probe count

Comparison source: Go `pkg/planner/core/plan.go` (`propagateProbeParents`,
`getEstimatedProbeCntFromProbeParents`), `pkg/planner/core/operator/physicalop/
base_physical_plan.go` (`GetEstRowCountForDisplay`), and
`pkg/planner/core/optimizer.go:980`. No Go source was edited.

## Go behavior (the oracle)

`propagateProbeParents(plan, parents)` records, for every physical operator,
the index-join/apply ancestors it runs under: an index join's INNER child
inherits the parent list PLUS that join, while its OUTER child keeps the
parent list unchanged; every other operator passes the list to its children.

`GetEstRowCountForDisplay()` returns
`StatsInfo().RowCount * GetEstimatedProbeCntFromProbeParents(probeParents)`,
where the factor is the product of each recorded join's OUTER child row count.
`EXPLAIN` prints that display count, not the raw `StatsInfo.RowCount`.

For the stock-level probe the inner scan's statistics are clamped to one row
(`maxOneRow`), so the display is `1 * 1.25 = 1.25` — the row count the scan
really reads across all probes — and the `Selection` above it is scaled by the
same factor.

## The Rust gap

`tidb_planner::physical::BasePhysicalPlan` already carried `probe_parents` as
plan ids, but nothing propagated them and
`est_row_count_for_display()` returned `None` for any plan with parents. The
explain renderer printed `stats_info().row_count()` directly, so every
index-join inner subtree showed its per-probe statistics instead of Go's
probe-scaled display (the stock probe printed `1.00` where Go prints `1.25`).

## Change

`explain::physical_explain_operator` now threads a `probe_count` down the
tree. At a `PhysicalIndexJoin`/`PhysicalApply` the inner child's count is
multiplied by the outer child's `StatsInfo.RowCount`; the outer child and every
non-join child keep the incoming count. A node's displayed `estRows` is
`StatsInfo.RowCount * probe_count`. Roots start at `1.0`, and CTE definition
parts render as their own roots.

This is display-only: no statistics, costs, or execution behavior change.

## Regressions

* New
  `driver::tests::joins::an_index_join_probe_displays_the_outer_probe_count`
  explains the stock-level query and asserts the inner `TableRangeScan` shows
  `1.25`. It failed before with `1.00`.
* Full `tidb-executor` lib: 1245 passed / 13 failed — the identical failure
  set to the previous run (the extra pass is the new regression itself); no
  test regressed.

## Ready validation

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1245 passed; 13 failed; same set as before this change

cargo test -p tidb-planner
# 1278 passed; 0 failed

cargo test -p tidb-expr
# 1206 passed; 2 failed; the same two pre-existing failures

cargo check --locked --all-targets -p tidb-executor
# passed

rustfmt --edition 2021 --config skip_children=true --check \
  crates/tidb-executor/src/explain.rs \
  crates/tidb-executor/src/driver/tests/joins.rs
# passed

git diff --check
# passed
```
