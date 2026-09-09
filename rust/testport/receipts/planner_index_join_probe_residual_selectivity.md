# IndexJoin probe Selections apply the residual-filter selectivity

Comparison source: Go `pkg/planner/core/exhaust_physical_plans.go`
(`constructDS2TableScanTask`, `constructIndexJoinStatic`) and
`pkg/planner/cardinality/selectivity.go` (`Selectivity`). No Go source was
edited.

## Go behavior (the oracle)

`constructIndexJoinStatic` computes `avgInnerRowCnt = EqualCondOutCnt /
buildRows`. `constructDS2TableScanTask(ds, ranges, chosenRemained,
chosenAccess, ..., rowCount=avgInnerRowCnt, maxOneRow)` then:

* computes `selectivity = cardinality.Selectivity(ds.TableStats.HistColl,
  ts.FilterCondition, ...)` over the RESIDUAL filters only (the inner-only
  access conditions are re-attached to the Selection afterwards, at
  `exhaust_physical_plans.go:913-917`);
* sets `countAfterAccess = rowCount / selectivity`, the scan's statistics;
* sets the pushed-down `Selection` to `ts.StatsInfo().Scale(selectivity)`
  (`find_best_task.go:3210`), i.e. the scan's count times the residual
  selectivity.

For the TPC-C stock probe the residual filter is `lt(s_quantity, 18)` on a
pseudo table. Go's full `Selectivity` builds the column range and applies
`getPseudoRowCountByColumnRanges`' `lessCount - nullCount` formula:
`1/3 - 1/1000 = 0.332333`. With the one-row clamp and the 1.25 probe count
from `GetEstimatedProbeCntFromProbeParents`, `EXPLAIN` prints the scan as
`1.25` and the Selection/TableReader as `0.42`.

## The Rust gap

The index-join table arm set the pushed-down `Selection`'s statistics to the
scan's own count (`stats.clone()`), so the probe's post-filter estimate was
`1.25` instead of `0.42`; and it computed no residual selectivity at all. The
planner already had the right estimator (`logical::rewrite::
pseudo_range_filter_selectivity`, which ports Go's full pseudo `Selectivity`
through the StatsNode range builder) but it was private and only used by
`recursive_derive_stats`.

## Change

* `logical::rewrite::{analyzed_filter_selectivity,
  pseudo_range_filter_selectivity}` are `pub(crate)`.
* `DispatchContext` carries `selectivity_factor` (Go
  `tidb_opt_selectivity_factor`) with a builder, wired from the statement
  context in `planner_bridge`.
* The index-join table arm snapshots `table_filters` BEFORE re-attaching the
  inner-only access conditions and scales the pushed-down Selection by the
  residual selectivity: the pseudo estimator for `stats:pseudo` sources, the
  analyzed NDV estimator otherwise.

## Regressions

* `driver::tests::joins::tpcc_stock_level_bounds_both_join_leaves` passes
  (was a baseline failure): the probe `TableReader` now estimates `0.42`.
* `driver::tests::joins::an_index_join_probe_displays_the_outer_probe_count`
  now also asserts the probe `TableReader` is `0.42`; it failed at the previous
  commit with `1.25`.
* Full `tidb-executor` lib: 1247 passed / 12 failed — the stock-level test
  removed from the failure set, no additions.

## Ready validation

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1247 passed; 12 failed; only tpcc_stock_level_bounds_both_join_leaves left
# the previous failure set

cargo test -p tidb-planner
# 1278 passed; 0 failed

cargo test -p tidb-expr
# 1206 passed; 2 failed; the same two pre-existing failures

cargo check --locked --all-targets -p tidb-executor
# passed

rustfmt --edition 2021 --config skip_children=true --check \
  crates/tidb-planner/src/find_best_task/dispatch.rs \
  crates/tidb-planner/src/logical/rewrite.rs \
  crates/tidb-executor/src/driver/planner_bridge.rs \
  crates/tidb-executor/src/driver/tests/joins.rs
# no new drift (two pre-existing rewrite.rs hunks only)

git diff --check
# passed
```
