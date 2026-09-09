# `pkg/planner/core` + `pkg/planner/cardinality` — DataSource pseudo selectivity and join-reorder projection

This receipt covers the Rust `tidb-planner` port of Go's pseudo-statistics
DataSource estimate and the schema-restoring projection the advanced join
reorder inserts when its output order changes. It is a companion to
`planner_index_join_row_floor.md` (the same TPCC condition's inner-scan
stats) and `planner_physical_index_join_explain.md` (its `EXPLAIN` text).

## Go behavior

`deriveStats4DataSource` (`pkg/planner/core/stats.go`) forwards to
`cardinality.Selectivity(ctx, histColl, conds, possibleAccessPaths)`
(`pkg/planner/cardinality/selectivity.go:51`). For each extracted column Go
builds a `StatsNode`, and for the handle column it sets `Tp = PkType` and
estimates the node with
`GetRowCountByColumnRanges(ctx, coll, id, ranges, pkIsHandle=true)`
(`selectivity.go:123`). With pseudo statistics and an integer handle,
`row_count_column.go:47` routes that call to
`getPseudoRowCountBySignedIntRanges`, whose point range returns **one row**.
Every other column keeps `getPseudoRowCountByColumnRanges`, whose point range
returns `RealtimeCount / pseudoEqualRate` (ten rows for the 10,000-row pseudo
table).

That one-row estimate is what Go's advanced join reorder sees when it sorts
its vertices by cumulative cost (`pkg/planner/core/joinorder/join_order.go`,
`joinOrderGreedy.optimize`). For TPCC condition 08 the fixed
`warehouse.w_id = 1` row therefore sorts before `history`, the greedy starts
from the warehouse, and the reordered join's schema no longer matches the
original. `optimizeForJoinGroup` then wraps the join in
`LogicalProjection{Exprs: Column2Exprs(originalSchema.Columns)}` — the
`Projection` that sits between the aggregate and the join in Go's plan.

## Rust implementation

`crates/tidb-planner/src/logical/rewrite.rs::pseudo_range_filter_selectivity`
now marks the integer handle column and estimates it with
`ranger::stats_bridge::pseudo_count_by_int_ranges` instead of the generic
`pseudo_count_by_column_ranges`; the node type was already `PrimaryKey` for a
handle column, but its selectivity was the ten-row generic estimate. With the
warehouse priced at one row, the greedy join order matches Go and
`optimize_join_group` (`crates/tidb-planner/src/joinorder.rs`) emits the
schema-restoring projection its existing `Schema::equal` check was waiting for.

The same fix corrected `tpcc_grouped_join_matches_go_shared_planner_choice`'s
expectation. A `testkit.CreateMockStore` probe of that fixture's clustered
`PRIMARY KEY (d_w_id, d_id)` DDL produces `Projection -> StreamAgg ->
Projection -> MergeJoin -> [TableReader(Build) district range:[1,1]
keep order:true, Point_Get(Probe) warehouse]`, which is exactly what the port
now plans; the test's previous `HashAgg -> IndexHashJoin` expectation came from
a non-clustered fixture and was stale.

## Validation

```text
cargo test -p tidb-planner --lib -- --test-threads=1
# 1002 passed; 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1237 passed; 18 failed (17 deterministic + the known
# hash_agg_spill_tests::each_round flake); TPCC condition 08 is green

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
```

The remaining failures are the TPCC/costing cluster, the window executor, and
the planner-time scalar-subquery registration; none of them is a
pseudo-selectivity regression. `rewrite.rs` keeps its pre-existing rustfmt
drift outside this batch's hunks.

## Risk

The change only narrows a pseudo estimate for a proven integer-handle
equality; analyzed statistics take the other branch and are untouched. The
join-reorder projection was already implemented and only fires when the
reordered schema differs, so no new operator is introduced for joins whose
order is unchanged.
