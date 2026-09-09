# `pkg/planner/core` — index-join runtime probe paths over a pruned source

Comparison source: Go `pkg/planner/core/exhaust_physical_plans.go`
(`buildDataSource2IndexScanByIndexJoinProp`, `constructDS2IndexScanTask`,
`attach2Task4PhysicalHashAgg`) and `pkg/planner/core/find_best_task.go`
(`admitIndexJoinInnerChildPattern`). No Go source was edited.

## Go behavior (the oracle)

An index join pushes `IndexJoinProp` down to its inner child. A DataSource
that receives it builds a runtime range scan from the join keys, and the
chain of Projection/Selection/Aggregation nodes above it is planned by the
ordinary enumeration; the bottom-most aggregation is attached to the cop
task (`attach2Task4PhysicalHashAgg`), which splits it into a cop partial
(`SUM`) plus a root final aggregate that also carries the group-by
`firstrow` carriers.

The index path admission and the runtime key mapping compare the chosen
index's columns against the join keys by COLUMN IDENTITY
(`path.FullIdxCols`/`IdxCols`), never by a table position. `IndexColumn.Offset`
is a position in the TABLE's column list, which is not the DataSource's
pruned schema.

## The Rust gaps

1. `path_matches_index_join_runtime`, `index_join_path_is_max_one_row` and
   `index_join_feedback` read `schema.columns.get(index_column.offset)`. For
   TPC-C condition nine the history source prunes to
   `(h_d_id, h_w_id, h_amount)`; `idx_h_w_id`'s `h_w_id` has table offset 4,
   so the probe was refused and the join fell back to a HashJoin. All three
   now resolve through `DataSource::schema_column_for_index_column` (Go's
   `ds.Columns` alignment).
2. `get_hash_aggs` enumerated `CopSingleRead`, `CopMultiRead` AND `Root`
   child properties under an index-join runtime property, so the cost search
   could pick the root-only aggregate and lose the cop partial. Go's
   constructed inner side attaches the aggregation to the constructed cop
   task; under `IndexJoinProp` the port now enumerates only the two cop child
   properties. `attach_agg_over_cop` still falls back to a root aggregate
   when the split is impossible.
3. The executor's index-join inner reader assumed the reader's schema was a
   subset of the table's columns. With a cop partial aggregate the reader's
   output is the partial aggregate's schema (group keys plus partial states),
   so `index_inner_output_offsets` failed with "an index-join reader output is
   absent from its retained table". `build_index_inner_reader` now projects
   the aggregate's INPUT schema, and `build_index_inner_subtree` runs the
   partial aggregate locally above the lookup leaf.

## Result

`tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup` now plans
the Go shape `IndexHashJoin -> [Projection(Build) -> TableReader ->
Selection -> TableRangeScan, Selection(Probe) -> HashAgg -> IndexLookUp
(Selection(Build) -> IndexRangeScan, HashAgg(Probe) -> TableRowIDScan)]`
and executes it. The test still fails on the analyzed arm's join kind and
estimated rows, which are pinned to the older Go inner-side statistics model
(the constructed inner keeps each logical operator's own statistics); that
remainder is recorded in `rust/docs/go-physical-plan-parity-execplan.md`.

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1256 passed; 6 failed; no additions to the baseline set

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```
