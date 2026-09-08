# `pkg/executor` clustered index-lookup partial aggregate parity receipt

Status: completed Rust-only alignment for Go's `PhysicalIndexLookUpReader`
table-side partial aggregation. The Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the source owner is
`pkg/ddl/db_integration_test.go:115::TestUniqueKeyNullValueClusterIndex`,
with the package inventory and neighboring baseline notes recorded in
`receipts/b102.md`.

Go keeps a clustered composite-primary-key table's nullable unique-index
entries visible to `SELECT COUNT(*) ... USE INDEX(c)`: both NULL rows count,
and subsequent table/index consistency checks remain clean. Rust's physical
index reader previously treated the final aggregate's synthetic, id-less
`COUNT(*)` column as a stored table column, then dropped the table plan's
`Partial1` aggregate. The resulting source had no resolvable final input and
returned zero (or rejected the physical plan) instead of two.

The Rust executor now:

- permits the synthetic zero-column COUNT child while keeping real unresolved
  index outputs hard errors;
- extracts a `HashAgg`/`StreamAgg` `Partial1` table plan and hands it to the
  index source, matching Go's cop-side aggregate contract; and
- restores the planner-owned partial output schema so the root `FinalCount`
  descriptor resolves by its stable unique id.

Focused and source-shaped regressions:

- `tidb-executor::all::db_integration_ddl_types_source::unique_key_null_value_cluster_index_unique_index_allows_nulls`
  returns `[[Datum::Int(2)]]` through the clustered secondary index and then
  passes `admin check table` and `admin check index`.
- The complete `db_integration_ddl_types_source` module passes all 24 runnable
  tests (five source tests remain intentionally ignored).
- Existing aggregate/index unit coverage for global index COUNT, global AVG,
  clustered-handle grouped aggregation, and index-source internals remains
  green; two unrelated access-path tests remain the pre-existing baseline
  failures listed in the historical receipts.

No Go, generated, platform, Bazel, or module files changed.

## Follow-up: the index reader keeps the cop aggregate's INPUT columns (2026-09-09)

`build_index_reader` derived the index source's kept columns and row schema
from the IndexLookUp's OUTPUT schema. When the table plan is a cop partial
aggregate, that output is the aggregate's RESULT (`Column#12, Column#13,
Column#14, a`), whose id-less columns map to no stored column, so
`reader_output_offsets` kept only `a`. `reader_partial_aggregate` then resolved
the aggregate's arguments (`sum(b)`) against a row schema that no longer held
`b`, and `accept_partial_aggregate` additionally refused the stage because
`input_offsets` exceeded `keep.len()`; the partial aggregate was silently
dropped and the root HashAgg's `Column#12` reference failed with
`a physical expression does not resolve in its child`.

The reader now derives the kept columns from the cop aggregate's CHILD schema
(the table scan's row schema) whenever the table plan is a `HashAgg`/
`StreamAgg` — the same schema Go resolves the aggregate's arguments against.

Regression: `tests_partition_table_sql_source::{direct_reading_with_agg_matches_regular,
parallel_apply_over_partitions_matches_regular}` fail before and pass after.
Ready validation: `tidb-executor` lib serialized 1213 passed / 39 failed, those
two and no additions; `cargo check --locked --all-targets` clean;
`rustfmt --edition 2021 --check` clean; `git diff --check -- rust`.

`driver::tests::aggregates::tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup`
still fails on a SEPARATE stale reference: after the aggregation-elimination
rewrite, `not(isnull(cast_decimal(d_ytd)))` still names the pre-rewrite
`d_ytd` (`UniqueID 10`) while the child schema carries the cast's output
(`UniqueID 13`).

## Follow-up: an index-join inner reader's cop partial aggregate (2026-09-09)

`build_index_inner_reader` built the lookup leaf from the READER's schema and
projected those columns onto the retained table. When the reader's table plan
is a cop partial aggregate (`attach2Task4PhysicalHashAgg`), that schema is the
aggregate's output (group keys plus partial states), so the projection failed
with "an index-join reader output is absent from its retained table".

The leaf is now built against the partial aggregate's INPUT schema, and
`build_index_inner_subtree` runs the partial aggregate locally above the
leaf (`build_aggregation_over_child`); the root final aggregate consumes the
partial columns exactly as it consumes a remote partial. A reader with a
partial aggregate no longer takes the leaf shortcut in
`build_index_lookup_source`, so the composite subtree path wraps it.

Covered by
`driver::tests::aggregates::tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup`,
which executes the grouped index-join plan; before the change the same plan
answered `Unsupported("an index-join reader output is absent from its
retained table")`.

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1256 passed; 6 failed; no additions

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```

