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
