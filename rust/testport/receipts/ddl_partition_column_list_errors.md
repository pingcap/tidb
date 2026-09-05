# `pkg/ddl` partition column-list arity parity receipt

Status: completed Rust-only alignment for Go's `ErrPartitionColumnList`
(1653). Go's parser preserves malformed RANGE/LIST COLUMNS bound tuples in the
AST; DDL validation then checks tuple arity before resolving column names and
returns `Inconsistency in usage of column lists for partitioning`.

Rust's parser previously rejected these shapes with un-coded parser text
(`RANGE partition value count does not match columns`, or
`LIST COLUMNS values require tuples`). The parser now preserves the shape, and
the RANGE/LIST COLUMNS builders return a dedicated `PartitionColumnList`
variant. RANGE validation performs the arity check before column lookup, so a
missing column combined with a malformed tuple follows Go's 1653 check order.

Focused regressions:

- `partition_db_partition_ddl_source::create_table_with_range_column_partition_value_count_rows_report_1653`
  covers a normal RANGE COLUMNS mismatch and the missing-column + mismatch
  ordering case.
- `partition_db_partition_ddl_source::create_table_with_list_columns_partition_column_list_rows_report_1653`
  covers scalar and one-element tuple values against a two-column LIST
  COLUMNS key.

Validation:

- focused cargo tests: pass (2 tests)
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass

No Go, generated, platform, Bazel, or module files changed.
