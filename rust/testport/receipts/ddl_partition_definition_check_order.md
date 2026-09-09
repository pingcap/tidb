# `pkg/ddl` partition-definition check-order parity receipt

Status: completed Rust-only alignment for the CREATE-table ordering in Go's
`checkTableInfoValidWithStmt` (`pkg/ddl/create_table.go:517-533`) at Go-master
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. Go validates partition definitions
before the partition expression's return type. Consequently an early
`MAXVALUE` in a scalar RANGE definition returns 1481 before a FLOAT partition
field's later 1659 error, and a non-integer scalar LIST bound returns 1697
before the same later type check.

Rust now builds and validates the definitions first, then runs
`check_partition_expression_type`, preserving the existing duplicate-name,
partition-count, strict-bound, and deferred-duplicate checks in their Go
order. No LIST or RANGE expression semantics were broadened; only the
ordering of already-existing checks changed.

Focused regressions:

- `partition_db_partition_ddl_source::create_table_with_range_column_partition_check_order_row_is_maxvalue_1481`
  covers `FLOAT` plus an early `MAXVALUE` and asserts Go's exact 1481
  diagnostic.
- `partition_db_partition_ddl_source::create_table_with_list_partition_values_not_int_rows_report_1697`
  covers timestamp, decimal, text, blob, enum, and set columns with scalar
  LIST bounds and asserts Go's 1697 diagnostics.

Validation:

- focused regressions and existing RANGE/LIST error matrices: pass
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass
- running the entire partition source test file still shows three unrelated
  pre-existing `tidb_isolation_read_engines` access-path failures in existing
  valid/ALTER tests; the changed tests are independent and green.

No Go, generated, platform, Bazel, or module files changed.
