# `pkg/ddl` RANGE COLUMNS bound-kind parity receipt

Status: completed Rust-only alignment for Go's
`checkAndGetColumnsTypeAndValuesMatch` (`pkg/ddl/partition.go:1200-1255` at
Go-master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`). Go checks the evaluated
literal kind before converting it to the partition column type, so an integer
bound for a `DATETIME` RANGE COLUMNS column returns
`dbterror.ErrWrongTypeColumnValue` (1654) instead of being accepted by a
permissive conversion.

`fold_range_column_value` now keeps the raw evaluated `Datum` kind and applies
Go's case-by-case compatibility matrix for temporal, integer, floating-point,
and string column types before conversion. LIST COLUMNS retains its separate
parser/cast path and continues to use the broader `fold_column_value` helper.

Focused regression:

- `partition_db_partition_ddl_source::create_table_with_range_column_partition_datetime_int_bounds_are_1654`
  creates a `DATETIME` RANGE COLUMNS table with integer bounds and asserts the
  exact 1654 code/message from Go's `db_partition_test.go:677-681`.

Validation:

- focused cargo test: pass (1 test)
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass

No Go, generated, platform, Bazel, or module files changed.
