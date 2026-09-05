# `pkg/ddl` RANGE COLUMNS bound-COLLATE parity receipt

Status: completed Rust-only alignment for Go's per-bound expression allowlist
in `buildRangePartitionDefinitions` (`pkg/ddl/partition.go:1682-1686`) at
Go-master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. A `COLLATE` clause in a
RANGE COLUMNS bound is not an allowed partition expression and returns
`dbterror.ErrPartitionFunctionIsNotAllowed` (1564), even though the evaluator
can otherwise pass the string through.

`fold_range_column_value` now rejects a COLLATE node anywhere in the bound
expression tree before conversion, preserving the Go allowlist diagnostic.

Focused regression:

- `partition_db_partition_ddl_source::create_table_with_range_column_partition_collate_bound_is_1564`
  covers `VALUES LESS THAN ('G' COLLATE utf8mb4_unicode_ci)` and asserts the
  exact 1564 code/message from `db_partition_test.go:842-845`.

Validation:

- focused cargo test: pass (1 test)
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass

No Go, generated, platform, Bazel, or module files changed.
