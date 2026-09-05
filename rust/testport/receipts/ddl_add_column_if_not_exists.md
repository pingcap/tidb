# `pkg/ddl` ADD COLUMN IF NOT EXISTS parity receipt

Status: completed Rust-only alignment for Go's `checkAndCreateNewColumn`
duplicate-column guard. The Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the complete part-3 inventory is
recorded in `receipts/b102.md`.

Go checks the column definition, then turns `ErrColumnExists` into a Note
when the individual `ADD COLUMN` specification carries `IF NOT EXISTS`.
`resolveAlterTableAddColumns` applies the same per-spec flag to each member
of a grouped `ADD COLUMN IF NOT EXISTS ( ... )`, so an existing member is
skipped while later new members still commit. Rust's action dispatcher had
discarded the parsed `if_not_exists` flag and raised 1060 for every duplicate.

The Rust dispatcher now passes the guard through both AST forms. Duplicate
columns append the mapped 1060 as a Note and return without changing the
table; unguarded duplicates retain the error. The focused regression
`tidb-executor::all::db_integration_ddl_types_source::add_column_if_not_exists_skips_duplicates_and_continues_grouped_adds`
covers a single duplicate, a new single column, a grouped duplicate plus a
new column, final column order, and the two Note warnings.

The source-shaped concurrency documentary remains ignored because its same
Go matrix also covers the DDL job race; serial ordinary ADD/CREATE INDEX
guards are covered by the companion index receipt, while columnar-index
variants remain an explicit gap. No Go, generated, platform, Bazel, or module
files changed.
