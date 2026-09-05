# `pkg/ddl` RENAME COLUMN foreign-key metadata parity receipt

Status: completed Rust-only alignment for Go's column-rename metadata rewrite.
Go authority is `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`;
the complete recursive `pkg/ddl` inventory remains in
`receipts/ddl_foreign_key_owner_inventory.md`.

Go's `TestRenameColumnWithForeignKeyMetaInfo`
(`pkg/ddl/tests/fk/foreign_key_test.go:990-1092`) carries `FKInfo.Cols` when a
child column is renamed and carries `FKInfo.RefCols` for every child when a
parent column is renamed. Self-references and tables with multiple constraints
follow the same rules. Rust already handled the equivalent `CHANGE COLUMN`
path, but the metadata-only `RENAME COLUMN` action still refused every table
participating in a foreign key.

`alter_metadata::rename_column_action` now rewrites the renamed table's
declared `cols` and all matching children's `ref_cols` through
`foreign_key::rewrite_column_name` after the column name is changed. The
ALTER-table blanket refusal is limited to DROP COLUMN; generated/check/
partition-dependent columns keep their existing Go-compatible validation.

Focused regressions:

- `tidb-executor::fk_alter_meta_and_privilege_source::rename_column_follows_the_constraint_meta`
  covers a self-reference, child-side rename, parent-side rename, and two
  constraints sharing the parent column, asserting both declared and referred
  metadata.
- `tidb-session::tests_foreign_key::rename_column_rewrites_foreign_key_references`
  verifies SQL-visible `SHOW CREATE TABLE` output for those shapes.

Multi-action ALTER atomicity and DROP COLUMN's distinct error semantics remain
documented boundaries; no Go, generated, platform, Bazel, or module files
changed.
