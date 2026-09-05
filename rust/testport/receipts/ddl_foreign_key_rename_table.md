# `pkg/ddl` RENAME TABLE foreign-key metadata parity receipt

Status: completed Rust-only alignment for Go's table-rename metadata rewrite.
Go authority is `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`;
the complete recursive `pkg/ddl` inventory remains in
`receipts/ddl_foreign_key_owner_inventory.md`.

Go's `TestRenameTableWithForeignKeyMetaInfo`
(`pkg/ddl/tests/fk/foreign_key_test.go:358-474`) accepts renaming a table that
declares or is targeted by a foreign key. The owner updates `FKInfo.RefSchema`
and `FKInfo.RefTable` for every affected child; a self-reference on the moved
table follows the new schema and table name, and a later parent rename updates
the external child. Rust previously rejected every participating-table rename
from `table_lifecycle.rs` and the ALTER rename arm.

`foreign_key::rewrite_table_references` now walks all catalog tables before the
source entry is moved, rewriting matching parent names. Both `RENAME TABLE` and
`ALTER TABLE ... RENAME TO` call the helper; normal duplicate/schema checks and
multi-pair staging remain unchanged. Column renames and DROP COLUMN still need
their separate column-name maintenance and remain refused.

Focused regressions:

- `tidb-executor::fk_alter_meta_and_privilege_source::rename_table_rewrites_the_constraint_reference`
  covers a checks-off self-reference moved `test.t1` → `test2.t2`, then a
  cross-schema child moved to `test2.tt2` and its parent moved to `test3.tt1`.
  It asserts the stored child references and live referred-owner discovery.
- `tidb-session::tests_foreign_key::rename_table_rewrites_foreign_key_references`
  verifies SQL-visible `SHOW CREATE TABLE` references for the same shapes.

No Go, generated, platform, Bazel, or module files changed. The existing
column-rename and multi-action ALTER atomicity boundaries remain documented in
the shared FK ledger.
