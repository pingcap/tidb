# `pkg/ddl` table-lifecycle foreign-key error parity receipt

Status: completed Rust-only alignment for Go's TRUNCATE/DROP owner errors.
Go authority is `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`;
the complete recursive `pkg/ddl` inventory remains in
`receipts/ddl_foreign_key_owner_inventory.md`.

Go's `TestTruncateOrDropTableWithForeignKeyReferred` and the serialized
`TestTruncateOrDropTableWithForeignKeyReferred2` owner race require two
different diagnostics when a child outside the statement references a parent:
`TRUNCATE TABLE` raises 1701 (`ErrTruncateIllegalForeignKey`), while
`DROP TABLE` raises 3730 (`ErrForeignKeyCannotDrop`) naming the parent table,
constraint, and child table. With `foreign_key_checks=0`, both operations are
allowed.

Rust previously reused the 1701 variant for DROP TABLE. The catalog check now
returns a dedicated `ForeignKeyTableCannotDrop` error for DROP, while the
truncate path keeps `ForeignKeyTableReferenced`; both checks continue to
ignore children listed in the same multi-table statement.

Focused regressions:

- `tidb-executor::foreign_key_ddl_owner_checks_source::truncate_or_drop_referenced_table_reports_go_errnos`
  covers the serialized owner shape and exact 1701/3730 messages.
- `tidb-executor::fk_alter_meta_and_privilege_source::truncate_or_drop_of_a_referenced_table_reports_go_errnos`
  covers the main `pkg/ddl/tests/fk` shape and checks-off bypass.
- `tidb-session::tests_foreign_key::drop_table_is_refused_while_a_foreign_key_still_points_at_it`
  verifies SQL-visible 3730 behavior while the existing truncate test keeps
  the 1701 contract.

Schema-state race timing and partial-index prefix metadata remain outside this
serialized tier; no Go, generated, platform, Bazel, or module files changed.
