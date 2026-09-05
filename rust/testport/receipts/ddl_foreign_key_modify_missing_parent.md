# `pkg/ddl` CHANGE COLUMN missing-parent foreign-key parity receipt

Status: completed Rust-only alignment for Go's `TestFix59705` missing-parent
leg (`pkg/ddl/foreign_key_test.go:447-486`). Go's
`checkModifyColumnWithForeignKeyConstraint` resolves each declared foreign
key's referenced table before comparing the requested and stored types. An
unchecked, deferred foreign key therefore still reports
`[schema:1146]Table 'test.parent' doesn't exist` when a later `CHANGE COLUMN`
touches the child column; it does not silently accept the local rename.

`foreign_key::check_modify_column` now performs the same parent lookup and
returns `SchemaErrorKind::UnknownTable` before type checking. A stale
referenced column is likewise surfaced as Go's 1054 unknown-column error once
the parent exists. The lookup is limited to type/width-changing MODIFY or
CHANGE operations, preserving Go's early return for nullability-only edits.

Focused regression:

- `tidb-executor::foreign_key_ddl_owner_checks_source::fix_59705_change_column_toward_a_missing_parent_reports_1146`
  creates the unchecked child, changes `pid_test` to `pid varchar(10)` while
  `test.parent` is absent, and asserts the exact 1146 code and message.

Validation:

- focused cargo test: pass (1 test)
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass

No Go, generated, platform, Bazel, or module files changed.
