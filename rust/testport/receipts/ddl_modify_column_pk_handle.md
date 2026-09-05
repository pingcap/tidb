# `pkg/ddl` clustered-handle MODIFY type parity receipt

Status: completed Rust-only alignment for the clustered-handle branch of Go's
`checkModifyTypes` (`pkg/ddl/modify_column.go:1960-1972`). Go refuses a
`MODIFY`/`CHANGE` that requires reorganization of a primary-key handle, even
when both types are integers, because the handle is part of the row key. A
display-width-only integer edit keeps the same type code and signedness and
remains metadata-only.

`ddl::modify_column_action` now applies that same guard: integer family
changes (for example `INT` → `MEDIUMINT`) and signedness changes return
`[ddl:8200]Unsupported modify column: this column has primary key flag`, while
`INT` → `INT(5)` remains accepted.

Focused regression:

- `tidb-executor::tests_ddl_modify_column_types::clustered_handle_integer_type_changes_are_refused`
  asserts the exact 8200 code/message for `INT` → `MEDIUMINT`, then verifies
  the display-width-only control succeeds.

Validation:

- focused cargo test: pass (1 test)
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=... make lint`): pass

The remaining online-reorganization and generated-column rows in the broad
failpoint carrier stay explicit boundaries; no Go, generated, platform,
Bazel, or module files changed.
