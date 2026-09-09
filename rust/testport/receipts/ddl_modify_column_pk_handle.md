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

## Follow-up closure — session ALTER COLUMN carriers (2026-09-06)

The complete session-side inventory was rechecked before editing: `pkg/session`
contains 68 Go files, 21 Bazel build/ownership inputs, and one package
manifest; the related DDL source window is `pkg/ddl/db_integration_test.go`
(`TestAlterColumn`, lines 1208-1248) and `pkg/ddl/modify_column_test.go`
(`TestModifyColumnNullToNotNull`, lines 169-214). The Rust owners are
`tidb-session/src/tests_alter_column.rs` and
`tidb-session/src/tests_core/ddl.rs`; no Go, generated, fixture, platform, or
build artifact was changed.

The source fixture uses `a INT KEY NONCLUSTERED` for the successful widening,
index-preservation, and AUTO_INCREMENT transition cases. Rust had accidentally
omitted `NONCLUSTERED`, so those tests exercised the correctly-refusing
clustered-handle path and failed before the intended assertions. The fixtures
now match Go. The shared `tests_core::ddl::modify_column` carrier also now
asserts Go's exact clustered-handle 8200 refusal for `BIGINT PRIMARY KEY` →
`INT`, adds a nonclustered success control, and expects Go's 1138
`Invalid use of NULL value` when converting a stored NULL to `NOT NULL`.

Focused evidence:

- Before the fixture/expectation corrections,
  `tests_alter_column::modify_column_preserves_the_primary_key_and_unique_index`
  failed on the clustered-handle 8200 guard, and
  `tests_core::ddl::modify_column` failed on the stale 1138 expectation.
- Afterward, all 13 `tests_alter_column` tests pass and the focused
  `tests_core::ddl::modify_column` test passes, including the exact error codes
  and the nonclustered positive control.
