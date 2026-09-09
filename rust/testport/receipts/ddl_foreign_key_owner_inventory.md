# `pkg/ddl` foreign-key owner inventory

Authority: `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

This batch read the complete recursive `git ls-tree -r --name-only origin/master pkg/ddl` tree before editing: 293 tracked artifacts (262 Go sources, 30 BUILD/Bazel inputs, and one OWNERS file), 133,120 Go source lines (124 production files and 138 tests/benchmarks). The inventory included generated/platform-tagged files, fixtures, nested packages, and build artifacts. A sorted `<lines> <path>` manifest was generated during the audit; its SHA-256 was `9ffa0f63ba8a8e9cc09b5bd98a76c2e13c7b656622920ce2c07a81df1541004e`.

The per-file walk covered these nested package counts: root `pkg/ddl` 145 artifacts; `bdr` 3; `copr` 3; `ingest` 22; `jobsubmit` 6; `label` 7; `logutil` 2; `mock` 3; `notifier` 8; `placement` 13; `resourcegroup` 3; `schematracker` 6; `schemaver` 5; `serverstate` 4; `session` 4; `systable` 5; `testargsv1` 3; `tests` 42; `testutil` 3; and `util` 6.

The owner functions/tests reread before editing were:

- `pkg/ddl/foreign_key.go:381-442`: `checkTableHasForeignKeyReferred`, `checkDropTableHasForeignKeyReferredInOwner`, `checkTruncateTableHasForeignKeyReferredInOwner`, and `checkTableHasForeignKeyReferredInOwner`.
- `pkg/ddl/executor.go:77-84,477-483`: DROP/TRUNCATE owner call sites and error propagation.
- `pkg/ddl/foreign_key_test.go:209-259`: `TestTruncateOrDropTableWithForeignKeyReferred2`, including exact 1701 text for both statements.
- `pkg/ddl/foreign_key_test.go:261-293`: adjacent index-owner contract, used to keep this batch scoped to table lifecycle errors.

Rust owners inspected were `tidb-executor::foreign_key::referring/check_drop_tables`, `ddl::table_lifecycle::run_truncate_table_in`, the driver error renderer, and `tidb-session::dispatch`'s DDL routes. No Go file was changed.
