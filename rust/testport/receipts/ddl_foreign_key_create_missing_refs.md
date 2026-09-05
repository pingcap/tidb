# `pkg/ddl` CREATE FOREIGN KEY missing-reference diagnostics

Status: completed Rust-only alignment for the missing referenced-table and
referenced-column rows in Go's `TestCreateTableWithForeignKeyError`. Go
authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the complete recursive `pkg/ddl`
inventory remains in `receipts/ddl_foreign_key_owner_inventory.md` and the
source-shaped matrix is recorded in `receipts/b111.md`.

## Behavior restored

`pkg/ddl/foreign_key.go` resolves a referenced table and its columns while
`foreign_key_checks` is enabled. Go distinguishes those failures from the
ordinary schema lookups: a missing parent table is `ErrNoReferencedTable`
(1824), and a missing parent column is `ErrForeignKeyNoColumn` (3734), with the
constraint name included in the latter message. Rust's shared
`ddl::table_constraints::build_foreign_key` previously returned generic 1146
and 1054 errors, so both CREATE TABLE and ALTER TABLE callers lost the FK
diagnostic.

The builder now raises dedicated `DriverError` variants before metadata is
staged, and the driver mapping renders Go's exact text:

```text
[schema:1824]Failed to open the referenced table 'T_unknown'
[schema:3734]Failed to add the foreign key constraint. Missing column 'c_unknown' for constraint 'fk_b' in the referenced table 't1'
```

The checks-off path remains deferred: no parent lookup is attempted when
`foreign_key_checks=0`.

## Focused regression

`tidb-executor::fk_create_error_matrix_source::fk_create_missing_referenced_table_and_column_report_fk_errnos`
is now live (it was previously ignored as a measured generic-error gap). It
creates the source table, exercises both missing-reference rows through
`run_create_table_in`, and asserts the exact errno/message pairs above.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  fk_create_error_matrix_source::fk_create_missing_referenced_table_and_column_report_fk_errnos \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. Temporary-table
FK refusals remain the only explicit CREATE-matrix ignored family in
`fk_create_error_matrix_source.rs`; parent-index, SET NULL/nullability, type
compatibility, self-reference, deferred-parent, constraint-name, partitioning,
and pass-matrix rows are live carriers.
