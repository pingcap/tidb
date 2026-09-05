# `pkg/ddl` temporary-table foreign-key parity receipt

Date: 2026-09-05  
Go authority: `pkg/ddl/tests/fk/foreign_key_test.go:553-575,596-606` and
`pkg/ddl/foreign_key.go`  
Rust carrier: `crates/tidb-executor/src/ddl.rs`,
`crates/tidb-executor/src/ddl/table_constraints.rs`, and
`tests/fk_create_error_matrix_source.rs`

## Scope

Go's `checkReferInfoForTemporaryTable` refuses every foreign-key relationship
involving a temporary table. A temporary child cannot declare a constraint
(local and global forms both return `1215 Cannot add foreign key constraint`).
A local temporary parent is session-only and therefore invisible to a normal
child (`1824 Failed to open the referenced table`), while a global temporary
parent is visible metadata but still rejected as a foreign-key target (`1215`).

Rust now applies the child guard immediately after temporary-table parsing,
before normal FK resolution. Parent lookup distinguishes the local overlay from
global temporary metadata, preserving those Go diagnostics without publishing
an invalid constraint.

## Regression carrier

`fk_create_refuses_temporary_tables_in_both_directions` is a table-driven
four-row carrier matching Go's temporary cases:

1. local temporary parent → normal child: `1824`;
2. global temporary parent → normal child: `1215`;
3. normal parent → local temporary child: `1215`;
4. normal parent → global temporary child: `1215`.

## Validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_refuses_temporary_tables_in_both_directions -- --nocapture
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_ -- --test-threads=1
```

Result: the temporary carrier and the complete FK CREATE matrix pass with
14 tests, zero ignored, and zero failures.
