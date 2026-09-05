# `pkg/ddl` CREATE FOREIGN KEY self-reference parity receipt

Date: 2026-09-05  
Go authority: `pkg/ddl/foreign_key.go:186-211` and
`pkg/ddl/tests/fk/foreign_key_test.go:530-538,645-747`  
Rust carrier: `crates/tidb-executor/src/ddl/table_constraints.rs` and
`tests/fk_create_error_matrix_source.rs`

## Scope

Go resolves a self-reference against the `TableInfo` being built rather than
looking only in the catalog. The Rust CREATE path now passes that in-flight
`KvTable` into the shared foreign-key builder; ALTER TABLE uses the same
builder with the current table. This keeps CREATE and ALTER on one validation
surface while allowing the CREATE table to remain unpublished until all
constraints pass.

The in-flight validation covers the Go owner checks relevant to this matrix:

- a self-reference that maps each child column back to the same column is
  refused with `1215 Cannot add foreign key constraint`;
- a reordered self-reference must have a covering parent index and otherwise
  returns `1822 Failed to add the foreign key constraint. Missing index ...`;
- distinct-column self-references and valid composite references succeed;
- existing parents are still validated when `foreign_key_checks=0`, while a
  missing parent remains deferred under that switch and is revalidated when
  the parent is later created;
- partitioned self-references are refused with `1506`, using the same check as
  ordinary CREATE/ALTER foreign keys.

## Regression carriers

`fk_create_self_reference_rows_match_go` is a table-driven three-row error
carrier matching Go's rows 16-18:

1. one-column same-name self-reference → `1215`;
2. two-column same-order self-reference → `1215`;
3. reordered self-reference without the required `(b,a)` index → `1822`.

`fk_create_with_checks_off_defers_validation_to_the_parent` runs the two Go
child-first rows and rechecks the parent-side index and type rules when the
parent lands. `fk_create_pass_matrix_succeeds` runs nine Go success controls,
including
distinct-column and reordered composite self-references, a parent that is
`NOT NULL` without `SET NULL`, wider compatible varchar/decimal columns, a
full-length prefix index, checks-off unknown-parent deferral, shared parent
indexes, and a legal 64-character constraint name.

The previously ported metadata carriers now run their self-reference legs
against the live in-flight table rather than disabling checks to avoid the old
catalog-visibility gap.

## Validation

Focused commands:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_self_reference_rows_match_go -- --nocapture
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_ -- --test-threads=1
```

Result: the focused self-reference carrier passed; the complete `fk_create_`
filter passed with 13 tests and 1 explicit ignored carrier (temporary-table
refusals), with zero failures.

The full Ready profile for this batch is recorded in the commit and consists
of workspace format check, `tidb-executor` all-targets check, `git diff --check`,
and `make lint`.
