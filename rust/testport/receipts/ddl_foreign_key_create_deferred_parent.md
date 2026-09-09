# `pkg/ddl` deferred-parent foreign-key parity receipt

Date: 2026-09-05  
Go authority: `pkg/ddl/foreign_key.go:186-211` and
`pkg/ddl/tests/fk/foreign_key_test.go:539-551`  
Rust carrier: `crates/tidb-executor/src/ddl.rs`,
`crates/tidb-executor/src/ddl/table_constraints.rs`, and
`tests/fk_create_error_matrix_source.rs`

## Scope

Go allows a child table to be created first with `foreign_key_checks=0`, but
when the referenced parent is subsequently created it revalidates every
stored child constraint before publishing the parent. Rust now scans the
catalog's table paths at the same pre-publication point and validates each
matching child constraint against the in-flight parent. A failed parent
creation therefore leaves the catalog unchanged.

The shared validator checks the parent-side rules that can become knowable at
that point: partitioning, referenced-column existence, virtual generated
columns, SET NULL/nullability, type/unsigned/charset/collation compatibility,
and full-length leading index (or single-column clustered-handle) coverage.

## Regression carrier

`fk_create_with_checks_off_defers_validation_to_the_parent` is a table-driven
two-row carrier for Go rows 19-20:

1. child `a int` references a parent `id int` with no covering index →
   `1822 Failed to add the foreign key constraint. Missing index ...`;
2. child `a int` references a parent `id bigint key` → `3780 Referencing
   column 'a' and referenced column 'id' ... are incompatible.`

Both rows first create the child with checks disabled and then assert that the
parent CREATE fails with Go's exact code and message.

## Validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_with_checks_off_defers_validation_to_the_parent -- --nocapture
```

Result: both deferred-parent rows pass. The complete `fk_create_` carrier now
has 13 passing tests and one explicit ignored temporary-table carrier.
