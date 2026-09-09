# `pkg/ddl` MODIFY NULL-to-NOT-NULL error parity

Status: bounded Rust parity fix implemented and validated against Go master.
This is a follow-up within the package-scoped `pkg/ddl` receipt set; it does
not claim that the entire package has no remaining gaps.

Go source: `origin/master` at `f2c346fe4f3` (2026-09-05).
Rust owners: `rust/crates/tidb-executor/src/kv_table.rs` and
`rust/crates/tidb-executor/src/ddl/alter_table.rs`.

## Inventory completed before editing

The complete `pkg/ddl` tree was inventoried before this edit: 293 tracked
artifacts, including 262 Go production/test files and 30 build/ownership
inputs, with generated/platform/build artifacts included in the ledger
receipts `b102`, `b105`–`b118`, and `b109`. The package-level `pkg/ddl/doc.go`
was read first. The relevant Go authority was then reread in
`modify_column.go` (`checkModifyColumnData`) and `column.go`
(`checkModifyColumnData`): a stored NULL rejected by a new `NOT NULL` column
returns `dbterror.ErrInvalidUseOfNull`, MySQL code 1138, message
`Invalid use of NULL value`.

The Rust owner inventory covered the complete `tidb-executor` source/test
surface relevant to ALTER execution, including `kv_table.rs`,
`ddl/alter_table.rs`, driver error rendering, and
`tests_ddl_modify_column_types.rs`. No Go, Bazel, module, generated, or build
artifact was changed.

## Mismatch and fix

Before this batch, `KvTable::modify_column_in` classified a NULL rejected by
`NOT NULL` as `DataTruncatedAtRow`, and the DDL adapter rendered MySQL 1265
(`Data truncated for column 'a' at row 4`). Go returns the unformatted DDL
error 1138 instead. The Rust storage layer now carries an explicit
`InvalidUseOfNull` outcome and the DDL adapter maps it to Go's 1138 catalog
error and exact message. Value-conversion failures retain their existing
1265/1292 paths.

## Regression proof

The existing Go-derived regression
`tests_ddl_modify_column_types::modify_column_null_to_not_null_rejects_rows_holding_nulls`
was tightened to assert code 1138 and the exact message while still checking
that all rows survive the refused alteration.

Fail-before (after tightening the assertion, before the production change):

```text
... cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib \
  tests_ddl_modify_column_types::modify_column_null_to_not_null_rejects_rows_holding_nulls \
  -- --exact --nocapture
assertion failed: left 1265, right 1138
```

After the fix the same command passes (1 test).

## Ready validation

Profile: **Ready**. This Rust-only change does not require `make bazel_prepare`.

Commands run:

```text
env OPENSSL_DIR=$PWD/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 \
  cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib \
  tests_ddl_modify_column_types::modify_column_null_to_not_null_rejects_rows_holding_nulls \
  -- --exact --nocapture
# passed (1 test)

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml \
  --package tidb-executor -- --check
# passed

git diff --check
# passed

make lint
# passed (exit 0)
```

## Risk and boundaries

Correctness risk is low: only the NULL-to-`NOT NULL` branch changes; ordinary
value conversion and row-preservation behavior are unchanged. Compatibility
improves to Go's client-visible errno/message. Performance is unchanged apart
from avoiding construction of a row-numbered truncation error on this refusal.
The broader DDL job/reorg lifecycle remains outside this Rust owner and is
recorded by the existing package receipts.
