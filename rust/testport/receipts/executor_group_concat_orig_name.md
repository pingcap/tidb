# Rust/Go parity receipt: GROUP_CONCAT truncation source names

Date: 2026-09-06

## Scope and inventory

The Go inventory for `pkg/executor` was walked from the `master` source tree,
including production files, `_test.go` files, integration fixtures, generated
and platform variants, Bazel/build metadata, and package documentation. The
Rust inventory covered the corresponding `tidb-executor` aggregate driver,
hash/stream aggregation implementations, tests, and testport fixtures.

## Go behavior

`pkg/errno/errname.go` formats `ErrCutValueGroupConcat` (1260) with the
aggregate argument's `Expression.StringWithCtx`. A bare source column therefore
uses its `Column.OrigName` (`test.g.s`), while a computed/projected argument
falls back to Go's internal column spelling. `baseGroupConcat4String` emits the
diagnostic once per aggregate function after truncating the byte buffer.

## Rust gap and fix

`tidb-executor::driver::aggregate_function` resolved the aggregate argument but
discarded its `Column.orig_name`, leaving `AggFunc.arg_orig_name` empty. The
runtime consequently reported `GROUPCONCAT(Column#1)` for a bare table column,
instead of Go's `GROUPCONCAT(test.g.s)`. The builder now copies the resolved
first argument's source name for `GROUP_CONCAT`; computed arguments retain the
existing `Column#<id>` fallback.

## Focused regression

`tidb-session` test
`tests_core::aggregates::group_concat_max_len_truncates_by_bytes_and_warns_1260_once`
now passes in isolation and asserts the exact 1260 text for a bare source
column, grouped truncation, multiple aggregate functions, byte-boundary cuts,
and DISTINCT input.

## Validation

Focused command:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib \
  tests_core::aggregates::group_concat_max_len_truncates_by_bytes_and_warns_1260_once \
  -- --exact --nocapture --test-threads=1
```

The Ready validation profile (format check, diff check, executor all-targets
check, and `make lint` with isolated Go temp paths) passed for this batch.
