# `pkg/expression` real-to-UNSIGNED cast warning receipt

Status: completed Rust-only alignment for Go-master
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Inventory completed before editing

The complete Go `pkg/expression` inventory was reused from the preceding
planner-cast audit: production builtins and registries, unit/integration test
fixtures, generated expression tables, platform variants, and package build
artifacts were inspected before editing. The behavior-bearing Go sources are
`pkg/expression/builtin_cast.go` (`builtinCastRealAsIntSig.evalInt`) and
`pkg/types/convert.go` (`ConvertFloatToUint`). The Rust owners are
`tidb-expr/src/cast.rs`, `tidb-expr/src/constant_fold.rs`, and the session
cast regression module. No Go, generated, platform, Bazel, or module files
changed.

## Go behavior restored

Go rounds a real input with `RoundFloat`, then `ConvertFloatToUint` returns the
low 64 bits plus `ErrOverflow` for a negative value when
`AllowNegativeToUnsigned` is enabled. It also returns `UNSIGNED BIGINT`'s
upper bound plus `ErrOverflow` for a positive value at or beyond `2^64` (and
for positive infinity). Rust now routes `Datum::Float32` through the same
real conversion, reports both signed and unsigned real overflow through the
statement warning context, and preserves numeric integer-cast carriers from
planner-side `NoColumns` folding so those warnings remain runtime-owned.

## Focused regressions

- `cast::tests::float32_unsigned_cast_reports_negative_overflow` pins the
  Float32 low-64-bit result and Go's 1690 message.
- `cast::tests::real_unsigned_cast_reports_positive_overflow` pins the upper
  bound and positive-real 1690 diagnostic.
- `tests_cast_int_truncation::a_negative_real_cast_to_unsigned_keeps_its_low_64_bits`
  covers negative real, scientific, rounded-zero, and decimal source rules
  through the session planner.
- `tests_cast_int_truncation::a_positive_real_unsigned_overflow_warns_at_runtime`
  proves planner folding does not discard a positive-real overflow warning.

## Validation

- Focused expression and session regressions: PASS.
- Full `tests_cast_int_truncation` module (10 existing/new tests): PASS.
- Ready profile (`cargo fmt --check`, `git diff --check`, locked offline
  `tidb-executor` all-target check, and `GOPATH=/tmp/tidb-codex-gopath
  TMPDIR=/tmp/tidb-codex-tmp make lint`): PASS.
