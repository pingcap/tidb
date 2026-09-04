# `pkg/types` current-master explain-format delta

## Inventory

The complete Go `pkg/types` tree at `origin/master` contains 61 tracked
artifacts: 56 root-package files and five `parser_driver` files. The root
inventory is:

```text
BUILD.bazel benchmark_test.go binary_literal.go binary_literal_test.go
compare.go compare_test.go const_test.go context.go context_test.go convert.go
convert_test.go core_time.go core_time_test.go datum.go datum_eval.go
datum_test.go enum.go enum_test.go errors.go errors_test.go etc.go etc_test.go
eval_type.go explain_format.go export_test.go field_name.go field_type.go
field_type_builder.go field_type_test.go format_test.go fsp.go fsp_test.go
helper.go helper_test.go json_binary.go json_binary_functions.go
json_binary_functions_test.go json_binary_test.go json_constants.go
json_path_expr.go json_path_expr_test.go main_test.go mydecimal.go
mydecimal_benchmark_test.go mydecimal_test.go overflow.go overflow_test.go
set.go set_test.go string.go time.go time_test.go truncate.go vector.go
vector_functions.go vector_test.go
```

The nested `pkg/types/parser_driver` inventory is:

```text
BUILD.bazel accept_in_place_test.go main_test.go value_expr.go value_expr_test.go
```

There are no generated, platform-specific, fixture, or external data files in
this package tree. The existing b034–b037 receipts cover the complete source
test inventory (206 `Test*` functions). This audit rechecked the current
`origin/master` delta in `explain_format.go`, `vector.go`, and the
parser-driver files before editing; the two self-contained root-package
runtime deltas are restored below.

## Current-master behavior and owner decision

Go master adds the public `ExplainFormatRU = "ru"` literal and appends it to
the validator's `ExplainFormats` slice. The Go package now exports the same
constant and ordered entry, with `TestExplainFormatRU` pinning the value and
position. Rust's `tidb-datatype::explain_format` is the dependency-closed
owner and retains the matching 14-entry validator regression.

Go master also changes `PeekBytesAsVectorFloat32` to use checked `uint64`
size arithmetic. The Go package now performs the checked multiplication and
`TestVectorDeserializeOverflow` covers both peek and zero-copy paths; the Rust
vector owner retains its source-derived overflow regression.

The new parser-driver `AcceptInPlace` methods are API-shaped visitor hooks on
Go driver nodes. Rust represents these expressions as `tidb-ast::Expr` enum
variants and has no dependency-closed driver-node owner or caller that could
accept an invented parallel API. They remain an explicit boundary rather than
a Rust-only carrier.

## Rust-only parity follow-up: float-to-decimal conversion

The same complete 61-artifact `pkg/types` inventory remains the owning package
for this follow-up. Go's `ConvertDatumToDecimal` passes both `KindFloat64` and
`KindFloat32` through `MyDecimal.FromFloat64`, preserving the best-effort
decimal beside an `ErrOverflow`; `GetFloat32` first narrows and widens a
`KindFloat32` payload. Rust's `Datum::to_decimal` previously called
`Decimal::from_signed_literal` on the raw payload for both kinds, discarding
the parse disposition and making a float32 payload look like a double.

The Rust owner now uses the existing Go-compatible shortest-`%g` formatter and
`Decimal::parse_mysql` event mapping. `Real` preserves the saturated
81-digit decimal plus `ScalarConversionEvent::Overflow` for `1e308`; `Float32`
narrows through `f32` before widening and produces the same decimal digits Go
does for `3.1`. No Go or generated/Bazel file changed.

The focused regression is
`datum::convert::tests::source_float_to_decimal_preserves_error_and_float32_precision`.
Fail-before was captured with the test present and the production change
absent: the unfixed path returned no overflow event for `Datum::Real(1e308)`.
After the fix:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib source_float_to_decimal_preserves_error_and_float32_precision -- --nocapture
# passed: 1 test

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib -- --test-threads=1
# passed: 372 tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype -p tidb-expr
# passed; existing warnings only

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib -- --test-threads=1
# passed: 1114 tests; 130 source-carrier gaps ignored

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# passed

git diff --check
# passed
```

The conversion is still context-free like the source `ToDecimal` API; no
timezone or session-warning policy is introduced here. Broader SQL warning
disposition and all non-UTF-8 datum comparison boundaries remain separate
`pkg/types` follow-ups.

## Rust-only parity follow-up: binary-literal signed conversion

The same complete 61-artifact `pkg/types` inventory remains the owning package
for this follow-up. Go's `Datum.ToInt64` special-cases `KindMysqlBit` and
reinterprets its unsigned payload as `int64`, but sends `KindBinaryLiteral`
through `toSignedInteger`. That bounded path returns `math.MaxInt64` plus
`ErrOverflow` for an eight-byte `0xffffffffffffffff` literal, and returns the
zero value plus `ErrTruncatedWrongVal` when `BinaryLiteral.ToInt` rejects a
non-zero payload wider than eight bytes. Rust previously combined the two kinds
and unconditionally cast the unsigned payload to `i64`, producing `-1` for the
eight-byte hex literal and retaining `u64::MAX` for a too-wide literal.

The Rust `tidb-datatype` owner now keeps the source split: direct
`Datum::to_i64` bounds `BinaryLiteral` while preserving BIT reinterpretation,
and `Datum::convert_to` uses the source bounded path for both kinds. The
focused regression is
`datum::convert::tests::source_binary_literal_to_i64_saturates_but_mysql_bit_reinterprets`;
with the old combined arm it failed (`-1` instead of `9223372036854775807`),
and after the fix it covers the overflow, too-wide, BIT, and `ConvertTo`
results. No Go, generated, or Bazel file changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib source_binary_literal_to_i64_saturates_but_mysql_bit_reinterprets -- --nocapture
# pre-fix: failed (`-1` vs `9223372036854775807`); after fix: 1 passed
```

The remaining `Datum::compare` context/non-UTF-8 findings stay separate
follow-ups because they require a broader warning and timezone context API.
Because this follow-up changes only Rust production/tests plus its receipt and
plan, it does not add a new Go/Bazel preparation requirement.

## Rust-only parity follow-up: non-UTF-8 numeric comparison bytes

The complete `pkg/types` inventory above also owns `Datum.Compare`. Go's
`compareFloat64` sends `KindBytes` and `KindString` through `StrToFloat`, whose
prefix scanner works on the raw string bytes. A value beginning with an
invalid byte therefore keeps the zero numeric prefix and produces an ordering
(plus the source truncation event for a context that can publish it). Rust's
comparison helper previously required `std::str::from_utf8`, turning the same
`Datum::Bytes([0xff])` versus integer zero comparison into a Rust-only
`InvalidUtf8` error.

The Rust owner now uses a lossy UTF-8 view at the numeric comparison and
decimal-prefix boundaries. Numeric prefixes are ASCII, so valid source digits
are unchanged; invalid sequences become a non-ASCII replacement and stop the
same prefix scan. The focused regression
`datum::compare::tests::non_utf8_numeric_bytes_keep_go_zero_prefix_ordering`
failed before the changes with `InvalidUtf8` and passes after them, returning
`Ordering::Equal` like Go's zero prefix for both integer and decimal targets.
This deliberately does not claim the separate comparison warning-sink/context
API (D4/D5).

## Rust-only parity follow-up: Unicode whitespace in fixed-word decimal parsing

The complete 61-artifact `pkg/types` inventory above remains the owning package
for this storage/chunk-layout parser. Go's `MyDecimal.FromString` uses
`strings.TrimSpace` for the exponent text and for trailing non-exponent input
(`pkg/types/helper.go:134` and `pkg/types/mydecimal.go:527`). Rust's
`mydecimal.rs` used a byte loop named `trim_ascii_space`, so valid non-ASCII
Unicode whitespace remained significant even though the source removes it.
ASCII vertical tab is already covered by Rust's `is_ascii_whitespace`; the
distinguishing source case is U+00A0 NO-BREAK SPACE.

The Rust owner now trims valid UTF-8 Unicode whitespace at both boundaries while
preserving malformed bytes as significant input. The byte-preserving decoder
keeps `MyDecimal`'s raw-input contract and is used by both the exponent parser
and trailing-suffix check. The focused regression is
`mydecimal::tests::from_string_trims_unicode_whitespace_like_go`: before the
helper change, `"1\u{00a0}"` returned `DecimalError::Truncated` and
`"1e\u{00a0}5"` failed to apply the exponent; after the change they produce
`1` and `100000`, respectively, with no error. No Go, generated, or Bazel file
changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib from_string_trims_unicode_whitespace_like_go -- --nocapture
# pre-fix: failed (`Some(Truncated)` for `"1\u{00a0}"`); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib mydecimal::tests -- --test-threads=1
# passed: 17 tests
```

## Rust-only parity follow-up: clamped exponent error precedence

The same complete `pkg/types` inventory owns the value-layer decimal parser.
Go's `MyDecimal.FromString` (`pkg/types/mydecimal.go:498-510`) zeroes the
coefficient when `strToInt` reports a bad exponent, but continues into the
`MaxInt32/2` and `MinInt32/2` bound checks. A clamped `MaxInt64`/`MinInt64`
exponent therefore upgrades the result to the source maximum plus
`ErrOverflow`, or zero plus `ErrTruncated`. Rust's `Decimal::parse_mysql`
previously returned immediately on `BadNumber`, losing both the bound decision
and the source error precedence (`rust/crates/tidb-datatype/src/decimal/mod.rs`).

The Rust owner now clears the intermediate value and retains `BadNumber` while
continuing through the bound checks. The focused regression
`decimal_tests::parse_mysql_clamped_exponent_keeps_go_error_precedence` covers
both signs: `1e9223372036854775808` now yields 81 nines plus `Overflow`, while
`1e-9223372036854775809` yields zero plus `Truncated`. The existing
`1e18446744073709551620` fixture still remains zero plus `BadNumber`, proving
that a parser overflow which clamps to zero is not upgraded spuriously. No Go,
generated, or Bazel file changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib parse_mysql_clamped_exponent_keeps_go_error_precedence -- --nocapture
# pre-fix: failed (zero instead of the 81-digit maximum); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib decimal_tests::test_from_string_my_decimal -- --nocapture
# passed: 1 test (including the existing BadNumber fixture)
```

## Validation

Profile: Ready for this bounded production change.

- Before editing, `TestVectorDeserializeOverflow` failed (`an error is expected but got nil`) with the wrapped `uint32` size calculation, and `TestExplainFormatRU` failed to compile because the exported symbol was absent; both focused probes pass after the fix.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/types -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/types/parser_driver -count=1` — passed (unchanged boundary support package).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make bazel_prepare` — blocked because no `bazel` executable is installed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib explain_format -- --test-threads=1` — passed (the focused owner test; existing workspace warnings only).
- `rustfmt +nightly-2026-08-22 --edition 2021 --check crates/tidb-datatype/src/explain_format.rs crates/tidb-datatype/src/lib.rs` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

For the binary-literal follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib source_binary_literal_to_i64_saturates_but_mysql_bit_reinterprets -- --nocapture` — pre-fix failed on the unsaturated `-1`; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (373 tests).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,114 tests; 130 documented source-carrier gaps ignored).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.

For the non-UTF-8 comparison follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib non_utf8_numeric_bytes_keep_go_zero_prefix_ordering -- --nocapture` — pre-fix failed with `InvalidUtf8`; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib compare::tests -- --test-threads=1` — passed (6 comparison tests).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (374 tests).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib builtin_compare -- --test-threads=1` — passed (13 tests; 2 documented vectorized gaps ignored).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

`make bazel_prepare` is required by the new top-level Go regression and was
attempted; the local toolchain blocks it before metadata generation.

## Risks and not verified

- Correctness: the RU literal/order and checked vector-size arithmetic now match
  Go in both source implementations; the overflow regression prevents a
  truncated length header from being accepted.
- Compatibility: this adds one exported Go/Rust constant and one validator
  entry; consumers that enumerate the array observe the new Go-compatible
  value. The parser-driver API remains an explicit boundary.
- Performance: the validator list remains a static array; no hot-path policy
  changed.
- Full Go root and parser-driver suites pass locally. Bazel metadata generation
  remains unverified because the executable is unavailable; the parser-driver
  API additions in Go master remain outside the Rust owner boundary.
