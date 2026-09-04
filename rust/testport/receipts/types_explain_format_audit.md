# `pkg/types` current-master explain-format delta

The rolling Go-master source was fetched at `origin/master` commit
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). The `pkg/types`
inventory below is unchanged from the previous fetched commit; this receipt
records the newer source pin explicitly for this follow-up.

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

The remaining `Datum::compare` context and ordering-plus-error findings stay
separate follow-ups because they require a broader warning and timezone
context API.
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

## Rust-only parity follow-up: signed conversion error precedence

The complete 61-artifact `pkg/types` inventory also owns
`Datum.ConvertTo`'s integer conversion paths. Go keeps `StrToInt`'s parse
error on the signed string/bytes path when `ConvertIntToInt` reports a second
range error (`pkg/types/datum.go:2002-2008`); the unsigned path intentionally
keeps the clamp error (`pkg/types/datum.go:1329-1335`). Rust used one
`prefer_event(parsed, bounded)` ordering for both paths, so converting
`"999abc"` to signed `TINYINT` surfaced an overflow event for `999` instead
of the source truncation event for the original text.

The Rust `tidb-datatype` owner now reverses precedence only for signed
string/bytes conversion, leaving unsigned behavior unchanged. The focused
regression `datum_convert::tests::signed_string_conversion_prefers_source_truncation_over_clamp`
covers both source byte carriers and both signed/unsigned target policies:
signed conversion clamps to 127 with `ScalarConversionEvent::Truncated`,
while unsigned conversion remains 255 with an overflow event. Before the
production change the signed assertion failed with `Overflow { value: "999" }`.
No Go, generated, or Bazel file changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib signed_string_conversion_prefers_source_truncation_over_clamp -- --nocapture
# pre-fix: failed (`Overflow` instead of `Truncated`); after fix: 1 passed
```

## Rust-only parity follow-up: temporal FSP byte counting

The complete 61-artifact `pkg/types` inventory owns the temporal literal
helpers used by the Rust parser. Go's `GetFsp` (`pkg/types/time.go:569-581`)
defines FSP as the number of bytes after the selected dot, capped at six; it
does not stop at the first non-digit, so timezone suffixes and trailing text
contribute to the count. Rust's `get_fsp` previously counted only an initial
ASCII-digit run, producing a different FSP on the live `parse_datetime` path.

The Rust `tidb-datatype` owner now uses the source byte-length calculation. The
focused regression `mysql_time::tests::get_fsp_counts_source_suffix_bytes`
covers a timezone suffix (`"... .1+05:00"` → FSP 6), trailing text
(`"... .5xyz"` → FSP 4), and the resulting `parse_datetime` metadata. Before
the change the first assertion returned FSP 1. No Go, generated, or Bazel file
changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib get_fsp_counts_source_suffix_bytes -- --nocapture
# pre-fix: failed (1 instead of 6); after fix: 1 passed
```

## Rust-only parity follow-up: Unicode punctuation in `STR_TO_DATE`

The complete 61-artifact `pkg/types` inventory above remains the owning
package for this parser boundary. Go's `STR_TO_DATE` `%.'` token calls
`unicode.IsPunct` (`pkg/types/time.go:3534-3543`), which consumes every Unicode
code point in general categories `Pc`, `Pd`, `Pe`, `Pf`, `Pi`, `Po`, and `Ps`.
Rust previously used `char::is_ascii_punctuation`, so it rejected a valid
Unicode punctuation separator such as U+00BF INVERTED QUESTION MARK and
incorrectly consumed U+002B PLUS SIGN, which is a math symbol rather than Go
punctuation.

The Rust `tidb-datatype` owner now classifies the token with the
`unicode-general-category` table. Go 1.25's source table is Unicode 15.0 while
the locked dependency is generated from Unicode 16.0; the 13 punctuation code
points introduced only by Unicode 16.0 are explicitly excluded so the lookup
matches the fetched Go source exactly. The focused regression
`str_to_date::tests::punctuation_token_matches_go_unicode_punctuation` covers
both distinguishing characters and one Unicode-16-only punctuation code point.
With the old ASCII predicate it failed before the production change (`¿` was
rejected); after the change it passes and rejects the symbol/newer-table cases
as Go does. No Go, generated, or Bazel file changed; the Rust workspace lock
records the direct dependency for the package owner.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib str_to_date::tests::punctuation_token_matches_go_unicode_punctuation -- --exact
# pre-fix: failed (`¿` returned InvalidDate); after fix: 1 passed
```

## Rust-only parity follow-up: decimal shift carry exhaustion

The complete 61-artifact `pkg/types` inventory above remains the owning
package for the fixed-word decimal shift path. Go's `MyDecimal.Shift`
(`pkg/types/mydecimal.go:599-606`) checks the pre-round digit bounds after
rounding the excess fractional words. If the rounding carry is the only digit
that would survive, Go clears the value and keeps `ErrTruncated`; the carry is
not allowed to resurrect a value that was shifted entirely out of the
nine-word buffer. Rust's `Decimal::shift_mysql_with_word_limit` previously
checked only whether the rounded value was numerically zero, so `9e-82`
became `1e-81` instead of zero.

The Rust `tidb-datatype` owner now checks the retained prefix of the pre-round
digit string before accepting a rounding carry. When that prefix is all zero,
it returns the Go-compatible zero plus `DecimalCodecWarning::Truncated`; a
carry with any surviving source digit is still retained. The focused
regression `decimal_tests::parse_mysql_shift_discards_carry_after_fraction_exhaustion`
failed before the production change (`1e-81` instead of `0`) and passes after
it. No Go, generated, or Bazel file changed.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib decimal_tests::parse_mysql_shift_discards_carry_after_fraction_exhaustion -- --exact
# pre-fix: failed (`1e-81` instead of `0`); after fix: 1 passed
```

## Rust-only parity follow-up: float-to-decimal shortest formatting

The complete 61-artifact `pkg/types` inventory above remains the owning
package for float-to-decimal conversion. Go's `MyDecimal.FromFloat64`
(`pkg/types/mydecimal.go:1164-1167`) formats with
`strconv.FormatFloat(value, 'g', -1, 64)` before entering the fixed-word
parser. Rust previously used `f64::to_string()` and expanded its positional
output, so a value such as `1e-73` was rounded away by the parser's decimal
word limit instead of preserving the source exponent; the same positional
path also changes the 81-digit overflow boundary.

The Rust `tidb-datatype` owner now uses ryu's shortest digits, applies Go's
`%g` fixed/scientific threshold, and feeds the resulting exponent text
directly to `Decimal::parse_mysql`. The focused
regression `decimal_tests::from_f64_uses_go_shortest_exponent_format` covers
the 73-place fractional value and the 81-digit overflow boundary. With the
old positional formatter the tiny value became zero; after the change both
values match Go's shortest-format parse. No Go, generated, or Bazel file
changed; the Rust workspace lock records the direct formatter dependency.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib decimal_tests::from_f64_uses_go_shortest_exponent_format -- --exact
# pre-fix: failed (`1e-73` became zero); after fix: 1 passed
```

## Rust-only parity follow-up: decimal division codec disposition

The complete 61-artifact `pkg/types` inventory above remains the owning
package for decimal division. Go's `DecimalDiv` applies `fixWordCntError` to
the quotient's integer and fractional word counts and returns
`ErrTruncated` when the nine-word buffer cannot retain all fractional words;
the value remains usable and the expression layer routes that event through
`HandleTruncate`. Rust's digit-string `div_mysql` previously retained all
hidden quotient digits and exposed only `Option<Decimal>`, so this event was
silently lost.

The Rust datatype owner now exposes the source disposition through
`div_mysql_with_warning` and `true_div_with_warning`, while the existing
value-only methods remain compatibility wrappers. The expression `/` path
consumes the warning with its session truncation policy. No Go, generated, or
Bazel file changed; MOD remains an explicit follow-up because its evaluator
needs a separate raw-error policy.

The focused regression
`decimal_tests::decimal_division_clamps_fractional_words_at_the_codec_boundary`
failed before the change because the 20-digit/30-scale quotient retained 72
fractional digits instead of Go's 54 and passes after the fix. The dependent
expression regression `ops::tests::decimal_division_reports_codec_truncation_warning`
also confirms the retained value plus one 1292 warning.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib decimal_tests::decimal_division_clamps_fractional_words_at_the_codec_boundary -- --exact
# pre-fix: failed (72 retained digits instead of 54); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib ops::tests::decimal_division_reports_codec_truncation_warning -- --exact
# passed: 1 test
```

## Rust-only parity follow-up: runtime default float widths

The complete 61-artifact `pkg/types` inventory above remains the owning
package for default field-type metadata. Go's `DefaultTypeForValue` formats
`float32` and `float64` with `strconv.FormatFloat(..., 'f', -1, bits)` before
setting `flen`; this spells positive infinity as `+Inf`, negative infinity as
`-Inf`, and NaN as `NaN`. Rust's runtime path previously measured the native
`to_string()` spelling, so `Datum`-equivalent positive infinity reported
`flen = 3` instead of Go's `4`. The parser-driver twin already had the source
spelling helpers, but the runtime owner did not reuse them.

The Rust owner now uses the existing `go_fixed_shortest_f32` and
`go_fixed_shortest_f64` helpers for both runtime float cases. The focused
regression `field_type::tests::default_float_type_width_uses_go_infinity_spelling`
failed before the change (`3` instead of `4`) and passes after the fix for
positive/negative infinity and NaN. No Go, generated, or Bazel file changed.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib field_type::tests::default_float_type_width_uses_go_infinity_spelling -- --exact --nocapture
# pre-fix: failed (positive infinity flen 3 instead of 4); after fix: 1 passed
```

## Rust-only parity follow-up: explicit empty charset in cast restoration

The complete 61-artifact `pkg/types` inventory above remains the owning
package for this formatter boundary. Go's
`FieldType.RestoreAsCastType` (`pkg/parser/types/field_type.go:642-645`) emits
`" CHARSET " + ft.charset` whenever the charset is not `binary` or `utf8mb4`;
an empty source charset therefore still produces the observable (degenerate)
`"CHAR CHARSET "` result when `explicitCharset` is true. Rust's
`FieldType::restore_as_cast_type` previously added an extra
`!charset_name.is_empty()` guard and returned only `"CHAR"`, introducing
Rust-only output suppression.

The Rust `tidb-datatype` owner now follows the source predicate literally. The
focused regression
`field_type::tests::restore_as_cast_type_keeps_explicit_empty_charset_clause`
was run with the production guard present first and failed (`"CHAR"` instead
of `"CHAR CHARSET "`); after removing the guard it passes and also pins the
unchanged `explicitCharset = false` result. No Go, generated, or Bazel file
changed.

The adjacent audit entry for `SetElems(nil)` was also stale, not a remaining
implementation gap. Current `GoSharedSlice` state preserves nil versus
allocated-empty headers through JSON as `null` versus `[]`; the existing
`field_type::json::tests::slice_json_preserves_go_growth_duplicate_and_null_element_rules`
test covers both forms and passed in the complete owner suite. No code change
was needed for that boundary.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib field_type::tests::restore_as_cast_type_keeps_explicit_empty_charset_clause -- --exact --nocapture
# pre-fix: failed (`CHAR` instead of `CHAR CHARSET `); after fix: 1 passed
```

## Rust-only parity follow-up: signed subtraction at `MinInt64`

The same complete 61-artifact `pkg/types` inventory remains the owning
package for this checked-arithmetic boundary. Go master
(`fc7788ff517c3407dc7e000be989ab23e6648211`) implements `SubInt64` with a
guard that negates `b` before comparing it with the signed limits. When
`b == math.MinInt64`, Go's integer negation wraps back to `MinInt64`, so a
positive minuend is accepted and the final `a-b` operation wraps as well:
`SubInt64(1, MinInt64)` returns `MinInt64+1`, and
`SubInt64(MaxInt64, MinInt64)` returns `-1`, both without an error. Rust's
`checked_sub` previously rejected these inputs, introducing Rust-only
overflow behavior.

The Rust `tidb-datatype::sub_int64` owner now mirrors the source guard with
`wrapping_neg` and performs the accepted subtraction with `wrapping_sub`.
The explicit Go guard for `0 - MinInt64` remains an overflow error. The focused
regression `overflow_tests::sub_int64_min_rhs_positive_lhs_wraps_like_go`
failed before the production change with `OverflowError` and passes after it,
covering both positive boundary shapes. No Go, generated, platform, fixture,
or Bazel file changed.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib overflow_tests::sub_int64_min_rhs_positive_lhs_wraps_like_go \
  -- --exact --nocapture
# pre-fix: failed (`OverflowError` for 1 - MinInt64); after fix: 1 passed
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

For the Unicode-punctuation follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib str_to_date::tests::punctuation_token_matches_go_unicode_punctuation -- --exact` — pre-fix failed on U+00BF; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (379 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,114 tests; 130 documented source-carrier gaps ignored).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

For the decimal-shift follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib decimal_tests::parse_mysql_shift_discards_carry_after_fraction_exhaustion -- --exact` — pre-fix failed on the carried `1e-81`; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (380 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,114 tests; 130 documented source-carrier gaps ignored). The first run had one transient local HTTP-fixture `WouldBlock`; its isolated retry and this full rerun passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

For the float-format follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib decimal_tests::from_f64_uses_go_shortest_exponent_format -- --exact` — pre-fix failed on the positional `1e-73`; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (381 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,114 tests; 130 documented source-carrier gaps ignored). A first run and isolated retry exposed the pre-existing local HTTP-fixture `WouldBlock` race; the complete rerun passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

For the decimal-division disposition follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib decimal_tests::decimal_division_clamps_fractional_words_at_the_codec_boundary -- --exact` — pre-fix failed with 72 retained fractional digits instead of 54; after the fix, 1 focused test passed.
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib ops::tests::decimal_division_reports_codec_truncation_warning -- --exact` — passed (1 focused test).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (382 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,116 tests; 130 documented source-carrier gaps ignored).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

For the runtime default float-width follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib field_type::tests::default_float_type_width_uses_go_infinity_spelling -- --exact --nocapture` — pre-fix failed with positive infinity `flen = 3` instead of `4`; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (383 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — passed (1,116 tests; 130 documented source-carrier gaps ignored).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

For the explicit empty-charset cast-restoration follow-up specifically:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib field_type::tests::restore_as_cast_type_keeps_explicit_empty_charset_clause -- --exact --nocapture` — pre-fix failed with `CHAR` instead of `CHAR CHARSET `; after the fix, 1 focused test passed.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (384 tests).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,117 tests passed, 130 documented gaps were ignored, and the known loopback HTTP JSON-schema fixture failed with `WouldBlock`; its isolated retry reproduced the same unrelated resource error.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `git diff --check`, and the pinned `make lint` command above — passed.

## Rust-only parity follow-up: multiplication overflow preserves Go's signed zero

The complete `pkg/types` decimal inventory above also owns `DecimalMul`.
Go assigns the output sign before returning `ErrOverflow`, so an overflowing
product whose operands have opposite signs renders as `-0` through
`MyDecimal.ToString`. Rust's bounded `Decimal::mul_mysql` previously returned
an ordinary normalized positive zero from every overflow exit, losing that
source-visible sign.

The Rust decimal owner now constructs overflow zeros with the operand sign and
the source result scale while preserving the existing warning-bearing API.
The focused regression `decimal_tests::decimal_mul_overflow_preserves_negative_zero`
uses the source's 61-digit overflow shape, asserts `Overflow`, `-0`, and the
retained negative flag; the existing `test_mul_my_decimal` table also carries
the signed row. No Go, generated, or Bazel file changed.

Ready validation (commands run from `rust/`):

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib decimal_tests::decimal_mul_overflow_preserves_negative_zero -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
# passed: 402 unit tests; 63 source/integration tests
cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
# 1,141 passed, 1 known loopback HTTP JSON-schema fixture failed, 122 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
cargo +nightly-2026-08-22 check --offline --locked -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --offline --locked -p tidb-executor --all-targets
# all three passed (existing warnings only)
cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# both passed
```

## Rust-only parity follow-up: decimal no-digit error identity

The `pkg/types` `MyDecimal.FromString` source has two distinct malformed-input
outcomes. Go returns `ErrTruncatedWrongVal("DECIMAL", str)` when trimming leaves
no digits (`pkg/types/mydecimal.go:415,443`), but reserves `ErrBadNumber` for
malformed exponent text. Rust's fixed-word parser previously returned
`DecimalError::BadNumber` for both cases, making the source-visible diagnostic
identity impossible to preserve.

`tidb-datatype::MyDecimal::from_string` now returns the new
`DecimalError::TruncatedWrongValue` for empty or digit-less input (including
`abc`, `-`, and `.`), while exponent overflow continues to return
`DecimalError::BadNumber`. The focused regression
`mydecimal::tests::from_string_preserves_no_digit_error_identity` covers both
branches and asserts the zero receiver shape.

Ready validation (commands run from `rust/`):

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib mydecimal::tests::from_string_preserves_no_digit_error_identity -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
# passed: 403 unit tests; 63 source/integration tests
cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
# 1,141 passed, 1 known loopback HTTP JSON-schema fixture failed, 122 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
```

The owner `cargo check --all-targets`, workspace format check, and
`git diff --check` are run before commit; existing strict-clippy diagnostics
remain outside this bounded parser change.

## Rust-only parity follow-up: field-type source display width

Go's `FieldType.String` calls `CompactStr` using the process-wide
`TiDBStrictIntegerDisplayWidth` switch. TiDB server startup sets that switch
from `DeprecateIntegerDisplayWidth`, whose shipped default is strict
(`pkg/config/config.go:1279`, `cmd/tidb-server/main.go:1154`). Rust's
`FieldType::source_string` previously hardcoded `compact_str(false)`, so a
BIGINT with `flen = 22` and `BINARY` rendered as `bigint(22) BINARY`.

The formatter now passes `STRICT_INTEGER_DISPLAY_WIDTH`, producing Go's
`bigint BINARY` spelling while leaving the explicit `compact_str(false)` API
available for legacy callers. The focused regression
`field_type_source::source_string_uses_strict_integer_display_width_default`
asserts both branches.

Ready validation (commands run from `rust/`):

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib --test field_type_source source_string_uses_strict_integer_display_width_default -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
# passed: 403 unit tests; 64 source/integration tests
cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
# 1,141 passed, 1 known loopback HTTP JSON-schema fixture failed, 122 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
```

Owner `cargo check --all-targets`, workspace format, and diff checks are
required before commit; the existing strict-clippy diagnostics are unrelated
to this formatter boundary.

## Rust-only parity reconciliation: empty collation classification

Go's `FieldType.IsBinaryStr` compares the stored collation string to the exact
lower-case spelling `binary`. Legacy JSON metadata can carry an empty
`Collate` while the Rust runtime collator cache falls back to the `Binary`
enum; using that cache for the predicate would incorrectly classify the field
as binary and suppress restored-data handling.

Rust's `FieldType::is_binary_string` already reads `collation_name`, so the
behavior is source-compatible. The focused regression
`field_type::json::tests::empty_collation_name_does_not_inherit_binary_cache`
decodes `{"Tp":253,"Charset":"utf8mb4","Collate":""}` and asserts
non-binary character classification plus `need_restored_data() == true`.

Ready validation (commands run from `rust/`):

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib field_type::json::tests::empty_collation_name_does_not_inherit_binary_cache -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
# passed: 404 unit tests; 64 source/integration tests
cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
# 1,141 passed, 1 known loopback HTTP JSON-schema fixture failed, 122 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
```

The owner compile, format, and diff checks are run before commit; no
production implementation change was needed because the spelling-authoritative
predicate was already present.

## Rust-only parity fix: retain comparison ordering beside parse errors

Go's `Datum.Compare` returns an ordering and an error together. In particular,
`compareString` parses temporal and duration strings into a zero value beside a
parse failure, then compares that zero value; numeric and decimal string paths
keep their best-effort prefix/value beside a truncation event. Rust's original
`Datum::compare` exposed only `Result<Ordering, DatumValueError>`, so a strict
caller could not recover the source ordering after an error.

`Datum::compare_with_error` now exposes the paired source result while leaving
the existing strict `compare` wrapper unchanged. Temporal and duration parse
failures return the ordering against the zero value plus the original error;
numeric and decimal string conversions return their best-effort ordering plus a
source-shaped truncation diagnostic. The statement-context warning policy
remains the separate D5 boundary.

Focused regressions:

- `datum::compare::tests::compare_with_error_keeps_temporal_ordering_beside_parse_error`
  asserts `Greater`/`Less` in both directions for a valid time versus
  `"not a date"`, with an error retained beside each ordering.
- `datum::compare::tests::compare_with_error_keeps_numeric_prefix_ordering_beside_error`
  asserts `1 == "1abc"` while retaining
  `Truncated incorrect DOUBLE value: '1abc'`.

Ready validation (commands run from `rust/`):

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib datum::compare::tests::compare_with_error_keeps_temporal_ordering_beside_parse_error -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib datum::compare::tests::compare_with_error_keeps_numeric_prefix_ordering_beside_error -- --exact --nocapture
# passed: 1 test
cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
# passed: 406 unit tests; 64 source/integration tests
cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
# 1,144 passed, 1 known loopback HTTP JSON-schema fixture failed, 121 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
cargo +nightly-2026-08-22 check --offline --locked -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --offline --locked -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# checks, format, and diff all passed; the two owner suites retain the
# documented baseline test failures above
```

## Rust-only parity fix: comparison context carries date flags and timezone

Go's `Datum.Compare` receives a statement `types.Context`; temporal string
operands therefore use the statement's zero-in-date/invalid-date flags and
location. Rust's context-free `Datum::compare` had those choices pinned to
`allow_zero_in_date = true`, `allow_invalid_date = false`, and UTC, which made
`ALLOW_INVALID_DATES` and explicit timezone-offset inputs diverge.

The Rust datatype owner now exposes `Datum::compare_with_context`, threading
the two date flags and an explicit `SessionTimeZone` through every temporal
string conversion. It preserves Go's ordering/error pair so a caller can apply
its own warning policy without losing the zero-value ordering. The legacy
`compare` and `compare_with_error` wrappers retain their documented UTC
behavior for dependency-leaf callers.

Focused regressions:

- `datum::compare::tests::compare_with_context_uses_statement_date_flags`
  compares `2020-02-31` strictly (error plus zero ordering) and under
  `ALLOW_INVALID_DATES` (accepted calendar ordering).
- `datum::compare::tests::compare_with_context_uses_statement_timezone`
  compares a `+01:00` offset under UTC versus `+02:00` and proves the result
  follows the supplied session zone.

Ready validation (commands run from the dedicated worktree):

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib compare_with_context_uses_statement -- --nocapture
# passed: 2 focused tests
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,150 passed, 1 known loopback HTTP JSON-schema fixture failed, 121 ignored
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

The live `tidb-expr::ops::time_compare_ordering` caller is covered by the
following D5 caller-integration batch, which wires its `Columns` date modes,
timezone, and 1292 warning sink.

## Rust-only parity fix: live temporal comparison uses statement context

The expression evaluator's temporal comparison path had independently pinned
`allow_zero_in_date = true`, `allow_invalid_date = true`, and UTC, even after
the datatype seam gained an explicit context API. It now reads
`Columns::date_modes()` and `Columns::time_zone()`, rejects invalid dates in
strict mode, accepts them under `ALLOW_INVALID_DATES`, and publishes the
existing 1292 diagnostic through the resolver warning sink.

Focused regressions:

- `ops::tests::time_comparison_uses_statement_date_modes_and_warning_sink`
  proves strict `2020-02-31` returns NULL plus the exact warning while the
  relaxed mode returns the calendar ordering without a warning.
- `ops::tests::time_comparison_uses_statement_timezone_for_offset_text`
  proves a `+01:00` offset orders differently under UTC and `+02:00`.

Ready validation (commands run from the dedicated worktree):

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 \
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib time_comparison_uses_statement -- --nocapture
# passed: 2 focused tests
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 \
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,147 passed, 1 known loopback HTTP JSON-schema fixture failed, 121 ignored
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 \
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 \
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 \
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/openssl@3/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

The complete owner Ready profile above retains the known single loopback
JSON-schema fixture failure and executor planner/storage baseline failures.

## Rust-only parity fix: decimal add/sub leading-word overflow heuristic

Go `pkg/types/mydecimal.go:1909-1926` computes the destination integer-word
count from the wider operand, then increments it when the leading base-1e9
word can carry (`x > wordMax-1`). The check happens before the remaining words
are added, so a nine-word value beginning with `999999999` plus a smaller
operand reports `ErrOverflow` and is overwritten with the nine-word maximum,
even when the exact sum would otherwise fit. `DecimalSub` reaches the same
`doAdd` branch for opposite-sign operands.

Rust `Decimal::add_mysql` and opposite-sign `sub_mysql` now use
`add_leading_word_overflow` in `rust/crates/tidb-datatype/src/decimal/mod.rs`.
The helper mirrors the leading-word carry and only reports overflow when that
carry would exceed the fixed nine-word buffer; ordinary `999999999 + 1`
therefore remains valid. The source-only 81-digit distinguishing input is
covered by `decimal_tests::add_overflow_uses_go_leading_word_heuristic`.

Ready validation (commands run from the dedicated worktree):

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --lib add_ -- --nocapture
# passed: 5 tests (including the complete add source rows and focused regression)
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 407 unit tests; 64 source/integration tests
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,146 passed, 1 known loopback HTTP JSON-schema fixture failed, 121 ignored
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,052 passed, 121 existing planner/storage/fixture failures, 0 ignored
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only
git diff --check
# passed
```

The M6 behavior is unreachable through ordinary SQL because the distinguishing
input exceeds `DECIMAL(65)`, but the fixed-word codec remains source-compatible
for direct callers and row-codec boundaries.

## Rust-only parity follow-up: unspecified decimal wrapper scale

The complete `pkg/types` inventory above is also exercised by the Rust
`tidb-expr` aggregate-cast wrappers. Go's `WrapWithCastAsDecimal`
(`pkg/expression/builtin_cast.go:2736-2765`) carries an unspecified source
scale through `BuildCastFunction`; `ProduceDecWithSpecifiedTp` therefore keeps
the value's natural fraction and the `ConstStrict` tail narrows the result
metadata to `PrecisionAndFrac`. Rust previously converted the `-1` scale in a
`FieldType` to the AST cast's ordinary scale `0`, rounding a REAL such as
`123.555` to `124`, and omitted the metadata refinement entirely.

The Rust owner now carries an internal unspecified-scale sentinel through the
`cast_decimal` dispatch. `eval_cast` skips precision/scale rounding when that
sentinel is present, while still reporting source-string parse diagnostics.
`wrap_with_cast_as_decimal` evaluates strict constants in the value-only
`NoColumns` context and records the natural precision/fraction on the wrapper,
matching Go's observable result type. No Go, generated, or Bazel file changed.

Focused regression:

- `tests::aggregation_arithmetic_cast_source::test_wrap_with_cast_as_types_classes_real_to_decimal_keeps_fraction`
  proves `REAL(123.555)` remains a decimal `123.555` and the wrapper metadata
  is `(flen=6, decimal=3)`; this test was previously ignored as a documented
  parity gap.

The neighboring `cast_decimal` signature table and all wrapper-family tests
also pass after the sentinel is introduced:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_cast_func_sig_as_decimal -- --nocapture
# passed: 1 test

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib wrap_with_cast_as_types_classes_ -- --nocapture
# passed: 6 tests, including the formerly ignored regression
```

Ready validation for the batch:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,151 passed, 1 known loopback HTTP JSON-schema fixture failed, 120 ignored

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

## Rust-only parity follow-up: UNION source-specific decimal casts

The Go `BuildCastFunction4Union` path (`pkg/expression/builtin_cast.go:2570-2573`)
builds a cast with `inUnion=true`; the decimal function class then chooses a
source-specific signature (`:263-290`). `builtinCastIntAsDecimalSig` clamps a
negative signed integer to a zero decimal before
`ProduceDecWithSpecifiedTp` (`:1050-1070`), the REAL arm does the same for a
negative real (`:1405-1420`), the DECIMAL arm preserves positive decimal
precision while zeroing a negative unsigned UNION source (`:1538-1551`), and
the string arm discards a negative textual value before parsing (`:1864-1901`).
Rust previously selected the generic `cast_decimal` name from the merged target
type, so these source-specific UNION semantics were not reachable through the
normal expression builder.

The Rust `tidb-expr` owner now selects `cast_real_to_decimal_in_union`,
`cast_int_to_decimal_in_union`, `cast_string_to_decimal_in_union`, or
`cast_decimal_in_union` from the source eval type and target unsigned flag.
The values dispatcher implements the Go pre-parse zeroing/clamping rules, and
the scalar evaluator applies the merged target precision/scale afterward. A
positive DECIMAL source remains a DECIMAL rather than being narrowed through a
REAL intermediate. No Go, generated, or Bazel file changed.

Focused regressions:

- `builtin_cast_semantics::union_unsigned_string_decimal_cast_discards_negative_before_parse`
  proves that an unsigned UNION string target maps `-1.25` to decimal zero
  without a warning, while positive `1.256` rounds to `1.26`.
- `tests::aggregation_arithmetic_cast_source::test_cast_string_as_decimal_sig_with_unsigned_flag_in_union`
  is now active and covers the Go-derived `"1"`/`"-1"` rows, including the
  no-warning negative branch.
- `func::tests::cast_decimal_in_union_keeps_positives` pins preservation of a
  positive DECIMAL value through the source-specific signature.
- `builtin_cast_semantics::union_signed_integer_decimal_cast_preserves_negative_values`
  pins Go's signed-target integer branch, which keeps a negative source on the
  generic decimal path rather than applying the unsigned UNION clamp.

The focused string-signature command passed before the Ready run:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib string_as_decimal_sig_with_unsigned -- --nocapture
# passed: 1 test
```

Ready validation for this batch:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,155 passed, 1 known loopback HTTP JSON-schema fixture failed, 119 ignored

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

## Rust-only parity follow-up: cast-wrapper metadata tables

Go's `TestCastConstAsDecimalFieldType` and `TestCastAsCharFieldType`
(`pkg/expression/builtin_cast_test.go:1573-1832`) exercise the strict-constant
metadata tails of `WrapWithCastAsDecimal` and `BuildCastFunctionWithCheck`.
They cover signed/unsigned integer widths, decimal shapes, float and
scientific-notation limits, strings and blob families, temporal FSP widths,
JSON widening, and the value-derived decimal precision/fraction. These rows
were previously ignored in Rust because the wrapper refinement and
unspecified-width string adjustment were documented as missing.

The Rust source-derived test now activates the complete 51-row decimal table
and 40-row CHAR-width table. It uses the normal wrapper/builder paths and
asserts every `(flen, decimal)` result, including the 65-digit/30-scale caps
and JSON `LongBlob` width. No production behavior changed in this batch; the
tests close stale parity receipts for the behavior implemented in the prior
cast-wrapper batch. The vectorized cast differential remains a separate
unmodeled tier.

Focused validation:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_cast_const_as_decimal_field_type -- --nocapture
# passed: 1 test (51 Go rows)

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_cast_as_char_field_type -- --nocapture
# passed: 1 test (40 Go rows)
```

Ready validation for this batch:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,159 passed, 1 known loopback HTTP JSON-schema fixture failed, 117 ignored

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

## Rust-only parity follow-up: BINARY-source CAST AS CHAR conversion

Go's `TestWrapWithCastAsString` (`pkg/expression/builtin_cast_test.go:1437-1496`)
builds `CAST(expr AS VAR_STRING)` over a BINARY-charset source. The string cast
class calls `HandleBinaryLiteral(..., explicitCast=true)`, which inserts the
`from_binary` decoder. A valid payload is rendered normally; an invalid UTF-8
payload publishes `ErrCannotConvertString` (3854) and returns the successfully
decoded prefix in non-strict mode (empty for the single byte `0x91`).

Rust previously called `Datum::sql_string` before this boundary, so the same
payload raised an internal invalid-UTF-8 evaluation error and emitted no
warning. `tidb-expr/src/cast.rs` now recognizes a BINARY source for a
non-binary CHAR target, decodes with the target charset's `TransformOp::DECODE`
policy, emits the source-shaped 3854 warning, and preserves BinaryLiteral/Bit
raw bytes. `scalar_function.rs` carries the resolved target charset through the
internal `cast_char` dispatch so explicit character-set casts use the same
target as Go.

Focused validation:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_wrap_with_cast_as_string_binary_literal_warns_invalid_utf8 -- --nocapture
# passed: 1 test (invalid 0x91 warning + valid 0x61 no-warning rows)

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_wrap_with_cast_as_string -- --nocapture
# passed: 2 tests (the focused pair plus the existing wrapper table)
```

Ready validation for this batch:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,160 passed, 1 known loopback HTTP JSON-schema fixture failed, 116 ignored

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

## Rust-only parity follow-up: TO_BASE64 session charset

Go's `TestToBase64` (`pkg/expression/builtin_string_test.go:2557-2647`)
rebuilds each constant after changing `character_set_connection`. With a GBK
connection, `TO_BASE64('一二三')` must encode the literal as GBK before the
base64 step (`0ru2/sj9`), while the empty-charset case remains UTF-8
(`5LiA5LqM5LiJ`).

The Rust evaluator already has the ordinary `HandleBinaryLiteral` equivalent:
the connection-aware rewriter stamps string literals with the resolver's
charset, the `to_base64` result derives that same connection charset, and the
binary-aware argument is wrapped in `to_binary` before encoding. The former
ignored row used the process-default resolver and therefore could not exercise
this path. The active source-derived regression builds and evaluates the
expression with a GBK resolver, covering ASCII, multibyte, and suffix rows.

Focused validation:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib test_to_base64_gbk_session_rows -- --nocapture
# passed: 1 test (ASCII, GBK multibyte, and GBK multibyte-plus-suffix rows)
```

Ready validation for this batch:

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets -- --test-threads=1
# passed: 409 unit tests; 64 source/integration tests

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets -- --test-threads=1
# 1,161 passed, 1 known loopback HTTP JSON-schema fixture failed, 115 ignored

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets -- --test-threads=1
# 1,058 passed, 121 existing planner/storage/fixture failures, 0 ignored

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-datatype --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-executor --all-targets
# all three owner checks passed with existing warnings only

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed
```

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
- Unicode-category compatibility: the punctuation helper is pinned to the
  fetched Go 1.25 Unicode 15.0 table by excluding the 13 newer punctuation
  code points present in the Rust dependency's Unicode 16.0 table. If Go's
  Unicode edition advances, that exclusion list must be re-audited.
- Performance: `%.'` now performs one table lookup per consumed Unicode scalar
  instead of the previous ASCII predicate; this is limited to the explicit
  STR_TO_DATE punctuation token.
- Decimal division compatibility: the existing value-only division methods
  remain wrappers around the warning-bearing API, while callers that carry a
  statement context can consume Go's truncation disposition. MOD's raw-error
  handling remains an explicit follow-up rather than being assigned the
  division warning policy.
- Default float metadata compatibility: runtime field-type widths now reuse
  Go's explicit infinity/NaN spellings; finite-value formatting remains the
  existing fixed-format path.
- Cast-restoration compatibility: callers that explicitly request charset
  restoration now observe Go's trailing `CHARSET ` clause for an empty source
  charset. The ordinary non-explicit path and the `binary`/`utf8mb4`
  exclusions are unchanged; no allocation or hot-path behavior changed.
