# Rust `tidb-datatype` json-path and field-type divergence receipts

Status: bounded Rust-only alignment batches against Go `origin/master`
`a85e0fd5df`.

## Batch 1: `$[N to]` degrades to a plain index (divergence item 8)

Go `parseArray` (`pkg/types/json_path_expr.go:462-480`) consumes the `to`
keyword via `tryReadString`, which only rewinds on failure; the whitespace
check that follows can then fail with the stream already past `to`, leaving
a PLAIN index selection (`$[0 to]` parses as `$[0]`). The Rust
`parse_array` returned `invalid()` (error 3143) whenever `to` was not
followed by whitespace. The fix reproduces Go's three-way shape: whitespace
after `to` opens a range; `to` consumed with a non-whitespace follower
degrades to a plain index with the stream left past `to` (`$[0 tox]`/
`$[0 to3]` still invalid at the `]` check); no `to` rewinds as before.

Regression: `json_path_array_index_with_dangling_to_is_a_plain_index_like_go`
in `tests/json_ops_go_source.rs` — proven to FAIL against the unfixed parser
(captured by stashing the production edit), plus the shared rejection cases.

## Batch 2: lone-surrogate U+FFFD (divergence item 6)

Three Go-verified surfaces now substitute U+FFFD instead of erroring:

- `unquote_json_string`'s `\u` arm mirrors Go's
  `decodeOneEscapedUnicode` + caller (`json_binary_functions.go:136-160`):
  a surrogate first rune consumes an ADJACENT `\u` escape and combines via
  `utf16.DecodeRune` — U+FFFD for an invalid pair — while no adjacent
  escape propagates Go's decode error.
- `decode_escaped_unicode` substitutes U+FFFD for a lone 4-hex surrogate or
  an invalid 8-hex pair (Go `utf16.DecodeRune`), replacing the
  `LoneSurrogate`/`InvalidText` refusals.
- `BinaryJSON::parse` retries once through
  `replace_lone_surrogate_escapes` when serde rejects the text: Go
  `json.Valid` accepts lone `\uXXXX` escapes and `encoding/json` decodes
  each to U+FFFD; the sanitizer rewrites exactly those escapes (a valid
  pair combines) and copies everything else verbatim, so non-surrogate
  failures re-fail identically.

Per-surface regressions in `tests/json_ops_go_source.rs`
(`lone_surrogates_substitute_fffd_like_go`): parse+unquote U+FFFD, the
invalid pair's TWO FFFDs (Go `encoding/json` per-escape), the escape-surface
combine, and the lone-high unquote error — the rejection/combine assertions
were proven to fail against the unfixed code during development.

Item 7 (invalid UTF-8 inside a JSON string) stays deferred: the audit's own
note found no SQL path producing it; a Rust node reading a Go-written value
with invalid UTF-8 remains an `InvalidBinary` (documented gap).

## Batch 3: FieldTypeBuilder zero value (divergence item F2)

`FieldTypeBuilder::new()` seeded from `FieldType::parser(...)` (flen/decimal
-1); Go `&FieldTypeBuilder{}` (`pkg/types/field_type_builder.go:23-25`) holds
the ZERO value — it is `parser.NewFieldType` that seeds -1. The builder now
starts at flen/decimal 0, and a regression pins the zero (`varchar(0)`
rendering, no default-flen substitution of -1) — proven to FAIL against the
seeded builder (captured by hand-reverting `new()`).

Fallout sweep: the five crates using `FieldTypeBuilder::new()` were fully
re-run — tidb-datatype (371+63, green), tidb-expr (1105+18, green),
tidb-schemacmp/tidb-ttl (green). `tidb-executor` shows 136 SQL-source
failures at baseline that reproduce IDENTICALLY with and without this
change (verified by stashing the edit) — a pre-existing condition of that
crate's absorbed state, queued for its own sweep; this batch neither adds
nor removes any of them.

## Batch 4: `json.Number` preserves the unsigned-integer tier

The complete `pkg/types` root inventory contains 60 Go production, test,
benchmark, support, and build artifacts (28,703 lines); its nested
`pkg/types/parser_driver` package is a separate owner. No Go platform-specific,
generated, fixture, or additional build variant exists in the root package.
The relevant source is `json_binary.go` (1,043 lines), with its source tests in
`json_binary_test.go` (850 lines). The Rust `tidb-datatype` owner inventory is
104 artifacts (production modules, inline/source tests, benches, fuzz target,
generated collation inputs, and the 830-row JSON fixture).

Go's `appendBinaryNumber` (`pkg/types/json_binary.go`) tries `Int64`, then
base-10 `ParseUint`, and only then `Float64`. Rust's `BinaryJSONValue::Number`
previously tried only `i64` and `f64`, so `u64::MAX` was stored as a DOUBLE
(type code `0x0b`) instead of an UNSIGNED INTEGER (`0x0a`). The conversion now
keeps the unsigned tier before the floating fallback. The existing create
binary vector and the focused `json_number_uint64_uses_unsigned_storage_like_go`
regression both pin the type code, payload, and rendered value. The focused
test failed before the production change with `left: 11, right: 10`.

## Validation

Profile: Ready for this bounded Rust package batch.

- Pre-fix baseline: the regression FAILS against the unfixed parser
  (stashed production edit), passes after.
- Current full `tidb-datatype` suite: 411 lib + 64 aggregate tests, 0 failed.
- `cargo fmt --all -- --check`, workspace `make lint`, `git diff --check`:
  clean (recorded in `TESTPORT_EXECPLAN.md`).
- Batch 4 focused regression: `cargo +nightly-2026-08-22 test
  --manifest-path rust/Cargo.toml -p tidb-datatype --lib
  binary_json::tests::json_number_uint64_uses_unsigned_storage_like_go --
  --exact --nocapture` — failed before the production edit and passed after.
- Batch 4 owner checks: the `binary_json` lib subset passed 30 tests, the
  generated aggregate source binary passed 64 tests, the complete datatype
  lib passed 411 tests, and `go test ./pkg/types -count=1` passed against the
  Go authority.

## Risks

- Compatibility: only paths Go itself accepts change behavior
  (client-visible JSON_EXTRACT alignment); the three shared rejection cases
  stay rejected.
