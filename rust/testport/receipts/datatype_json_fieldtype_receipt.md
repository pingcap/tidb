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

## Batch 3 (queued): FieldTypeBuilder F2

The FieldTypeBuilder flen/decimal zero-value divergence (audit F2,
`field_type/builder.rs:34`; Go zero is 0/0, Rust seeds -1) is documented and
still owes implementation.

## Validation

Profile: Ready for this bounded Rust package batch.

- Pre-fix baseline: the regression FAILS against the unfixed parser
  (stashed production edit), passes after.
- Full `tidb-datatype` suite: 370 lib + 62 aggregate tests, 0 failed.
- `cargo fmt --all -- --check`, workspace `make lint`, `git diff --check`:
  clean (recorded in `TESTPORT_EXECPLAN.md`).

## Risks

- Compatibility: only paths Go itself accepts change behavior
  (client-visible JSON_EXTRACT alignment); the three shared rejection cases
  stay rejected.
