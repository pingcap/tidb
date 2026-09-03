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

## Batch 2 (queued): lone-surrogate U+FFFD and FieldTypeBuilder F2

Documented, code-confirmed, not yet implemented: binary JSON lone-surrogate
handling (`binary_json.rs:575`/`:1246`; audit item 6/7) and the
FieldTypeBuilder flen/decimal zero-value (audit F2, `field_type/
builder.rs:34`).

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
