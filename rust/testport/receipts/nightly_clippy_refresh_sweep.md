# nightly clippy refresh gate sweep — parser/chunk/util/expr (batch #38)

## Trigger

The toolchain lint refresh behind batches #36/#37 also surfaced warnings in the
four remaining owned crates once compiled with `--no-deps` isolation: 6 in
`tidb-parser`, 2 in `tidb-chunk`, several in `tidb-util` (AES), and a cluster in
`tidb-expr` (json pair-zips, dead format helpers, trig shadow, case_when).

## Fixes

### chunks_exact_to_as_chunks family (mechanical)
- `tidb-parser/src/auth.rs:203` hex decode; `:526` SM3 message schedule
  (`as_chunks::<4>().0.iter().enumerate()`, `*chunk`)
- `tidb-parser/src/ddl/field_type.rs:448/:460` bit-decimal packing
- `tidb-parser/src/load_data.rs:334` binary literal bits
- `tidb-parser/src/user.rs:672` hex decode
- `tidb-expr/src/builtin_ext/json/construct.rs:106` +
  `modify.rs:91/:125/:181` — the paired `arg_types[1..]` zips now use
  `as_chunks::<2>().0` so both zip sides are array views
- `tidb-expr/src/builtin_ext/misc.rs:467` UUID canonical hex
- `tidb-expr/src/scalar_function.rs:1299` `case_when`: `as_chunks::<2>()`
  returns `(pairs, remainder)`, replacing the iterator's `by_ref()` +
  `remainder()` dance with the same lazy semantics (selected branch still the
  only evaluated one; odd-arg ELSE still gated on the tail)
- `tidb-chunk/src/codec.rs:320` var-element offset table

### dead code (verified superseded)
- `tidb-expr/src/simple_expr.rs`: removed `format_bytes`/`format_nano_time`/
  `format_float_e_go` (95 lines) — the live FORMAT_BYTES/FORMAT_NANO_TIME arms
  use `builtin_ext/info.rs`'s own captures; also removed the orphaned
  `BuildCastFunction` doc block (empty-line-after-doc-comment) and RE-ATTACHED
  it to `pub(crate) fn build_cast_function`, which had no doc; deduplicated the
  repeated passage in `wrap_cast_for_hybrid_push`'s doc
- `tidb-chunk/src/chunk.rs`: `append_format_row`/`assert_format_row` gated
  `#[cfg(test)]` (unit-test helpers)

### other mechanical
- `tidb-util/src/encrypt/aes.rs` ×4: after `as_chunks_mut` the
  `let block: &mut [u8; 16] = block.try_into()` shadows became
  `useless_conversion` — deleted; `cipher.encrypt_block(block)` takes the
  array view directly
- `tidb-expr/src/math_fn/go_trig.rs:244`: removed `let mut sign = sign;`
  shadow (redundant binding; outer `mut sign` reused, identical semantics)
- `tidb-chunk/src/mutrow.rs:333`: `clean_col_of_mut_row` offset loop →
  `column.offsets.fill(0)` (clippy manual_fill)

### design-level, allow-with-reason per codebase convention
- `tidb-expr/src/time_fn/calendar.rs:1358` `datetime_composite_value`:
  `#[allow(clippy::too_many_arguments, reason = "mirrors Go's composite-unit
  extraction signature")]`
- `tidb-expr/src/constant_propagation.rs:508` `pick_outer_constants`:
  `#[allow(clippy::result_large_err, reason = "the large Option<Constant> Err
  payload is the source shape")]`

### deliberately untouched (sibling-owned / generated)
- `tidb-vardef` missing-doc (sysvar stream), `tidb-model` unused-mut ×2
  (model stream), generated `tidb-proto`/`tikv-client` warnings
- `tidb-util` API-shape lints (Default impls ×4, len-without-is_empty ×3,
  mvmap `next` rename) — additive API changes deferred; recorded here so the
  next gate sweep picks them up deliberately

## Verification

- fmt clean on all four crates
- clippy `--no-deps`: parser 0, chunk 0, util 0, expr 0 own warnings
- `nextest` parser+chunk+util+expr+datatype: 3439/3439

Behavior-neutral except where the compiler now proves conversions total; the
case_when lazy-evaluation semantics are pinned by existing tests.
