# `pkg/util/chunk` clamped decimal read-back — visible-scale parity

Follow-up to `chunk_a1_datum.md` (finding A-1, `c59b2bd60e`): the datum
representation decision and the infallible boundary stand; this batch closes
the one observable gap the first pass left open.

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03).

## The gap

`Decimal::to_chunk_my_decimal_lossy` builds the clamped cell through
`MyDecimal.FromString`, whose tail (`pkg/types/mydecimal.go:541-543`) stamps
`resultFrac = digitsFrac`. For a fraction-truncated value that is the KEPT
fraction — Go-correct for a truncated *literal* (the parser's own producer) —
but for a value whose visible scale is smaller than the kept words (exact
`mul`/`true_div` hidden-word products) the clamped cell then reads back with a
HIDDEN digit exposed past the value's own scale. That diverges from both:

- the engine's exact path, where `to_chunk_my_decimal` carries
  `resultFrac = visible scale` and the chunk read-back renders exactly the
  visible digits (pinned by
  `decimal_datum_append_preserves_hidden_fraction_words`), and
- Go's producer semantics, where the producing conversion stamps `resultFrac`
  from the target/visible scale — the client-visible decimal text is the
  `resultFrac` digit count on both engines' protocol paths.

## The fix

The lossy fallback now pins `resultFrac = min(visible scale, kept fraction)`
(`kept = cell.digits_frac()`), mirroring the exact path's convention. The
`#[cfg(test)]` gate on `MyDecimal::set_result_frac` is widened to `pub(crate)`
with its invariant (`0 <= resultFrac <= digitsFrac`) intact — production code
now has the same stamping primitive Go producers have. The
`DecimalError` re-export at the crate root keeps the bridge's error type
nameable downstream.

Go-master anchors re-verified for the clamped cell semantics:
`fixWordCntError` (`mydecimal.go:185-193`), the `FromString` written clamp and
digit-fill (`:447-493`), and the tail normalizations — `if allZero {
d.negative = false }` and `resultFrac = digitsFrac` (`:531-543`) — which the
Rust `set_from_string` port reproduces faithfully, including the negative-zero
collapse to `0`.

## Regressions

- `tidb-datatype decimal_tests::chunk_bridge_lossy_clamps_integer_overflow_like_go_from_string`
  — integer-overflow cells byte-equal to Go's `MyDecimal.FromString` output
  for the same over-wide literal (100 nines, 82 nines, `1` + 85 zeros + `.5`),
  sign survival on a 90-nines negative.
- `tidb-datatype decimal_tests::chunk_bridge_lossy_clamps_excess_fraction_like_go_from_string`
  — FAIL-BEFORE for this batch (resultFrac 72 vs the visible scale 71 on the
  hidden-word product); pins the byte-equal truncated cell, the kept-72
  rendering, and the visible-scale read-back of the `mul`/`true_div` product.
- `tidb-datatype decimal_tests::chunk_bridge_lossy_zero_clamp_renders_like_go_to_string`
  — pins the all-zero clamp rendering `0` with the sign normalized away,
  byte-equal to Go's `FromString` output.
- `tidb-chunk chunk::tests::decimal_datum_read_back_matches_go_to_string_after_clamp`
  — the datum read-back after `append_datum` renders the 72 kept digits for a
  truncated literal, and the integer-overflow cell equals Go's clamp bytes.

## Validation

Profile: **Ready** for this follow-up batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-datatype -p tidb-chunk --no-fail-fast
# 777 run, 737 passed, 40 failed, 4 skipped
# the 40-name failure set is IDENTICAL to the stashed-base control run
# (the documented pre-existing spill/row-container clean-env failures).
# Pre-patch control: chunk_bridge_lossy_clamps_excess_fraction FAILED
# (resultFrac 72 vs visible 71); post-patch the full set passes.
```

## Risk

- Correctness: low; only the lossy (never-before-representable) cells change
  their `resultFrac` byte, toward the same convention the exact path already
  pins. Exact-path bytes are untouched.
- Compatibility: `set_result_frac` widens from test-only to `pub(crate)` inside
  `tidb-datatype`; no public API changes beyond the `DecimalError` re-export.
