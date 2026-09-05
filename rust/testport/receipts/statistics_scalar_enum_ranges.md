# `pkg/statistics/scalar.go` — EnumRangeValues walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3` line set), function
`EnumRangeValues` (scalar.go:232) and constant `maxNumStep = 10`
(scalar.go:229).

## Function inventory for scalar.go

`calcFraction`, `convertDatumToScalar`, `PreCalculateScalar`,
`calcFraction`, `commonPrefixLength`, `convertBytesToScalar`,
`calcFraction4Datums`, `EnumRangeValues` — all eight present in the Rust
port: `scalar_geometry.rs` (calc_fraction, common_prefix_length,
convert_bytes_to_scalar, convert_datum_to_scalar, calc_fraction_from_datums),
`histogram.rs` (calc_fraction, common_prefix_length, pre-calculated scalar
state), `scalar_enum.rs` (enum_range_values). Existing pinned tests cover
the scalar-geometry family (`common_prefix_length_matches_go`,
`convert_bytes_to_scalar_matches_go_byte_widths`,
`calc_fraction_matches_edge_cases`).

## Walk findings

Line-by-line comparison of `EnumRangeValues` against
`scalar_enum.rs::enum_range_values` confirmed equivalence for:

* the kind-mismatch `nil`;
* the Int64 sign-crossing pre-check (`low <= 0 && high >= 0` with either
  side escaping `[-maxNumStep, maxNumStep]`) that the unsigned arm lacks;
* the unsigned arm's plain difference arithmetic;
* the duration arm's half-away-from-zero low-bound rounding and the
  `10^(MaxFsp-fsp)` microsecond step;
* the time arm's DATE midnight normalization, kind mismatch refusal, and
  the per-offset `Add` error-to-nil mapping;
* the empty-but-non-nil `make([]types.Datum, 0, remaining)` result, which
  the Rust port preserves as `Some(vec![])` distinct from `None`.

The boundary contract — Go's two gates (`difference >= maxNumStep+1`,
then `remaining >= maxNumStep` after `+1 - exclusions`) mean AT MOST NINE
values ever enumerate — was previously unpinned.

## Tests added

`scalar_enum.rs::enum_range_values_tests` (8 tests): inclusive ends,
per-bound exclusions, the empty-but-present vs `nil` distinction, the
maxNumStep boundary ladder (11/10/9/8-wide spans), the sign-crossing
anchor rules, the unsigned arm, kind mismatch, duration FSP steps, and
DATE midnight normalization with kind mixes.

## Validation

`cargo +nightly-2026-08-22 nextest run -p tidb-stats` — 294/294 passed
(286 pre-existing + 8 new), fmt clean.
