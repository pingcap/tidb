# DST-adjusted `convert_kind` type/fsp metadata (T14 parity)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This closes finding
T14 of `rust/docs/types-datatype-divergence-audit.md`.

## Go behavior (the oracle)

`Time.Convert`'s DST-transition branch (`pkg/types/time.go:467`) returns
`Time{FromGoTime(tAdj)}`: the composite literal zeroes the wrapper's type and
fsp fields, so the adjusted value REVERTS to DATETIME (the zero type) with
fsp 0 — regardless of the source fsp. The warning (`ErrTimestampInDSTTransition`)
rides alongside.

## The Rust fix

`Time::convert_kind`'s `NonexistentLocalTime` arm (Timestamp target) now
applies the same reversion after the adjusted-instant substitution:
`set_kind(TimeType::DateTime)` + `set_fsp(0)`. The adjusted-instant mapping
itself (`adjusted_datetime`) and the `adjusted=true` flag are unchanged —
they are the established, separately pinned behavior
(`test_convert_kind_dst_gap_source_row`).

## Regression

- `mysql_time::tests::dst_adjusted_convert_reverts_to_datetime_with_zero_fsp`
  — FAIL-BEFORE (pre-fix the converted kind stayed `Timestamp`): pins
  `kind == DateTime`, `fsp == 0`, and the adjusted rendering
  `2018-03-11 03:00:00` for the 2018 US DST gap with a fractional source.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-datatype --no-fail-fast
# 474 run, 474 passed
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-datatype --all-targets
# clean in touched code
```

## Risk

- Correctness: low; the reversion matches Go's composite literal exactly and
  only fires on the DST-gap path whose `adjusted=true` flag the callers
  already handle.
- Compatibility: metadata-only (type code + fsp); no API change.
