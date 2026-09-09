# Rust `tidb-codec` chunk wire-decode strictness receipt

Status: bounded Rust-only alignment batch. Closes divergence item A-2 of
`rust/docs/chunk-and-stats-divergence.md` (wire/decode strictness).

Comparison source: Go `origin/master` `a85e0fd5df`, owning file
`pkg/util/chunk/codec.go` byte-identical. Go's `decodeColumn`
(`codec.go:130-133`) reads the variable-column offset table with NO
first-zero and NO monotonicity validation — `numDataBytes` comes from the
LAST offset alone, and `Decoder.ReuseIntermChk` (`codec.go:297`+) exists
precisely to rebase a table whose first offset is non-zero (a producer
re-encoding a partially consumed intermediate chunk emits
`offsets = [40, 45, 51]`).

## Implementation

`tidb-codec/src/column.rs`'s offset-table loop drops both checks and stores
the table verbatim; the data length still derives from the LAST offset, and
a negative last offset maps to the crate's `InvalidOffset` error where Go's
`buffer[:negative]` slice bounds would panic — the established
error-for-panic representation. Row-window access keeps its existing
per-row error mapping.

## Regressions

`tests/column_source.rs`: the decreasing-table case now asserts a
SUCCESSFUL decode preserving the table verbatim (`[0, 2, 1]`, one data
byte), and a non-zero-first table (`[1, 2, 3]`, the ReuseIntermChk shape)
asserts the same. Both were proven to FAIL against the stricter checks
(captured during development); the pre-existing data-overrun and
fixed-truncation cases are unchanged and still pass.

## Validation

Profile: Ready for this bounded Rust package batch.

- Full `tidb-codec` suite: 45 + 166 tests, 0 failed.
- `cargo fmt --all -- --check`, workspace `make lint`, `git diff --check`:
  clean.

## Risks

- Compatibility: only wire inputs Go itself accepts change behavior
  (previously `InvalidOffset`); malformed tables still fail at the data
  take or row windows.
- Latent, not observed: TiKV's coprocessor and Go's own `Codec.Encode`
  always start at zero — this closes the cross-node readability difference
  the audit documented.
