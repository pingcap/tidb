# `pkg/types` `ConvertFloatToUint` NaN panic receipt

Status: bounded Rust-only parity fix against Go `origin/master`
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Inventory completed before editing

The complete Go package inventory was reused from the current `pkg/types`
audit: 60 production, test, benchmark, parser-driver, support, and build
artifacts (28,703 lines), with no platform-specific or generated source. The
Rust `tidb-datatype` owner inventory contains 104 production/test/manifest,
benchmark, fuzz, fixture, and generated collation-data artifacts. The
behavior-bearing source is `pkg/types/convert.go` (`ConvertFloatToUint`) and
its package test surface; no Go source or build artifact changed.

## Go behavior restored

Go rounds the input and passes it to `new(big.Float).SetFloat64`. `SetFloat64`
panics for NaN before `Uint64` can return a saturated result. Rust previously
treated every non-finite value as an ordinary overflow, which silently
returned the unsigned upper bound for NaN. The Rust conversion now preserves
Go's NaN panic while retaining the existing infinity handling (`big.Float`
accepts infinities and `Uint64` reports them as out of range).

## Focused regression

`convert::tests::convert_float_to_uint_nan_panics_like_go` uses the source
conversion flags and `BIGINT UNSIGNED` bound and requires the
`Float.SetFloat64(NaN)` panic. The test failed before the production edit
(`test did not panic as expected`) and passes after it.

## Ready validation

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p
  tidb-datatype --lib
  convert::tests::convert_float_to_uint_nan_panics_like_go -- --exact
  --nocapture` — PASS after the fix; fail-before output is recorded above.
- Complete `tidb-datatype` owner profile: 412 lib tests and 64 generated
  aggregate/source tests — PASS.
- Go authority package check: `go test ./pkg/types -count=1` — PASS.
- `cargo fmt --manifest-path rust/Cargo.toml -p tidb-datatype -- --check`,
  `git diff --check`, and Ready `make lint` — PASS. A workspace-wide
  `cargo fmt --all -- --check` also reports two pre-existing formatting-only
  diffs in the remote `tidb-session` base files; those unrelated files remain
  untouched.

## Risks and remaining boundaries

Only the previously silent NaN conversion path changes; finite and infinity
conversion results retain their source-compatible saturation/error behavior.
This receipt does not claim repository-wide parity or close the broader
executor/session paths that can manufacture non-finite values.
