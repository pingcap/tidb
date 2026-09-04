# `pkg/types` decimal `FromBin` corrupt-payload parity receipt

- Go oracle: `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`
- Rust branch: `codex/hparser-parity-latest`
- Finding: M7 (`FromBin` cursor/value state on corrupt payload)
- Source owner: `pkg/types/mydecimal.go::MyDecimal.FromBin`
- Rust owner: `crates/tidb-datatype/src/decimal/codec.rs`

## Inventory

The complete Go `pkg/types` decimal codec source, tests, and fixtures and the
Rust `tidb-datatype` decimal codec, source vectors, generated test target, and
benchmark consumers were read before editing. The downstream `tidb-codec`
callers were traced; no Go, generated, platform, fixture, or build artifact
changed.

## Source rule

Go mutates the receiver to `zeroMyDecimal` and returns the legal fixed
`DecimalBinSize(precision, frac)` even when a payload word is corrupt, then
returns `ErrBadNumber`. Illegal shapes and empty input return size zero. This
lets a caller advance over the field while preserving the hard error.

## Implementation

`Decimal::from_bin_with_failure` now exposes the zero value, consumed payload
size, and hard error together. The existing `Decimal::from_bin` API remains a
strict compatibility wrapper that maps the structured failure back to
`DecimalCodecError`, so valid decoding and existing callers are unchanged.

## Focused regression

`decimal_tests::from_bin_corrupt_word_keeps_go_zero_and_consumed_size` feeds a
legal `DECIMAL(10, 0)` payload containing the out-of-range word `1_000_000_000`
and asserts zero, `consumed = 5`, and `BadNumber`; it also checks malformed
shape input reports `consumed = 0` and that the strict wrapper still errors.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-executor --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo check --offline --locked -p tidb-expr --all-targets
cargo check --offline --locked -p tidb-executor --all-targets
cargo fmt --all -- --check
git diff --check
```

Focused regression passed. Observed full package results: datatype `400
passed, 0 failed` plus `63 passed, 0 failed` in its source target; expression
`1139 passed, 1 failed, 122 ignored` with the known loopback HTTP JSON-schema
fixture; executor `1051 passed, 122 failed, 0 ignored` with existing
planner/storage and fixture failures. Compile,
formatting, and whitespace checks pass. Strict datatype clippy remains blocked
by the unrelated `tidb-mysql/src/consts.rs:117-120` `map_or_identity` lint.
