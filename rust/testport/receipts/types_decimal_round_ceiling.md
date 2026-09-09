# `pkg/types` decimal `ModeCeiling` parity receipt

- Go oracle: `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`
- Rust branch: `codex/hparser-parity-latest`
- Finding: M5 (`ModeCeiling` discarded-digit inspection)
- Source owner: `pkg/types/mydecimal.go::MyDecimal.Round`
- Rust owner: `crates/tidb-datatype/src/decimal/mod.rs`

## Inventory

The complete Go `pkg/types` decimal source/test/fixture inventory and the Rust
`tidb-datatype` decimal implementation, source vectors, generated test target,
and benchmark inputs were read before editing. The value-layer `Decimal` path
was distinguished from the already-faithful fixed-word `MyDecimal` path; no Go,
generated, platform, fixture, or build artifact changed.

## Source rule

Go’s `MyDecimal.Round` has two source branches. A word-aligned fractional cut
scans all discarded words for `ModeCeiling`; its non-word-aligned branch carries
a documented TODO and examines only the single digit immediately after the cut.
This source inconsistency is observable at `1.0001` rounded to one fractional
digit: Go returns `1.0`, not `1.1`.

## Implementation

`Decimal::round_ceiling_to_scale` now checks whether the requested non-negative
scale is a multiple of the nine-digit word width. It scans the complete
discarded suffix only for aligned cuts and otherwise tests the first discarded
byte, preserving Go’s current behavior while retaining the existing away-from-
zero sign policy.

## Focused regression

`decimal_tests::decimal_round_ceiling_uses_one_digit_for_non_word_aligned_scale`
asserts `1.0001 @ scale 1 → 1.0`, `1.0001 @ scale 3 → 1.001`, and the aligned
`1.000000001 @ scale 0 → 2` control.

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

Observed Ready results: datatype `399 passed, 0 failed` in the unit target plus
`63 passed, 0 failed` in the source/integration target; expression `1139
passed, 1 failed, 122 ignored` (the known loopback HTTP JSON-schema fixture);
executor `1052 passed, 121 failed, 0 ignored` (existing planner/storage and
fixture failures). All three `cargo check` commands, `cargo fmt --all -- --check`,
and `git diff --check` passed. Strict datatype clippy remains blocked by the
unrelated `tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics.
