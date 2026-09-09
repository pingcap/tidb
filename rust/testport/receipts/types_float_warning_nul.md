# `pkg/types` float-prefix NUL warning parity receipt

- Go oracle: `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`
- Rust branch: `codex/hparser-parity-latest`
- Finding: D9 (`getValidFloatPrefix` NUL-byte diagnostic subject)
- Source owners: `pkg/types/convert.go::getValidFloatPrefix`,
  `pkg/expression/builtin_cast.go::builtinCastStringAsRealSig`
- Rust owners: `crates/tidb-datatype/src/convert.rs`,
  `crates/tidb-expr/src/cast.rs`, `ops/real_coerce.rs`,
  `builtin_compare.rs`, `sessionexpr.rs`, and `exprstatic/evalctx.rs`

## Inventory

The complete Go `pkg/types` conversion source/tests and the Rust datatype
conversion plus every Rust DOUBLE warning callsite were read before editing.
No Go, generated, platform, fixture, or build artifact changed.

## Source rule

Go trims Unicode whitespace before scanning a float prefix and, when the input
contains NUL, shortens the diagnostic subject at the first NUL. The numeric
value and truncation disposition are unchanged; only the Warning 1292 text
uses the shortened subject.

## Implementation

`tidb_datatype::float_warning_input` centralizes the trim-and-NUL rule. Every
Rust DOUBLE warning builder now uses it, including explicit casts, real
coercion, string comparison, statement timestamp parsing, and static eval
context diagnostics.

## Focused regressions

- `convert::tests::float_warning_input_truncates_at_nul_like_go` covers a
  leading NUL, an embedded NUL after padding, and an ordinary truncated value.
- `cast::tests::double_cast_warning_subject_stops_at_nul_like_go` exercises the
  live `CAST(... AS DOUBLE)` warning path and asserts the exact empty subject.

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

The two focused regressions pass. Observed full profile results: datatype `401
passed, 0 failed` plus `63 passed, 0 failed` in its source target; expression
`1141 passed, 1 failed, 122 ignored` with the known loopback HTTP JSON-schema
fixture; executor `1052 passed, 121 failed, 0 ignored` with existing
planner/storage and fixture failures. All three `cargo check` commands,
formatting, and whitespace checks pass.
