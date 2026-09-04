# `pkg/expression` decimal `DIV` unsigned-width receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The arithmetic owner was inventoried before editing, including production
sources, scalar/vector tests, generated sources, fixtures, build metadata, and
the nested expression support packages:

| Tree | Files | Lines |
| --- | ---: | ---: |
| `pkg/expression` recursive tree | 202 | 144,556 Go lines |
| `rust/crates/tidb-expr` | 176 | 107,126 Rust lines |

The Go inventory includes `builtin_arithmetic.go`,
`builtin_arithmetic_vec.go`, their scalar/vector tests, `BUILD.bazel`, and all
nested aggregation/context/generator/integration/test-fixture artifacts. The
Rust inventory includes `src/ops.rs`, `src/ops/real_coerce.rs`,
`src/ops/integer_coerce.rs`, `src/builtin_arithmetic.rs`, the decimal and
operand-dispatch source tables, all in-module tests, standalone source tests,
`tests/all.rs`, benchmarks, and `Cargo.toml`. No Go, generated, fixture, or
platform artifact changed.

## Go behavior restored

Go's `builtinArithmeticIntDivideDecimalSig.evalInt` performs `DecimalDiv`, then
converts the quotient through `ToUint` if either input carries `UnsignedFlag`
and through `ToInt` otherwise. The unsigned path accepts values through
`u64::MAX`; negative values overflow except for quotients truncated to zero in
`(-1, 0]`. Rust's decimal `div_rem` previously parsed the quotient as `i64`
before `decimal_binary` selected the result kind, so a valid upper-half
unsigned result was reported as overflow.

`Decimal::div_rem_unbounded` now retains the complete scale-zero quotient as a
`Decimal`; the existing `div_rem` API continues to expose the signed `i64`
view for callers that require it. `decimal_binary` applies the source
`to_u64_trunc`/`to_i64_trunc` conversions, preserving zero-divisor handling and
leaving `MOD` on its existing remainder path.

## Focused regressions

- `decimal_tests::div_rem_unbounded_preserves_unsigned_bigint_range` proves
  `18446744073709551615 / 1.5` keeps quotient
  `12297829382473034410`, which fits `u64` but not `i64`.
- `tests::go_arithmetic_values::go_test_arithmetic_int_divide` covers the
  expression boundary: the upper-half unsigned result, negative unsigned
  overflow, and Go's `(-1, 0]` zero exception.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --lib \
  decimal_tests::div_rem_unbounded_preserves_unsigned_bigint_range \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-expr --lib \
  tests::go_arithmetic_values::go_test_arithmetic_int_divide \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype -p tidb-expr --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype -p tidb-expr --all-targets -- -D warnings
```

Results:

- Both focused regressions: PASS.
- `tidb-datatype` serialized owner profile: PASS (391 unit tests and 63
  generated/source integration tests; benchmark targets compiled).
- `tidb-expr` serialized owner profile: 1,121 passed, one pre-existing
  external HTTP JSON-schema fixture failure, and 130 documented gap tests
  ignored; the new arithmetic regression passed.
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict clippy is expected to remain blocked by the pre-existing
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics (and any
  generated workspace diagnostics), not by this batch.

## Risks and remaining boundaries

This closes decimal `DIV` quotient-width and result-signedness parity. Decimal
`ROUND`/`TRUNCATE` declared-scale caps, `FLOOR`/`CEIL` declared-width result
metadata, binary `LIKE` byte matching, and `CONCAT` packet limits remain
separate expression boundaries. Vectorized Go `DIV` still uses a signed `int64`
result column by design in the source; this batch changes only the Rust scalar
value path currently owned by `tidb-expr`.
