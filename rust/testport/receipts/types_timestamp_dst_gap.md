# `pkg/types` TIMESTAMP DST-gap receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete temporal and write owners were rechecked before editing,
including production files, tests/benchmarks, parser-driver fixtures,
generated/platform variants, and build artifacts:

| Tree | Files | Scope |
| --- | ---: | --- |
| `pkg/types` | 60 | temporal parser, conversion, timezone, tests, and `BUILD.bazel` |
| `rust/crates/tidb-datatype` | 104 | temporal parser/conversion, tests, benches, manifests, and source fixtures |
| `pkg/expression` | 202 | cast signatures, context warning policy, tests, and build metadata |
| `rust/crates/tidb-expr` | 176 | cast evaluator, context traits, tests, manifests, and generated test targets |
| `rust/crates/tidb-executor` | 291 | write-cast/error mapping, driver tests, manifests, and generated targets |

The Go behavior-bearing files were `pkg/types/time.go`
(`parseTime`, `adjustTimestampErrForDST`, and `Time.Convert`),
`pkg/table/column.go` (`castColumnValue`/`handleZeroDatetime`),
`pkg/executor/insert_common.go` (`completeInsertErr`), and their complete
tests/build manifests. The Rust owner chain was
`tidb-datatype::time_parse`, `Datum::convert_to_time_target`,
`tidb-expr::cast_to_time_value`, and `tidb-executor::driver::write_cast`.

## Go behavior restored

Go's `parseTime` does not discard a TIMESTAMP that falls into a spring-forward
gap. After `Time.Check` reports the nonexistent local wall clock,
`adjustTimestampErrForDST` calls `AdjustedGoTime`, returns the first valid
transition boundary beside `ErrTimestampInDSTTransition` (8179), and lets
the caller's truncate policy decide warning versus error. For
`America/Los_Angeles`, `2018-03-11 02:00:16` therefore becomes
`2018-03-11 03:00:00` and reports the original text in the diagnostic.

Rust's string parser previously propagated `NonexistentLocalTime`, leaving no
value for either a SELECT cast or a write. `ParsedTime` now carries a
`dst_adjusted` bit; both string and packed numeric TIMESTAMP parsing adjust to
the same boundary. Expression casts append 8179 while returning the adjusted
value. The write conversion carries a dedicated
`TimestampInDSTTransition` event through `Datum::convert_to_in`; the driver
stores the adjusted value and emits the exact Go 8179 warning in lenient mode
or returns it in strict mode. The event is kept separate from ordinary
truncation so existing 1292/1265 policies are unchanged.

## Focused regressions

- `tidb_datatype::time_parse::tests::timestamp_string_in_dst_gap_is_adjusted_with_marker`
  asserts the adjusted wall clock and parser marker.
- `tidb_expr::cast::tests::a_string_cast_to_timestamp_adjusts_dst_gap_and_warns`
  asserts the SELECT cast value and warning code/message.
- `tidb_executor::driver::write_cast::source_tests::timestamp_dst_gap_keeps_adjusted_value_and_8179_write_diagnostic`
  asserts lenient storage + warning and strict 8179 failure.

## Ready validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --offline --locked -p tidb-datatype time_parse::tests::timestamp_string_in_dst_gap_is_adjusted_with_marker -- --exact --nocapture
cargo test --offline --locked -p tidb-expr cast::tests::a_string_cast_to_timestamp_adjusts_dst_gap_and_warns -- --exact --nocapture
cargo test --offline --locked -p tidb-executor driver::write_cast::source_tests::timestamp_dst_gap_keeps_adjusted_value_and_8179_write_diagnostic -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo check --offline --locked -p tidb-expr --all-targets
cargo check --offline --locked -p tidb-executor --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

Results:

- All three focused regressions: PASS.
- `tidb-datatype` serialized owner: PASS (395 unit tests and 63 generated /
  source integration tests; benchmark targets compiled).
- `tidb-expr` serialized owner: 1,132 passed, 1 known external HTTP
  JSON-schema fixture failure (`json_schema_valid_resolves_file_and_http_references`;
  loopback fixture unavailable), 124 ignored.
- `tidb-executor` serialized owner: 1,048 passed and 121 existing planner,
  storage, and fixture failures; the focused write-cast regression passed.
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict datatype clippy remains blocked only by the pre-existing
  `tidb-mysql/src/consts.rs:117-120` `map_or_identity` diagnostics.

## Risks and remaining boundaries

This batch closes the T7 parser/cast/write diagnostic boundary. It does not
claim package-complete parity: numeric zero-date context (T8),
`STR_TO_DATE` zero-in-date mode (T9), and the remaining temporal findings stay
tracked in the divergence audit. The external expression fixture and the
unrelated clippy diagnostics are environment/base failures, not regressions
from this change.
