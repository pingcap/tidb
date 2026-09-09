# `pkg/types` float-string invalid-date receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete temporal owners were rechecked before editing, including every
production file, test/benchmark, parser-driver fixture, generated/platform
variant, and build artifact:

| Tree | Files | Lines |
| --- | ---: | ---: |
| `pkg/types` (including `parser_driver`) | 60 | 28,703 Go lines |
| `rust/crates/tidb-datatype` | 104 | 52,114 Rust source/test/manifest lines plus 8 data/docs artifacts |

The Go behavior-bearing files read were `pkg/types/time.go`
(`ParseTimeFromFloatString`, `parseTime`, `parseDatetime`, and
`parseDateTimeFromNum`), `pkg/types/time_test.go`
(`TestParseTimeFromFloatString`), and the complete `pkg/types/BUILD.bazel`.
The Rust owner chain was `src/time_parse.rs::parse_time`,
`parse_datetime_core`, `parse_time_from_num`, and the source-derived temporal
tests. No Go, generated, fixture, platform, or build file changed.

## Go behavior restored

Go's `ParseTimeFromFloatString` sends a numeric string through
`parseDatetime`; its numeric branch calls `ParseTimeFromNum`, and the final
`Time.Check(ctx)` honors both `IgnoreZeroInDate` and `IgnoreInvalidDateErr`
from the caller's session context. Rust's `parse_datetime_core` previously
hardcoded `allow_zero_in_date = true` and `allow_invalid_date = false` for that
branch. Consequently `ALLOW_INVALID_DATES` could not preserve a date such as
`2020-02-31` when the value arrived through the float-string parser.

`parse_datetime_core` now receives both date-mode booleans from `parse_time`
and forwards them unchanged to `parse_time_from_num`. Non-numeric parsing,
FSP rounding, timezone handling, and the existing zero-number compatibility
guard are unchanged.

## Focused regression

`time_parse::tests::float_string_numeric_path_preserves_allow_invalid_date`
pins the source boundary:

- `parse_time("20200231", DATETIME, 0, is_float=true, allow_invalid=false)`
  rejects the invalid calendar date;
- the same input with `allow_invalid=true` returns
  `2020-02-31 00:00:00`.

## Ready validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --offline --locked -p tidb-datatype --lib time_parse::tests::float_string_numeric_path_preserves_allow_invalid_date -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --lib time_parse::tests::test_parse_time_from_float_string -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

Results:

- Focused invalid-date and existing float-string source regressions: PASS.
- Serialized owner profile: PASS (394 unit tests and 63 generated/source
  integration tests; benchmark targets compiled).
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict clippy remains blocked by the pre-existing
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics, not by
  this batch.

## Risks and remaining boundaries

This change is limited to the numeric branch of the float-string datetime
parser. Numeric zero's separate `NO_ZERO_DATE` context contract, DST-adjusted
TIMESTAMP parsing, `STR_TO_DATE` modes, and remaining temporal findings retain
their existing boundaries. T7–T11, T14, and T16 remain open or separately
tracked; this receipt makes no package-complete claim.
