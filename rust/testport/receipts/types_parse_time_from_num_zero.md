# `pkg/types` `ParseTimeFromNum(0)` zero-date flag parity receipt

- Go oracle: `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`
- Rust branch: `codex/hparser-parity-latest`
- Finding: T8 (`ParseTimeFromNum(0)` ignored `FlagIgnoreZeroDateErr`)
- Source owner: `pkg/types/time.go::ParseTimeFromNum`
- Rust owner: `crates/tidb-datatype/src/time_parse.rs`

## Inventory

The complete package inventory was read before editing: Go `pkg/types` has 60
production/test/fixture files at the oracle revision; the Rust datatype owner
has 104 source, test, generated, benchmark, and platform-variant files. All
`parse_time_from_num` call sites were enumerated across `tidb-datatype` and
`tidb-expr`, including the private `TemporalOutcome` test helper and the
numeric datetime branches used by `ADDTIME`/`SUBTIME` and duration parsing.

## Source rule

Go returns `NewTime(ZeroCoreTime, tp, DefaultFsp)` beside
`ErrTruncatedWrongVal` for `num == 0` when
`!ctx.Flags().IgnoreZeroDateErr()`. `DefaultStmtNoWarningContext` sets the bit,
so expression and permissive conversions retain the zero value.

## Implementation

- Added `TimeError::ZeroDate` and an explicit `ignore_zero_date_err` parameter
  to `parse_time_from_num` and its error-preserving helper.
- Numeric DATETIME/TIMESTAMP/DATE datum conversion now passes
  `ConversionFlags::ignore_zero_date_err()`, returning the zero target value
  beside `DatumValueError::IncorrectTemporal` when strict flags clear it.
- All expression and internal parser callers pass `true`, preserving Go's
  default read-path context and existing numeric zero cast behavior.

## Focused regression

`time_parse::tests::parse_time_from_num_zero_honors_zero_date_error_flag`
asserts all three target kinds reject zero with the flag cleared and retain a
zero value when it is set. `datum_convert::tests::numeric_zero_temporal_conversion_obeys_zero_date_flag`
asserts strict `Datum::Int(0)` conversion returns the zero fallback beside the
temporal error while `DEFAULT_STATEMENT_FLAGS` accepts it.

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

Observed Ready results: datatype all-targets passed 397 unit and 63 generated
source integration tests. The expression all-targets profile passed 1,136
tests, with one known loopback HTTP JSON-schema fixture failure and 123
ignored gap tests. The broad executor all-targets profile passed 1,063 tests,
with 110 existing planner/storage/fixture failures. Strict datatype clippy
remains blocked by the unrelated `tidb-mysql/src/consts.rs:117-120`
`map_or_identity` lint.
