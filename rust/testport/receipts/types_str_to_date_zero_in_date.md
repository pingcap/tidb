# `pkg/types` `StrToDate` zero-in-date flag parity receipt

- Go oracle: `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`
- Rust branch: `codex/hparser-parity-latest`
- Finding: T9 (`Time.StrToDate` hardcoded `allow_zero_in_date = true`)
- Source owner: `pkg/types/time.go::StrToDate` and `Time.Check`
- Rust owner: `crates/tidb-datatype/src/str_to_date.rs`

## Inventory

The complete `pkg/types` production/test/fixture inventory and Rust datatype
owner inventory were read before editing. All Rust `Time::str_to_date` callers
were enumerated, including the benchmark target and every source-vector helper;
the expression `STR_TO_DATE` evaluator was inspected separately because it
owns the signature-specific `NO_ZERO_DATE` check and does not call this method.

## Source rule

Go's `Time.StrToDate(typeCtx, ...)` ends in `t.Check(typeCtx)`, which consults
`FlagIgnoreZeroInDate`. A partial format such as `%Y-%m` therefore returns
`ErrWrongValue` when zero month/day values are not allowed, while the default
expression context explicitly enables `IgnoreZeroInDate` before the signature
applies its own `NO_ZERO_DATE` rule.

## Implementation

`Time::str_to_date` now takes `allow_zero_in_date` and forwards it to
`Time::validate` instead of hardcoding `true`. Existing source-vector and
benchmark callers pass `true`, preserving their default context while exposing
the strict path to callers that clear the flag.

## Focused regression

`str_to_date::tests::str_to_date_zero_in_date_flag_is_not_hardcoded` checks
`STR_TO_DATE('2013-05','%Y-%m')` rejects with `TimeError::ZeroInDate` when the
flag is false and returns `2013-05-00 00:00:00` when it is true.

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

Observed Ready results: datatype all-targets passed 398 unit and 63 generated
source integration tests. The expression all-targets profile passed 1,139
tests, with one known loopback HTTP JSON-schema fixture failure and 122
ignored gap tests. The broad executor all-targets profile passed 1,052 tests,
with 121 existing planner/storage/fixture failures. Strict datatype clippy
remains blocked by the unrelated `tidb-mysql/src/consts.rs:117-120`
`map_or_identity` diagnostics.
