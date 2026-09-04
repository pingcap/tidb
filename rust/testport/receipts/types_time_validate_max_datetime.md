# `pkg/types` DATETIME maximum-precision validation receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete owners were enumerated before editing, including production
sources, tests and benchmarks, fixtures/data, generated/platform artifacts,
fuzz/script inputs, and build/manifest files:

| Tree | Files | Source/test lines |
| --- | ---: | ---: |
| `pkg/types` (including `parser_driver`) | 60 | 28,703 Go lines |
| `rust/crates/tidb-datatype` | 104 | 52,114 Rust source/test/manifest lines plus 8 data/docs artifacts |

The Go build owner is `pkg/types/BUILD.bazel`: 30 production files and 28
test/benchmark files. The Rust build owner is
`rust/crates/tidb-datatype/Cargo.toml`; its source owner is
`src/mysql_time.rs`, with unit tests in the same module and companion time,
duration, and parser source suites. The behavior-bearing Go chain was read in
`pkg/types/time.go` (`Time.Check`, `checkDateType`, `checkDateRange`,
`checkDatetimeType`) and `pkg/types/core_time.go` (`FromDate`,
`FromDateChecked`, and field comparison).

## Go behavior restored

Go's `checkDateRange` compares the complete `CoreTime` with
`MaxDatetime = 9999-12-31 23:59:59.999999`. Rust previously checked only year,
month, day, and clock ranges, even though the packed microsecond field can
hold values through `1,048,575`. A synthetic
`9999-12-31 23:59:59.1000000` therefore passed Rust validation while Go
returned its wrong-value error.

`Time::validate` now rejects the exact upper-bound second when its microsecond
field exceeds `999_999`. Earlier calendar values remain valid even with a
synthetic large microsecond field, matching Go's lexicographic `compareTime`
ordering rather than imposing a new global Rust-only microsecond rule.

## Focused regression

`mysql_time::tests::test_validate_datetime_max_precision_boundary` pins all
three source boundaries:

* the exact Go maximum is accepted;
* one microsecond beyond that maximum is rejected as `InvalidDate`;
* an earlier date with the same synthetic field remains accepted.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --lib \
  mysql_time::tests::test_validate_datetime_max_precision_boundary \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

Results:

* Focused regression: PASS (1/1).
* Serialized owner profile: PASS (389 unit tests and 63 generated/source
  integration tests; all benchmark targets compiled).
* Owner compilation: PASS.
* Formatting and whitespace: PASS.
* Strict clippy: BLOCKED only by pre-existing `tidb-mysql/src/consts.rs:117-120`
  `clippy::map-or-identity` diagnostics; no diagnostic points at this batch.

## Risks and remaining boundaries

This closes the DATETIME `MaxDatetime` precision escape only. The analogous
TIMESTAMP upper-bound precision check and the other time-audit items (DST
error/value disposition, zero-date context, `STR_TO_DATE` context, and packed
conversion edge cases) remain separate boundaries. No package-complete
`pkg/types` or `tidb-datatype` claim is made by this receipt.
