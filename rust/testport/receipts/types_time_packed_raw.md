# `pkg/types` raw `ToPackedUint` receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete temporal owners were rechecked before editing, including
production files, tests/benchmarks, parser-driver fixtures, generated and
platform/build artifacts:

| Tree | Files | Lines |
| --- | ---: | ---: |
| `pkg/types` (including `parser_driver`) | 60 | 28,703 Go lines |
| `rust/crates/tidb-datatype` | 104 | 52,114 Rust source/test/manifest lines plus 8 data/docs artifacts |

The behavior-bearing Go files read were `pkg/types/time.go` (`ToPackedUint`,
`FromPackedUint`) and `pkg/types/time_test.go` (`TestCodec`), together with the
complete `pkg/types/BUILD.bazel`. The Rust owner chain was
`src/mysql_time.rs::Time::to_packed_uint`, `src/packed_time.rs`, and the codec
and expression consumers that encode temporal literals. No Go, generated,
fixture, or platform file changed.

## Go behavior restored

Go's `Time.ToPackedUint` only checks the zero value and then packs the stored
calendar fields. It does not invoke `Check` or reject a synthetic hour,
minute, second, or microsecond outside the SQL-valid range. Rust previously
routed the fields through `PackedTime::from_parts`, whose constructor is
intentionally strict for other codec entry points, turning this raw storage
operation into a fallible validation step.

`Time::to_packed_uint` now performs the same direct bit packing as Go and keeps
its existing `Result<u64, TimeError>` API (the result is always `Ok` at this
boundary). `PackedTime::from_parts` remains strict for callers that explicitly
construct validated payloads, and `FromPackedUint` is unchanged.

## Focused regression

`mysql_time::tests::test_to_packed_uint_preserves_raw_fields_without_validation`
constructs `2020-01-01 24:60:60.1000000` in a raw `CoreTime` and asserts the
exact Go bit layout is returned instead of `TimeError::OutOfRange`.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-datatype --lib \
  mysql_time::tests::test_to_packed_uint_preserves_raw_fields_without_validation \
  -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --lib \
  mysql_time::tests::test_codec -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

Results:

- Focused raw-pack regression and existing codec regression: PASS.
- Serialized owner profile: PASS (392 unit tests and 63 generated/source
  integration tests; benchmark targets compiled).
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict clippy remains blocked by the pre-existing
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics and
  generated workspace diagnostics, not by this batch.

## Risks and remaining boundaries

This changes only `Time::to_packed_uint`'s raw storage contract. SQL parsing,
`Time::validate`, TIMESTAMP epoch checks, and the strict `PackedTime::from_parts`
constructor retain their existing validation. T14/T16 timezone/type metadata
and normalization boundaries, plus the remaining zero-date/context rows,
remain open.
