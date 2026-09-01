# `pkg/util/column-mapping` — complete package transcreation

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `column.go` — partition sizing, expression and rule validation, selector and
  mapping lifecycle, cache behavior, row/DDL handling, and value rewrites;
- `column_test.go` — all seven package tests;
- `README.md` — rule syntax and partition-ID layout;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, generated/platform source, fixture, testdata, benchmark,
fuzz target, example, or additional harness. The package has no production Go
or Rust consumer in this repository.

## Rust ownership and audit result

`rust/crates/tidb-util/src/column_mapping.rs` is the sole owner and uses the
already-transcreated table-rule selector and Go-compatible simple Unicode
lowercasing.

The audit removed supplemental lifecycle and concurrency tests and the stale
semantic manifest. It also removed Rust-only checked partition-size policy:
the setter now accepts signed Go `int` values and reproduces Go's wrapping
subtraction and oversized-shift results. Column positions likewise use the
native signed pointer-width type corresponding to Go `int`.

Rust's value model now preserves every Go integer runtime type used by the
source type switch. It accepts exactly `int`, `int8`, `int32`, `int64`,
`uint`, `uint16`, `uint32`, `uint64`, and decimal strings; it rejects `int16`,
`uint8`, and unrelated dynamic values. Successful numeric rewrites become
`int64`, as in Go, while successful string rewrites remain strings.

DDL handling now retains the original statement on both success and error,
matching Go's three-result behavior instead of discarding it inside a
Rust-only `Result` path. Clonable error semantics were removed.

Two retained regressions cover source-observed behavior not exercised by the
seven package tests: Go's simple Unicode lowercase mapping and zero-valued
fields during configuration deserialization.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/column-mapping`
- `cargo test -p tidb-util --lib 'column_mapping::tests' --locked -- --test-threads=1`
- `cargo test -q -p tidb-util --locked -- --test-threads=1`
- `cargo check -p tidb-util --all-targets --locked`
- `cargo clippy -p tidb-util --lib --no-deps --locked -- -A clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D warnings`
- `cargo fmt --all --check`
- `git diff --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; invalid partition configurations, numeric dynamic
  types, numeric output type, signed positions, and DDL return values now
  follow Go.
- Compatibility: the changes affect an unconsumed Rust API and remove only
  Rust-specific behavior and artifacts.
- Performance: unchanged in the row-mapping path; synchronization remains a
  native implementation detail for Go's process-global rule and mapping cache.
