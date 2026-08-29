# `pkg/util/prefetch` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `reader.go`,
`reader_test.go`, and `BUILD.bazel`. They define the background prefetch
reader, two alternating buffers and an unbuffered handoff, partial-range EOF
conversion, explicit idempotent close, and exactly four tests. There is no
package doc, README, fixture, benchmark, generated or platform variant, or
ownership file. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/prefetch.rs` is the production owner. Rust's
standard `Read` trait has no `Close`, so the one native constructor accepts the
reader plus its close callback; together they represent Go's single
`io.ReadCloser`. The audit removed the second convenience constructor, whose
no-op close could not reproduce Go, and removed the explicit `Drop`-time close
that Go does not perform. Explicit `close` retains Go's source-close, producer
cancel/join, error return, and idempotence order.

The test module now contains exactly the four Go source tests. Two
supplemental Rust close tests were removed; their extra scenarios are not
artifacts of this Go package.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/prefetch -run '^(TestBasic|TestConvertUnexpectedEOF|TestCloseBeforeDrainRead|TestFillPrefetchBuffer)$' -count=1` — passed.
- `cargo test -p tidb-util --locked 'prefetch::tests::'` — passed (4 tests).
- `cargo fmt --all --check` — passed.
- `cargo test -p tidb-util --locked` — prefetch and 646 other unit tests passed; one unrelated parallel logger-capture test observed a concurrent SEM v2 log and failed. `cargo test -p tidb-util --locked 'logutil::tests::zap_logger_with_keys' -- --exact` passed on isolated rerun.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the four source tests pass in both implementations.
- Compatibility: callers of the artificial no-close constructor must supply
  the source close callback; there were no repository callers.
- Performance: the background handoff and one-buffer-ahead behavior are
  unchanged.
