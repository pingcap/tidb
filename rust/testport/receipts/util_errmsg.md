# `pkg/util/errmsg` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains production `errmsg.go`, source test
`errmsg_test.go`, and `BUILD.bazel`. It has five top-level source tests and no
`doc.go`, package harness, fixture, generated source, benchmark, fuzz target,
example, platform variant, or build-tagged production variant. The checkout
package is byte-identical to the pin.

The dedicated `tidb-errmsg` crate and its `errmsg_test` integration target map
to the Go library and package-local test. The Bazel target's flaky scheduling
annotation has no Cargo semantic analogue.

## Rust ownership and integration

`tidb-errmsg::extend` owns the complete package behavior over
`tidb-error::mysql::SqlError`. Rust `Option<&mut SqlError>` is the native
representation of Go's nullable pointer. The function reads the prepared
configuration snapshot, skips empty suffixes and absent compiled regexps,
applies only the first match, trims every trailing period from message and
suffix, and appends the fixed `", suffix."` form.

`tidb-config` owns the Go `pkg/config` prerequisite: invalid regexps are
dropped and prepared extensions are ordered by longest pattern before the
snapshot is published. The ordinary `tidb-server` ERR-packet writer invokes
`tidb_errmsg::extend` before encoding valid SQL-error text, while preserving
raw non-UTF-8 packet bytes at the protocol boundary.

The former Rust test names did not map to the five Go identities and included
a supplemental nil-error assertion inside `TestExtendByRegex`. That assertion
was removed and the suite now has exactly `TestExtendByRegex`,
`TestExtendWithoutConfig`, `TestExtendSkipsInvalidRegex`,
`TestExtendPrefersLongestPattern`, and
`TestExtendConcurrentWithStoreGlobalConfig` with the source cases.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/errmsg` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/errmsg -count=1` — passed; five tests.
- `cargo check -p tidb-errmsg -p tidb-server` — passed.
- `cargo test -p tidb-errmsg --test errmsg_test` — passed; five tests.
- `cargo test -p tidb-server connection_writers::tests::error_packets_apply_configured_suffixes_and_preserve_raw_bytes` — passed; ordinary wire consumer.
- `cargo fmt -p tidb-errmsg` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: production and consumer behavior are unchanged; test scope now
  follows the source exactly.
- Compatibility: first-match ordering, invalid-regex skipping, punctuation,
  empty configuration, and concurrent configuration publication are covered.
- Performance: unchanged one-snapshot linear matcher scan with early return.
- Not verified locally: configuration reload from a live server process. The
  config preparation/store path and the ordinary packet boundary are both
  exercised in-process.
