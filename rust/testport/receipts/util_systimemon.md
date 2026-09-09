# `pkg/util/systimemon` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `systime_mon.go`: blocking `StartMonitor`, its 100 ms ticker, start/error
  logs, backward wall-clock comparison, and callback.
- `systime_mon_test.go`: `TestSystimeMonitor`.
- `main_test.go`: common Go test setup and goroutine-leak exclusions,
  including the intentionally process-lifetime `StartMonitor` goroutine.
- `BUILD.bazel`: one library and one short, flaky test target.

There is no package doc, benchmark, fixture, generated source, platform
variant, README, or ownership file. The local Go package is byte-identical to
the pin. The Go-only common-test and goleak harness has no production port;
the Rust source test likewise leaves the monitor detached until its test
process exits.

## Rust ownership and audit result

`rust/crates/tidb-util/src/systimemon.rs` owns the complete package and
`rust/crates/tidb-server/src/lib.rs` is its ordinary server caller, matching
`cmd/tidb-server/main.go`: the package function blocks forever and the caller
launches the background worker. The monitor logs the same messages, samples
the previous wall-clock value before each 100 ms ticker event, logs its signed
Unix-nanosecond value after a regression, and invokes the callback.

The audit removed `SystemTimeMonitor`, its stop state, condition variable,
`Drop` join, `start_with_interval`, and the public `MONITOR_INTERVAL`. Those
APIs added cancellation, ownership, and cadence policy absent from Go. It also
removed the duplicate inline test and the Rust-only log-file lifecycle test;
the external test now directly transcreates the sole Go test.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

Passed:

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/systimemon`
- `cargo test --offline --locked -p tidb-util --test systimemon_source`
- `cargo check --offline --locked -p tidb-server --lib`
- `cargo clippy --offline --locked -p tidb-util --lib --test
  systimemon_source --no-deps -- -A clippy::map-or-identity -A
  clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A
  clippy::new-without-default -D warnings`
- `cargo clippy --offline --locked -p tidb-server --lib --no-deps`
- `rustfmt --edition 2021 --check crates/tidb-util/src/systimemon.rs
  crates/tidb-util/tests/systimemon_source.rs crates/tidb-server/src/lib.rs`
- `git diff --check`

`go test ./pkg/util/systimemon -count=1` did not reach this package because
the existing gRPC transport dependency fails first with undefined
`http2.TrailerPrefix`.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the source test passes and the production server consumer
  compiles with caller-owned thread launch.
- Compatibility: the Rust-only stoppable guard API is intentionally removed;
  the monitor now has Go's process-lifetime ownership.
- Performance: one fixed-rate 100 ms ticker and two clock reads per iteration
  match the source loop.
