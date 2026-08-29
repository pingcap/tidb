# `pkg/util/sqlkiller` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `sqlkiller.go` and
`BUILD.bazel`. There is no package doc, test, benchmark, fixture, generated or
platform variant, README, or ownership file. The local Go package is
byte-identical to the pin.

Production behavior includes the raw exported `uint32` signal and its seven
named values, first-signal-wins CAS,
kill-event creation/trigger/reset, optional event reason, exact error mapping,
result-set finish callback locking, connection-liveness polling, immediate
liveness checking, reset, warning emission, and the `randomPanic` failpoint.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sqlkiller.rs` owns the complete package. Native
receivers represent Go's close-broadcast channel and guarded callback slots
represent its function fields. The connection registration token preserves
the pinned server caller's conditional atomic-pointer removal, so an old
connection cannot clear a replacement callback.

The signal is a transparent raw value rather than a closed Rust enum, and the
atomic field is public like Go's `Signal`. `get_kill_signal` therefore returns
zero and unknown values verbatim. Installing zero or an unknown value follows
Go's nil-error logging panic instead of silently accepting it.
Connection polling keeps the source's separate timestamp load/store boundary,
so concurrent handlers are not given a Rust-only deduplication guarantee.

The audit removed ten supplemental Rust tests because the Go package has no
tests. It also removed the extra generation/condition-variable subscription
API and the Rust-only timed kill wait. The remaining `get_kill_event_chan`
receiver is the native `GetKillEventChan` boundary used by the memory
arbitrator. SQL `SLEEP` now follows pinned Go's separate `doSleep` behavior by
polling `HandleSignal` every 10 ms instead of using the removed immediate
event wait.

The previously omitted `randomPanic` injection is now compiled under the
`tidb-util/failpoints` feature. Its probability comparison, nonzero connection
guard, random status range `[0, 5)`, and direct signal store match the Go hook.
Normal builds retain Go's uninstrumented behavior.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/sqlkiller` — passed.
- `go test ./pkg/util/sqlkiller -count=1` — blocked before package compilation
  by the existing module graph: gRPC's transport package references undefined
  `http2.TrailerPrefix`.
- `cargo test --offline --locked -p tidb-util --features failpoints --lib sqlkiller` —
  passed compilation with zero tests, matching the source inventory.
- `cargo test --offline --locked -p tidb-executor is_session_done_input_signal_roundtrip_reads_query_interrupted_as_one` —
  passed the source-derived numeric signal test.
- `cargo test --offline --locked -p tidb-executor --lib mem_quota` — passed.
- `cargo test --offline --locked -p tidb-ttl test_session_kill` — passed.
- `cargo test --offline --locked -p tidb-stats source_kill_signal_aborts_the_merge` —
  passed.
- `cargo check --offline --locked -p tidb-util -p tidb-executor -p tidb-ttl -p tidb-stats --all-targets` —
  passed; existing warnings remain outside this change.
- `cargo clippy --offline --locked -p tidb-util --lib --no-deps --features failpoints -- -A clippy::needless-borrows-for-generic-args -A clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D warnings` —
  passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

The Go package has no tests. A temporary Rust probe confirmed that fresh zero
and arbitrary public raw signals round-trip, unknown signals yield no handled
error, and newly installed zero or unknown signals panic before triggering the
event; the probe was removed after it passed. No Go or Bazel file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: signal, event, callback, liveness, failpoint, and downstream
  consumers compile; targeted executor, TTL, and statistics tests pass. The
  Go package test remains unverified because of the unrelated dependency
  compilation failure above.
- Compatibility: unused Rust-only public event helpers were intentionally
  removed; the repository has no remaining reference to them.
- Performance: SQL `SLEEP` now performs Go's 10 ms signal polling rather than
  the faster Rust-only immediate wake; other runtime paths are unchanged.
