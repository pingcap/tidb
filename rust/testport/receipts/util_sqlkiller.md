# `pkg/util/sqlkiller` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `sqlkiller.go` and
`BUILD.bazel`. There is no package doc, test, benchmark, fixture, generated or
platform variant, README, or ownership file. The local Go package is
byte-identical to the pin.

Production behavior includes the seven signal values, first-signal-wins CAS,
kill-event creation/trigger/reset, optional event reason, exact error mapping,
result-set finish callback locking, connection-liveness polling, immediate
liveness checking, reset, warning emission, and the `randomPanic` failpoint.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sqlkiller.rs` owns the complete package. Native
receivers represent Go's close-broadcast channel and guarded callback slots
represent its function fields. The connection registration token preserves
the pinned server caller's conditional atomic-pointer removal, so an old
connection cannot clear a replacement callback.

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
- `cargo check -p tidb-util -p tidb-executor --locked` — passed; existing
  warnings remain outside this change.
- `cargo check -p tidb-util --features failpoints --locked` — passed.
- `cargo test -p tidb-util --features failpoints --lib sqlkiller --locked` —
  passed compilation with zero tests, matching the source inventory.
- `cargo test -q -p tidb-util --locked -- --test-threads=1` — passed: 613 unit
  tests passed, 3 were ignored, and every integration and doc test passed.
- `cargo test -p tidb-executor sleep_keeps_a_kill_installed_after_a_physical_table_reader_is_built --lib --locked -- --test-threads=1` — passed.
- `cargo test -q -p tidb-executor table_reader_build_records_go_stmtctx_table_access --lib --locked -- --test-threads=1` — passed.
- `cargo fmt --all` and `git diff --check` — passed.

The Go package has no tests. No Go or Bazel file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: signal, event, callback, liveness, failpoint, and downstream
  sleep paths compile; the serial owner suite and targeted executor tests
  pass.
- Compatibility: unused Rust-only public event helpers were intentionally
  removed; the repository has no remaining reference to them.
- Performance: SQL `SLEEP` now performs Go's 10 ms signal polling rather than
  the faster Rust-only immediate wake; other runtime paths are unchanged.
