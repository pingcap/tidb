# `pkg/util/sqlkiller` — complete Go-master package transcreation

Status: Ready for this atomic package batch. This is not a repository-wide
parity or PR-readiness claim.

Go source: `origin/master`
`db35d47066648fe73abce6318d53fc625df51490` (which includes
`5af03a2e108d70b151c32ba4620cfcaedcaf4502`, “prevent SQLKiller state races
during concurrent reset”).

Rust comparison branch: `origin/hparser-integration`
`5a005978dda57fbb3373a303660ea0a5f7990b38`.

## Complete inventory

The Go package has exactly three direct artifacts, all read in full:

- `sqlkiller.go` — 277 lines; the raw `uint32` signal and seven values,
  first-signal-wins CAS, guarded kill-event channel/reason state, exact error
  mapping, result-set callbacks, liveness polling, immediate liveness check,
  reset, warning emission, and `randomPanic` failpoint.
- `sqlkiller_test.go` — 110 lines; one test,
  `TestSQLKillerConcurrentReset`, with the two source subcases “reset after
  successful kill signal CAS” and “kill signal after reset clear”. It also
  contains the lock-state, event-state, and open/closed-channel helpers.
- `BUILD.bazel` — 30 lines; one library and one flaky short test target with
  the source failpoint, logging, testify, zap, and observer dependencies.

There is no `doc.go`, `main_test.go`, benchmark, fuzz test, fixture or
`testdata`, generated source/input, platform/build-tag variant, README, or
ownership artifact. The checkout's Go copy is the pre-`5af03a2e10` version;
the Go-master delta is the three-artifact race-fix/test addition listed
above.

## Function inventory and Rust mapping

The complete Go function inventory is:

`GetKillEventChan`, `triggerKillEventLocked`, `resetKillEventLocked`,
`SendKillSignalWithKillEventReason`, `sendKillSignal`,
`sendKillSignalLocked`, `logKillSignal`, `SendKillSignal`, `GetKillSignal`,
`getKillError`, `FinishResultSet`, `SetFinishFunc`, `ClearFinishFunc`,
`HandleSignal`, `CheckConnectionAlive`, and `Reset`.

The atomic Rust owner is `rust/crates/tidb-util/src/sqlkiller.rs`, exported by
`rust/crates/tidb-util/src/lib.rs`; its downstream owners are
`tidb-util::memory::{action,arbitrator,tracker}`, `tidb-util::servermemorylimit`,
`tidb-executor::mem_quota`, and the server ANALYZE/statistics seams. The
owner's existing connection-registration adaptation remains the native form
of Go's conditional `atomic.Pointer` callback removal; no caller writes the
Go-private callback fields directly.

The Go-master race fix is now represented atomically:

- kill-event state and all waiters share one mutex, so signal CAS, reason,
  trigger, and reset cannot observe mixed generations;
- `SendKillSignal` captures the reason while locked, unlocks before logging,
  and exposes the `beforeLogKillSignal` interleave used by the source test;
- `HandleSignal` reloads signal and reason under the event lock for the memory
  arbitrator status, while liveness and failpoint stores use the same lock;
- `Reset` swaps the signal to zero while holding that lock, runs the
  `afterResetKillSignalSwap` interleave, then clears the event generation;
- dropping pre-reset senders closes their receivers, matching Go's closed
  channel rather than sending a Rust-only one-shot reset token. A triggered
  receiver is signaled and then disconnected, which is permanently ready to
  the cancellation consumer just like a closed Go channel.

The focused Rust regression
`concurrent_reset_keeps_signal_and_event_state_consistent` (compiled under
the `failpoints` feature) ports both Go subcases: reset from the
post-CAS/pre-log hook leaves signal/event state clean, and a signal started
while Reset holds the lock is applied only after reset clears the old state,
retaining its reason and closed event.

## Validation

Profile: Ready. Commands run from the repository root:

- `git ls-tree -r --name-only origin/master -- pkg/util/sqlkiller`, full-file
  reads, declaration inventory, and `git diff HEAD origin/master --
  pkg/util/sqlkiller` — passed; confirmed all three source artifacts and the
  Go-master race-fix delta.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/sqlkiller -count=1` — passed compilation (`[no test files]` on
  this pre-`5af03a2e10` checkout).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked
  -p tidb-util --features failpoints --lib sqlkiller` — passed, including the
  focused regression (1 test).
- The same locked workspace toolchain with
  `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked
  -p tidb-util -p tidb-executor -p tidb-server --all-targets` — passed;
  existing warnings are outside this change.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
  --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go source, import block, Bazel file, or module dependency changed in this
Rust-only batch, so `make bazel_prepare` is not required. The Go-master test
itself was read in full but not executed from an alternate worktree; the
pre-race checkout package compiled successfully.

## Risks and unverified scope

- Correctness: signal/event lock ordering and both deterministic race
  interleaves pass; affected utility/executor/server targets compile.
- Compatibility: the Rust public owner keeps the raw signal and existing
  downstream method surface. Its native receiver reports disconnection after
  the one delivered trigger token, which is the cancellation equivalent of a
  permanently closed Go channel.
- Performance: lock scope now intentionally covers the signal/event state
  transition but logging remains outside the lock, matching Go's fix.
- Not verified locally: executing Go master's newly added test against a
  separate Go-master worktree, non-host platform variants (none are in this
  package), and full TiDB integration tests beyond the affected Rust
  consumers.
