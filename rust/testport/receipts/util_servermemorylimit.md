# `pkg/util/servermemorylimit` — Go-master parity audit receipt

Status: complete dependency-closed audit against current Go `master`; the
Rust owner now exposes Go's discardable constructor contract.

Go source authority: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

All three Go-master artifacts were read in full: `servermemorylimit.go`,
`servermemorylimit_test.go`, and `BUILD.bazel`. The package contains one
production file, one unit test, and one Bazel library/test pair (375 lines
total). It has no
package doc, README, fixture, benchmark, generated file, platform variant,
test harness, or ownership file. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/servermemorylimit.rs` owns the controller and its
50-entry history ring. The former cache-like injected snapshot, caller-owned
top-tracker slot, public one-tick helper, and SEED/narrowing documentation were
removed. The package now has Go's live 100ms handle, exit signal, session
manager, process-memory reads, global-arbitrator handoff, top-consumer kill
state machine, observation atomics, failpoint, and exact history rows.

The required `pkg/util/memory` authorities are shared process globals in
`tidb-util`: server limit, session minimum, global-arbitration mode, runtime
stats, and the atomic top-session tracker. Tracker consumption publishes the
top session at the same point and with the same comparison as Go. The old
independent server sampler was deleted; the memory-limit handle is the single
Go-shaped runtime cadence.

The public `new_server_memory_limit_handle` constructor carried one explicit
Rust-only `#[must_use]` annotation. The new
`return_values_may_be_ignored_like_go` regression failed before the fix with
one `unused_must_use` error; removing that annotation lets Go-style discarded
construction compile without weakening global lint policy.

Every production SQL-node factory supplies its real process registry. Session
roots are published through `ProcessInfo`; text and prepared direct-TiKV
queries keep their SQL identity active until the lazy result set finishes,
and synchronous writes keep it active for execution. Prepared definitions
retain the original statement text instead of manufacturing a cache-only
identity.

`INFORMATION_SCHEMA.MEMORY_USAGE_OPS_HISTORY` now has Go's exact 12-column
schema and reads the package's global history manager. The private history
record stores only those externally returned values, removing the former fake
20-column `ProcessInfo.ToRow` reconstruction and its non-Go boundary values.

## Validation (Ready profile)

Profile: Ready for this package batch; the repository-wide audit is still
continuing.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/servermemorylimit` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/servermemorylimit -run '^TestMemoryUsageOpsHistory$' -count=1` — passed; the wrapper enabled and disabled failpoints successfully.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/servermemorylimit -count=1` from detached `/tmp/tidb-go-latest-c605` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib servermemorylimit::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact` — passed after the one-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib servermemorylimit::tests::test_memory_usage_ops_history --offline --locked -- --exact` — passed.
- `cargo check -p tidb-util --lib --offline --features failpoints --message-format short` — passed.
- `cargo test -p tidb-session --lib --offline process::tests::running_statement_becomes_info_and_is_cleared_again -- --exact` — passed.
- `cargo test -p tidb-server --test all --offline mysql_client_lifecycle_source::prepared_point_range_keeps_its_two_marker_contract -- --exact` — passed.
- `cargo test -p tidb-server --test all --offline mysql_client_lifecycle_source::a_prepared_write_answers_with_an_ok_packet_carrying_affected_rows -- --exact` — passed with loopback-socket permission.
- `cargo check -p tidb-util -p tidb-session -p tidb-server --lib --offline --message-format short` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
failpoint wrapper was used because the production package contains
`failpoint.Inject`; it restored the source tree's failpoint state on exit.

## Risk

- Correctness: reduced; the controller now reads the same live authorities,
  sees real statement lifetimes, and exposes its history through the same SQL
  table as Go.
- Compatibility: the former public seed-only snapshot/tick API is removed;
  repository consumers use the live handle and shared process authorities.
- Performance: one former duplicate 100ms sampler is removed. Top-consumer
  publication adds Go's atomic comparison only for positive session memory
  consumption while global arbitration is disabled.
