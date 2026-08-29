# `pkg/session/syssession` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The complete pinned package was read before implementation:

- `pool.go` and `session.go` — all production source;
- `session_test_util.go` — the `!codes` support build variant;
- `pool_test.go`, `session_test.go`, and `session_integration_test.go` — all
  functional tests;
- `main_test.go` — setup and goroutine-leak harness;
- `BUILD.bazel` — production/test sources, dependencies, timeout, flakiness,
  and shard metadata.

There is no `doc.go`, fixture, benchmark, generated source, platform-specific
source, or other package-local artifact at the pin.

## Rust ownership and integration

`rust/crates/tidb-syssession` owns the package. Its `Session` and
`AdvancedSessionPool` retain Go's one internal context, owner identity and
hooks, ownership transfer, operation sequence/in-use counters, thread-unsafe
collision detection, panic quarantine, avoid-reuse flag, deferred close,
pending-transaction rejection, rollback-before-pooling, bounded idle pool,
factory fallback, idempotent close, callback cleanup, internal-session
registry hooks, and force-block-GC retry/cancellation behavior.

Rust uses an RAII operation guard for Go's returned `exit` closure and `defer`.
The guard runs on success, error, and panic; it marks the session avoid-reuse
while unwinding and performs the deferred context close when the final active
operation exits. Context-generic package types preserve the concrete
`sessionctx.Context` capability set through consumers without a second
wrapper or execution path.

The former `tidb-exec` `SessionReuseState` and pool-capacity fragments and
their supplementary tests were removed. They represented only two policies
and explicitly omitted the source state machine. The ignored empty
`tidb-session` syssession carriers were also removed. Timer table storage now
imports the package-owned session and pool directly; its previous local
`SysSession`, `AtomicBool` reuse state, and one-method `SessionPool` imitation
were deleted. The existing upstream-shaped timer test continues to override
the real pool interface's `WithSession` method, preserving Go interface
dispatch and callback-error identity.

The Rust package's 14 tests consolidate the source suite by behavior rather
than reproducing testify scaffolding. Together they cover capacity, factory
and reuse, registry transfer, dirty/pending/valid transaction disposal,
successful/error/panic callback cleanup, pool close, deferred close,
thread-unsafe collisions, panic quarantine, context replacement owner gates,
ordinary/restricted executor proxying, and transfer-hook failure cleanup.
The timer source test supplies the external proxy/pool integration case. Go's
`TestMain` is harness-only; Rust starts no background worker in this package,
and panic/close tests prove deterministic guard cleanup.

## Validation

Profile: WIP. This completes one atomic Go package inside the continuing
repository parity audit; it is not a repository-wide readiness claim.

- Complete pinned-package diff gate: passed.
- The deterministic pinned Go unit suite passed. The complete Go target and
  an exact retry of
  `TestDomainAdvancedSessionPoolPutBackDirtySession/resultSetNotClose` both
  failed in the unchanged pinned Go package because the pool retained one
  session where the test expected zero. The package's Bazel target is marked
  flaky, and `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909
  -- pkg/session/syssession` passed.
- The pinned Go internal-session registry integration test passed.
- `cargo test -p tidb-syssession --lib`: passed, 14 tests.
- `cargo test -p tidb-timer --test all table_store_sql_test`: passed, 8 tests.
- `cargo check -p tidb-exec -p tidb-session -p tidb-timer
  -p tidb-syssession`: passed.
- Scoped `cargo fmt --check` and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: ownership, callback, panic, transaction, and close transitions
  now reside in one state machine rather than independent policy fragments.
- Compatibility: consumers retain the concrete session-context trait through
  generic types; ordinary and restricted SQL errors remain the same boxed
  error object through pool interface dispatch.
- Performance: idle sessions use a bounded `VecDeque`; acquisition avoids
  allocation when a clean context is available, matching Go's nonblocking
  channel fast path. Locks cover the same state transitions as Go's mutex.
