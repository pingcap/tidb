# `pkg/session/syssession` — complete package transcreation

Pinned Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly eight artifacts and 3,130 lines: `BUILD.bazel` (57
lines), `main_test.go` (34), `pool.go` (354), `pool_test.go` (433), `session.go`
(603), `session_integration_test.go` (230), `session_test.go` (1,356), and
`session_test_util.go` (63). The production library is `pool.go`, `session.go`,
and the `!codes` support variant `session_test_util.go`; the test surface has
14 session tests, five pool tests, two integration tests, and the
package-wide `TestMain` harness. `BUILD.bazel` records a flaky, 21-shard
target and all source/dependency metadata. There is no `doc.go`, fixture,
benchmark, generated source, platform-specific source, or other
package-local artifact at the pin. Default and `codes` file selection was
verified in both the active and detached checkouts. The checkout package is
byte-identical to the pin.

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

Profile: **Ready**. This completes one atomic Go package inside the
continuing repository parity audit; it is not a repository-wide readiness
claim.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/session/syssession` — empty; all eight Go artifacts are unchanged at Go master.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/session/syssession -count=1` — passed in the active worktree (7.5s) and on the exact detached Go-master worktree `/tmp/tidb-go-latest-c605` after a retry; the target is marked flaky in BUILD metadata. The first detached run exposed the known `resultSetNotClose` pool-size race, while the immediate isolated subtest and full retry passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go list -f '{{.GoFiles}}|{{.TestGoFiles}}|{{.XTestGoFiles}}' ./pkg/session/syssession` and the same command with `-tags=codes` — passed in both checkouts; the `!codes` support variant is selected only in the default build.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-syssession --lib` — passed, 14 tests.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-timer --test all table_store_sql_test` — passed, 8 integration tests.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check -- rust/testport/receipts/session_syssession.md rust/docs/operations/session-syssession-audit-execplan.md rust/testport/TESTPORT_EXECPLAN.md` — passed.
- Commit, push, pull, and remote SHA verification are recorded for this receipt refresh.

No Go or Bazel source changed, so `make bazel_prepare` is not required. The
existing owner validation also passed the focused `tidb-exec`/`tidb-session`/
`tidb-timer`/`tidb-syssession` compile checks; no new regression carrier was
needed because this audit batch changed no production behavior.

## Risk

- Correctness: ownership, callback, panic, transaction, and close transitions
  now reside in one state machine rather than independent policy fragments.
- Compatibility: consumers retain the concrete session-context trait through
  generic types; ordinary and restricted SQL errors remain the same boxed
  error object through pool interface dispatch.
- Performance: idle sessions use a bounded `VecDeque`; acquisition avoids
  allocation when a clean context is available, matching Go's nonblocking
  channel fast path. Locks cover the same state transitions as Go's mutex.
