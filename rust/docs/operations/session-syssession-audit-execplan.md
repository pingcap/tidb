# `pkg/session/syssession` parity audit ExecPlan

## Objective

Keep the complete system-internal session package aligned with Go's
ownership, operation, transaction-cleanliness, registry, proxy, pool, and
force-block-GC contracts while maintaining one dependency-closed Rust owner.

## Progress

- [x] Re-read all eight current Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 3,130 lines across production,
  the `!codes` support variant, the full unit/integration test surface,
  `TestMain`, and BUILD metadata. The target is explicitly flaky and has 21
  shards in BUILD metadata.
- [x] Confirm there are no package docs, fixtures, benchmarks, generated
  outputs, or platform-specific artifacts beyond the `!codes` support build
  variant.
- [x] Verify `rust/crates/tidb-syssession` is the single owner. Its generic
  `SessionContext`, owner transfer state machine, RAII operation guard,
  panic quarantine, transaction checks, registry hooks, ordinary/restricted
  executor proxies, bounded pool, callback cleanup, and force-block-GC retry
  path cover the source behavior. The former executor-local partial reuse and
  pool policies, ignored empty carriers, and timer-local session/pool
  imitation remain removed.
- [x] Refresh the receipt to current Go master and Ready status. No Go or
  Bazel file changed and no new Rust behavior or duplicate regression carrier
  was introduced in this audit batch.

## Validation gate

This is a Ready authority refresh within the continuing repository audit. No
Go, Bazel, or module file changed, so `make bazel_prepare` is not required.

- [x] Active and exact detached Go-master failpoint-wrapped package suites
  pass after the documented flaky retry; default and `codes` file selection
  were checked in both worktrees.
- [x] Fourteen focused `tidb-syssession` tests and eight timer integration
  tests pass.
- [x] Rust formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Future changes must preserve owner identity, operation cleanup on errors and
panics, deferred context close, pending/valid transaction rejection,
registry transfer, proxy error identity, pool capacity normalization,
force-block-GC cancellation, and timer interface dispatch. Timer and
statistics consumers remain downstream boundaries with their own receipts.
