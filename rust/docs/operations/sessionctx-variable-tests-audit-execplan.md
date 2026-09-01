# `pkg/sessionctx/variable/tests` parity audit ExecPlan

## Objective

Audit the nested variable integration tests independently from the variable
root and slow-log child, map every source test to an executable Rust owner,
and preserve failures whose dependencies are not yet portable.

## Completed

- Read all four Go-master artifacts (1,904 lines): the 47-shard BUILD target,
  goleak harness, 18 session tests, and 29 variable-registry/validation tests.
- Ran the exact Go-master scalar registry/validation subset successfully with
  failpoint cleanup. The full package attempt reached the tests but hit the
  existing background `TestHookContext` assertion panic while
  `TestTiDBOptPartialOrderedIndexForTopNSessionAndGlobal` bootstrapped.
- Confirmed Rust's `tidb-session::sysvar` and `tidb-exec` slow-log owners cover
  the dependency-closed registry, validation, native-value, scope/cache,
  dependency-ordering, and formatting leaves. No Rust-only behavior was
  removed and no speculative TestKit/session implementation was added.

## Validation gate

- [x] Complete four-artifact inventory, including absent fixtures,
      generated/platform variants, fuzz/benchmark inputs, and extra targets.
- [x] Focused Go-master failpoint suite passes; failpoints are disabled.
- [x] Rust owner tests, formatting, lint, and diff checks pass (Ready profile).
- [ ] Push this receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote SHAs, and fast-forward pull the explicit branch ref.

## Remaining boundaries

The session behavior tests still require dependency-closed owners for full
TestKit/Domain setup, storage transactions/savepoints, plan cache, hooks,
execution details, and live SessionVars mutation. The full nested Go suite's
bootstrap panic is retained as evidence and should be rechecked when that
environment or owner split changes.
