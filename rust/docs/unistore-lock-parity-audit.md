# unistore lockstore/lockwaiter parity audit (baseline a85e0fd5df)

Audit of the two Go packages this crate claims ported whole:
`pkg/store/mockstore/unistore/lockstore` (arena skiplist) vs
`src/lockstore.rs` (+arena.rs), and `.../util/lockwaiter` vs
`src/lockwaiter.rs`. The crate's other modules exceed the claim and are
out of scope.

## Result: no behavior-breaking divergences

- lockstore: constants/node layout (maxHeight 16, header 16, nexts
  8/level), findGreater/findLess/findSpliceForLevel/findLast incl. the
  forced-descend and head guards, Put/PutWithHint height heuristic and
  stale-hint fast path, replace/delete splice semantics and length
  accounting, randomHeight p=1/4 geometric cap 16, MaxEntrySize formula,
  Get buf-refill, arena alignment/overflow/free-window/grow — all match.
- lockwaiter: sentinels (LOCK_NO_WAIT -1, WaitTimeout -1,
  WakeUpThisWaiter 0, WakeupDelayTimeout 1), channel cap 32, 100 ms
  default delay, oldest-by-startTS grant, non-blocking wakes outside the
  manager lock, deadlock blocking send, match predicate, delayed-wait
  timer shortening with already-fired guard, CleanUp draining — match.

## Aligned this batch (low)

1. `lockwaiter.rs`: the delayed-wait deadline now uses Go's raw signed
   arithmetic — a negative configured `wake-up-delay-duration` keeps the
   original deadline (the port clamped to 0, returning immediately).
2. `lockstore.rs` replace/delete: assert non-null `hint.prev[i]` before
   `node_set_next`, panicking like Go's nil index instead of silently
   writing block 0 (unreachable with valid hints).

## Documented narrowings (already at sites)

- Height RNG is splitmix64 vs Go's seeded lagged-Fibonacci source (same
  distribution); `with_seed` is a Rust-only test aid.
- Atomics-vs-borrow discipline replaces Go's atomic link load/store;
  iterator lives in lockstore/iterator.go -> iterator.rs, outside this
  scope.
- The waiter timer is per-call (Go keeps a persistent timer field); no
  caller waits twice. Equal-startTS waiter order is stable here,
  unstable in Go.

## Validation

- `cargo build -p tidb-unistore` and `cargo test -p tidb-unistore --lib`
  note: the lib TEST target currently fails to compile from a pre-existing
  trait-bound break between the sibling distsql realignment and
  `client::InProcessClient` (verified identical with and without this
  batch's changes); the lib itself compiles clean.
- `cargo fmt`, `git diff --check`, `make lint`.

## Follow-up (same session)

The pre-existing lib-test compile break is resolved: `InProcessClient` now
implements `SynchronousBatchRequestDispatcher` (in-process coprocessor
dispatch; address/forwarding irrelevant, cancellation short-circuits via
`CallerCancelled`), completing the glue the sibling distsql realignment
needed from this crate. `cargo test -p tidb-unistore --lib`: 114 tests
pass — this suite was entirely unrunnable before the impl.
