# `pkg/sessionctx` parity audit ExecPlan

## Objective

Inventory the complete Go-master root session-context package, including all
interfaces, context-key behavior, snapshot-read validation, TestMain, and race
/flaky BUILD target; compare each contract with Rust owners and record a
dependency-closed boundary.

## Completed this batch

1. Read all four Go-master artifacts (319 lines): the 206-line context
   contract, public BUILD metadata, context-key test, and goleak TestMain.
   There is one production helper, six interface contracts, one test, and no
   fixture/generated/platform/benchmark/fuzz/generator artifact.
2. Ran the exact Go-master package suite; `TestBasicCtxTypeToString` and the
   harness passed in 0.488s.
3. Compared the context keys with Rust's executable `ContextKey` owner and
   source-derived tests, then screened plan-cache, session-state, transaction,
   cursor, advisory-lock, and timestamp-oracle owners. Rust covers selected
   leaves but not the dependency-closed Context composition.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded exact hashes, inventory, validation evidence, and the
   explicit boundary in `rust/testport/receipts/sessionctx_root.md`.

## Validation gate

- [x] Complete Go-master production/test/build inventory and Rust comparison.
- [x] Exact Go-master package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires a session composition root implementing state
serialization, plan caches, transaction futures, storage-oracle validation,
and all embedded subsystem interfaces. The rolling audit continues with the
next unrecorded package; this plan does not claim whole-repository parity or
completion.
