# `internal/mockstore/deadlock` source-artifact audit

This is the atomic completion receipt for client-go's
`internal/mockstore/deadlock` package at pinned commit
`52c1e76cec993571493c81de442bcbef90cdc106`. The audit includes the package's
production graph, complete original unit test, goleak harness, and the concrete
mocktikv consumer that makes the graph observable.

## Immutable source inventory

The package contains exactly three artifacts and 257 lines. Its Git tree is
`cfb8c846bd3335fea3a7d21b9bc8ac62cd43ccbb`.

| Kind | Source artifact | Lines | SHA-256 |
| --- | --- | ---: | --- |
| production | `internal/mockstore/deadlock/deadlock.go` | 151 | `0bf80f7274dc34ad8d22afb87cb52619eb23164fe001ac9659e0cb00547bd241` |
| unit test | `internal/mockstore/deadlock/deadlock_test.go` | 81 | `3cad761aaea07d773a97d54182bbde6809c54837469c9614e8f1fb8e61c2f92e` |
| test harness | `internal/mockstore/deadlock/main_test.go` | 25 | `ce3fa70dd3453cdf2da694d8b32a987480231eaa035274172dc69aee124ac2ff` |

There is no `doc.go`, external test package, fixture, metadata file, package
build file, benchmark, example, generated input/output, build-tag/platform
variant, or `go:generate` directive.

## Production mapping and differential corrections

`unistore/src/deadlock.rs` is now the single reusable implementation. It owns
the mutex-protected transaction-to-ordered-edge map, `DeadlockDetector`, and
`DeadlockError`. `unistore::MockEngine` uses that exact component; the former
private `tikv-client` copy and the engine's separate one-edge map were removed.
The public UniStore export lets other in-process modules reuse the detector
without introducing a dependency on `tikv-client`.

The component preserves every source branch:

- recursive, registration-order traversal while holding one graph lock;
- the key hash from the existing edge that reaches the source transaction;
- registration only after detection succeeds;
- multiple edges per source transaction;
- exact `(wait-for transaction, key hash)` duplicate collapse while distinct
  hashes remain distinct;
- cleanup of all outbound edges, first exact-edge cleanup with empty-list
  removal, and strict-below timestamp expiry;
- source self-edge behavior: the first self-edge is accepted, and a later edge
  reports the existing hash.

The live-consumer audit exposed three real divergences in the previous engine:

1. its `HashMap<u64, (u64, u64)>` overwrote a transaction's earlier wait edge,
   so a later cycle could be missed or report the wrong key hash;
2. source `Commit`, `Rollback`, and deprecated `Cleanup` clean the graph in a
   deferred path even when their storage operation fails, while Rust cleaned
   only successful commit/rollback and never cleaned `Cleanup`;
3. Rust removed graph entries from `ResolveLock` and `BatchResolveLock`, while
   the source does not.

`MockEngine` now calls the shared detector at pessimistic-lock conflict time,
performs source-exact unconditional cleanup for the three terminal APIs, and
retains entries across both range-resolve APIs.

## Original tests and executable integration evidence

The sole ordinary source test, `TestDeadlock`, is ported directly as
`deadlock::tests::source_test_deadlock`. It preserves every assertion: indirect
cycle hash `200`, cleanup, cycle breaking, distinct versus duplicate key
hashes, exact-edge cleanup, and both expiry thresholds. `TestMain` maps to a
detector that owns no worker plus explicitly joined concurrency tests; no
background task can escape the crate.

Additional source-uncovered tests cover synchronized duplicate registration,
multiple-edge registration order, self-edges, and external-crate reuse. Five
live-engine regressions execute the behavior through pessimistic MVCC:

- two simultaneous wait edges and the first edge's exact farmhash;
- cleanup after failed commit;
- cleanup after failed rollback;
- cleanup after a live-lock `Cleanup` error;
- retained graph state after ordinary and batch range resolution.

The same consumer pass also ports the source-uncovered `currentTS == 0`
`Cleanup` branch, which unconditionally expires a lock even when its TTL would
otherwise remain live.

These tests fail against the former shadow map/cleanup behavior and pass with
the shared detector.

## Dependencies and consumers

The package depends only on synchronization, formatting, and integer/hash
values. Rust uses `std` synchronization and the already required `farmhash`
crate at the mocktikv call site. `Result<(), DeadlockError>` is the typed
mapping of Go's nullable `*ErrDeadlock`.

Mechanical source matching finds exactly one direct Go importer:
`internal/mockstore/mocktikv/mvcc_leveldb.go`. It detects conflicts during
pessimistic locking and unconditionally cleans transaction edges from commit,
rollback, and cleanup. The one native consumer is now `unistore::MockEngine`;
`src/mock/mocktikv/rpc.rs` converts its typed deadlock into the exact kvproto
fields. There is no duplicate graph owner.

## Validation

The original package passes ordinary and race tests with pinned Go 1.25.12.
Rust validation uses `nightly-2026-08-22`:

```text
go test ./internal/mockstore/deadlock -count=1
go test -race ./internal/mockstore/deadlock -count=1
cargo test -p unistore
# 32 unit tests + 3 external-consumer tests
cargo test -p tikv-client --lib mock::mocktikv
# 8 adapter tests
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps --document-private-items
cargo test --workspace --doc --all-features
cargo fmt --all -- --check
git diff --check
```

No real-cluster validation applies to this deterministic in-process graph;
wire conversion and transaction behavior remain covered by the complete
mocktikv and transaction receipts.
