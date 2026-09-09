# `pkg/session/test/txn` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session transaction test
package and map its lifecycle, retry, conflict, timestamp, read-only, and
membuffer semantics to existing Rust owners without inventing a duplicate
transaction client or storage protocol.

## Completed this batch

1. Inventoried all three artifacts (622 lines): the TestMain/goleak harness,
   eleven tests, lazy-initialize helper, and eleven-shard flaky BUILD target.
   No production, fixture, generated, benchmark, fuzz, or platform artifact
   was omitted.
2. Ran the exact Go-master failpoint-managed suite; all eleven tests passed in
   72.033s and failpoints were disabled during teardown.
3. Compared every test with Rust. Session autocommit/status and transaction
   primitives exist, with a narrow lazy-state predicate test and typed
   storage retry/membuffer contracts; the full Go mock-TiKV conflict,
   timestamp, read-only, memory, and cleanup choreography is not
   dependency-closed.
4. Added the missing ignored source carrier for Go master’s
   `TestPanicOnRollbackKilledTxn`; no production behavior was invented. Found
   no safe missing behavior to implement and no Rust-only behavior to remove.
   Recorded inventory, hashes, validation evidence, and the explicit SEED
   boundary in `rust/testport/receipts/session_test_txn.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [x] Go-master `TestPanicOnRollbackKilledTxn` has an explicit ignored Rust
  source carrier.
- [x] Ready `rustfmt` and `make lint` checks passed; the targeted Rust carrier
  test was attempted and is blocked only by missing OpenSSL/pkg-config.
- [ ] Fetch remote, create one meaningful batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session transaction status, TiKV
conflict/retry and assertion handling, Oracle timestamp ordering,
read-only privilege checks, UnionScan/membuffer execution, memory accounting,
and killed-transaction cleanup owners. The package loop continues with the
next unrecorded session test package; this plan does not claim whole-repo
completion.
