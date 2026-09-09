# `pkg/sessiontxn` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the root `pkg/sessiontxn`
package and record a safe package-atomic Rust ownership boundary. Read every
Go production, test, support, and Bazel artifact before editing; do not
replace only the public interfaces or test hooks while the transaction
provider stack remains split across Rust crates.

## Completed this batch

1. Inventoried all seven root artifacts (3,113 lines), including the manager
   and provider interfaces, constructors, constant timestamp future,
   failpoint/test counters, all 27 top-level tests, and the Bazel target. No
   fixtures, generated outputs, benchmarks, fuzz inputs, or platform variants
   were omitted. Nested `isolation` and `staleread` packages were kept as
   separate audited units.
2. Ran the exact Go-master package suite through the repository failpoint
   wrapper; all tests passed and failpoints were disabled during teardown.
3. Compared the complete root package with Rust. Rust owns transaction state,
   cluster-session routing, isolation metadata, and KV snapshot traits in
   adjacent crates, but lacks a dependency-closed equivalent of this Go
   manager/provider API and its test-only hooks.
4. Found no safe missing behavior to implement in this interface/support
   package and no Rust-only behavior to remove. Recorded the complete
   inventory, hashes, counts, validation evidence, and explicit SEED boundary
   in `rust/testport/receipts/sessiontxn.md`.

## Validation gate

- [x] Complete Go source/test/support/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The integrated transaction manager/provider implementation, isolation and
stale-read providers, snapshot and infoschema overlays, lock/retry policy,
and Go-specific failpoint/test support remain explicit boundaries across
their own package receipts. The repository package loop continues after this
receipt; this plan does not claim whole-repository completion.
