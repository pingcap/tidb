# `pkg/sessiontxn/internal` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the nested
`pkg/sessiontxn/internal` support package and record a safe package-atomic
Rust ownership boundary. Read every Go source and build artifact before
editing; do not port an isolated assertion or snapshot helper while the
session transaction owners remain distributed across crates.

## Completed this batch

1. Inventoried both artifacts (98 lines): the three helper functions in
   `txn.go` and its 16-line Bazel target. No tests, fixtures, generated
   outputs, benchmarks, fuzz inputs, or platform variants were omitted.
2. Compiled the exact Go-master package; it reported `[no test files]`.
   Root `pkg/sessiontxn` tests exercise the helpers through their isolation
   and stale-read callers.
3. Compared the package with Rust. Rust owns assertion parsing,
   transaction-boundary routing, request-source/replica-read propagation,
   and snapshot/interceptor traits in adjacent crates, but lacks one
   dependency-closed equivalent of this Go support package.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the complete inventory, hashes, validation evidence, and
   explicit SEED boundary in `rust/testport/receipts/sessiontxn_internal.md`.

## Validation gate

- [x] Complete Go source/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master package compilation passed (`[no test files]`).
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The transaction manager/provider API, isolation and stale-read providers,
snapshot/interceptor integration, and full TiKV option semantics remain
explicit boundaries in their owning package receipts. The repository
package loop continues after this receipt; this plan does not claim
whole-repository completion.
