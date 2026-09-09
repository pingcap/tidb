# `pkg/session/sessmgr` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session manager/process-info
package and record a safe package-atomic Rust ownership boundary. Read every
Go source, test, and build artifact before editing; do not substitute a
minimal process registry for the full session-manager contract.

## Completed this batch

1. Inventoried all three artifacts (392 lines): process and transaction row
   conversion, status and kill helpers, manager/coordinator interfaces, the
   shallow-clone test, and the flaky Bazel target. No fixtures, generated
   outputs, benchmarks, fuzz inputs, or platform variants were omitted.
2. Ran the exact Go-master failpoint-managed package suite; the focused test
   passed in 0.530s and failpoints were disabled during teardown.
3. Compared the complete package with Rust. Rust's process registry is a
   partial owner and lacks the full Go process-info fields, row conversions,
   manager/coordinator lifecycle, and server-control APIs.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the complete inventory, hashes, validation evidence, and
   explicit SEED boundary in `rust/testport/receipts/session_sessmgr.md`.

## Validation gate

- [x] Complete Go source/test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The processlist/kill registry, transaction-info rows, session manager and
infoschema coordinator, resource/memory/CPU snapshots, and server-control
operations remain explicit cross-crate boundaries. The repository package
loop continues after this receipt; this plan does not claim whole-repository
completion.
