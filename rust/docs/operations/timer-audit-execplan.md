# `pkg/timer` parity audit ExecPlan

## Objective

Keep the complete Go `pkg/timer` package and Rust `tidb-timer` owner aligned as
one atomic package claim. Inventory every Go production/test/fixture/generated/
platform/build artifact before edits; record the ownership decision and
validation in `rust/testport/receipts/timer.md`.

## Completed this batch

1. Read and inventoried all 31 Go artifacts under `pkg/timer` (10,028 lines),
   including API/runtime/table-store tests, the 1,030-line integration harness,
   README, and all four Bazel files. No separate fixture, generated, or
   platform-specific file exists.
2. Read the complete Rust `tidb-timer` source/test owner and confirmed the
   only production omission was `tablestore/notifier.go`; the existing
   `tidb-pd-client` etcd watch/lease client is the dependency owner for that
   behavior.
3. Implemented the etcd notifier, Go-compatible JSON codec, watch filtering and
   cancellation, lease/throttle/timeout loop, and source-shaped table-store
   constructor. Added an exact-timeout leased PUT API to `tidb-pd-client`.
4. Added focused codec regressions for Go JSON shape/escaping and malformed
   event filtering. The pre-fix focused target failed to compile because the
   owner was absent; it passes after the implementation. Complete Rust timer
   unit and aggregated integration suites pass.

## Validation gate

- [x] Focused notifier regression passes (2 tests).
- [x] Complete Rust timer library suite passes (11 tests).
- [x] Complete aggregated Rust timer suite passes (48 tests).
- [x] `cargo fmt --all -- --check` passes.
- [x] Ready profile `make lint` passes.
- [ ] Review the staged diff and create one meaningful timer batch commit.
- [ ] Push the batch to `origin/hparser-integration` and force-fetch the remote
  tracking tip.

## Remaining boundaries

The Go embedded-etcd integration test is environment-dependent and remains an
explicit unverified boundary. The repository-wide package loop continues after
this batch; this plan must not be read as a whole-repository completion claim.
