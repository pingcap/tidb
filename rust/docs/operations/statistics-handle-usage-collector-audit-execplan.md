# `pkg/statistics/handle/usage/collector` parity audit ExecPlan

## Objective

Keep the complete generic statistics usage collector aligned with Go master,
including bounded normal/high-priority channels, timeout escalation,
nonblocking and synchronous sends, worker priority/drain behavior, once-only
close, and the source test surface.

## Progress

- [x] Recheck the complete three-artifact inventory at current Go master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; no package delta exists.
- [x] Remove the two Rust-only `#[must_use]` diagnostics from the source
  constructor and session-spawn counterparts. The focused regression failed
  with exactly two diagnostics before the edit and passes afterward.

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 289 lines across BUILD metadata,
  the complete production collector, and all three source tests.
- [x] Confirm there is no package doc, fixture, benchmark, fuzz target,
  generated input/output, or build/platform variant.
- [x] Re-read the complete `tidb-stats-handle-usage-collector` owner, its
  source-derived integration test, Cargo metadata, and all
  `usage/indexusage` consumers.
- [x] Compare every production behavior and test contract against the pinned
  source. No missing Go behavior or Rust-only production behavior remains;
  the existing close regression documents the nil `closeCh` source boundary.
- [x] Refresh the receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready package authority refresh. No Go, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package tests pass.
- [x] Four owner tests pass with the locked offline Rust toolchain.
- [x] The updated owner passes five integration tests, its all-target check,
  workspace formatting, repository lint, and diff hygiene.
- [x] Ready Rust formatting, pinned `make lint`, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Continue with the next unrefreshed statistics usage boundary. Future collector
changes must preserve the source's two independent channel capacities,
five-minute timeout, accepted-update timestamp semantics, high-priority
selection, close/drain behavior, and nil close-channel compatibility.
