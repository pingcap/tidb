# `pkg/statistics/handle/cache/metrics` parity audit ExecPlan

## Objective

Inventory the complete cache-metrics leaf and preserve its dependency boundary.
The Go package owns eight exported child handles, but those handles must remain
children of the shared `pkg/metrics` vectors; a private Prometheus registry or
leaf-local vector is not equivalent behavior.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel` and `metrics.go`,
  67 lines total, with no tests, fixtures, generated/platform variants, or
  benchmarks.
- [x] Read the complete Rust leaf owner and its direct LFU/cache consumers.
- [x] Confirm the eight source handles and exact labels (`miss`, `hit`,
  `update`, `del`, `evict`, `reject`, `track`, `capacity`). The obsolete
  label-only carrier is gone.
- [x] Keep the current private-vector crate explicitly as seed evidence only;
  its collector identity is Rust-only and must not be reported as parity.
- [x] Refresh the audit receipt and parent/top-level plans to the current Go
  pin with exact hashes and Ready blocker evidence.

## Validation gate

This is a Ready documentation-only blocker refresh. No Go or Bazel source
changed, so `make bazel_prepare` is not required.

- [x] Current and detached Go package probes pass (`[no test files]`).
- [x] Offline locked Rust leaf test/check and clippy pass.
- [x] Rust formatting, pinned repository lint, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Open boundary

Completion requires the atomic `pkg/metrics` owner, including construction,
registration, reset/gather identity, and all shared collectors. Replacing that
dependency with vectors private to this leaf would preserve labels but change
observable collector identity and is therefore intentionally unclaimed.

## Next boundary

Audit the complete parent `pkg/statistics/handle/cache` package after its LFU
and shared-metrics dependencies are reconciled.
