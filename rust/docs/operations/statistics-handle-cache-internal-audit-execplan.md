# `pkg/statistics/handle/cache/internal` parity audit ExecPlan

## Objective

Keep the complete cache-internal interface aligned with Go master: both BUILD
targets, the exact eleven pointer-oriented methods over `*statistics.Table`,
and the visibility/ownership boundary shared by map and LFU implementations.

## Progress

- [x] Read both Go-master artifacts at `c6054025ed4c32ab3672a2a24ea46892714d21ec`:
  67 lines across the dual-target BUILD metadata and complete interface.
- [x] Confirm there are no package tests, fixtures, benchmarks, generated
  inputs/outputs, or build/platform variants.
- [x] Re-read the complete Rust owner and its `tidb-stats::Table` dependency.
  The trait preserves all eleven source methods and intentionally adds no
  source-absent operation or generalized value carrier.
- [x] Refresh the receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready source-test-free interface refresh. No Go, Bazel, or module
file changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package probes pass (`[no test files]`).
- [x] Offline locked Rust check and crate-scoped clippy with `-D warnings` pass.
- [x] Rust formatting, pinned `make lint`, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Audit the nested `cache/internal/lfu` implementation next. Keep this interface
bound to shared actual statistics tables, preserve pointer identity, and keep
map/LFU implementations behind the eleven-method contract.
