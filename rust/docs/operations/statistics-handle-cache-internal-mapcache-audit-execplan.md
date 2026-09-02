# `pkg/statistics/handle/cache/internal/mapcache` parity audit ExecPlan

## Objective

Keep the native map-backed statistics cache aligned with the complete Go
package at the rolling `master` pin. The atomic unit includes its BUILD target
and the full `MapCache` implementation, including cost accounting, copy
semantics, no-op lifecycle methods, and the package's visibility boundary.

## Progress

- [x] Read all Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: two files and 151 lines, with
  no package docs, tests, fixtures, benchmarks, generated inputs/outputs, or
  platform variants.
- [x] Read the complete Rust crate manifest and implementation, including its
  focused native tests and all direct cache consumers.
- [x] Confirm source parity for shared table identity, signed key handling,
  memory-cost replacement/deletion, independent copy state, iteration
  behavior, and the four source no-op lifecycle methods. No Rust-only
  production operation or source-vs-owner gap remains.
- [x] Refresh the package receipt and top-level test-port plan to the current
  Go pin and Ready validation evidence.

## Validation gate

This is a Ready documentation-only parity refresh. No Go or Bazel source
changed, so `make bazel_prepare` is not required.

- [x] Current and detached Go package probes pass (`[no test files]`).
- [x] Offline locked Rust owner tests pass (2 tests).
- [x] Crate-scoped clippy with `-D warnings` passes.
- [x] Rust formatting, pinned repository lint, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Audit `pkg/statistics/handle/cache/internal/lfu` as its own complete package
unit. Preserve the shared `StatsCacheInner` contract while checking every
source artifact and Ristretto-specific ownership seam before any edit.
