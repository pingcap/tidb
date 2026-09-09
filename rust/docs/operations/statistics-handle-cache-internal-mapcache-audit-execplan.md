# `pkg/statistics/handle/cache/internal/mapcache` parity audit ExecPlan

## Objective

Keep the native map-backed statistics cache aligned with the complete Go
package at the rolling `master` pin. The atomic unit includes its BUILD target
and the full `MapCache` implementation, including cost accounting, copy
semantics, no-op lifecycle methods, and the package's visibility boundary.

## Progress

- [x] Recheck both Go artifacts at current master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; all 151 lines remain
  byte-identical to the historical pin.
- [x] Re-read the complete two-file Rust owner, both native tests, and every
  direct parent-cache consumer.
- [x] Add a two-call discard regression, observe exactly two pre-fix
  diagnostics, remove `#[must_use]` from the direct `NewMapCache` and `Keys`
  counterparts, and verify the regression passes.
- [x] Run the full three-test owner suite and all-target compile.
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

This follow-up uses the Ready profile. No Go, Bazel, Cargo metadata, or module
source changed, so `make bazel_prepare` is not required.

- [x] Focused fail-before/pass-after return-contract regression.
- [x] Full owner test and all-target check pass.
- [x] Current and detached Go package probes pass (`[no test files]`).
- [x] Offline locked Rust owner tests pass (2 tests).
- [x] Crate-scoped clippy with `-D warnings` passes.
- [x] Rust formatting, pinned repository lint, and `git diff --check` pass.
- [x] Commit once for this Go package, rebase/push, and verify the remote SHA.

## Next boundary

Audit `pkg/statistics/handle/cache/internal/lfu` as its own complete package
unit. Preserve the shared `StatsCacheInner` contract while checking every
source artifact and Ristretto-specific ownership seam before any edit.
