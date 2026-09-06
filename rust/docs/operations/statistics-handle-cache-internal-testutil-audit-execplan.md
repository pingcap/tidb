# `pkg/statistics/handle/cache/internal/testutil` parity audit ExecPlan

## Objective

Keep the cache test-support package aligned with Go master as one complete
support unit: its BUILD target, table constructor, optional sketches and
histogram allocation, load status, memory accounting, and append helpers.

## Progress

- [x] Recheck both Go artifacts at current master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; all 109 lines remain
  byte-identical to the historical pin.
- [x] Re-read the complete two-file Rust owner and inventory every LFU,
  map-cache, parent-cache, and benchmark call site.
- [x] Add a focused discard regression, observe exactly one pre-fix
  diagnostic, remove the Rust-only `#[must_use]` annotation from the direct Go
  constructor counterpart, and verify the regression passes.
- [x] Run the full owner test and all-target compile gates.
- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 109 lines, no package tests,
  fixtures, generated inputs/outputs, benchmarks, or platform variants.
- [x] Read the complete Rust owner and every direct LFU/mapcache consumer.
- [x] Confirm constructor parity for negative counts, one-based IDs, optional
  CMS/TopN/histogram payloads, full-load status, and real native memory
  accounting. Confirm append helpers add the next map-length ID with only a
  CMS sketch and leave load status at zero.
- [x] Refresh the package receipt and top-level/parent plans to the current
  Go pin with exact hashes and Ready evidence. No source-vs-owner gap or
  Rust-only production behavior was found.

## Validation gate

This follow-up uses the Ready profile. No Go, Bazel, Cargo metadata, or module
source changed, so `make bazel_prepare` is not required.

- [x] Focused fail-before/pass-after return-contract regression.
- [x] Full owner test and all-target check pass.
- [x] Current and detached Go package probes pass (`[no test files]`).
- [x] Offline locked Rust owner check/tests and clippy pass.
- [x] Rust formatting, pinned repository lint, and `git diff --check` pass.
- [x] Commit once for this Go package, rebase/push, and verify the remote SHA.

## Next boundary

Audit `pkg/statistics/handle/cache/metrics` and then reconcile the parent cache
receipt only after the internal LFU external-dependency boundary is resolved.
