# `pkg/statistics/handle/cache/internal/testutil` parity audit ExecPlan

## Objective

Keep the cache test-support package aligned with Go master as one complete
support unit: its BUILD target, table constructor, optional sketches and
histogram allocation, load status, memory accounting, and append helpers.

## Progress

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

This is a Ready documentation-only support-package refresh. No Go or Bazel
source changed, so `make bazel_prepare` is not required.

- [x] Current and detached Go package probes pass (`[no test files]`).
- [x] Offline locked Rust owner check/tests and clippy pass.
- [x] Rust formatting, pinned repository lint, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Audit `pkg/statistics/handle/cache/metrics` and then reconcile the parent cache
receipt only after the internal LFU external-dependency boundary is resolved.
