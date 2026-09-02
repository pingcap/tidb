# `pkg/statistics/handle/logutil` parity audit ExecPlan

## Objective

Keep the complete statistics logger package aligned with Go's base logger,
category field, sampled factory lifetime, window, admission, and error-verbose
contracts while retaining one shared logging backend.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 67 lines across BUILD metadata
  and the complete production source.
- [x] Confirm there are no package docs, tests, fixtures, benchmarks, fuzz
  targets, generated inputs, or build/platform variants.
- [x] Re-read the complete `tidb-stats-handle-logutil` owner and its live
  auto-analyze consumers. All four constructors compose the shared background,
  error-verbose, and sampled factories with source-exact fields and windows;
  no Rust-only behavior is present.
- [x] Refresh the package receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready package authority refresh. No Go, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package probes pass (`[no test
  files]`).
- [x] The Rust owner tests/checks, formatting, pinned lint, and scoped diff
  gates pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Audit `pkg/statistics/handle/internal` as the next complete package. Future
logger changes must preserve the category, base logger choice, shared sampled
state, five-/ten-minute windows, and first-one admission without adding a
statistics-local logging backend.
