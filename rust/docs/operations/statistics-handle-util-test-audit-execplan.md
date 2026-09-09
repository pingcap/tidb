# `pkg/statistics/handle/util/test` parity audit ExecPlan

## Objective

Keep the complete statistics utility test-support package aligned with Go's
typed request-context matcher, strict assertion behavior, request-source
extraction, and diagnostic text without creating a second statistics owner.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 49 lines across BUILD metadata
  and the complete matcher source.
- [x] Confirm there are no package docs, tests, fixtures, benchmarks, fuzz
  targets, generated inputs, or platform/build variants.
- [x] Re-read the complete `tidb-stats-handle-util-test` crate and its
  dependency/source-context integration. It downcasts the real typed TiKV
  `TraceContext`, preserves the wrong-type panic, extracts the request source,
  and emits Go's exact matcher description. The obsolete string predicate and
  source-absent tests remain deleted.
- [x] Refresh the package receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready support-package authority refresh. No Go, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package probes pass (`[no test
  files]`).
- [x] `tidb-stats-handle-util-test` tests/checks and the root owner/consumer
  gates pass.
- [x] Rust formatting, pinned lint, scoped diff, commit, push, pull, and
  remote SHA checks pass.

## Next boundary

The parent `pkg/statistics/handle/util` package remains the owner of ordinary
statistics execution behavior. This crate should only be extended when Go adds
another support artifact or when a typed request-context contract changes.
