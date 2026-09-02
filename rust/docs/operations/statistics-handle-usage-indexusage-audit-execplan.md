# `pkg/statistics/handle/usage/indexusage` parity audit ExecPlan

## Objective

Keep the complete index-usage collector aligned with Go master: index identity,
bucket boundaries, samples and zero-time behavior, pooled map ownership,
global/session/statement collectors, model-driven garbage collection, source
tests, and the parallel benchmark surface.

## Progress

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 568 lines across BUILD metadata,
  the complete production collector, four tests, and the three-case benchmark.
- [x] Confirm there is no package doc, fixture, generated input/output, fuzz
  target, or build/platform variant.
- [x] Re-read the complete Rust owner, Cargo metadata, source tests, benchmark,
  generic collector dependency, `tidb_model::TableInfo` pointer semantics, and
  all live statistics/session/server consumers.
- [x] Compare every production function, test, benchmark, and BUILD contract
  against the pinned source. No missing Go behavior or Rust-only production
  path remains; the native benchmark harness adaptation is documented in the
  receipt.
- [x] Refresh the receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready package authority refresh. No Go, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package tests pass.
- [x] Four owner tests pass, including the 64 × 100,000 concurrent workload.
- [x] The owner benchmark target compiles and the three report-frequency
  cases are present.
- [x] `tidb-stats`, `tidb-session`, and `tidb-server` consumers compile.
- [x] Ready Rust formatting, pinned `make lint`, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Continue with the next unrefreshed statistics usage boundary. Future changes
must preserve wrapping counter merges, the exact seven bucket boundaries,
year-1 zero timestamps, pooled map transfer ownership, nil-pointer GC
semantics, statement query deduplication, and the source benchmark's three
report frequencies.
