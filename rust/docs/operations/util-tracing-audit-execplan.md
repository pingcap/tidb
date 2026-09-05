# `pkg/util/tracing` parity audit ExecPlan

## Objective

Keep the complete root tracing package aligned with Go's context, span,
category, event, region, CE-deduplication, and benchmark contracts while
maintaining one dependency-closed Rust owner.

## Progress

- [x] (2026-09-06) Removed all 32 Rust-only `#[must_use]` diagnostics from
  Go-shaped tracing APIs, retaining no Rust-only return contract. The complete
  discard regression failed with exactly 32 diagnostics on the detached
  pre-fix owner and passes with eight tracing tests and the benchmark compile
  gate. Formatting, repository lint, and diff hygiene pass; the package commit
  is ready for the required remote push and SHA verification.

- [x] Re-read all six current Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 715 lines comprising
  `BUILD.bazel`, two production files, the package test harness, six source
  tests, and four benchmarks.
- [x] Confirm there are no package docs, fixtures, generated outputs,
  platform/build-tag variants, fuzz targets, examples, or ownership files.
- [x] Verify `rust/crates/tidb-util/src/tracing.rs` and
  `rust/crates/tidb-util/benches/tracing.rs` retain the dependency-closed
  implementation: shared span-handle completion and baggage, the distinct
  global/parent tracer paths, open phase strings, pointer-preserving CE
  deduplication, empty `OptimizeTracer`, private trace-ID context state,
  region sink events, source-derived tests, and all four benchmark carriers.
  The prior supplemental Rust-only APIs/tests remain removed.
- [x] Refresh the receipt to current Go master and Ready status. No Go or
  Bazel file changed and no new Rust behavior or duplicate regression carrier
  was introduced in this audit batch.

## Validation gate

This is a Ready authority refresh within the continuing repository audit. No
Go, Bazel, or module file changed, so `make bazel_prepare` is not required.

- [x] Active and exact detached Go-master `-tags=intest,deadlock` suites pass.
- [x] Seven focused Rust tracing tests pass and the tracing benchmark target
  compiles.
- [x] Rust formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Future changes must preserve shared span state, tracer selection, category
gating, open phase values, CE record identity, context propagation, and the
four source benchmark workloads. Downstream traceevent and nested integration
packages remain separate receipt boundaries.
