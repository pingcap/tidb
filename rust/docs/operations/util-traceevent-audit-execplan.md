# `pkg/util/traceevent` parity audit ExecPlan

## Objective

Keep the complete root trace-event package aligned with Go's flight-recorder,
client-go adapter, structured-field, sink, and benchmark contracts while
maintaining one live Rust owner.

## Progress

- [x] Re-read all seven current Go-master root artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 2,359 lines, 84 production
  declarations, all adapter/recorder/event tests, both benchmarks, and BUILD
  metadata. The nested `pkg/util/traceevent/test` package remains a separate
  boundary with its own receipt.
- [x] Confirm there are no root package docs, fixtures/testdata, generated
  outputs, platform/build-tag variants, fuzz targets, examples, or ownership
  files.
- [x] Verify the dependency-closed `tidb-util::traceevent` owner retains the
  live vendored client hooks, typed fields/context, trigger truth tables,
  recorder modes, ring-buffer ordering, trace IDs, structured rendering, and
  both source benchmark carriers. Prior Rust-only registry/category/control
  surfaces and supplemental tests remain removed.
- [x] Remove 26 Rust-only `#[must_use]` diagnostics from the complete owner
  surface. The deny-on-discard regression fails with all 26 diagnostics on the
  detached pre-fix owner and passes on the corrected owner.
- [x] Refresh the receipt to current Go master and Ready status; no new Rust
  behavior or duplicate regression carrier was introduced in this audit batch.

## Validation gate

This is a Ready authority refresh. No Go, Bazel, or module file changed, so
`make bazel_prepare` is not required.

- [x] Current and exact detached Go-master `-tags=intest,deadlock` suites pass.
- [x] Thirteen focused Rust traceevent tests pass, including live client-hook
  registration and the discard-contract regression; the benchmark and server
  compile checks are recorded.
- [x] Rust formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Future changes must preserve client-go hook registration, context propagation,
field typing/redaction, category and trigger semantics, cooling-off behavior,
and benchmark coverage. The nested integration-test package and full
cross-platform/runtime validation remain separate boundaries.
