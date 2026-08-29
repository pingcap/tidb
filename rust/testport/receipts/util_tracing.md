# `pkg/util/tracing` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package inventory is `BUILD.bazel`, `main_test.go`, `noop_bench_test.go`,
`opt_trace.go`, `util.go`, and `util_test.go`. It contains two production
files, six top-level source tests, and four benchmarks. It has no package doc,
fixture, generated source, platform/build-tag variant, fuzz target, example,
or ownership file. The checkout package is byte-identical to the pin.

`main_test.go` supplies Go's package-wide test setup and goroutine-leak check;
Rust's test harness has no corresponding process hook or Go goroutines, so it
does not require a production or test artifact. `BUILD.bazel` is represented
by the `tidb-util` crate ownership plus its explicit `tracing` benchmark
target.

## Rust ownership and audit result

`rust/crates/tidb-util/src/tracing.rs` owns the package. The audit removed the
five supplemental Rust-only tests, the cache-like category helper, the
field-less event constructor, the public region inspection hook, and stale
gap documentation. It restored the empty `OptimizeTracer`, the four source
benchmarks, an open phase fallback for arbitrary Go phase strings, and
pointer-preserving CE-trace deduplication using shared records.

Rust's span type is the native replacement for Go's opentracing interface.
Span clones now share baggage and completion state, so finishing two handles
records one underlying span as Go does. `ChildSpanFromContxt` starts its child
through the global tracer, while `StartRegion` uses the parent span's tracer,
matching the two distinct Go paths. Trace IDs are private context state exposed
through the Go-shaped `ExtractTraceID` function rather than a public field.
Go's `runtime/trace.Region` is a runtime profiler integration with no Rust
runtime equivalent; the package-owned opentracing span and event interval are
both retained.

The six source tests remain. One focused regression covers shared span-handle
completion because the prior value-clone implementation emitted the same span
twice. All four source benchmark workloads are executable in
`benches/tracing.rs`.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/tracing` — passed.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/tracing` — passed.
- `cargo test -p tidb-util --lib --offline tracing::tests -- --nocapture` — passed; seven tests.
- `cargo test -p tidb-util --lib --offline traceevent::tests -- --nocapture` — passed; twelve downstream tests.
- `cargo check -p tidb-util --bench tracing --offline` — passed.
- `cargo check -p tidb-server --offline` — passed; existing warnings outside this package remain.
- `rustfmt --edition 2021 --check crates/tidb-util/src/tracing.rs crates/tidb-util/src/traceevent/mod.rs crates/tidb-util/src/traceevent/adapter.rs crates/tidb-util/benches/tracing.rs` — passed.
- `git diff --check` — passed.

The shared-span regression failed before the fix with two recordings and
passed afterward with one. No Go or Bazel file changed, so
`make bazel_prepare` is not required. Targeted Clippy was attempted but is
blocked before reaching this package by pre-existing `tidb-mysql`
`map_or_identity` and generated `tidb-proto` `double_must_use` errors.

## Risk

- Correctness: improved; span aliases can no longer double-record, and child
  tracer selection and CE record identity now match Go.
- Compatibility: intentional strict-parity cleanup removes Rust-only public
  helpers. Phase values accept arbitrary source strings through `Other`.
- Performance: shared span state adds one mutex around baggage and completion,
  matching the synchronization implied by shared Go span handles. The four
  source workloads compile; no comparative benchmark was run in this WIP
  checkpoint.
