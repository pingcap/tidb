# `pkg/util/traceevent` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The root package inventory is `BUILD.bazel`, `adapter.go`, `adapter_test.go`,
`flightrecorder.go`, `flightrecorder_test.go`, `traceevent.go`, and
`traceevent_test.go`. It has eleven top-level source tests, six ordered suite
subtests, and two benchmarks. It has no package doc, fixture, generated source,
platform/build-tag variant, fuzz target, example, or ownership file. The
nested `pkg/util/traceevent/test` integration-test package is a separate Go
package and is not included in this root-package claim. The checkout root
package is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/traceevent` owns the package. The audit removed the
disconnected Rust-only `ClientGoTraceRegistry`, category enum, control flags,
public private-helper surface, five supplemental tests, and stale narrowing
documentation. `register_with_client_go` now installs the three handlers in
the real vendored `tikv-client`; ordinary server startup invokes it once.

The client trace field boundary now carries typed zap-compatible scalar,
duration, binary, array, object, and error values. Region-cache fields use the
same count/ranges/regions/locations object shapes and redaction branches as
client-go rather than a debug-string fallback. The shared `Sink` contract now
carries its context through `Record`, so `LogSink`, `MultiSink`, and region end
events preserve Go's context logger behavior. `GenerateTraceID` and dump
trigger checks recover `Trace` through a concrete sink assertion, matching Go,
and HTTP/log recorder construction uses one logged publication path.

The eleven source tests remain, with one additional regression that invokes
the installed live client hooks and verifies trace ID plus structured fields.
Both source benchmarks are executable in `benches/traceevent.rs`.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/traceevent` — passed.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/traceevent` — passed.
- `cargo test -p tidb-util --lib --offline traceevent::tests -- --nocapture` — passed; 12 tests.
- `cargo check -p tidb-util --bench traceevent --offline` — passed.
- `cargo check -p tidb-server --offline` — passed; existing warnings outside this package remain.
- Direct `rustfmt --edition 2021 --check` over every changed Rust source — passed.
- `git diff --check` — passed.

The live-hook regression could not compile against the prior fake registry API;
after the fix it emits through `tikv_client::trace` into the real recorder.
Targeted Clippy was attempted but is blocked before reaching this package by
pre-existing `tidb-mysql` `map_or_identity` and generated `tidb-proto`
`double_must_use` errors. No Go or Bazel file changed, so `make bazel_prepare`
is not required.

## Risk

- Correctness: improved; client events now reach the real recorder with their
  typed fields and context.
- Compatibility: intentional strict-parity change. The fake registry and
  private Go internals are no longer public Rust API, and `Sink::record` now
  receives the source context.
- Performance: the disabled path still exits before allocating an event. The
  enabled path owns fields exactly once and both source benchmark workloads
  compile; no comparative benchmark was run in this WIP checkpoint.
