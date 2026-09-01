# `pkg/util/traceevent` — complete package transcreation

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The root package has exactly seven tracked artifacts and 2,359 lines. Every
production, test, and BUILD line was read in full before the ownership
decision. It has 84 function declarations, 13 top-level test/benchmark
functions, six ordered suite subtests, and two benchmarks. There is no package
doc, fixture, generated source, platform/build-tag variant, fuzz target,
example, or ownership file.

| Go-master artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 41 | `43020ca97f2913fb5b85814444d6da7aec74c8ce` | library and flaky test targets |
| `adapter.go` | 111 | `1236f7bd6119916f6d58780243e591a996e6dea6` | client-go trace adapter |
| `adapter_test.go` | 229 | `4dcbee29c30c1b13660ca78bec7c0a191c60b835` | adapter tests |
| `flightrecorder.go` | 583 | `15263b571f73d4d7eb909462c5e240dff5f510e7` | flight recorder implementation |
| `flightrecorder_test.go` | 420 | `c3dc804fd1d84501a0a5b48070c7409e30fd388f` | recorder tests and benchmarks |
| `traceevent.go` | 588 | `bb06395600b014fb9b64574f0493f70a8b33b283` | event emission and triggers |
| `traceevent_test.go` | 387 | `34e578db1c9faf849539fef129f4612991b62bb7` | event tests and benchmarks |

The nested `pkg/util/traceevent/test` integration-test package is a separate
two-artifact package and is inventoried in `receipts/util_traceevent_test.md`.
The root checkout is byte-identical to the pinned Go master.

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

Profile: Ready for this receipt refresh; this remains one package boundary in
the continuing repository audit, not a repository-wide readiness claim.

- `git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9 -- pkg/util/traceevent` — PASS.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/traceevent` — PASS; 0.468s.
- `cargo +nightly-2026-08-22 test -p tidb-util --lib traceevent::tests -- --test-threads=1` — PASS; 12 tests.
- `cargo +nightly-2026-08-22 check -p tidb-util --bench traceevent` — PASS.
- `cargo +nightly-2026-08-22 check -p tidb-server` — PASS; existing warnings outside this package remain.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — PASS.
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
