# `pkg/util/tracing` — complete package transcreation

Pinned Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package inventory is exactly six artifacts: `BUILD.bazel` (38 lines),
`main_test.go` (33), `noop_bench_test.go` (54), `opt_trace.go` (41),
`util.go` (398), and `util_test.go` (151), for 715 lines. It contains two
production files, six top-level source tests, and four benchmarks. It has no
package doc, fixture, generated source, platform/build-tag variant, fuzz
target, example, or ownership file. The checkout package is byte-identical to
the pin.

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

Profile: **Ready**; this is one completed package within the continuing
repository audit, not a repository-wide readiness claim.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/tracing` — passed in the active worktree and in the exact detached Go-master worktree `/tmp/tidb-go-latest-c605`.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/tracing` — empty; all six Go artifacts are unchanged at Go master.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --lib 'tracing::tests' -- --nocapture` — passed; seven tests.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --bench tracing` — passed; all four benchmark carriers compile.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check -- rust/testport/receipts/util_tracing.md rust/docs/operations/util-tracing-audit-execplan.md rust/testport/TESTPORT_EXECPLAN.md` — passed.
- Commit, push, pull, and remote SHA verification are recorded for this receipt refresh.

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
