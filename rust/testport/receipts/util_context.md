# `pkg/util/context` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full:

- `context.go` — value-store context interface and atomic context IDs;
- `plancache.go` — plan-cache state and one-shot range-fallback warnings;
- `warn.go` — warning levels, JSON, handlers, retention, and test appender;
- `warn_test.go` — the four package tests;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, generated/platform source, fixture, testdata, benchmark,
fuzz target, example, or additional harness.

## Rust ownership and audit result

`rust/crates/tidb-util/src/context/{mod,plancache,warn}.rs` owns the package.
The warning and plan-cache surfaces have live consumers in `tidb-expr`,
`tidb-distsql`, `tidb-executor`, `tidb-exec`, and `tidb-session`.

The audit removed Rust-only public constants for the five saved tracker fields
and the warning limit. The source uses a five-value return directly and
`math.MaxUint16` at each warning-retention site; Rust consumers now likewise
use the `u16` limit without a cross-package API absent from Go.

The source's `IgnoreWarn` is a singleton variable whose concrete type is not
exported. Rust now exposes one non-constructible singleton value instead of a
freely constructible unit value. The source's exported
`NewFuncWarnAppenderForTest` constructor is now represented by a function;
its private implementation type is no longer part of the public Rust API.

The integration contract's Rust-specific typed-key demonstration was
removed. Retained owner and integration tests exercise Go behavior: warning
JSON, warning storage and limits, atomic IDs, plan-cache decisions, callback
panic recovery, and one-shot range fallback.

The stale semantic manifest and historical audit plan that accepted the
removed Rust-only API were deleted.

`pkg/util/breakpoint` is now a live `ValueStoreContext` consumer. The native
trait value is `Any + Send + Sync` because a Rust session moves between
connection workers; the Go-visible heterogeneous key/value and concrete-type
lookup behavior is unchanged. `tidb-session::Session` is the canonical owner,
and its breakpoint integration is inventoried in `util_breakpoint.md`.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/context`
- `cargo test -p tidb-util --lib 'context::' --locked -- --test-threads=1`
- `cargo test -p tidb-util --test context_contract --locked -- --test-threads=1`
- `cargo check -p tidb-util -p tidb-distsql -p tidb-executor -p tidb-exec -p tidb-session -p tidb-expr --lib --locked`
- focused consumer tests for DistSQL, executor, exec, session, and expression
- `cargo test -q -p tidb-util --locked -- --test-threads=1`
- `cargo fmt --all --check`
- `git diff --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; warning limits and singleton behavior now use fewer
  Rust-only API layers while preserving the source state transitions.
- Compatibility: intentionally removes Rust-only public constants and direct
  construction of implementation types; all repository consumers are updated.
- Performance: unchanged; locking, atomic ordering, warning storage, and
  one-shot behavior retain their existing implementation.
