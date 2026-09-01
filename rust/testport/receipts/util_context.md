# `pkg/util/context` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02), unchanged from
extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts (757 textual lines), all read in full:

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

The authority refresh removed the remaining Rust-only `#[must_use]`
diagnostics from `StaticWarnHandler::new`, `StaticWarnHandler::with_handler`,
and the three `PlanCacheTracker` accessors (`save`, `use_cache`, and
`plan_cache_unqualified`). A focused `#[deny(unused_must_use)]` regression
failed with five errors before the change and passes afterward, matching Go's
discardable return values.

`pkg/util/breakpoint` is now a live `ValueStoreContext` consumer. The native
trait value is `Any + Send + Sync` because a Rust session moves between
connection workers; the Go-visible heterogeneous key/value and concrete-type
lookup behavior is unchanged. `tidb-session::Session` is the canonical owner,
and its breakpoint integration is inventoried in `util_breakpoint.md`.

## Validation

Profile: **Ready** for this focused parity fix within the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/context` — passed; no Go package drift.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/context` — passed; no current-branch Go package drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/context` — passed; exactly the five artifacts listed above.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/context -count=1` — passed in current and exact detached latest-master (`/tmp/tidb-go-latest-c605`) worktrees.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test context_contract --offline --locked -- --test-threads=1` — passed; six tests including the five-return discard regression.
- `cargo check -p tidb-util -p tidb-distsql -p tidb-executor -p tidb-exec -p tidb-session -p tidb-expr --lib --locked`
- focused consumer tests for DistSQL, executor, exec, session, and expression
- `cargo test -q -p tidb-util --locked -- --test-threads=1`
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; warning limits and singleton behavior now use fewer
  Rust-only API layers while preserving the source state transitions.
- Compatibility: intentionally removes Rust-only public constants and direct
  construction of implementation types; all repository consumers are updated.
- Performance: unchanged; locking, atomic ordering, warning storage, and
  one-shot behavior retain their existing implementation.
