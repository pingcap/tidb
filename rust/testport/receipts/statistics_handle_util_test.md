# `pkg/statistics/handle/util/test` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package has exactly two artifacts and 49 lines, both read in full from the
detached Go-master worktree before this authority refresh:

- `BUILD.bazel` — 12 lines defining the public `go_library`, its single
  `ctx_matcher.go` source, and the KV/TiKV request-source dependencies;
- `ctx_matcher.go` — 37 lines defining the zero-sized `CtxMatcher`, strict
  `context.Context` match, and exact diagnostic string.

There is no `doc.go`, package test, fixture, benchmark, fuzz target, generated
source/input, or platform/build-tag variant.

## Rust ownership and integration decision

`rust/crates/tidb-stats-handle-util-test` is the distinct test-support owner.
Its `CtxMatcher::matches` accepts a type-erased request context, preserves the
source's strict type assertion (including panic on a wrong type), extracts the
request source through the vendored TiKV utility, and compares it with
`internal_` plus the shared statistics-foreground constant. `Display` returns
the exact Go matcher description.

The former `tidb-stats::is_internal_stats_foreground_source` string predicate
and its two supplemental Rust tests remain removed: they had no Go package
identity and bypassed the context extraction this matcher exists to verify.
The root `pkg/statistics/handle/util` owner now constructs `StatsCtx` with the
same typed TiKV request-source facility, so the support matcher observes the
ordinary execution context.

## Validation

Profile: Ready. This is one atomic support-package authority refresh inside
the continuing repository-wide parity audit, not a whole-repository claim.

- Exact current and detached Go-master package probes passed:
  `go test ./pkg/statistics/handle/util/test -count=1` (`[no test files]`).
- `cargo test -p tidb-stats-handle-util-test`: passed (no source tests).
- `cargo check -p tidb-stats-handle-util-test -p tidb-stats-handle-util -p tidb-stats`: passed.
- The root owner’s Ready package, consumer, server-compile, and pinned lint
  gates pass with this support crate in the workspace.
- Rust formatting, scoped diff hygiene, commit integrity, push, pull, and
  remote SHA verification pass.

No Go, Bazel, or module file changed in this batch, so `make bazel_prepare`
was not required.

## Risk and unverified boundaries

- Correctness: request-source extraction remains typed and strict; arbitrary
  strings cannot satisfy the matcher, and the wrong-type failure is retained.
- Compatibility: the crate remains a test-only support boundary and does not
  duplicate SQL execution or statistics behavior.
- Performance: matching is an in-memory request-source lookup, as in the Go
  TiKV utility.
- The Go package has no executable tests of its own; its behavior is validated
  by the root auto-analyze tests and the Rust typed-context integration.
