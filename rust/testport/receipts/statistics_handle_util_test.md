# `pkg/statistics/handle/util/test` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full and byte-compared
against the pin:

- `BUILD.bazel` — one public Go library over `ctx_matcher.go`, depending on
  TiDB KV request-source constants and the TiKV client context utility;
- `ctx_matcher.go` — the zero-sized `CtxMatcher`, its request-context match,
  and exact diagnostic string.

There is no `doc.go`, package test, fixture, benchmark, generated source/input,
or build/platform variant.

## Rust ownership and integration decision

`rust/crates/tidb-stats-handle-util-test` is the distinct test-support owner.
Its `CtxMatcher::matches` accepts a type-erased value, performs the same strict
request-context type assertion (and therefore panics on a wrong type), extracts
the request source through the vendored TiKV client utility, and compares it
with `internal_` plus the shared KV statistics-foreground constant. `Display`
returns the exact Go matcher description.

The prior `tidb-stats::is_internal_stats_foreground_source` string predicate
did not perform Go's context extraction and was removed together with its two
supplemental tests, which have no counterpart in this test-free Go package.
The main `pkg/statistics/handle/util` owner now builds `StatsCtx` with the same
typed TiKV request-source facility, so the support matcher observes the
ordinary execution context rather than copied metadata.

## Validation

Profile: WIP. This completes one atomic support package in the continuing
repository parity audit, not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed; the package has no test files.
- `cargo check -p tidb-stats-handle-util-test -p tidb-stats-handle-util -p tidb-stats`: passed.
- `cargo test -p tidb-stats-handle-util --features intest,failpoints`: passed.
- Scoped `cargo fmt -p tidb-stats-handle-util-test -p tidb-stats-handle-util -p tidb-stats --check`: passed.
- `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: the matcher reads the real request-local value and retains the
  source type-assertion failure mode; it no longer accepts unverified strings.
- Compatibility: the deleted Rust functions had no Go package API identity;
  future Rust auto-analyze tests should import the distinct support crate.
- Performance: request-source extraction is an in-memory typed-context lookup,
  matching the Go dependency behavior.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
