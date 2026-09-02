# `pkg/statistics/handle/util` parity audit ExecPlan

## Objective

Keep the complete statistics-handle utility package aligned with Go master's
process tracking, lease, worker/session pool, table lookup, session reset,
transaction, executor, failpoint, timestamp, and index-classification
contracts while maintaining one dependency-closed Rust owner.

## Progress

- [x] Read all seven root Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 927 lines across five
  production files, four tests, and BUILD metadata.
- [x] Confirm the root package has no package doc, fixture, benchmark, fuzz
  target, generated source/input, or platform/build-tag variant. Inventory the
  nested 49-line `util/test` package as a separate ownership boundary.
- [x] Re-read the complete five-module Rust owner, its crate/build metadata,
  all source-derived tests, the `tidb-stats` re-export, and the sole concrete
  server session-context implementation before editing.
- [x] Add regressions that fail against the previous owner for the omitted
  analyze-store batch refresh and the misplaced explicit-context timeout
  failpoint.
- [x] Restore the exact Go-master session reset order, delete the Rust-only
  merge-concurrency reset, move the failpoint to `ExecRowsWithCtx`, and add
  `ExecWithOptsWithCtx` with caller-context forwarding.
- [x] Refresh the receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready package batch inside the continuing repository audit. No Go,
Bazel, or module file changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go suites pass through the mandatory
  failpoint wrapper with pinned Go 1.25.10.
- [x] The focused regressions fail before and pass after the implementation.
- [x] Twenty-one owner tests with `intest,failpoints`, 258 `tidb-stats`
  consumer tests, and a `tidb-server` compile pass.
- [x] Ready Rust formatting, repository lint, scoped diff, and commit checks
  pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Audit the nested `pkg/statistics/handle/util/test` support package independently.
Future root-package changes must preserve the exact ordered session reset,
partial mutation/error behavior, caller context, failpoint boundary, one
owner, and the separation between session synchronization and downstream
merge-worker concurrency policy.
