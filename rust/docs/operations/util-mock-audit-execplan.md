# `pkg/util/mock` parity audit ExecPlan

## Objective

Keep the complete Go-master mock infrastructure boundary current while
preserving its Go-only testkit contract and avoiding a speculative shared Rust
mock framework.

## Progress

- [x] Read all ten Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 1,318 lines covering the
  session/plan/expression context, fake transaction, KV client/store, iterator,
  metrics counter, source tests, goleak harness, `!codes` constructor variant,
  and Bazel dependency graph.
- [x] Confirm there is no package `doc.go`, README, nested package, fixture or
  testdata tree, generated output, platform variant, or fuzz target; `fortest.go`
  is the only build-tag variant and `main_test.go` is harness-only.
- [x] Compare the complete interface method sets and tests with Rust's
  crate-local SQL-executor, session, timer, TiKV, and statistics mocks. They are
  trait-specific and do not provide a dependency-closed equivalent of Go's
  cross-package `sessionctx.Context`/`kv.Storage` test double.
- [x] Keep the boundary explicitly unclaimed as Go-only test infrastructure:
  no Rust-only production behavior was found, no missing Go behavior is safe to
  implement in isolation, and no duplicate regression carrier was added.

## Validation gate

This is a docs-only Ready authority refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and exact detached Go-master default package tests pass.
- [x] Default package file selection and the `codes` build-tag probe pass in
  both worktrees, confirming the `!codes` constructor boundary.
- [x] Rust ownership search and boundary review complete.
- [x] Ready formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Porting this package requires the complete Go sessionctx/planctx/expression,
infoschema, KV storage/client, iterator, and metrics testkit stack plus every
dependent Go test suite. Do not replace it with one crate-local Rust mock or
introduce production behavior solely to make a test double compile.
