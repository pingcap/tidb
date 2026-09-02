# `pkg/util/sqlexec/mock` parity audit ExecPlan

## Objective

Keep the complete generated restricted-SQL-executor support package aligned
with Go's context-key identity and three-method mock contract while preserving
the real `tidb-sqlexec` interface as the only executor surface.

## Progress

- [x] Re-read all three current Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 152 lines of BUILD metadata,
  the key type, and the complete MockGen output.
- [x] Confirm there are no package docs, tests, fixtures, benchmarks,
  platform/build-tag variants, or generator inputs beyond the generated file's
  recorded MockGen command.
- [x] Verify `rust/crates/tidb-sqlexec-mock` is the sole test-support owner. It
  preserves the exact key string and all three typed executor methods with a
  native expectation recorder; GoMock controller/reflection mechanics are not
  SQL behavior and no second executor interface was introduced.
- [x] Refresh the receipt to current Go master and Ready status. No Go or
  Bazel file changed and no new Rust production behavior or duplicate
  regression carrier was needed in this docs-only batch.

## Validation gate

This is a Ready authority refresh within the continuing repository audit. No
Go, Bazel, or module file changed, so `make bazel_prepare` is not required.

- [x] Active and exact detached Go package probes pass; both report no source
  tests, and package scans confirm no failpoint or hidden variant surface.
- [x] Three focused Rust owner tests and doc tests pass.
- [x] Rust formatting and scoped diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Future changes must preserve the exact context-key spelling, typed argument and
result forwarding, per-method expectation order, unexpected-call panic, and
missing-expectation verification. The statistics consumer and its context
dispatch remain the separate `pkg/statistics/handle/util` boundary.
