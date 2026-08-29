# `pkg/util/sqlexec/mock` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `BUILD.bazel` — one public Go library over the two source files;
- `mock.go` — the zero-sized restricted-executor context key and its exact
  `"__MockRestrictedSQLExecutor"` string identity;
- `restricted_sql_executor_mock.go` — the MockGen output for all three methods
  of `sqlexec.RestrictedSQLExecutor`.

There is no `doc.go`, package test, fixture, benchmark, platform variant, or
generator input in this package. The generated file records its external
MockGen command in its header. The checkout is byte-identical to the pin.

## Rust ownership and integration decision

`rust/crates/tidb-sqlexec-mock` is the distinct test-support package owner. It
does not add a production executor. It preserves the source-defined key type
and string, and implements the generated mock's complete three-method surface
against the package-owned `tidb_sqlexec::RestrictedSqlExecutor` trait.

GoMock's controller, reflection, matchers, and generated recorder are Go
framework mechanics rather than TiDB SQL behavior. The Rust owner therefore
uses a native recorder: each expectation supplies the method callback and
return value; unexpected calls panic; `verify` and drop reject missing calls.
This retains the generated artifact's observable test-double contract without
adding a second SQL interface or a third-party Rust mocking framework. The
statistics consumer's `intest` context-value dispatch belongs to the complete
`pkg/statistics/handle/util` package and is not claimed here.

## Validation

Profile: WIP. This is one atomic support package in the continuing parity
audit, not a repository-wide readiness claim.

- Complete pinned-package diff gate: passed.
- Pinned Go package test: passed; the package has no test files.
- `cargo test --manifest-path rust/Cargo.toml -p tidb-sqlexec-mock`: passed,
  3 tests plus doc tests.
- Scoped `cargo fmt --check` and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all three generated interface calls share the real
  `tidb-sqlexec` argument and result types; no narrowed executor exists.
- Compatibility: cross-method expectations remain unordered, as GoMock does
  unless a test explicitly requests ordering; calls of one method are consumed
  in registration order.
- Performance: this is test-only support; one mutex and queue operation are
  performed per mocked call.
