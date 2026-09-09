# `pkg/util/sqlexec/mock` — complete package transcreation

Pinned Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly three artifacts and 152 lines, all read in full:
`BUILD.bazel` (18 lines), `mock.go` (23), and
`restricted_sql_executor_mock.go` (111). BUILD defines one public Go library
over the two source files; `mock.go` owns the zero-sized restricted-executor
context key and exact `"__MockRestrictedSQLExecutor"` identity; the generated
file covers all three methods of `sqlexec.RestrictedSQLExecutor`.

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
`pkg/statistics/handle/util` package and is integrated by the owner recorded in
`statistics_handle_util.md`.

## Validation

Profile: **Ready**. This is one atomic support package in the continuing parity
audit, not a repository-wide readiness claim.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/sqlexec/mock` — empty; all three Go artifacts are unchanged at Go master.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/sqlexec/mock -count=1` — passed in the active worktree and in the exact detached Go-master worktree `/tmp/tidb-go-latest-c605` (`[no test files]`). Failpoint and generated/platform scans found no additional package-local test or variant surface.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-sqlexec-mock` — passed, 3 tests plus doc tests.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check -- rust/testport/receipts/util_sqlexec_mock.md rust/docs/operations/util-sqlexec-mock-audit-execplan.md rust/testport/TESTPORT_EXECPLAN.md` — passed.
- Commit, push, pull, and remote SHA verification are recorded for this receipt refresh.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all three generated interface calls share the real
  `tidb-sqlexec` argument and result types; no narrowed executor exists.
- Compatibility: cross-method expectations remain unordered, as GoMock does
  unless a test explicitly requests ordering; calls of one method are consumed
  in registration order.
- Performance: this is test-only support; one mutex and queue operation are
  performed per mocked call.

## Follow-up: discardable generated API returns (2026-09-06)

The complete three-artifact, 152-line Go package inventory was rechecked at
current `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
It remains byte-identical to the receipt pin: the public Bazel target, context
key source, and full generated MockGen output are the entire package. There
are still no docs, tests, fixtures, benchmarks, generator inputs, or
platform/build-tag variants. The Rust owner remains exactly `Cargo.toml` and
`src/lib.rs`.

Go callers may discard both `NewMockRestrictedSQLExecutor` and `EXPECT`
results. Rust imposed two extra `#[must_use]` diagnostics on their direct
counterparts, `MockRestrictedSqlExecutor::new` and `expect`. The annotations
were removed without changing construction, expectation recording, or drop
verification. The focused in-owner regression calls both APIs under
`#[deny(unused_must_use)]`; before the production edit it failed with exactly
two diagnostics, and after the edit it passes.

Ready validation for this follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-sqlexec-mock --lib tests::generated_constructor_and_expect_result_may_be_ignored_like_go --offline --locked -- --exact --nocapture --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-sqlexec-mock --offline --locked -- --test-threads=1
PASS; 4 unit tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-sqlexec-mock --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source and parity documentation changed. No Go, generated Go,
Bazel, Cargo metadata, or module dependency changed, so `make bazel_prepare`
is not required.
