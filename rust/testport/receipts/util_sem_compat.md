# `pkg/util/sem/compat` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full: `sem.go`,
`testhelper.go`, `compat_test.go`, `sem_integration_test.go`, and
`BUILD.bazel`. They define the six compatibility predicates selecting SEM v1
or v2, test configuration installation, five predicate tests, and the
restricted-SQL integration test. There is no package doc, README, fixture,
benchmark, generated or platform variant, or ownership file. The checkout is
byte-identical to the pin.

## Rust ownership and integration decision

`rust/crates/tidb-util/src/sem_compat.rs` owns the six production wrappers.
Existing session visibility and privilege consumers now call this compatibility
layer instead of bypassing configured SEM v2. Test-only Go helpers are native
private Rust support rather than new public API. The five direct Go tests are
retained, while the integration behavior is exercised at the session's common
statement funnel together with the v2 package integration regression.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --locked 'sem_compat::tests::'` — passed (5 tests).
- `cargo test -p tidb-session --locked 'tests_sem_v2::configured_sem_v2_policy_reaches_statement_hint_and_privilege_gates' -- --exact` — passed.
- `cargo test -p tidb-util --locked` — passed (658 unit tests and all integration/doc tests; 3 ignored helpers).
- `cargo check -p tidb-util -p tidb-executor -p tidb-session -p tidb-server --locked` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.
- `go test ./pkg/util/sem/compat -run '^(TestIsInvisibleSchema|TestIsInvisibleTable|TestIsInvisibleStatusVar|TestIsInvisibleSysVar|TestIsRestrictedPrivilege)$' -count=1` — blocked before this package compiled by the workspace's existing missing `pkg/util/hack.checkMapABI` build selection and gRPC `http2.TrailerPrefix` dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: compatibility dispatch now follows the active SEM version.
- Compatibility: no new public policy surface is introduced.
- Performance: six wrappers add only the same version dispatch as Go.
