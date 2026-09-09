# `pkg/util/sem/compat` — Go-master parity audit receipt

Status: complete dependency-closed audit against current Go `master`; the
Rust owner now exposes Go's discardable predicate contract.

Go source authority: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly five artifacts, all read in full: `sem.go`,
`testhelper.go`, `compat_test.go`, `sem_integration_test.go`, and
`BUILD.bazel` (522 lines total). They define the six compatibility predicates selecting SEM v1
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

All six public wrappers carried explicit Rust-only `#[must_use]` annotations.
The new `return_values_may_be_ignored_like_go` regression failed before the
fix with six `unused_must_use` errors; removing those annotations preserves Go's
discardable return behavior without weakening global lint settings.

## Validation (Ready profile)

Profile: Ready for this package batch; the repository-wide audit is still
continuing.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/sem/compat -run '^(TestInvisibleSchema|TestIsInvisibleTable|TestIsRestrictedPrivilege|TestIsInvisibleStatusVar|TestIsInvisibleSysVar|TestRestrictedSQL)$' -count=1` — passed; the wrapper enabled and disabled failpoints around the six selected tests.
- Same failpoint-wrapper command from detached `/tmp/tidb-go-latest-c605` — not run because the wrapper is repository-local.
- A direct detached latest-master `go test ./pkg/util/sem/compat -count=1` was started twice and stopped after two minutes per run because the integration-heavy suite did not terminate on this host; it is not claimed as locally verified.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib sem_compat::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact` — passed after the six-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib sem_compat::tests --offline --locked -- --test-threads=1` — passed, six Rust tests.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
failpoint wrapper was used because `sem_integration_test.go` calls
`testfailpoint.EnableCall`; it restored the source tree's failpoint state on
exit.

## Risk

- Correctness: compatibility dispatch now follows the active SEM version.
- Compatibility: no new public policy surface is introduced.
- Performance: six wrappers add only the same version dispatch as Go.
