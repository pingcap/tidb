# `pkg/util/sem/v2` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly ten artifacts, all read in full: `config.go`,
`config_test.go`, `restricted_hint.go`, `restricted_hint_test.go`, `sem.go`,
`sem_test.go`, `sql_rule.go`, `sql_rule_test.go`, `testhelper.go`, and
`BUILD.bazel`. They define configuration parsing and validation, version
compatibility, process-wide SEM selection, sysvar default overrides,
restricted SQL and hint rules, restricted privilege policy, and six direct
tests. There is no package doc, README, fixture, benchmark, generated or
platform variant, or ownership file. The checkout is byte-identical to the
pin.

The composed `github.com/coreos/go-semver` parser and the relevant Go
`net/url` and `pkg/objstore.IsLocal` behavior were also read before the Rust
implementation was changed.

## Rust ownership and integration decision

`rust/crates/tidb-util/src/sem_v2/` owns the policy implementation. The audit
removed public Rust-only internals and supplementary tests, and retained the
six Go source tests. Rust-native boundary traits represent Go's session
variable registry and AST interfaces without expanding the observable policy.

The complete package requires consumers outside `tidb-util`. The server now
selects SEM v2 when `[security] enable-sem` is true and `sem-config` is
nonempty. Session execution applies the configured restricted-SQL,
read-only-variable, restricted-privilege, and restricted-hint decisions at the
same common boundaries used by fresh and bound statements. The executor maps
the SEM refusal to MySQL error 8132. These integrations cover every direct
production use of `pkg/util/sem/v2` at the pinned revision; they are not a
claim that the wider planner or executor packages are complete.

The follow-up audit removed 22 explicit Rust-only `#[must_use]` diagnostics
from Go-shaped sysvar/release getters, `semImpl` and package predicates, the
configuration builder, and the five exported SQL-rule functions. The focused
`return_values_may_be_ignored_like_go` regression discards every result under
`#[deny(unused_must_use)]`: the detached pre-fix owner failed with exactly 22
diagnostics, while the corrected owner passes. Four annotations remain on
Rust-native AST-view constructors/accessors and private helper lookups that do
not correspond to Go return APIs.

## Validation

Profile: Ready; this is one completed package within the continuing repository
audit, with package-scoped checks and repository lint for this fix.

- `cargo test -p tidb-util --locked 'sem_v2::tests::'` — passed (6 tests).
- Prior package checkpoint: `cargo test -p tidb-session --locked 'tests_sem_v2::configured_sem_v2_policy_reaches_statement_hint_and_privilege_gates' -- --exact` — passed before the later planner regression recorded below.
- `cargo test -p tidb-server --locked --test all 'node_config_source::configured_sem_is_installed_before_startup_resource_admission' -- --exact` — passed.
- `cargo test -p tidb-util --locked` — passed (658 unit tests and all integration/doc tests; 3 ignored helpers).
- `cargo check -p tidb-util -p tidb-executor -p tidb-session -p tidb-server --locked` — passed.
- `cargo test -p tidb-server --locked --no-run` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.
- `go test ./pkg/util/sem/v2 -run '^(TestParseConfig|TestConfigValidate|TestSemMethods|TestEnable|TestSQLRules|TestRestrictedHint)$' -count=1` — blocked before this package compiled by the workspace's existing missing `pkg/util/hack.checkMapABI` build selection and gRPC `http2.TrailerPrefix` dependency mismatch.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sem_v2::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed; the detached pre-fix owner failed with exactly 22 `unused_must_use` diagnostics.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sem_v2::tests -- --test-threads=1` — 7 owner tests passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --test all node_config_source::configured_sem_is_installed_before_startup_resource_admission -- --exact --nocapture` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util -p tidb-session -p tidb-executor -p tidb-server --all-targets` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_sem_v2::configured_sem_v2_policy_reaches_statement_hint_and_privilege_gates -- --exact --nocapture` — reached the isolated child, then failed at the unrelated existing `tidb-planner/src/logical/rule_aggregation_elimination.rs:211` nil-child-schema panic before the SEM assertions.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (Ready profile).

A full parallel `tidb-session` sweep is not usable as a completion gate in
this checkout: it has hundreds of unrelated shared-state and pre-existing
planner failures. The targeted SEM v2 global-state regression runs its body in
an isolated test subprocess, but the current branch now reaches the unrelated
aggregation-elimination nil-schema panic recorded above before the SEM
assertions.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: policy is now wired through its pinned Go production consumers.
- Compatibility: Rust-only public implementation details were removed; native
  dependency-boundary adapters remain.
- Performance: only startup and statement-policy checks are added, matching
  the Go execution points.
- Not verified locally: the session SEM assertions beyond the unrelated
  planner panic; the owner and server paths plus all affected targets compile.
