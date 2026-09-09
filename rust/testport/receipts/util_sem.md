# `pkg/util/sem` — complete package transcreation

Pinned Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The root package has exactly four artifacts, all read in full: `sem.go`,
`sem_test.go`, `main_test.go`, and `BUILD.bazel`. They define the SEM enable
state, sysvar-default changes, schema/table/status/sysvar visibility rules,
restricted privilege recognition, five unit tests, and the common test
harness. There is no package doc, README, fixture, benchmark,
generated/platform variant, or ownership file. `pkg/util/sem/compat` and
`pkg/util/sem/v2` are separate Go packages and are not part of this claim. The
checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sem.rs` is the production owner. Its policy tables,
atomic enable state, Go-compatible schema folding, privilege rule, hostname
restore, enable log, and the two process-default sysvar effects were already
wired into the session and server crates. Every inlined policy value was
checked against the pinned `metadef`, `mysql`, and `vardef` sources.

The audit retained exactly the five Go source tests and removed Rust-only
assertions for an extra Unicode fold input, lowercase panic behavior, three
sysvars omitted by the source test, and the supplementary Enable/Disable
state-transition test.

The six source-shaped predicate helpers (`is_enabled`, `is_invisible_schema`,
`is_invisible_table`, `is_invisible_status_var`, `is_invisible_sys_var`, and
`is_restricted_privilege`) also carried explicit Rust-only `#[must_use]`
diagnostics. A focused `return_values_may_be_ignored_like_go` regression
discards all six under `#[deny(unused_must_use)]`: the detached pre-fix owner
failed with exactly six diagnostics, and the corrected owner passes. The
Rust-only `effective_sysvar_default` integration helper intentionally retains
its diagnostic because it has no direct Go package counterpart.

## Validation

Profile: **Ready**; this is one completed package within the continuing
repository audit, not a repository-wide readiness claim.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/sem -count=1` — passed in the active worktree (all five source tests).
- The same pinned command passed in the exact detached Go-master worktree `/tmp/tidb-go-latest-c605`.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/sem` — empty; the four-artifact package is unchanged at Go master.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --lib 'sem::tests'` — passed (5 tests).
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sem::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the six-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sem::tests -- --test-threads=1` — passed; 6 tests including the discard-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib 'vars::tests::sem_enable_and_disable_change_new_session_defaults' -- --exact` — passed (1 focused regression).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `git diff --check -- rust/testport/receipts/util_sem.md rust/docs/operations/sem-audit-execplan.md rust/testport/TESTPORT_EXECPLAN.md` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed as the Ready gate.
- Existing implementation validation also passed `cargo check -p tidb-session --lib --locked`, `cargo check -p tidb-server --lib --locked`, complete `tidb-util`/`tidb-session` suites with the documented unrelated partition errno baseline, all-target clippy, and pinned `make lint`.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; production behavior was already aligned.
- Compatibility: only supplementary Rust test cases are removed.
- Performance: unchanged.
