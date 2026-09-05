# `pkg/util/versioninfo` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `versioninfo.go` and
`BUILD.bazel`. There is no package doc, test, benchmark, fixture, generated or
platform source, README, or ownership file. The local Go package is
byte-identical to the pin.

Production behavior is five process-wide strings: build timestamp, git hash,
git branch, edition, and enterprise-extension git hash. Their source defaults
are `None`, `None`, `None`, `Community`, and the empty string. Link flags may
replace them before execution; classic startup may assign a nonempty configured
edition.

## Rust ownership and audit result

`rust/crates/tidb-util/src/versioninfo.rs` owns exactly those values. Build
injection uses the crate build script, and the edition uses a process-wide
`RwLock<String>` because safe Rust cannot expose Go's freely assignable mutable
string variable. `tidb_edition` and `set_tidb_edition` are the native read and
assignment operations.

The audit removed the Rust-only twelve-field `VersionInfo` carrier, its builder
methods, two supplemental tests, and all per-node, per-connection, per-session,
and per-statement propagation. Release/server versions remain with
`tidb-mysql`; compiler identity remains private to `printer`; effective config,
store, drop checking, kernel type, and deploy mode remain with `tidb-config`.
Startup assigns these process-wide owners before printer, handshake, sysvar,
`TIDB_VERSION()`, or server-info consumers read them.

The source-shaped `tidb_edition` getter also carried one explicit Rust-only
`#[must_use]` diagnostic. The focused
`return_values_may_be_ignored_like_go` regression discards it under
`#[deny(unused_must_use)]`: the detached pre-fix owner failed with exactly one
diagnostic, while the corrected versioninfo owner passes.

## Validation

Profile: Ready; this completes one package in the continuing package-by-package
audit, with the package-scoped Rust checks and repository lint required for a
completion claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/versioninfo pkg/util/printer` — passed; both Go packages are byte-identical to the pin.
- `go test ./pkg/util/versioninfo ./pkg/util/printer -count=1` — `versioninfo` passed (`[no test files]`); `printer` was blocked before package execution by the existing `google.golang.org/grpc/internal/transport` reference to missing `http2.TrailerPrefix`.
- `cargo check --offline --locked -p tidb-util` — passed.
- `cargo check --offline --locked -p tidb-session -p tidb-executor` — passed.
- `cargo check --offline --locked -p tidb-server --lib` — passed.
- `cargo test --offline --locked -p tidb-expr tests::builtin_info_json_math_source::version --lib -- --exact` — passed.
- `cargo test --offline --locked -p tidb-expr tests::builtin_info_json_math_source::tidb_version --lib -- --exact` — passed.
- `cargo test --offline --locked -p tidb-session tests_core::session_state::session_variables --lib -- --exact` — passed.
- `cargo test --offline --locked -p tidb-server --test all node_config_source::version_flag_prints_the_effective_source_identity_without_topology -- --exact --nocapture` — passed.
- `cargo test --offline --locked -p tidb-server --no-run` — passed with existing warnings.
- `cargo clippy --offline --locked -p tidb-util --all-targets --no-deps` — passed with existing warnings outside this package.
- `cargo test --offline --locked -p tidb-util` — 545 unit tests passed, 3 helpers were ignored, and the unrelated `memoryusagealarm::tests::test_if_need_do_record` assertion failed; the focused printer tests below pass.
- `cargo fmt --all --check` — reports only the pre-existing formatting difference in `tidb-datatype/src/mydecimal.rs`, which this checkpoint does not modify.
- `git diff --check` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib versioninfo::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed; the detached pre-fix owner failed with exactly one `unused_must_use` diagnostic.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib versioninfo::tests -- --test-threads=1` — 1 test passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util -p tidb-session -p tidb-executor -p tidb-server --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (Ready profile).

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: removing snapshots prevents identities from differing between
  connections in one process, which Go cannot represent.
- Compatibility: the Rust-only `VersionInfo` API is intentionally removed;
  callers now use the process-global owners matching Go.
- Performance: removes identity allocation/cloning from connection and
  statement setup; infrequent display paths take short read locks.
