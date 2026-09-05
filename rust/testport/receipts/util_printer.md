# `pkg/util/printer` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `printer.go`,
`printer_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, benchmark, generated or platform variant, or ownership file.
The local Go package is byte-identical to the pin.

Production behavior consists of classic/next-generation release rendering,
structured startup identity and configuration logging, textual version
identity, and byte-width ASCII-table rendering. The source tests contain
exactly `TestPrintResult`, `TestGetTiDBInfo`, and `TestPrintTiDBInfo`; the test
main only installs the common TiDB test environment and leak checker.

## Rust ownership and audit result

`rust/crates/tidb-util/src/printer.rs` is the production owner. Like Go, it
reads build identity from `versioninfo`, release/server versions from
`tidb-mysql`, effective configuration and drop checking from `tidb-config`,
and kernel/deploy mode from their process-wide config owners. Callers no longer
pass a Rust-only identity snapshot or serialized configuration. The runtime
compiler string remains package-local, matching Go's private `buildVersion`.
`get_print_result_bytes` is the native representation of Go strings'
arbitrary-byte domain; `get_print_result` is the UTF-8 convenience boundary
used by Rust callers. Both use byte lengths and preserve the source output.

The audit removed the supplemental invalid-byte test, Unicode-width branch,
and separate deploy-mode test, then consolidated validation into the source's
three test cases. The logging test follows Go's compile-time kernel branch and
reads the real process-wide deploy mode rather than fabricating a second
kernel identity inside one binary.

The three source-shaped return APIs also carried explicit Rust-only
`#[must_use]` diagnostics. The focused
`return_values_may_be_ignored_like_go` regression discards `get_tidb_info`,
`get_print_result_bytes`, and `get_print_result` under
`#[deny(unused_must_use)]`: the detached pre-fix owner failed with exactly
three diagnostics, while the corrected printer owner passes.

## Validation

Profile: Ready; this is one completed package in the continuing package-by-
package audit, with the package-scoped Rust checks and repository lint required
for a completion claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/printer` — passed.
- `go test ./pkg/util/versioninfo ./pkg/util/printer -count=1` — `versioninfo` passed (`[no test files]`); `printer` was blocked before package execution by the existing `google.golang.org/grpc/internal/transport` reference to missing `http2.TrailerPrefix`.
- `cargo test --offline --locked -p tidb-util printer::tests --lib` — passed (1 source-mapped unit test).
- `cargo test --offline --locked -p tidb-util --test printer_contract` — passed (1 source-mapped integration test).
- `cargo test --offline --locked -p tidb-util` — 545 unit tests passed, 3 helpers were ignored, and the unrelated `memoryusagealarm::tests::test_if_need_do_record` assertion failed.
- `cargo clippy --offline --locked -p tidb-util --all-targets --no-deps` — passed with existing warnings outside this package.
- `cargo fmt --all --check` — reports only the pre-existing formatting difference in `tidb-datatype/src/mydecimal.rs`, which this checkpoint does not modify.
- `git diff --check` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib printer::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed; the detached pre-fix owner failed with exactly three `unused_must_use` diagnostics.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib printer::tests -- --test-threads=1` — 2 tests passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --test printer_contract -- --test-threads=1` — 1 test passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util -p tidb-session -p tidb-executor -p tidb-server --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (Ready profile).

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: every identity/config field now has Go's process-wide owner;
  cached per-session copies cannot become stale or diverge.
- Compatibility: the Rust-only argument-bearing printer API was intentionally
  removed; SQL and server callers use the source-shaped common path.
- Performance: removes per-connection identity cloning and statement-context
  `Arc` propagation; version display performs the same infrequent global reads
  as Go.
