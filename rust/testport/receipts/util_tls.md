# `pkg/util/tls` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All three pinned artifacts were read in full: `tls.go`, `tls_test.go`, and
`BUILD.bazel`. The package has one production file, one unit test, and one
Bazel library/test pair. It exports one process atomic, one cipher-name support
set, and two naming functions. It has no package doc, README, fixture,
benchmark, generated file, platform variant, test harness, or ownership file.
The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/tls.rs` is now the sole package owner. Rust had two
independent copies of the version and cipher tables in `tls.rs` and
`tlsutil.rs`; the 131-line duplicate and its consumer path were deleted.
Server handshake reporting and status reporting now use the canonical module.

The canonical owner exposes Go's process-wide `RequireSecureTransport`
authority, complete 25-name `SupportCipher` set, exact version fallback, and
exact cipher lookup. The session global-variable hook publishes the atomic on
SQL writes, resets, startup values, cluster loads, and committed-image
replacement. Scratch cluster registries do not publish before commit. Starter
mode reports SQL `ON` while keeping the internal transport gate disabled, as
Go does. Connection admission reads the atomic directly instead of treating a
Rust-only global map as a second authority.

The audit made version constants and the backing cipher table private; removed
Rust-only support predicates, table iterators, `must_use` policy, expanded
module narratives, and two supplemental cipher-table tests; and retained only
the exact six-row `TestVersionName` translation.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/tls` — passed.
- `go test ./pkg/util/tls -count=1` — passed (one source test).
- `cargo test -p tidb-util --lib --locked tls::tests::test_version_name -- --exact` — passed (one source test).
- `cargo test -p tidb-session --lib --locked tests_global_vars::require_secure_transport_can_only_be_enabled_by_a_secure_session -- --exact` — passed.
- Both secure-transport cases in `cargo test -p tidb-server --test configured_user_store_source --locked <case> -- --exact` — passed.
- `cargo test -p tidb-exec --lib --locked status_registry --no-fail-fast` — passed (two TLS status cases).
- `cargo check -p tidb-session -p tidb-server -p tidb-exec --lib --offline` — passed; it regenerated the lock entry for the new internal `tidb-config` edge.
- `rustfmt --edition 2021 --check` on all changed Rust source files and
  `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; TLS naming, cipher membership, process-global secure
  transport state, starter behavior, cluster publication, and login admission
  now share Go's authority and ordering.
- Compatibility: intentionally removes the duplicate `tlsutil` module and
  Rust-only constants/helpers; all repository consumers were migrated.
- Performance: login admission replaces a sysvar lookup/string allocation
  with the same single atomic load Go uses. Naming/table costs are unchanged.
