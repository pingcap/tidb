# `pkg/owner` — complete package transcreation

Historical pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Complete inventory

The pinned package contains `BUILD.bazel`, `OWNERS`, `manager.go`, `mock.go`,
`mock_owner_state.go`, `main_test.go`, `manager_test.go`, and `fail_test.go`.
That is three production files, three test/harness files, eleven top-level
source tests, one build artifact, and one ownership artifact. It has no
`doc.go`, fixture, generated source, benchmark, fuzz target, example, or
platform/build-tag production variant. The checkout package is byte-identical
to the historical pin except for the current ten-line `OWNERS` routing file;
all production, test, and BUILD artifacts remain byte-identical.

`main_test.go` supplies Go's package setup and goroutine leak checker; Rust's
test harness has no Go goroutines or package-level setup hook to reproduce.
`OWNERS` is represented by repository ownership rather than a crate-local
approval file. `BUILD.bazel` maps to the explicit `tidb-owner` Cargo workspace
member and aggregate test target.

## Rust ownership and integration

`rust/crates/tidb-owner` owns the complete package. `lib.rs` contains the real
manager, operation value, listener broadcast, current-owner reads, stale-key
cleanup, force-owner transaction, session lifecycle, source-shaped owner-key
encoding, watch behavior, and distributed lock. `mock.rs` contains the local
store manager and the process-wide `(store ID, owner key)` state. The aggregate
test has the eleven source test identities plus one focused return-contract
regression.

The ordinary production adapter is `tidb-pd-client::EtcdClient`. Its existing
single etcd worker now exposes the MVCC metadata and transactions Go's
`concurrency.Election` needs: create-revision ordered prefix reads,
create-if-absent under a lease, mod-revision CAS preserving the lease, and one
atomic delete-candidates-plus-put operation. Its existing long-lived watcher
now resumes from an explicit revision and retains the client's TLS authority.
The owner manager consumes that watcher rather than polling etcd, so a
delete/recreate interval cannot be missed and an idle owner issues no range
requests.

The campaign key is exactly `<owner-path>/<lease-id-in-lowercase-hex>`. The
first creation revision wins; owner values remain `id` or `id + "_" + op-byte`;
unknown operation bytes remain representable and display as Go's `none`.
Session revocation and lease-not-found refresh the session, normal campaigns
wait without a Rust-only timeout, force-owner retains Go's five-second
per-attempt campaign bound, and breaking the loop preserves the session/key
while cancellation revokes it.

The immediate consumer is pinned Go `pkg/util/workloadrepo`, whose owner-only
table creation, housekeeping, and snapshot scheduling made this package a
required prerequisite. That integration is the next active package and is not
claimed by this receipt.

## Validation

Profile: WIP; this is a package checkpoint inside the continuing parity audit,
not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/owner` — passed.
- `rg -n --fixed-strings -- 'failpoint.' pkg/owner`; corresponding
  `testfailpoint` and Bazel dependency checks — found failpoints, so the
  failpoint-safe runner was required.
- `GOTOOLCHAIN=go1.25.10 ./tools/check/failpoint-go-test.sh pkg/owner -run
  'Test(ForceToBeOwner|Single|SetAndGetOwnerOpValue|GetOwnerOpValueBeforeSet|Cluster|WatchOwner|WatchOwnerAfterDeleteOwnerKey|ImmediatelyCancel|AcquireDistributedLock|ListenersWrapper|FailNewSession)$'
  -count=1` — passed; the wrapper enabled and then restored failpoints.
- `cargo test -p tidb-owner` — passed; eleven tests.
- `cargo test -p tidb-pd-client` — passed outside the local-socket sandbox;
  67 tests passed and one pre-existing live-PD probe remained ignored.
- `cargo clippy -p tidb-owner --lib --no-deps -- -D warnings` — passed; an
  existing vendored `tikv-client` `private_bounds` warning remains outside the
  crate. The broader two-package all-target Clippy command is blocked before
  these crates by generated `tidb-proto` `double_must_use` findings.
- `rustfmt --edition 2021 --check crates/tidb-owner/src/lib.rs
  crates/tidb-owner/src/mock.rs crates/tidb-owner/tests/all.rs
  crates/tidb-owner/tests/manager_source.rs crates/tidb-pd-client/src/etcd.rs
  crates/tidb-pd-client/src/lib.rs` — passed.
- `git diff --check` — passed.

The first reference test attempt under host Go 1.27 failed before the package
at the checkout's Go-version-specific map ABI and gRPC/http2 boundaries. The
same failpoint-safe command passed under the pinned Go 1.25.10 toolchain. The
first Rust regression run exposed a transient owner-flag polling assertion;
the source-shaped watch and lease-replacement assertions pass. No Go or Bazel
file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: improved; Rust now uses one lease-ordered election, revision
  watch, CAS owner-op update, and atomic force-owner mutation rather than any
  node-local owner assumption.
- Compatibility: a new crate/API is additive. Operation values and campaign
  key bytes match Go's shared-cluster format.
- Performance: owner wait/monitoring is stream-driven like Go; only lease
  keepalive and reconnect work remain while idle. No owner hot-path benchmark
  exists in the pinned Go package.
- Not verified locally: the pre-existing ignored Rust live-PD watch probe was
  not run. The production transaction calls compile through `etcd-client` and
  the Go reference suite exercised embedded etcd; Rust source-equivalent tests
  use the package's deterministic store seam.

## Follow-up: discardable owner API returns (2026-09-06)

The complete eight-artifact, 1,883-line Go package was re-read and inventoried
at current `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
It contains three production files, three test/harness files with eleven
ordinary top-level tests plus `TestMain`, one Bazel build file, and one
ownership-policy file. There is no `doc.go`, fixture, generated source,
generator input, example, benchmark, fuzz target, or platform/build-tag
variant. The historical-to-current delta is confined to `OWNERS`; no Go
source or test behavior changed.

The complete Rust owner is the five crate-local artifacts `Cargo.toml`,
`src/lib.rs`, `src/mock.rs`, `tests/all.rs`, and
`tests/manager_source.rs`, together with the shared
`rust/scripts/aggregate-tests.rs` build input and its ephemeral generated
`OUT_DIR/all_tests.rs` module list. Workspace membership and lockfile entries
were inspected and did not require changes.

Go permits callers to discard `OpType.IsSyncedUpgradingState`,
`NewListenersWrapper`, `NewOwnerManager`, `MockGlobalState.OwnerKey`,
`MockGlobalStateSelector.GetOwner` and `IsOwner`, and `NewMockManager`. Rust
imposed `#[must_use]` on all seven direct counterparts. Those annotations were
removed without changing election, watch, lease, listener, serialization, or
mock-state behavior. The focused aggregate regression invokes all seven APIs
under `#[deny(unused_must_use)]`; it failed before the implementation edit
with exactly seven diagnostics and passes afterward.

Seven annotations remain intentionally: `OpType::from_byte` and `as_byte` are
Rust encoding helpers; the three `Context` constructors are the Rust adapter
for an external Go package; `MockManager::global_state` is the Rust access
adapter for Go's exported global; and `mock_owner_op_value` is a private Rust
atomic-load helper. None is a direct callable API declared by Go `pkg/owner`.

Ready validation for this follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-owner --test all source_return_values_may_be_ignored_like_go --offline --locked -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-owner --offline --locked -- --test-threads=1
PASS; 12 aggregate tests passed, 0 failed; unit and doc targets had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-owner --all-targets --offline --locked
PASS; the pre-existing vendored tikv-client private-bounds warning remains outside this crate.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required. The live-PD probe and Go suite were not rerun because this
return-contract-only edit does not change runtime behavior; the deterministic
source-equivalent Rust suite and all-target compile cover the affected owner.
