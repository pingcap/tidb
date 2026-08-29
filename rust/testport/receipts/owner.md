# `pkg/owner` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, `OWNERS`, `manager.go`, `mock.go`,
`mock_owner_state.go`, `main_test.go`, `manager_test.go`, and `fail_test.go`.
That is three production files, three test/harness files, eleven top-level
source tests, one build artifact, and one ownership artifact. It has no
`doc.go`, fixture, generated source, benchmark, fuzz target, example, or
platform/build-tag production variant. The checkout package is byte-identical
to the pin.

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
test has exactly the eleven source test identities and no supplemental Rust
test.

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
