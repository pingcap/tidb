# `pkg/util/etcd` — Go-master parity receipt

Status: dependency-closed delete-operation batch; namespace mutation remains
an explicit integration boundary. This receipt does not claim repository-wide
parity.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly three tracked artifacts, all read in full:

- `etcd.go` (60 lines): the three operation constants,
  `SetEtcdCliByNamespace`, and `DeleteKeyFromEtcd`.
- `etcd_test.go` (51 lines): one embedded-etcd test for namespace-prefixed
  writes and its setup helper.
- `BUILD.bazel` (29 lines): one public library and one short, flaky
  embedded-etcd test target.

There is no `doc.go`, benchmark, fuzz test, fixture/testdata directory,
generated input/output, platform/build-tag variant, nested package, or other
build artifact. The production file has two functions and three constants;
the test file has one `Test*` function and one helper. The Go checkout has no
source delta from the selected `origin/master` revision.

## Rust owners and implemented delta

The transport owner is `rust/crates/tidb-pd-client/src/etcd.rs`, exported by
`tidb-pd-client`, with the server binding in
`rust/crates/tidb-server/src/serverinfo_etcd.rs`. Before this batch,
`EtcdClient::delete` performed one operation using the client's configured
timeout. Go's `DeleteKeyFromEtcd` creates a fresh deadline for every attempt,
retries up to the caller's count, waits 30 ms between failures, and returns
the final error.

The native client now exposes the exact constants, `delete_with_timeout`, and
`delete_with_retry`. The worker carries the call-site timeout through to the
etcd RPC, and retry logging is warning-only. The real server-info adapter now
uses the source's five attempts and one-second deadline from
`pkg/domain/serverinfo`, so stale/current server entries receive the same
bounded cleanup behavior. Existing prefix deletion, watcher, lease, and
transaction operations remain unchanged.

`SetEtcdCliByNamespace` has no Rust caller or dependency-closed owner: the
Rust client does not expose a mutable clientv3 KV/Watcher/Lease namespace
wrapper, and adding an unused prefixing client would fabricate a second
transport path. The Go embedded-etcd namespace test is therefore inspected
and recorded as an integration boundary, not claimed as implemented.

## Focused regressions

`tidb-pd-client` unit tests assert the three Go constants, exercise retry
until a third attempt succeeds, and verify the zero-count no-op behavior of
Go's `errors.Trace(nil)` return. Existing unreachable-endpoint and per-call
timeout tests continue to cover the concrete worker path.

## Validation

Profile: Ready for this batch. Commands run from `rust/` unless noted:

- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-pd-client --lib`
  — 26 passed, one ignored live-PD probe.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 check --offline --locked -p tidb-server` — passed;
  existing warnings only.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go, Bazel, or Go-module files changed, so `make bazel_prepare` is not
required. The embedded-etcd Go test and live Rust PD probe were not run in
this environment.

## Risks and unverified scope

Correctness risk is limited to timeout/retry ordering and final-error
propagation; both are covered by deterministic helper tests and the worker
compile/test path. Compatibility risk is the unimplemented namespace wrapper,
which remains unused by the Rust startup paths. Performance impact is limited
to cleanup failures: retries add the source-mandated 30 ms delay and do not
affect successful deletes or SQL request paths.
