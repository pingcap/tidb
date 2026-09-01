# `pkg/domain/serverinfo` — Go-master parity audit receipt

Status: complete inventory and one dependency-closed current-master behavior
batch. This receipt does not claim the whole Go package is transcreated; the
remaining package boundaries are listed below.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
exactly five tracked artifacts and 2,295 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 55 | one library and one five-shard, embedded-etcd test target |
| `info.go` | 179 | server-info model, JSON behavior, cloning, and topology conversion |
| `status_endpoint_claim.go` | 294 | leased duplicate-status-endpoint claim state machine |
| `syncer.go` | 612 | registration, sessions, topology, stale cleanup, reads, and loops |
| `syncer_test.go` | 1,155 | six harness/tests plus embedded-etcd and fault-injection support |

There is no package `doc.go`, generated source, platform/build-tag variant,
fixture or testdata directory, benchmark, or nested package. The three
production files contain 49 function/method declarations. The test artifact
contains `TestMain`, `TestTopology`, `TestBuildStatusEndpointClaim`,
`TestStatusEndpointClaim`, `TestCleanupStaleServerAndOwnerInfo`, and
`TestAssumedServerInfoSyncer`, plus 23 local helper methods/functions. Every
source, test, helper, and Bazel dependency entry was read from `origin/master`
before editing Rust.

## Current-master delta implemented

The requested Rust branch predates `status_endpoint_claim.go` and Go's
`ServerInfo.String`. This batch adds both to the ordinary server-info path:

- `tidb-domain::status_endpoint_claim` normalizes literal IP and DNS hosts,
  applies the production `10080` fallback, brackets IPv6, and derives the
  raw URL-safe base64 etcd key. Disabled status reporting, empty hosts, and
  assumed-keyspace syncers skip the claim.
- Registration performs Go's best-effort create, conflict, and same-ID restart
  state machine. A claim conflict or etcd error is warning-only and cannot
  block the server-info PUT. Same-ID reattachment is guarded by the observed
  value and modification revision.
- `tidb-pd-client` now supplies the atomic create-or-observe transaction and a
  revision-guarded delete. Cleanup first verifies owner ID and lease, then
  compares the observed revision, so an old or losing generation cannot delete
  a newer claim.
- Failed registration removes only its own claim and revokes its lease.
  Graceful removal releases the claim before deleting the server-info entry.
- Both serving node startup paths derive claim enablement from
  `report_status`; the no-client and disabled paths retain Go's no-op result.
- `ServerInfo::string` and its `Display` carrier sample the live server-ID
  getter and return the same JSON representation as `Marshal`.

Focused regressions cover the source endpoint normalization matrix, disabled
and assumed syncers, conflict-is-warning-only registration, same-ID restart,
loser/old-lease cleanup safety, and current-server-ID string formatting.

## Remaining package boundaries

The earlier `syncer.go` owner remains incomplete for `NewCrossKSSyncer`,
minimum-start-TS reporting, DDL-owner-key cleanup, and the exact
`concurrency.Session` done/cancellation surface. The source's embedded-etcd
matrix also covers concurrent claim winners, namespaces, transaction fault
injection, parent cancellation, lease-expiry observation, and bounded cleanup;
this checkout has no embedded-etcd Rust harness, so those transport-level
cases were inspected but not claimed from the in-memory state-machine tests.
These boundaries keep this receipt from making an atomic full-package claim.

## Validation

Profile: Ready for this behavior batch; repository-wide parity remains open.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/domain/serverinfo -run 'TestTopology|TestCleanupStaleServerAndOwnerInfo|TestServerInfo'`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain --lib status_endpoint_claim -- --test-threads=1` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain --lib serverinfo::tests -- --test-threads=1` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain --lib serverinfo_syncer::tests -- --test-threads=1` (from `rust/`)
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-domain -p tidb-pd-client -p tidb-server` (from `rust/`)
- `cargo +nightly-2026-08-22 fmt --all -- --check` (from `rust/`)
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`
- `git diff --check`

The selected failpoint-enabled Go tests pass and the harness disables
failpoint instrumentation afterward. The Rust claim, model, and syncer filters
pass 3, 4, and 17 tests respectively; the three-crate owner/consumer check,
formatting, Ready lint, and diff gates pass. The working Go branch predates the
new status-claim artifact, so the current-master embedded-etcd status-claim
tests were not executable in-place. No Go or Bazel file changed; therefore
`make bazel_prepare` is not required.

Risk is concentrated in the unrun live-etcd concurrency/fault matrix. The
implemented path is startup/shutdown-only and adds bounded etcd transactions;
it does not alter SQL compatibility or hot-path performance.
