# `pkg/domain/serverinfo` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly five tracked artifacts and 2,295 lines: four
production files (including the status-endpoint claim implementation) and one
test file. All production source, test/support code, build metadata, generated
inputs, and platform variants were read in full before editing. There is no
package `doc.go`, fixture directory, `testdata`, benchmark, fuzz target, or
`OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 55 | `290b7078dbcc91e53c10e65cf6a7564bf8d250ee` | `45d293b28432cf3a05bb205bb549967061f3ebf73075d9551eb270807e6cbace` | server-info library and five-shard flaky test target |
| `info.go` | 179 | `95f307646d28415f44efaffe067c0b4715dc5ee8` | `b846f2b3613cd9772c62a5fc216f367ef0de3c1fb4825e8bc5ad3125ba3bfd88` | static/dynamic server and topology information |
| `status_endpoint_claim.go` | 294 | `f073f886be56db97b227ae2919db217de6724475` | `f1b9eaf28c4a08b6ee18f3ebe0f233c9b7c03dea2db2b541f212df0bfe3d6192` | best-effort etcd advertised status-endpoint claims |
| `syncer.go` | 612 | `51d550336f8cc4b6bbb7265384b7d92a177d1d1a` | `01ea2f83265836bfe573a74ff3b973401468bf7f3034e5869e5889519d1623ac` | server-info sessions, cleanup, restart, and topology loops |
| `syncer_test.go` | 1,155 | `af2ec72651ee688e4a9e0ba0a73c3e6ac7fd0976` | `700b67653be53b8c211fe44ab2c200b6f4efb4c49caf2e7aeee6c677266f62f` | topology, stale cleanup, claim, failure, and shutdown regressions |

The production inventory contains 68 declaration lines and the test inventory
contains six top-level test functions (including `TestMain`). The package now
matches Go master byte-for-byte.

## Native integration decision and fix

Server-info synchronization is Go-native infrastructure coupled to etcd
leases, TiDB owner election, topology, status configuration, and domain
lifecycle. Rust's `tidb-dxf` crate has no dependency-closed server-info or
status endpoint owner, so no speculative Rust implementation was introduced.

The branch lacked Go master's endpoint-claim ownership and lease lifecycle.
This batch restores normalized IPv4/IPv6/DNS endpoint keys, conflict-safe
reattachment, namespaced claims, warning-only claim failures, bounded cleanup,
and the `WithoutStatusEndpointClaim` option for non-serving domains. Server
registration now cleans up failed claims, `RemoveServerInfo` removes only its
own claim, `RevokeSession` stops refresh and revokes the lease, and shutdown
checks `exitCh` before restarting a dead session. `ServerInfo.String` and the
five-shard embedded Bazel target are restored as well.

## Validation and risk

Profile: **Ready** for this lifecycle/etcd behavior restoration. Before the
implementation, the new claim regression could not compile because the
canonical claim types and methods were absent. Afterward the focused endpoint
normalization test and complete claim integration test passed with failpoints
enabled and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/domain/serverinfo \
  -run '^TestBuildStatusEndpointClaim$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/domain/serverinfo 0.795s

./tools/check/failpoint-go-test.sh ./pkg/domain/serverinfo \
  -run '^TestStatusEndpointClaim$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/domain/serverinfo 2.883s

./tools/check/failpoint-go-test.sh ./pkg/domain/serverinfo -count=1
# PASS; ok github.com/pingcap/tidb/pkg/domain/serverinfo 3.324s
```

`make lint`, Rust formatting, and `git diff --check` are required Ready gates
and pass for this batch. `make bazel_prepare` is required because a Go
production file was added and the Bazel target/import shape changed; the local
environment has no `bazel` executable, so that gate is recorded as blocked.

The main risk is etcd lease/claim cleanup ordering: claim operations are
best-effort and never block registration, while confirmed cleanup errors are
logged. The integration suite covers conflicts, races, restarts, namespaces,
failed writes, bounded cleanup, cancellation, and lease revocation.

## Outcome

The complete server-info inventory and Go-only boundary are recorded here.
Go-master endpoint claim and lease lifecycle behavior is restored for the
cross-keyspace consumer; the rolling audit continues after publication.
