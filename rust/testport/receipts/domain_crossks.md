# `pkg/domain/crossks` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly eight tracked artifacts and 2,094 lines: five
production/test-support Go files, three test Go files, and one Bazel build file.
All production source, tests, fixtures/support code, and build metadata were
read in full before editing. There is no package `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 97 | `8a0a1844be22d0dc9c7ac88298183bd2d239e2be` | `c9c89bb1a4d54eda8e717989bb270499be5b557ad832f0d5b06bf2dfc168e169` | cross-keyspace library and eight-shard flaky test target |
| `coordinator.go` | 79 | `fafad9223c7e06bb342741ce7e8baab5ce570ba2` | `79e4632ad14e347295a1baa32b3ca1461a07a6cab1a2102cd39736988de644df` | internal-session schema coordinator |
| `cross_ks.go` | 548 | `7cb7315a187605ed344096be083b57e25fbf1d82` | `dfc25f888463e50edda7d2e30d63d793b0f79bce00206d517568fa9cfab396f8` | runtime acquisition, lifecycle, session manager, and cleanup |
| `cross_ks_internal_test.go` | 387 | `c8a84d868e0da52672717025981358d12b651fa9` | `083fd5e06730580cb0017fcc7bd2500ee15cb70b1750ac476e2430b85986772a` | runtime handle, eviction, and manager-close tests |
| `cross_ks_test.go` | 713 | `1bbef630a667b6dcbf24a81062f20a43a38db880` | `013923b9477e6cdbe181637f346cf070afeb49e75fbb475c8d050b7b9326c8ad` | keyspace sessions, server-info cleanup, and DDL submit integration tests |
| `ddl_submit.go` | 202 | `84fdece09e32de1d73108f8525e7a754fc780313` | `b0a25d280b847a90845019461a272f4ed394a27c776ca93b4e427994daf296a6` | cross-keyspace DDL submit-only client |
| `export_test.go` | 42 | `d88c8699b202a8a6be7e086466c865221f27affd` | `518631e31f4bdf14b62dd745564570ed92611722afad7c7faa84c0c75a0d0768` | test accessors for runtime and server-info state |
| `reporter.go` | 26 | `7cfaf282c9311c2ea65c67b9c3c51ffb2fd93ac8` | `6466e0653b4fd55dc90eef282d2e32e8eb596aea06ced1c9618ac6069918c380` | cross-keyspace min-start-TS reporter seam |

The production inventory contains 61 declaration lines and the test inventory
contains eight top-level test functions. The production files and all test
behavior match Go master. Four test-only `keyspacepb.KeyspaceMeta` literals use
the branch's older `Id` field instead of Go master's newer oneof `Keyspace`
field; upgrading `go.mod`/`kvproto` solely for those literals is out of scope.

## Native integration decision and fix

Cross-keyspace management is Go-native infrastructure coupled to TiDB domain
sessions, etcd schema/version sync, server-info leases, and DDL submit-only
semantics. Rust's `tidb-dxf` has no dependency-closed keyspace/domain owner, so
no speculative Rust implementation was added.

The branch had removed server-info cleanup on failed runtime bootstrap and
manager close, disabled the canonical min-job-ID refresher seam, and omitted
the corresponding regression support. This batch restores registration cleanup
and lease revocation, manager-close removal/revocation, the refresher failpoint
control, the server-info test accessor, and the close/bootstrap-failure tests.

## Validation and risk

Profile: **Ready** for this cross-keyspace lifecycle fix. The focused cleanup
regressions and complete failpoint-aware package suite passed with failpoints
enabled and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/domain/crossks \
  -run '^TestManager$/(close removes virtual server info|bootstrap failure removes virtual server info)$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/domain/crossks 1.415s

./tools/check/failpoint-go-test.sh ./pkg/domain/crossks -count=1
# PASS; ok github.com/pingcap/tidb/pkg/domain/crossks 2.508s
```

`make lint`, Rust formatting, and `git diff --check` are required Ready gates
and pass for this batch. `make bazel_prepare` is required because Go test
support changed and the serverinfo dependency/build shape changed; the local
environment has no `bazel` executable, so that gate is recorded as blocked.

The main compatibility risk is cleanup ordering across etcd leases and
cross-keyspace domain shutdown. The focused tests verify both normal close and
partial bootstrap failure, while the complete suite covers concurrent runtime
acquisition, idle eviction, DDL submission, and cancellation.

## Outcome

The complete cross-keyspace inventory and Go-only boundary are recorded here.
The missing server-info lifecycle behavior is restored with regression coverage;
the rolling audit continues after publication.
