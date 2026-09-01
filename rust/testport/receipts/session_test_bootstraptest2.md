# `pkg/session/test/bootstraptest2` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 377 lines. Every bootstrap
upgrade test, helper, TestMain/goleak harness, and six-shard/flaky Bazel target
was read before this receipt was written. There is no `doc.go`, fixture or
testdata directory, generated output, platform-specific variant, or generator
input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 27 | `dc889ab54a60039800b6fb6392b698fe20b39cd2` | `edc3e190bd22d459e3bb4eaf1f5b69e51d97c823941e2fa039dcd623c2c277b5` | six-shard flaky upgrade-test target and dependency closure |
| `boot_test.go` | 284 | `5af7a460bd1a84cf64128f2b9b12d3b70ab126d1` | `fe868719a759cd6db33e54afd4f92b52a1eaa55d35341e87b662e64e3892e462` | six versioned bootstrap/DDL upgrade tests, one helper, and SQL/meta assertions |
| `main_test.go` | 66 | `e61dc30b987be6558b71964cc86071b6c0c691d2` | `7efd5ab9867cc57e99022fab0214d7b353f3a863747ff1598a1c01b7242e94d1` | common setup, TiKV failpoints, timezone-independent goleak harness |

`boot_test.go` declares `TestWriteDDLTableVersionToMySQLTiDBWhenUpgradingTo178`,
`TestTiDBUpgradeToVer179`, `testTiDBUpgradeWithDistTask`,
`TestTiDBUpgradeWithDistTaskEnable`, `TestTiDBUpgradeWithDistTaskRunning`,
`TestTiDBUpgradeToVer211`, and `TestTiDBUpgradeToVer212`. Together they cover
version 177/178/198/210 migrations, DDL-table-version persistence, global
variable schema changes, distributed-task states, paused/failed upgrade
logging, and historical table-column repair. `main_test.go`'s `TestMain`
configures common test setup, async-commit safety, client failpoints, and
goleak exclusions, with a one-second shutdown callback for MVCC LevelDB.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all three artifacts.

## Rust ownership and parity status

Rust has partial bootstrap carriers in
`rust/crates/tidb-session/src/tests_session_bootstrap_common_source.rs` and
lower-level owners in `tidb-session`, `tidb-meta`, `tidb-metadef`,
`tidb-exec`, and `tidb-server`. Those cover selected bootstrap table
definitions, metadata/version constants, and first-boot publication. They do
not expose Go's dependency-closed `BootstrapSession` + Domain + mock TiKV + DDL
owner + failpoint + historical-version upgrade runner required by these tests.

The six upgrade tests and TestMain remain explicit `#[ignore]`
go-parity-gap carriers (see `rust/testport/receipts/b146.md`), not empty
placeholders that claim behavior. No Rust-only behavior was found to remove,
and no safe package-local Go behavior can be implemented without duplicating
the session bootstrap, Domain/DDL ownership, metadata persistence, and
failpoint lifecycle. The correct implementation unit is that complete upgrade
pipeline.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no new regression test or package-complete
Ready claim is made. The exact Go-master targeted test was run from a detached
worktree with the required `intest,deadlock` tags:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/session/test/bootstraptest2 \
  -run '^TestTiDBUpgradeToVer212$' -count=1                       # passed
```

The package source/build metadata has no direct `failpoint.` references, so no
failpoint wrapper was required for this targeted run. Rust source, Bazel, and
module files were unchanged; `make bazel_prepare` was not required. Not
verified: the remaining five upgrade tests, six Bazel shards, full bootstrap
suite, live TiKV/etcd upgrade paths, and Rust's ignored carriers. Correctness
risk is concentrated in versioned schema/variable migration ordering and DDL
pause/failpoint choreography; runtime behavior is unchanged by this receipt.

This receipt certifies the bounded `bootstraptest2` inventory and explicit
upgrade-pipeline boundary; it is not a repository-wide parity claim.
