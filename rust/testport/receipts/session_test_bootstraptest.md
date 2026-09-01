# `pkg/session/test/bootstraptest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: four tracked artifacts and 2,967 lines in the
comparison source. Every production/test/fixture/generated/platform/build
artifact path under this package was checked before comparing Rust. There is
no `doc.go`, fixture directory, generated output, benchmark, fuzz target, or
platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 46 | `fb586f65661eee0d04db07f2d127790c94528faf` | `6afb6456da231104e02f822bf4c26d16367a3e20f19a7f2d69233335cdf5a8d1` | flaky 45-shard bootstrap/upgrade test target and dependency inventory |
| `main_test.go` | 66 | `dd86e800eb799a586641337290d31e662c7da942` | `a100b8141480c751c3fe9238f5e1234bdc38c993da17e2bb475e4859a3cbfc1d` | TestMain, common-test setup, failpoints, and goleak harness |
| `boot_test.go` | 1,061 | `6084dfe408a576045a3fc46df0e2787e680ccc03` | `d1fc48fa02762d1aee91c41905e83123e83228a3df8837e15ea2da9d624bf3b5` | bootstrap schema, SQL mode, DDL notifier, and historical variable/optimizer upgrade tests |
| `bootstrap_upgrade_test.go` | 1,794 | `a6cbe5f540bbe2f899c22fe1ca3ba540eaeb6493` | `f95f5a1e5de1f0813c53550b3ee452bf611e9c14679fbfe143de40798fb3ada1` | versioned schema migration, DDL pause/resume, BDR, system tables, and upgrade rollback tests |

The inventory contains 61 top-level functions: 51 `Test*` functions (50
runnable tests plus `TestMain`) and ten helpers. `boot_test.go` contains 24
runnable tests, `bootstrap_upgrade_test.go` contains 26, and `main_test.go`
contains the harness. The test/helper names are recorded below so additions
or removals are review-visible:

* `boot_test.go` tests: `TestWriteDDLTableVersionToMySQLTiDB`,
  `TestTiDBHistoryTableConsistent`, `TestBootstrapMaskingPolicyTable`,
  `TestBootstrapOperateViewPrivilege`, `TestBootstrapMaterializedViewSystemTables`,
  `TestANSISQLMode`, `TestStmtSummary`, `TestReferencesPrivilegeOnColumn`,
  `TestTiDBEnablePagingVariable`, `TestDDLTableCreateDDLNotifierTable`,
  `TestIssue17979_1`, `TestIssue17979_2`, `TestIssue20900_2`,
  `TestUpgradeClusteredIndexDefaultValue`, `TestAnalyzeVersionUpgradeFrom300To500`,
  `TestAnalyzeVersionUpgradeRewritesLegacyV1To2`, `TestIndexMergeUpgradeFrom300To540`,
  `TestIndexMergeUpgradeFrom400To540Enable`, `TestIndexMergeUpgradeFrom400To540Disable`,
  `TestTiDBOptRangeMaxSizeWhenUpgrading`, `TestTiDBOptAdvancedJoinHintWhenUpgrading`,
  `TestTiDBCostModelUpgradeFrom300To650`, `TestTiDBCostModelUpgradeFrom610To650`,
  `TestIndexJoinMultiPatternByUpgrade650To840`.
* `boot_test.go` helpers: `match`, `checkOperateViewPrivilegeBootstrapSchema`,
  `checkMaterializedViewBootstrapSchema`, and
  `testIndexMergeUpgradeFrom400To540`.
* `bootstrap_upgrade_test.go` tests: `TestUpgradeVersion83AndVersion84`,
  `TestMysqlTablesWithoutClusteredPK`, `TestUpgradeVersion66`,
  `TestUpgradeVersion74`, `TestUpgradeVersion75`, `TestUpgradeVersionMockLatest`,
  `TestUpgradeVersionWithUpgradeHTTPOp`, `TestUpgradeVersionWithoutUpgradeHTTPOp`,
  `TestUpgradeVersionForPausedJob`, `TestUpgradeVersionForSystemPausedJob`,
  `TestUpgradeVersionForResumeJob`, `TestUpgradeWithPauseDDL`,
  `TestUpgradeWithCrossJoinDisabled`, `TestUpgradeBDRPrimary`,
  `TestUpgradeBDRSecondary`, `TestUpgradeBindInfo`,
  `TestUpgradeVersion280MaskingPolicy`, `TestUpgradeVersion285MaterializedViewBootstrap`,
  `TestUpgradeVersion286OperateViewPrivilege`, `TestUpgradeWithAnalyzeColumnOptions`,
  `TestAnalyzeDistsqlConcurrencyByUpgrade750To850`,
  `TestAutoAnalyzeConcurrencyDefaultOnlyAffectsFreshBootstrap`,
  `TestBootstrapInNextGenInvalidSystemTable`,
  `TestUpgradeVersion256PlanCacheSkipStatsOnBinding`,
  `TestUpgradeVersion284EnableTxnFile`, and
  `TestDefaultAnalyzeBackgroundOnlyAffectsFreshBootstrap`.
* `bootstrap_upgrade_test.go` helpers: `revertVersionAndVariables`,
  `checkDDLJobExecSucc`, `execute`, `startUpgrade`, `finishUpgrade`, and
  `checkTiDBMaskingPolicyTableSchema`.
* `main_test.go` harness: `TestMain`.

## Rust ownership and explicit boundary

Rust has partial source carriers in
`rust/crates/tidb-session/src/tests_session_bootstrap_common_source.rs` and
executable lower-level owners in `tidb-session`, `tidb-meta`,
`tidb-metadef`, `tidb-exec`, and `tidb-server`. Those owners cover selected
bootstrap table definitions, bootstrap-version/BDR/masking-policy metadata,
system-variable definitions, and first-boot publication. The source carriers
for the historical upgrade tests are intentionally ignored because the Rust
workspace does not expose Go's dependency-closed `BootstrapSession` + Domain
+ mock TiKV + DDL owner + failpoint + versioned upgrade runner composition.

Eighteen Go tests (the basic bootstrap/schema checks plus version 280/285/286
and transaction-file migrations) have no direct Rust carrier; adding empty
test names would not implement behavior or provide a regression. No
Rust-only behavior was found to remove, and no safe standalone Go behavior can
be implemented in this test-only package without duplicating the session,
domain, DDL, metadata, and variable-persistence pipelines. The package is
therefore recorded as an explicit SEED/boundary. The neighboring
`pkg/session/test/bootstraptest2` package remains a separate audit unit.

The working branch also carries separate changes relative to `origin/master`
inside these Go test files. They were preserved and not staged by this audit.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed in this
batch, so `make bazel_prepare`, the Ready lint gate, and a new regression test
were not required.

The exact Go-master failpoint-managed package command was run from a detached
`origin/master` worktree. It timed out after 10 minutes while
`TestUpgradeVersionForSystemPausedJob` was still running (exit status 2,
601.691s). The timeout is an existing environment/test-lifecycle boundary;
the stack was blocked in mock-store bootstrap/DDL scheduling rather than a
change from this documentation batch. Failpoints were disabled by the wrapper
during teardown.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/bootstraptest -count=1)
# timed out: TestUpgradeVersionForSystemPausedJob; exit status 2; 601.691s
```

No Rust code changed, so a Rust behavior regression test was not applicable.
Not verified here: Bazel execution, `make lint`, the full repository test
surface, live TiKV upgrades, or the Rust package carrier target. Correctness,
compatibility, and performance behavior remain unchanged because this batch
modifies documentation only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
