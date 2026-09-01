# `pkg/session/test/common` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 600 lines. Every session
metadata/prepared-statement test, prepare-dedup-cache regression, helper,
TestMain/goleak harness, and twelve-shard flaky Bazel target was read before
this receipt was written. There is no `doc.go`, fixture or `testdata`
directory, generated output, platform-specific variant, benchmark, fuzz
target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 28 | `c277caf12d56cc60fb08001ccc5054550f14de62` | `a87f9b8ee70108126606cdf6930bb260fd020058376e70351ca529fe125e7f0d` | twelve-shard flaky session test target and dependency closure |
| `common_test.go` | 288 | `1dd2a1589ef520db6d944ee81e7c8e3024e30b54` | `38650ca9491074aebd9381cb4c39ebc27caf1019b55d8eb4904f686297936662` | session lifecycle, prepare, metadata, last-message, query-string, and affected-row tests |
| `main_test.go` | 62 | `b51233b7b930f4a0d6b4dd93ae1119ff35ae8c0b` | `b1af40eb0cd4a36ab3d5c3f07b412f6e9c2f0f7a5efa6807120b198fe94eb24d` | common setup, TiKV failpoints, async-commit settings, and goleak harness |
| `prepare_dedup_cache_test.go` | 222 | `89ec0c09d6c18c7504a97b7c407425415b3b9e89` | `05feaef70f31f79ab66c56c8d01d2f3cc32de25a48fdd3f01036fc095e4ce957` | five prepared-statement dedup-cache lifecycle/schema/database tests |

`common_test.go` declares `TestMiscs`, `TestPrepare`, `TestIndexColumnLength`,
`TestTableInfoMeta`, `TestLastMessage`, `TestQueryString`, and
`TestAffectedRows`. These cover session values and close behavior, native and
SQL prepared statements, index metadata lengths, affected rows/insert IDs,
protocol last-message formatting and `CLIENT_FOUND_ROWS`, and query-string
publication. `prepare_dedup_cache_test.go` declares
`TestPrepareStmtDedupCacheBasic`, `TestPrepareStmtDedupCacheExecute`,
`TestPrepareStmtDedupCacheSchemaChange`,
`TestPrepareStmtDedupCacheIsolatedByDB`, and
`TestPrepareStmtDedupCachePrepareExecuteCloseLoop`; together they cover
PlanCacheStmt reuse, execution, schema invalidation, database isolation, and
prepare/execute/drop lifecycle. `main_test.go` configures common test state,
TiKV failpoints, and goleak exclusions.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all four artifacts.

## Rust ownership and explicit boundary

Rust has source-backed ignored carriers for all seven common tests and all
five prepare-dedup-cache tests in
`tidb-session::tests_session_bootstrap_common_source`. Lower-level Rust
session/executor tests cover individual affected-row and prepared-expression
contracts, but there is no dependency-closed owner for Go's TestKit + Domain +
storage transaction + PlanCacheStmt protocol. The ignored carriers therefore
remain explicit gaps rather than weak substitutes. No Rust-only behavior was
found to remove, and implementing this package locally would duplicate the
session/server ownership and risk diverging on schema versioning, database
scope, protocol state, or storage execution.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/session/test/common \
  -run '^TestMiscs$' -count=1                                      # passed
```

The exact detached Go-master worktree was used. The package source/build
metadata has no direct `failpoint.` calls (the harness only enables TiKV
client failpoints), so no failpoint wrapper was required for this targeted
run. Rust source, Bazel, and module files were unchanged;
`make bazel_prepare` and Ready lint were not required. Not verified: the
remaining common tests, all twelve Bazel shards, external client-protocol
interactions, or full PlanCacheStmt concurrency. Correctness and performance
risk are unchanged because this batch modifies documentation only.

This receipt certifies the bounded common test-package inventory and explicit
ownership boundary; it is not a repository-wide parity claim.
