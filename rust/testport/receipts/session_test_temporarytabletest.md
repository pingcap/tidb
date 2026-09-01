# `pkg/session/test/temporarytabletest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 512 lines. Every test
line and BUILD declaration was read before comparing Rust. There is no
production source, `doc.go`, fixture directory, generated output, benchmark,
fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 27 | `7e1b148d1cf589ec2393cbbd0c07b423ba5bb9e5` | `d82a742c182a70d63e9a36cdbdbc487649ce2ccaf1cfaeea7c381acc5e43420c` | flaky three-shard test target and dependency inventory |
| `main_test.go` | 62 | `b66e3e92c5921d5ed700887df53a7ec80eca73fa` | `b84f2bd93691382b91e1b4bc72547aecc6bf28322b027189dc6f30a36430f7b0` | TestMain, goleak, failpoint, and async-commit harness |
| `temporary_table_test.go` | 423 | `56e67917483e1742518360402b403fab859623a4` | `795899a305c4a4bfc84bf9aa9d3b1fa2db909c298a494c4a8c03b48a5c1557e5` | local temporary-table DML and schema-checker tests |

The package has four functions: TestMain and three tests. `TestLocalTemporaryTableUpdate`
exercises primary/unique point and batch updates, scans, duplicate-key errors,
rollback, commit, and index cleanup. `TestLocalTemporaryTableDelete` covers
point, batch, index, range, rollback, commit, and empty-result deletes. The
`TestSchemaCheckerTempTable` test checks global temporary-table DDL across
sessions, schema changes and truncation, pessimistic reads, and the expected
normal-table schema-change commit errors; it skips when NextGen has read-only
MDL.

## Rust ownership and explicit boundary

The source-carrier crate records the TestMain and schema-checker test as
ignored evidence and contains focused runnable assertions derived from the
local update and delete tests. Rust's `tidb-session` owns local/global
temporary-table catalogs, session overlays, row lifetime, and transaction
cleanup; `tidb-executor` owns temporary-table DDL guards, KV-table access, and
the local/global row filters. Its executable tests cover the core update,
delete, visibility, rollback, and temporary-table DDL contracts.

The exact Go package still depends on the mock TiKV execution matrix (point
and batch readers, index/table scans, duplicate-key and warning behavior) and
the multi-session schema lease/MDL lifecycle. The latter remains an ignored
Rust carrier, and the former is not represented by a dependency-closed
three-shard test target. No Rust-only behavior was found to remove, and no
safe missing behavior can be implemented in this test-only package without
creating a second temporary-table or schema authority. This package is
therefore recorded as an explicit SEED/boundary; remaining parity belongs to
coordinated session, executor, storage, and schema owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/temporarytabletest -count=1)
# passed: pkg/session/test/temporarytabletest (5.470s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. The focused
Rust carrier test command was attempted but could not compile because
`openssl-sys` could not find `pkg-config`/OpenSSL in this environment. Not
verified here: Bazel execution, full Go repository tests, Rust execution of
the ignored schema carrier, or a live multi-server schema lease/MDL cycle.
Compatibility and performance risk are unchanged because this batch modified
documentation only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
