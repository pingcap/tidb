# `pkg/session/test/schematest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 506 lines. Every test
line and BUILD declaration was read before comparing Rust. There is no
production source, `doc.go`, fixture directory, generated output, benchmark,
fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 32 | `9562a5bab6fd11effc70f882b45a2a71aaa72a30` | `ae85433cc0cb5bb4ff1772a6d710ad225bd72ba64696f42343e1f2cff66ef445` | flaky ten-shard test target and package dependency inventory |
| `main_test.go` | 63 | `92c0ad64170822b288e60a9a182d804f7d94705e` | `a046f9393dd1ed421668d8df4cacc428d20a28aeed11cf53fe79809579066145` | TestMain, goleak, failpoint, and async-commit harness |
| `schema_test.go` | 411 | `b7905fdd3b0d6b8bddb8fb2c87376cc78085b3cd` | `263de3313cc2cc11b1fd3fff9da88e8bd76fac6fda390b316109aaa31c1c1f18` | ten schema, chunk execution, transaction-size, and validation tests |

The twelve functions are TestMain, the mock-store helper, and ten tests.
They cover committing a prepared statement across a schema change, retrying
an empty transaction after a schema change, table/index lookup chunk reads,
insert/update/single-table and multi-table delete chunk execution, transaction
size accounting, and recursion protection in global-variable validation.

## Rust ownership and explicit boundary

The source-carrier crate records TestMain and all ten tests as ignored
evidence. Rust
has partial owners for session state and transactions (`tidb-session`), chunk
and recordset execution (`tidb-exec`/`tidb-executor`), and transaction limits
(`tidb-txnkv`). It does not yet provide a dependency-closed equivalent of the
Go schema lease/MDL lifecycle, mock TiKV split and DistSQL execution, the
session `Txn().Size()` observation, or the recursive `GlobalVarsAccessor`
validation seam needed to execute this package faithfully.

No Rust-only behavior was found to remove, and no safe missing behavior can be
implemented in this test-only package. Adding local substitutes would create
second authorities for schema visibility, distributed chunk boundaries,
transaction accounting, or variable validation and could make the carrier
pass without matching Go. This package is therefore recorded as an explicit
SEED/boundary; implementation belongs to coordinated schema, session,
executor, transaction, and variable owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, the Ready lint gate, and a new regression test were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/schematest -count=1)
# passed: pkg/session/test/schematest (9.895s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. Not
verified here: Bazel execution, full Go repository tests, Rust execution of
the ignored source carriers, live TiKV split behavior, or a multi-server
schema lease/MDL cycle. Compatibility and performance risk are unchanged
because this batch modified documentation only.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
