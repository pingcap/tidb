# `pkg/session` — shared-lock-loss rollback parity receipt

Comparison source: Go `origin/master` at commit
`a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (pulled 2026-09-02). The bounded
behavior is the shared-lock-loss rollback change introduced by Go commit
`94eb995357f34b7bab4889a82f0405797046447d`; later unrelated root-session
changes remain outside this batch.

## Complete direct-package inventory

The root package contains exactly 26 tracked direct artifacts and 20,094 lines:
all production files, tests, `BUILD.bazel`, and `OWNERS` were inventoried
before editing. It has no `doc.go`, fixture/testdata directory, generated Go
source, benchmark-only artifact, or platform variant. Nested packages under
`pkg/session/{metrics,sessionapi,sessmgr,txninfo}` and `pkg/session/test/**`
are separate package boundaries.

| Artifact | Lines | Surface |
| --- | ---: | --- |
| `BUILD.bazel` | 246 | library/test target and dependencies |
| `OWNERS` | 13 | ownership metadata |
| `advisory_locks.go` | 111 | advisory-lock session state |
| `bench_test.go` | 2,143 | benchmarks |
| `bootstrap.go` | 613 | store/domain bootstrap |
| `bootstrap_test.go` | 1,977 | bootstrap tests |
| `contextimpl.go` | 38 | session context adapter |
| `global_init.go` | 79 | global initialization |
| `main_test.go` | 84 | test harness |
| `mock_bootstrap.go` | 221 | mock bootstrap helpers |
| `nontransactional.go` | 873 | nontransactional session paths |
| `session.go` | 6,044 | session lifecycle and statement state |
| `session_nextgen_test.go` | 180 | next-gen session tests |
| `session_test.go` | 310 | session lifecycle tests |
| `starter_bootstrap_file.go` | 694 | starter bootstrap configuration |
| `starter_bootstrap_file_test.go` | 928 | starter bootstrap tests |
| `sync_upgrade.go` | 155 | schema/version upgrade synchronization |
| `testutil.go` | 111 | package test utilities |
| `tidb.go` | 473 | statement execution/autocommit boundary |
| `tidb_test.go` | 589 | statement/session regressions |
| `txn.go` | 766 | transaction control |
| `txnmanager.go` | 410 | transaction manager |
| `upgrade_backfill_test.go` | 500 | upgrade backfill tests |
| `upgrade_def.go` | 2,345 | upgrade definitions |
| `upgrade_run.go` | 122 | upgrade runner |
| `upgrade_test.go` | 69 | upgrade tests |

The package declares 113 top-level test/benchmark functions. The changed
behavior is owned by `tidb.go`/`tidb_test.go`; the remaining inventory is
receipt evidence, not an assertion that unrelated upgrade/bootstrap paths
were rewritten.

## Go behavior and Rust boundary

Go now centralizes the rollback predicate in `shouldRollbackTxnOnError`: a
valid transaction rolls back for `kv.ErrSharedLockLost`, while the existing
deadlock rollback remains restricted to pessimistic transactions. The error is
returned unchanged, rollback duration metrics are recorded, and the log
identifies shared-lock loss separately. `TestSharedLockLostRollsBackTransaction`
covers pessimistic shared-lock loss, optimistic mode mismatch (no rollback
because the transaction is not pessimistic), and the unchanged pessimistic
deadlock path, including transaction state and abort metric assertions.

The Rust session owner (`rust/crates/tidb-session`) has no dependency-closed
wire/driver path that currently produces `kv.ErrSharedLockLost`; its catalog
transaction model does not expose the Go `kv.Transaction` interface or
statement abort metrics. The typed error identity and low-level client
decoding are implemented in the `pkg/store/driver/txn` batch; session-level
rollback wiring remains an explicit Rust boundary until the session/transaction
owner is audited as a complete package.

## Validation

Profile: **Ready** for this bounded package behavior.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/session \
  -run '^TestSharedLockLostRollsBackTransaction$' -count=1 -vet=off
# passed in the clean Go-master validation worktree

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# passed in the clean Go-master validation worktree

make bazel_prepare
# attempted; Bazel is not installed locally

git diff --check
# passed for the package patch
```

The integration worktree's focused Go run is currently blocked by an
unrelated in-progress `pkg/store/copr` limiter migration; the detached
Go-master run above is the coherent package evidence. Rust formatting and the
dependency-closed `tidb-txnkv` owner suite passed in the adjacent package
batch. Not verified here: full root `pkg/session` tests, Bazel analysis, live
TiKV shared-lock races, and Rust session rollback wiring.

## Risks and remaining boundary

- Correctness: only valid transactions are rolled back; optimistic
  transactions retain the source behavior for this storage error, and the
  original deadlock predicate is preserved.
- Compatibility: the new path reuses the existing 9015 catalog identity and
  returns the original error object, so warning/error formatting is unchanged.
- Performance: the predicate is constant-time; the extra log field is emitted
  only on a shared-lock-loss failure.
- Remaining work: audit `pkg/executor` lock-context propagation and the Rust
  session/transaction owner before claiming repository-wide parity.
