# `pkg/sessiontxn` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The root package contains seven tracked artifacts and 3,113 lines. Every
production source, root test, and Bazel target was read in full before
comparing the Rust workspace. There is no `doc.go`, fixture directory,
generated output, benchmark, fuzz target, or platform/build-tag variant.
The nested `isolation` and `staleread` directories are separate Go packages;
their complete inventories and receipts are recorded independently.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 58 | `2c55667c660b2b56ff3d9294553b2cfeeb63f468` | `4bbbd6c97c5e6d4dc82a1041f39133ee6a381fd46edf4c2f59dbd9d303bec831` | session transaction library and test target |
| `failpoint.go` | 181 | `2c3dcb23b210ae9a41bb89240b73c60cbe1ce413` | `7aec8c91bd771ea044a1090b54dce0ec32db1d9607751c0839817bbb8065b0c1` | test-only assertions, counters, and hooks |
| `future.go` | 23 | `d29d3e5614f0f6fc99beaaab279a90806a9ed860` | `4441061a02d9fa1d203a4f77d0dd2ed937ff3326a66c7c98febfa541a8eed9ed` | constant timestamp future used by tests |
| `interface.go` | 241 | `b093f9c8e15d1b704ffef30e2a43614f2f13c06e` | `134009c00d1dc0706eed330e78eb78209f516cfd252565f042d530a968896ed5` | transaction manager/provider contracts and constructors |
| `txn_context_test.go` | 1,219 | `566126f66abbf2d7760023fd7ec769a18780079b` | `aecd9e070e7a6c35af4f2e3a4375d764e92b47e7323a1ab01b062c2878fd16e1` | transaction context lifecycle, retries, stale reads, and prepared statements |
| `txn_manager_test.go` | 577 | `d72ed5532016f93a1d3a3a5f5d79c857a6ec1bae` | `6935c25b8364a8e674fd4468193db2cfa14b5cac749fb24bd1950c26d92c429d` | manager entry, snapshot, infoschema, and temporary-table tests |
| `txn_rc_tso_optimize_test.go` | 814 | `a20c60ea9361e5ca61029aedacab3aa7196574ac` | `575d58c9f3a25001e118069008d15c1a44b41d2deeaa409ee1ebc1c37c60de97` | RC timestamp request and prepared/text execution regressions |

The production surface defines 16 declarations (nine failpoint helpers,
`ConstantFuture.Wait`, and six interface/package functions). The test surface
defines 45 helpers/methods and 27 top-level tests across the three test files.
The tests cover transaction entry modes, autocommit and explicit boundaries,
optimistic/pessimistic and RC behavior, retries after lock conflicts,
historical and stale reads, prepared and text execution, snapshot and
infoschema selection, temporary-table interception, failpoint assertions,
and RC TSO request/wait counters. All 61 function/method declarations and
all 27 top-level tests were checked individually.

## Rust ownership and explicit boundary

This Go root package is an API seam rather than an executable transaction
implementation. `TxnManager` and `TxnContextProvider` are consumed by the Go
session, executor, planner, infoschema, and KV layers; `failpoint.go` and
`ConstantFuture` are test-only support. Rust owns the corresponding behavior
across `tidb-session` transaction state, `tidb-server` cluster-session
transaction routing, `tidb-exec` isolation metadata, and `tidb-txnkv`
transaction/snapshot traits, but it has no dependency-closed crate that can
replace this Go package's public manager/provider contracts and test hooks.
The adjacent `isolation` and `staleread` packages remain separate ownership
boundaries and are not silently folded into this receipt.

No Rust-only behavior was found to remove, and no safe missing production
behavior can be implemented in this root package without inventing a second
transaction API or coupling only one provider to an incomplete session
stack. The existing Rust transaction code documents its remaining cluster
storage and lock differences; changing those through this interface-only
receipt would be speculative and could alter transaction conflict or
timestamp semantics. The complete Go root package is therefore recorded as
an explicit SEED/boundary. Future parity work must join the session,
executor, storage, isolation, stale-read, snapshot, and test-support owners
before claiming package completion.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/sessiontxn -count=1)
# passed: pkg/sessiontxn (24.088s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, real multi-store lock timing, or a
future dependency-closed Rust implementation of the Go manager/provider API.

This receipt certifies the bounded root `pkg/sessiontxn` inventory and
ownership decision; it is not a repository-wide transcreation claim.
