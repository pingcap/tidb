# `pkg/sessiontxn/internal` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The nested package contains two tracked artifacts and 98 lines. Every
production source and Bazel target was read in full before comparing the Rust
workspace. There is no `doc.go`, test file, fixture directory, generated
output, benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 16 | `a7e2729fe35a8ac980eac430b994cc627d2c3641` | `43c6772f6fe37375f3b98d20e4366c27b69ec0ff981c62704c4d718b199d22c0` | internal library target and visibility/dependencies |
| `txn.go` | 82 | `5b03a284c1440e06d641a676a937194c542f273b` | `940778c5e2663870b1f7264605c3476a6774d641344f1ab42c1aed041e3bdc12` | assertion-level, transaction-boundary, and snapshot-option helpers |

The production surface defines three functions: `SetTxnAssertionLevel`,
`CommitBeforeEnterNewTxn`, and `GetSnapshotWithTS`. The package has no
package-local tests; its callers in `pkg/sessiontxn/isolation`,
`pkg/sessiontxn/staleread`, and the root manager tests provide the exercised
coverage. The helpers set the TiKV assertion option, commit a valid prior
transaction before opening a replacement, and attach snapshot interceptors,
request-source flags, and load-based replica-read thresholds.

## Rust ownership and explicit boundary

Rust owns the underlying concerns in separate dependency owners:
`tidb-session` parses assertion levels and maintains session transaction
state, `tidb-server` performs transaction-boundary routing and propagates
request-source/replica-read options, and `tidb-txnkv` provides transaction,
snapshot, and interceptor traits. Those owners do not expose one
dependency-closed replacement for this Go internal helper package, and the
Go helpers are coupled to `sessionctx.Context`, `kv.Transaction`, and the
Go-specific failpoint/test contract.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added here without inventing a second cross-crate session API or moving
only one option-propagation path. The existing Rust transaction/session code
must be joined with the manager, isolation, stale-read, executor, and storage
owners before these helpers can be ported as one semantic unit; a standalone
assertion setter or snapshot constructor would risk diverging request-source,
interceptor, and transaction-boundary behavior. This complete package is
therefore recorded as an explicit SEED/boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/sessiontxn/internal -count=1)
# passed: pkg/sessiontxn/internal [no test files]
```

The package was compiled from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, real TiKV option propagation, or a
future dependency-closed Rust replacement for the Go helper API.

This receipt certifies the bounded `pkg/sessiontxn/internal` inventory and
ownership decision; it is not a repository-wide transcreation claim.
