# `pkg/sessionctx` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The root package contains four tracked artifacts and 319 lines. The complete
inventory includes the public BUILD target, all context/cache/state interfaces,
the snapshot-read timestamp helper, the context-key test, and the TestMain
goleak harness. There is no `doc.go`, fixture or `testdata` directory,
generated output, platform-specific variant, benchmark, fuzz target, or
generator input. Subdirectories (`stmtctx`, `sessionstates`, `variable`, and
others) are separate package units and are not folded into this receipt.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 42 | `45f223d6d40a9b24fd8ea3c418d07952d5949ced` | `3068430b2148b211b2fa4f904dbc55b953986948379806670e3fc84bab928431` | public library and race/flaky test target with 18 dependencies |
| `context.go` | 206 | `ede2c75b51e2d9b579a268f8db028c8c0cf5ea01` | `9cf7dd231f341a594dc013057b4d835ccd8353b050fa601ef5fe1be9227584dd` | SessionStatesHandler, plan-cache, Context, TxnFuture interfaces, context keys, and snapshot-read validation |
| `context_test.go` | 37 | `97cf9c63b79bcc265be7416ceead7cb8735c2cfb` | `44a527f92328e30c69f576b6ce177d03d6d32f5f66e3df1efb35bcb3d7e256fd` | context-key string regression test |
| `main_test.go` | 34 | `7dee5bea10200cbd5174e4d850ad483474845ba1` | `3c87644fc604aba3b6e444f4fb2f512825dbe5c9d58569bd868f12d321c19589` | common test setup and goleak allowlist |

`context.go` declares six public interfaces: `SessionStatesHandler`,
`SessionPlanCache`, `InstancePlanCache`, `Context`, and `TxnFuture` (with the
embedded dependency interfaces completing the contract). It defines the
`basicCtxType` integer key and `String` method, the `QueryString`, `Initing`,
and `LastExecuteDDL` constants, and one production function,
`ValidateSnapshotReadTS`, which delegates strict read-timestamp validation to
the storage oracle with the global transaction scope. `context_test.go`
contains `TestBasicCtxTypeToString`; `main_test.go` contains `TestMain`.

The package is unchanged from the comparison source; no Go production or test
file required a fix in this batch.

## Rust ownership and explicit boundary

Rust's `tidb-exec::session_context_key::ContextKey` preserves the integer key
domain, labels, and unknown-key formatting, with source-derived tests for the
three constants and arbitrary values. Rust has separate owners for selected
plan-cache, transaction/read-timestamp, session warning, cursor, and advisory
lock leaves, but no dependency-closed implementation of the Go `Context`,
`SessionStatesHandler`, cache interfaces, or `TxnFuture` composition. The
`ValidateSnapshotReadTS` helper has no direct Rust equivalent that can accept
the Go `kv.Storage` oracle contract; adding a generic stub would not validate a
real store. No Rust-only behavior was found to remove, and no safe
package-local implementation can be added without inventing the session
composition root and its storage/session-manager owners.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit. No production,
test, or Bazel source changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/sessionctx -count=1
# ok github.com/pingcap/tidb/pkg/sessionctx 0.488s
```

The exact Go-master package suite passed; its source has no failpoint calls,
so no failpoint wrapper was required. Rust source, Bazel, and module files were
unchanged; `make bazel_prepare` and Ready lint were not required. Not verified:
the full Rust aggregate, all race/Bazel shards, concrete session implementations,
or live storage-oracle timestamp validation. Correctness and compatibility risk
remain concentrated in the untranscreated Context composition and timestamp
oracle boundary; runtime behavior is unchanged because this batch modifies
documentation only.

This receipt certifies the bounded `pkg/sessionctx` root inventory and explicit
ownership boundary; it is not a repository-wide parity claim.
