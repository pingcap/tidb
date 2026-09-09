# `pkg/session/cursor` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 247 lines. Every production
source, test, and Bazel target was read in full before comparing the Rust
workspace. There is no `doc.go`, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 21 | `097f324a1cc4dfb62d9a030a2df21fe38abe060c` | `306e9d4f99f44d4882fddd2d56426ac82365fc7d7e9a854acf6f261c1f4ac343` | cursor library and five-shard flaky test target |
| `state.go` | 20 | `9c3ad343b6e70a2999b5c565fffdeaabf80e118d` | `b66ff94914be7621cbe219932be502955039ae19fdb2971a18824186e19e0602` | cursor transaction-start state |
| `tracker.go` | 91 | `3a8cd3cf0774ee01c03d130d3f14b7375eb7ec8e` | `a2cbf00f048aa76eb967dbb54bebaa2694bda7bb6308b0ee78282a1b3450158c` | concurrent cursor tracker and handle lifecycle |
| `tracker_test.go` | 115 | `0b89aa5704e1f5d8d8e52009fe0baa004523913d` | `48ceba9e4dea4371bcbaaeac7c9a7b136134e86d8a8a75beeb893a8488541527` | creation, lookup, range, close, and concurrent stress tests |

The production surface defines eight functions/methods: `NewTracker`, the
three tracker operations, the private removal hook, and the three handle
operations. The test surface defines five top-level tests covering IDs,
lookup identity, range short-circuiting, close removal, and concurrent
create/delete safety. `State.StartTS` is the only stored value and is copied
into each handle. All 13 declarations, all five tests, and the flaky
five-shard build configuration were checked individually.

## Rust ownership and explicit boundary

The Go package is the session-owned tracker used by `pkg/session`,
`pkg/executor/staticrecordset`, and domain infosync to retain result-set
handles and expose their transaction start timestamp. Rust's
`tidb-server::cursor_state` instead owns the MySQL prepared-protocol cursor
state and materialization lifecycle; it is not a session-wide concurrent
tracker and does not implement `RangeCursor`, handle closure, or the
static-recordset integration. No dependency-closed Rust owner currently
spans the session, result-set, and infosync consumers of this package.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added as a standalone tracker without duplicating protocol/session cursor
state or changing concurrent close/iteration semantics. The Go stress test's
`sync.Map` guarantees and the `StartTS` handoff belong with the session and
result-set owners, not with the unrelated prepared cursor crate. This package
is therefore recorded as an explicit SEED/boundary; future parity requires a
coordinated session result-set and infosync integration decision.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/session/cursor -count=1)
# passed: pkg/session/cursor (4.827s)
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, cross-session cursor lifetime under a
live server, or a future dependency-closed Rust session tracker.

This receipt certifies the bounded `pkg/session/cursor` inventory and
ownership decision; it is not a repository-wide transcreation claim.
