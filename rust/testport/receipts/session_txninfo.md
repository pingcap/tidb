# `pkg/session/txninfo` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 473 lines. Every production
source and Bazel target was read in full before comparing the Rust workspace.
There is no `doc.go`, test file, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `8062dd58b6d28ba6d8de0d735faed82e79711f16` | `19ac29cccba4cc0a7e454049bb8bb9cade19ea16481ed2bdb5ba3850afe5a23b` | transaction-info library target and dependencies |
| `summary.go` | 162 | `b4e102c9b2ad51dd1b04497bdcb4bc719d58330b` | `4f3791f8ee9503b7001e5c922ca7d44a3cdc91dac2266bccef267ed536edb63e` | FNV transaction-digest LRU history recorder |
| `txn_info.go` | 291 | `84c5478f6f59f0efdec53ee34474208fce17a835` | `2a23b8f33d956aa170b1bcc076a96532c6741d98df33fabca73cbd13eaf1df31` | running-state enum, Prometheus observers, and `TIDB_TRX` Datum conversion |

The production surface defines 16 function/method declarations: the digest
and LRU summary helpers, recorder methods, metric initialization/accessors,
and `TxnInfo.ToDatum`. There are no package-local tests; callers in session,
infoschema, and metrics own the executable integration surface. The source
contract includes five transaction states and labels, state-duration and
state-entry metrics, optional process/session metadata, JSON SQL-digest
encoding, related-table formatting, lock-wait timing, and capacity/age
filtering for transaction history. All 16 declarations and all three build
artifacts were checked individually.

## Rust ownership and explicit boundary

Rust has partial owners. `tidb-exec::txn_running_state` preserves the Go
state discriminants and labels, while `tidb-session::process` publishes live
transaction rows and `TIDB_TRX` dispatch consumes those rows. These owners do
not implement the complete Go `TxnInfo` Datum getter map, Prometheus observer
arrays, or the mutex-protected FNV/LRU `TrxHistoryRecorder` behind
`TRX_SUMMARY`; those behaviors cross session, infoschema, metrics, parser,
and type-conversion seams.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added as a leaf without inventing a second summary/metrics datasource or
silently changing row ordering, JSON, time-zone, and lock-wait semantics.
The existing Rust live transaction registry is intentionally documented as a
partial owner, not a claim of package parity. This complete Go package is
therefore recorded as an explicit SEED/boundary; future work must join the
session registry, infoschema table definitions, Prometheus metrics, and
Datum/type conversion before claiming completion.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/session/txninfo -count=1)
# passed: pkg/session/txninfo [no test files]
```

The package was compiled from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, live Prometheus scraping, or a future
dependency-closed Rust implementation of the transaction summary datasource.

This receipt certifies the bounded `pkg/session/txninfo` inventory and
ownership decision; it is not a repository-wide transcreation claim.
