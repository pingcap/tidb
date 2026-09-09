# `pkg/session/sessmgr` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 392 lines. Every production
source, test, and Bazel target was read in full before comparing the Rust
workspace. There is no `doc.go`, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 35 | `abffc7c479dfd75ca267efba18bc6b832e97f181` | `6761d2c3125a66bbf1a49988ef28dfb2f6a92deada22b771e60f4d386d1216ca` | process-info library and flaky test target |
| `processinfo.go` | 290 | `3642c810de6d9a9aa91a89c00868de6221342be4` | `c38c02abd600305cb380b590beaabb0afcdcef1c4cf148d6836ec307f57c6018` | process-list rows, transaction metadata, and session-manager interfaces |
| `processinfo_test.go` | 67 | `08d5c45cb5e89b538c967e83aa21c3c3569d49cf` | `da53c8439f51e146ed180128cf4b9a56e7d19c0e1fd6d88b1ac30d8aa6ce6545` | shallow-clone identity and tracker assertions |

The production surface defines seven functions/methods (`Clone`, process-list
row conversion/stringification, transaction-start formatting, full process
row conversion, status formatting, and normal-close kill dispatch), plus the
`InfoSchemaCoordinator`, `NormalCloseKiller`, and `Manager` interfaces. The
test surface defines one helper and one top-level test. The source contract
covers rune-limited/full process info, host/port and status formatting,
memory/disk/CPU and affected-row snapshots guarded by statement-context
reference counts, transaction start display, kill routing, internal-session
coordination, TLS and server identity, connection attributes, and status
variables. All nine declarations/interfaces, the test, and all three build
artifacts were checked individually.

## Rust ownership and explicit boundary

Rust's `tidb-session::process` is a partial owner: it maintains a concurrent
process registry, kill targets, statement metadata, transaction rows, and
basic process-list snapshots. It does not yet provide the Go `ProcessInfo`
shape and conversion rules for memory/disk/CPU, statement-context lifetime,
resource groups, aliases, redaction, plan/index metadata, or all status bits;
nor does it implement the Go session-manager/coordinator APIs for internal
sessions, TLS updates, connection attributes, and status variables. Those
behaviors cross server, session, infoschema, resource-group, and metrics
owners.

No Rust-only behavior was found to remove, and no safe missing behavior can
be added as a standalone row formatter or registry method without creating a
second processlist authority or changing kill, timing, and transaction-row
lifetimes. The existing Rust registry is intentionally a partial owner with
documented omissions. This complete Go package is therefore recorded as an
explicit SEED/boundary; future parity requires one coordinated session
manager, processlist, transaction-info, and server-control integration.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/sessmgr -count=1)
# passed: pkg/session/sessmgr (0.530s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, live processlist/kill races, or a future
dependency-closed Rust session-manager implementation.

This receipt certifies the bounded `pkg/session/sessmgr` inventory and
ownership decision; it is not a repository-wide transcreation claim.
