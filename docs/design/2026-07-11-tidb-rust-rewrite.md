# TiDB Rust Rewrite

- Author: qiliu
- Status: active
- Last revised: 2026-07-23

## Outcome

Build `tidb-rs`, a standalone Rust replacement for TiDB's Go SQL node. It must
remain compatible with MySQL clients, PD, TiKV, TiFlash, and mixed-version TiDB
clusters. Go and Rust never share a process through cgo or FFI.

The Go implementation is the behavioral authority. Rewriting proceeds by
transcreating complete Go packages, not by growing feature slices. A Go package
may become several Rust crates when that produces a cleaner dependency graph,
but its source, tests, generated files, and support data are accepted together.

## Boundaries

The deployment boundary is a whole SQL node:

```mermaid
flowchart LR
    C["MySQL clients and TiProxy"] --> G["Go TiDB"]
    C --> R["tidb-rs"]
    G --> PD["PD and etcd"]
    R --> PD
    G --> KV["TiKV through kvproto"]
    R --> KV
    G --> TF["TiFlash through tipb and MPP"]
    R --> TF
```

The stable cross-process contracts are MySQL wire protocol, PD and TiKV gRPC,
tipb DAG/MPP messages, and etcd-compatible coordination. Internal parser,
session, planner, expression, executor, and transaction APIs are pointer-rich
and chatty; turning them into an FFI boundary would create permanent migration
machinery.

## Whole-package rule

A package is complete only when all of these have been transcreated or given an
explicit equivalent disposition:

- every production file, platform/build-tag variant, generated output, and
  generator input;
- every internal and external test, subtest, benchmark, fuzz target, and
  example;
- fixtures, goldens, embedded assets, `testdata`, failpoints, helper programs,
  runner scripts, and build metadata;
- the complete observable behavior: values, errors, ordering, state changes,
  cancellation, concurrency, and wire/storage encodings.

Translation follows the Go control flow first. Rust-native ownership, crate
splits, arenas, RAII, typed registries, and supervised async tasks are welcome
when they remove complexity without changing behavior. Optimizer, SQL,
transaction, protocol, and storage redesigns wait until parity.

Direct internal Go dependencies must already be current, or the selected work
must include the dependency-closed group. Existing partial Rust code is seed
material, never evidence of package completion.

## Development loop

Work on exactly one dependency-ready Go package. Transcreate its production
owners in source order, then transcreate the complete original test/support
owners. Run a focused test only when it gives immediate feedback for the code
being changed. Fix shared-API consumers in place; do not add compatibility
layers.

At package close, compare the live Go directory with its Rust owners and run
the broad Go, Rust, differential, lint, and live checks once. Until that close
succeeds, the package is simply open; files, commits, crates, and test subsets
are not completion units.

No campaigns, queues, claims, receipts, ledgers, freezes, integration branches,
or manually maintained per-file status exist. Go is the specification, ordinary
tests are the feedback loop, and Git is recovery.

Organize Rust by stable cohesive responsibility. One Go package may map to
multiple Rust modules or crates, but never create a module merely to isolate a
partial port or a single test case.

## Target architecture

| Domain | Responsibility |
| --- | --- |
| Protocol/server | MySQL framing, TLS, authentication, commands, prepared statements, connection lifecycle |
| Parser/language | lexer, parser, AST, restore, identifiers, diagnostics |
| Datatype/codec | values, temporal/JSON/vector types, charset/collation, row/key/value encodings |
| Session/catalog | variables, privileges, prepared and transaction state, infoschema, leases, MDL, system tables |
| Planner | resolve, logical and physical plans, rules, cost, properties, hints, bindings |
| Expression/executor | scalar and aggregate evaluation, relational operators, batches, spill, admin execution |
| Distributed query | coprocessor request/result lifecycle and TiFlash MPP |
| Storage | PD client, region cache, snapshots, locks, transactions, retries, fault handling |
| Statistics/DDL | statistics lifecycle, ownership, jobs, backfill, reorg, schema transitions |
| Protocol definitions | kvproto and tipb generation and compatibility fixtures |

There must be one production authority for each runtime seam. Compatibility
aliases and duplicate implementations are removed when the complete owning
package moves.

## Runtime rules

- Tokio owns network I/O, timers, cancellation, and supervised long-lived
  tasks. Every task has an owner, shutdown path, and error policy.
- Parsing, planning, and expression evaluation stay synchronous until profiling
  proves that bounded offload is useful.
- Statement-scoped trees use arenas or indexed ownership where appropriate.
  Packets and row batches are reused with bounded retention.
- MySQL error number, SQLSTATE, message, warnings, affected rows, and status
  flags are compatibility surfaces. Panic is not a SQL error path.
- Connections, prepared statements, transactions, schema leases, caches, and
  background roles are node-local state even though SQL nodes are horizontally
  replaceable.
- Unsupported behavior fails before mutation or external publication.

## Verification

No single suite proves parity:

1. The complete Go package and its transcreated source tests prove the local
   translation.
2. Parser differential tests compare AST, restore output, offsets, warnings,
   flags, and errors.
3. Planner differential tests compare normalized plans, ranges, properties,
   costs where stable, hints, privileges, and errors.
4. Execution differential tests compare rows, types, ordering, warnings,
   affected rows, status, and errors.
5. Transaction tests cover isolation, locks, deadlocks, retries, pessimistic and
   optimistic modes, async commit, 1PC, stale reads, and failures on real TiKV.
6. Protocol tests cover handshake, TLS, authentication, commands, prepared
   statements, cursors, fragmentation, cancellation, and disconnects.
7. Mixed-cluster tests cover schema lease/MDL, topology, rolling upgrade,
   ownership, failover, and recovery.

There is no special acceptance operation. Formatting, owning-crate tests,
workspace Clippy/tests, repository lint, and relevant live checks run through
their ordinary commands. Mocks never replace live checks.

## Deployment ladder

1. **Shadow node.** Mirror eligible traffic, compare with Go TiDB, and discard
   Rust results.
2. **Read-only node.** Serve an explicitly supported read surface through real
   PD/TiKV/TiFlash with complete connection, session, schema, retry, and
   cancellation behavior for that surface.
3. **Read-write node.** Add full DML and transaction semantics after real-TiKV
   differential and failure testing.
4. **Full peer.** Add DDL, statistics, bootstrap, ownership, background jobs,
   mixed-version operation, and disaster recovery.

Rollback is a topology operation: stop routing new sessions to Rust, drain
Rust nodes, and continue on Go nodes. It must not require data conversion.

## Completion

The rewrite is complete only when every in-scope Go package and original
test/support obligation has a transcreated Rust implementation, all
differential and mixed-cluster rings pass, the Rust node serves production
read/write workloads through real services, full-peer roles work without a Go
node, and no migration-only duplicate authority remains.

Until then, report exact current packages and bounded live capabilities, never
an estimated percentage.
