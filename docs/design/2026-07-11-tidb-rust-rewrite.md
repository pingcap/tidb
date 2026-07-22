# TiDB Rust Rewrite: Design

- Author: qiliu
- Status: active
- Last revised: 2026-07-22

## Summary

The project will replace TiDB's Go SQL layer with a standalone Rust SQL node,
`tidb-rs`, while preserving TiDB behavior and the protocols used by MySQL
clients, PD, TiKV, TiFlash, and mixed-version TiDB clusters.

There are two different atomic boundaries:

- The **deployment boundary** is a complete SQL node. Go and Rust do not share
  an address space. A Rust node joins the cluster through existing serialized
  protocols and is introduced through shadow, read-only, read-write, and
  full-peer stages.
- The **implementation and acceptance boundary** is one complete upstream Go
  package or module. A package may map to several Rust crates, but no file,
  function, feature, SQL shape, or test subset can be declared complete on its
  own.

This is source-first transcreation. The Go implementation and its entire test
and support inventory remain the behavioral oracle until a Rust package closes
with checked evidence. Rust may change ownership, memory layout, task
supervision, and crate structure; it may not change observable SQL, wire,
transaction, storage, planner, or error behavior during the rewrite.

## Goals

1. Deliver a production-deployable Rust TiDB SQL node without cgo or any other
   in-process Go-to-Rust bridge.
2. Preserve exact TiDB compatibility across SQL semantics, MySQL protocol,
   plans, errors, transaction behavior, KV encodings, and cluster protocols.
3. Transcreate every in-scope Go package completely, including every original
   test and support artifact.
4. Make progress reviewable and reversible through one generated package proof,
   ordinary Git history, differential checks, and cluster-level rollout stages.
5. Use Rust-native crate boundaries, ownership, memory management, and
   concurrency where they simplify the implementation without changing its
   contract.
6. Produce useful intermediate systems: a reusable parser, a production-grade
   Rust TiKV client path, and a read-only SQL node.

## Non-goals

- No cgo, shared C ABI, FFI handles, or hybrid Go/Rust process.
- No SQL-language, optimizer, cost-model, wire-protocol, or KV-format redesign
  during transcreation.
- No TiKV or PD rewrite. They are external services reached through their
  existing protocols.
- No initial rewrite of BR, Lightning, Dumpling, DM, or TiCDC. They are
  separate executables and can be evaluated independently after the SQL node
  reaches parity.
- No progress claims based on Rust line counts, connected demos, or broad Rust
  test counts. Completion is package- and behavior-based.
- No new feature-slice porting. Existing partial code is seed material, not an
  acceptance unit.

## Sources of truth

The Go source and tests are the semantic authority. The design document defines
stable architecture and acceptance rules; it does not contain volatile progress
counts.

- Current architecture and open gaps: [`rust/HANDOFF.md`](../../rust/HANDOFF.md)
- Active whole-package ExecPlan:
  [`rust/workstreams/plans/2026-07-whole-package-transcreation.md`](../../rust/workstreams/plans/2026-07-whole-package-transcreation.md)
- Completed package proofs: [`rust/ports/`](../../rust/ports/)
- Whole-package tool: [`rust/scripts/package-port.py`](../../rust/scripts/package-port.py)

The tool reads the Go tree directly. There is no separately maintained queue or
global source/test ledger in the implementation loop.

## System boundary

`tidb-rs` is an independent process. It participates in a mixed cluster using
the same serialized interfaces as a Go TiDB node.

```mermaid
flowchart LR
    C["MySQL clients and TiProxy"] --> G["Go TiDB nodes"]
    C --> R["tidb-rs SQL nodes"]
    G --> PD["PD and etcd"]
    R --> PD
    G --> KV["TiKV through kvproto and gRPC"]
    R --> KV
    G --> TF["TiFlash through tipb and MPP"]
    R --> TF
```

The stable language-neutral boundaries are:

1. MySQL wire protocol, including authentication, TLS, commands, prepared
   statements, result framing, warnings, status flags, and connection lifetime.
2. PD and TiKV gRPC APIs defined by kvproto, including region routing,
   transactional KV, coprocessor requests, resource control, and errors.
3. tipb DAG and expression trees evaluated by TiKV's Rust coprocessor.
4. MPP task and exchange protocols used by TiFlash.
5. etcd and system-table protocols used for schema leases, metadata locks,
   server registration, ownership, statistics, and background work.

The SQL node is the correct deployment boundary because its internal parser,
session, planner, expression, executor, and transaction interfaces are
pointer-rich and highly coupled. Turning those interfaces into an FFI boundary
would add permanent serialization, scheduler, lifetime, and debugging costs.

A TiDB node is horizontally replaceable, but it is not literally stateless.
Connections, prepared statements, transaction state, caches, leases, and some
background-role state are node-local. Cluster topology makes incremental
replacement possible; exact session and ownership semantics still have to be
implemented before the relevant rollout stage.

## Whole-package transcreation contract

### The package is atomic

For an upstream Go package or separately versioned Go module, transcreation
includes all of the following:

- every production source file;
- generated outputs and their generator inputs;
- build-tag and platform variants;
- internal and external test packages;
- every test, subtest, benchmark, fuzz target, and example;
- fixtures, golden results, embedded assets, and `testdata` trees;
- failpoints, helper programs, runner scripts, and build metadata.

The inventory must be exhaustive and exclusive: every artifact is included in
the package digest, and no artifact silently falls outside the package proof.
Completion cannot be inferred from a working feature or a subset of tests.

### One Go package may become multiple Rust crates

Rust layout follows cohesion, ownership, dependency direction, and compile-time
cost rather than Go directory history. One Go package may therefore map to
several Rust crates or modules. This is encouraged when it produces smaller
stable interfaces or isolates heavy dependencies.

The split never weakens acceptance:

- one package proof owns the complete Go package;
- all Rust crates, files, tests, and shared consumer seams are declared in it;
- all constituent work is staged together;
- the package has one focused verification command;
- no constituent crate can promote package coverage independently.

Conversely, multiple Go packages may share a Rust foundation crate only when
ownership and dependency proofs remain unambiguous. Shared code is not a way
to collapse unported package obligations.

### Dependency closure

Package proofs form a directed acyclic graph. A package can finish only when
every direct internal Go dependency is either:

- included in the same dependency-closed umbrella; or
- represented by a current checked package proof.

Existing Rust helpers, legacy feature slices, mocks, and connected end-to-end
paths do not satisfy this rule. If a prerequisite changes, `check` invalidates
its proof on inventory drift and downstream completion stops until the package
is repaired and `finish` is rerun.

## Package state and evidence lifecycle

A package has only two states: no valid proof, or one valid proof. There is no
ready/claimed/gated/covered state machine. Work in progress is ordinary Git
working-tree state.

```mermaid
stateDiagram-v2
    [*] --> Unported
    Unported --> Proven: package-port finish
    Proven --> Unported: Go inventory or proof becomes stale
    Proven --> Proven: repair and rerun finish
```

`finish` derives the exact source, test, literal-subtest, benchmark, fuzz,
example, fixture, build, and `testdata` inventory from the Go directory; checks
direct internal dependencies; runs formatting, all-target Clippy, library
tests, and declared integration tests; then writes one proof. It writes nothing
on failure. The proof records the full file lists and digest, Rust crates,
Rust paths, test targets, and direct dependencies. Git provides content
history, review, rollback, and commit atomicity, so a second transaction log is
strictly redundant.

When an audit finds missing behavior or an incorrect translation, repair the
implementation and rerun `finish`. A false positive is a correctness defect;
do not preserve a progress number. Rust code without a valid package proof is
seed material and is never counted as a completed package.

## Execution model

The migration is executed by one implementation owner at the whole-package
level. There is no worker allocation or ownership subsystem.

For each package or dependency-closed frontier:

1. Select a whole package whose direct internal Go dependencies already have
   valid proofs. Use `package-port.py inventory <package>` when needed; do not
   maintain a scheduling queue.
2. Audit the selected package's complete Go source, test, support, generated,
   and build inventory.
3. Transcreate the complete behavior directly from Go, preserving control-flow
   order, constants, errors, state transitions, encodings, cancellation, and
   concurrency semantics.
4. Port or differentially cover every original test and support artifact.
5. Run focused crate/test commands during implementation.
6. Finish with one command:

   ```sh
   cd rust
   scripts/package-port.py finish pkg/example \
     --crate tidb-example \
     --rust-path rust/crates/tidb-example/src/lib.rs \
     --rust-path rust/crates/tidb-example/tests/package_source.rs \
     --test-target tidb-example:package_source
   ```

7. Run full workspace Clippy/tests, package-proof checks, and the repository
   Ready profile once before push/release or after shared-foundation changes,
   not after every package.
8. Run the applicable differential/live suite at the same explicit checkpoint.
9. If the audit or verification exposes a cross-package flaw, fix the owning
   package; do not add a downstream conditional workaround.

The normal control surface is one completion command. `inventory` and `check`
are read-only diagnostics, and `checkpoint` is the pre-push workspace sweep.
There are no claims, worktrees, queues, campaigns, transfers, generated global
ledgers, frozen-input digests, or completion receipts in the normal workflow.

## Translation rules

The normal method is structural Go-to-Rust transcreation:

- retain observable branch ordering and error precedence;
- retain integer widths, overflow behavior, hash/equality framing, and wire or
  SQL rendering;
- retain cancellation points, retry boundaries, and state transitions;
- preserve exact public diagnostic identity and text where clients or tests
  observe them;
- use original test vectors before inventing replacement cases;
- add Rust-only tests for ownership, memory safety, and runtime failure modes,
  but never use them as substitutes for original Go tests.

Rust-native redesign is required only at genuine runtime boundaries:

- GC pools become ownership, reuse, or arenas;
- goroutines and channels become supervised tasks and typed channels;
- pointer identity becomes explicit IDs, indices, or arena references;
- `any` registries and init-time globals become typed registries and explicit
  construction;
- defer/recover cleanup becomes RAII plus explicit panic and error policy.

If the faithful Rust form is unclear, preserve behavior first and isolate the
decision behind a narrow interface. Optimizer, SQL, transaction, protocol, and
storage redesign happens only after parity, under a separate design.

## Target Rust architecture

The workspace is organized by stable responsibility, not by a forced one-to-one
copy of Go directories. The exact current crate map belongs in `rust/HANDOFF.md`;
the target domains are:

| Domain | Responsibilities |
| --- | --- |
| Protocol and server | MySQL packet codec, TLS, auth plugins, command dispatch, prepared statements, connection lifecycle |
| Parser and language model | lexer, parser, AST, restore/format, MySQL identifiers, diagnostics |
| Datatype and codec | datum, decimal, temporal, JSON, collation, charset, row/key/value encodings |
| Session runtime | variables, statement context, privileges, prepared state, transaction lifecycle, retry/replay |
| Catalog and domain | infoschema snapshots, schema leases, metadata locks, bootstrap/system tables, server information |
| Planner | preprocess/resolve, logical and physical planning, rules, costs, hints, bindings, privilege checks |
| Expression and executor | scalar and aggregate evaluation, chunks/batches, relational operators, spill, admin execution |
| Distributed query | coprocessor request lowering, result streaming, TiFlash MPP planning and coordination |
| Storage client | PD client, region cache, snapshots, optimistic and pessimistic transactions, retries and fault handling |
| Statistics and DDL | statistics lifecycle, ownership, jobs, backfill/reorg, schema-state transitions |
| Protocol definitions | kvproto and tipb generation plus compatibility fixtures |

Crate roots expose contracts and routing; feature behavior lives in owned
modules. Compatibility aliases and duplicate authorities are removed when an
owner transition completes. There must be one production authority for
storage, one for session state, and one for each parser/planner/executor route.

## Runtime design rules

### Concurrency

- Tokio owns network I/O, timers, cancellation, and long-lived service tasks.
- CPU-bound parsing, planning, and expression work remains synchronous on the
  connection task until profiling proves that a bounded worker pool is needed.
- Every background task has an owner, shutdown path, error policy, and test.
- Per-connection state is not held across `.await` without an explicit reason;
  shared mutable state uses narrow typed synchronization.
- Blocking storage or filesystem work never runs silently on an async executor.

### Memory

- AST and plan trees use arenas or indexed ownership where lifetime is naturally
  statement-scoped.
- Row batches and protocol buffers are reused with bounded retention.
- Strings and byte slices borrow packet, schema, or arena storage when the
  lifetime is explicit; unsafe lifetime extension is prohibited.
- Caches have explicit size accounting and eviction behavior.
- Performance claims require end-to-end measurements, not allocation anecdotes.

### Errors and diagnostics

- MySQL error number, SQLSTATE, message, warning behavior, and status flags are
  compatibility surfaces.
- Internal errors keep structured context and source chains without leaking
  Rust implementation details to clients.
- Panic is never a normal SQL error path. Connection and background-task panic
  policy is explicit and fault-tested.

### Session and cluster state

- Session variables are typed definitions with defaults, scopes, validation,
  dependency rules, and protocol-visible behavior.
- Connection, prepared-statement, and transaction state have explicit state
  machines.
- Schema version, metadata-lock reporting, bootstrap version, ownership, and
  statistics tables use the same encodings and transition rules as Go TiDB.

## Verification architecture

No single test suite proves parity. Acceptance is layered from package inventory
to live mixed-cluster behavior.

### Package proof

Every package proof requires:

1. complete source/test/support inventory with no unowned or multiply owned
   artifacts;
2. focused Rust tests corresponding to every original Go test obligation;
3. explicit disposition for tests that cannot run unchanged, with equivalent
   differential or integration evidence;
4. exact direct dependencies and declared Rust paths;
5. formatting, compilation, strict Clippy, and crate-local tests;
6. required differential, fault, and live evidence for the package's contract.

Original TiDB tests are obligations, not suggestions. A rewritten or merged
Rust test must map back to every source test case, subtest, fixture, and golden
result it covers. Tests excluded by platform or build tags remain inventoried
with a checked disposition. Deleting a Go test from the upstream tree does not
silently delete historical compatibility evidence from an accepted revision.

### Differential rings

1. **Parser ring:** AST shape, restore output, flags, offsets, parameter markers,
   warnings, and exact diagnostics across the full corpus and fuzzed mutations.
2. **Planner ring:** normalized logical and physical plans, ranges, properties,
   costs where stable, hints, privileges, and errors on the same schemas and
   statistics.
3. **Execution ring:** rows, types, ordering, warnings, affected rows, status,
   and errors for SQL integration suites and generated edge cases.
4. **Transaction ring:** commit/rollback, isolation, lock conflicts, deadlocks,
   retries, pessimistic locking, async commit, 1PC, stale reads, and failover
   against real TiKV.
5. **Protocol ring:** handshake, TLS, authentication, every supported command,
   prepared statements, long data, cursors, packet fragmentation, cancellation,
   disconnects, and client compatibility.
6. **Mixed-cluster ring:** schema lease and MDL, rolling upgrade, server
   registration, capability routing, topology change, failover, and ownership
   transitions.

### Workspace checkpoint

An ordinary package finishes after exact inventory validation and touched-crate
checks. The full workspace check is a grouped checkpoint
before push/release and after mechanism or shared-foundation changes; it is not
a tax on every leaf package. Cheap static/generated checks run first, followed
by workspace all-target Clippy/tests, governance tests, isolation checks, and
the repository Ready profile including lint when code changed. Independent
read-only checks run concurrently. Digest traversal prunes excluded build and
runtime trees. Expensive live suites run when the accumulated package contract
touches their boundary; mocks never replace real PD/TiKV proof.

## Deployment ladder

### 1. Shadow node

TiProxy or a capture/replay harness mirrors eligible traffic to `tidb-rs`.
Rust results are compared with Go results and discarded. The shadow stage
measures semantic differences, crashes, resource use, and latency without
serving responses.

### 2. Read-only SQL node

The node accepts production MySQL connections and serves the statements in its
declared capability set through real PD/TiKV and TiFlash paths. Unsupported
statements are rejected before side effects with a stable capability error so
the proxy can route the session to Go TiDB.

Read-only deployment requires, at minimum:

- complete connection lifecycle, TLS, authentication, command, prepared-query,
  result, cancellation, and error behavior for the supported client surface;
- schema snapshot, schema lease, metadata-lock reporting, system-table reads,
  privilege checks, and session semantics needed by those statements;
- real region routing, snapshot reads, coprocessor DAG lowering/result decoding,
  retry, backoff, cancellation, and topology-change behavior;
- MySQL-client-to-real-PD/TiKV live tests and shadow differential evidence.

The first deployable milestone is a bounded read-only node, but it does not
close any Go package merely by connecting these paths.

### 3. Read-write SQL node

The node adds complete DML and transaction semantics. Entry requires the full
transaction differential ring against real TiKV, including pessimistic and
optimistic modes, autocommit, retries, lock resolution, async commit, 1PC,
schema changes during transactions, and client disconnect/cancellation.

### 4. Full peer

The node becomes eligible for DDL, statistics, and other background ownership.
It can bootstrap supported cluster versions, preserve job/system-table
encodings, survive rolling upgrades, and operate without a Go TiDB node.
Go nodes may drain only after full-peer mixed-cluster and disaster-recovery
gates pass.

Rollback at every mixed-cluster stage is a topology operation: stop routing new
sessions, drain Rust nodes, and continue on Go nodes. Rollback must not require
data conversion or in-process compatibility glue.

## Acceptance phases

The phases order acceptance dependencies, not partial feature completion.
Implementation may discover or stage code outside the current phase, but it
cannot skip package or runtime prerequisites.

| Phase | Deliverable | Exit gate |
| --- | --- | --- |
| 0. Language and datatype foundations | Fully proven parser/language, type, collation, codec, and protocol-definition packages needed above them | Package inventories closed; parser and datatype differential rings pass |
| 1. Storage and distributed-read foundations | Production PD/region/snapshot/coprocessor path plus complete prerequisite packages | Real-cluster read, retry, fault, topology, and compatibility suites pass |
| 2. Deployable read-only node | Standalone binary serving its declared read-only surface through the real stack | MySQL-client live test, shadow parity, TLS/auth/session/schema/MDL, and operational gates pass |
| 3. Read-write node | Complete DML and transaction client behavior without background ownership | Transaction differential, fault-injection, upgrade, and durability gates pass |
| 4. Full peer | DDL, statistics, bootstrap, ownership, background jobs, and operational parity | Full original inventory, mixed-cluster, upgrade/downgrade, failure-recovery, performance, and production canary gates pass |

## Failure recovery and drift handling

- Inventory drift invalidates the package proof and is reviewed before further
  package completion.
- Work in progress needs no state cleanup; discard or revert it with ordinary
  Git operations.
- A false proof is repaired by fixing the implementation and rerunning
  `package-port.py finish`.
- A stale prerequisite blocks dependent completion until its proof is current.
- A live proof that covers only a bounded statement or topology remains bounded
  evidence. It never expands itself into package coverage.
- A compatibility failure is fixed in the package that owns the violated
  invariant. Downstream special cases are rejected unless the Go contract
  itself requires them.
- Progress is derived from valid files under `rust/ports/`; there is no mutable
  status ledger.

## Principal risks and controls

| Risk | Control |
| --- | --- |
| The Go tree changes during transcreation | Content-hash the complete package inventory; invalidate stale proofs; rebase by package |
| Existing partial Rust code creates false confidence | Count only valid whole-package proofs with dependency closure |
| Original tests are silently omitted | Derive test files, entry points, literal subtests, benchmarks, fuzz targets, examples, fixtures, and support directly from Go at `finish` |
| A valid-looking proof later proves incomplete | Fix the implementation and regenerate the proof; Git retains the review trail |
| Rust crate splits hide missing Go behavior | One proof covers the complete Go package and every mapped Rust crate/path/test target |
| SQL semantics drift during cleanup | Structural transcreation first; redesign only after parity under a separate proposal |
| Rust client behavior lags client-go | Treat client-go as the specification; prove parity against real PD/TiKV and fault injection |
| Mixed nodes disagree on schema or ownership | Dedicated schema-lease, MDL, system-table, rolling-upgrade, and ownership suites |
| Performance work changes behavior | Differential correctness gate first; profile and benchmark only accepted paths |
| Long migration accumulates duplicate authorities | One production owner per seam; remove superseded compatibility and routing paths when ownership transfers |

## Rejected alternatives

### Big-bang replacement

A siloed full rewrite produces no deployable feedback until the end and lets
source drift accumulate. Package proofs plus cluster-level rollout provide
bounded, reviewable proofs earlier.

### In-process package replacement

The internal interfaces are too chatty and pointer-rich for a durable FFI seam.
Serialization and dual runtimes would become permanent complexity, while the
existing network boundaries already provide a clean process-level cut.

### Feature-slice porting

Porting only the code needed by the next demo hides unvisited branches and
tests, creates overlapping ownership, and makes progress impossible to audit.
Feature slices are therefore not accepted work units.

### Semantic redesign during translation

Changing optimizer, transaction, protocol, or SQL semantics while changing
language makes failures hard to localize and invalidates differential testing.
Parity and redesign are separate projects.

### Mock-only storage validation

Mocks cannot prove region routing, lock resolution, retries, commit protocols,
or topology behavior. They are useful for focused tests but cannot replace
required real PD/TiKV gates.

## Completion definition

The Rust rewrite is complete only when all of the following are true:

1. Every in-scope upstream Go package/module has a current valid proof at
   the accepted source revision, with complete source and original-test/support
   coverage.
2. The Rust node passes parser, planner, execution, transaction, protocol, and
   mixed-cluster differential rings with no unexplained divergence.
3. It serves production read and write workloads through real PD, TiKV, and
   TiFlash paths, including failures, retries, topology changes, and rolling
   upgrades.
4. It can perform full-peer ownership and background roles and can operate
   without a Go TiDB node for supported cluster versions.
5. The workspace contains no active legacy feature-slice authority, duplicate
   production routing, hidden FFI bridge, or compatibility path whose only
   purpose was the migration.
6. Correctness, compatibility, performance, resource use, observability,
   deployment, rollback, and disaster-recovery gates all pass on a clean
   release candidate.

Until then, progress is reported as exact covered packages and bounded runtime
capabilities, never as an estimated rewrite percentage.
