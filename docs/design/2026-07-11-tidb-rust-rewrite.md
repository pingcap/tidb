# TiDB Rust Rewrite: Design

- Author(s): qiliu
- Discussion PR: TBD
- Tracking Issue: TBD

## Abstract

This document proposes a design for rewriting the TiDB SQL layer in Rust. It is grounded in a study of the current codebase (~950k lines of non-test Go across `pkg/`, `br/`, `lightning/`, `dumpling/`, `cmd/`) and builds directly on the `hparser-integration` work: the hand-written, arena-allocated SQL parser that replaced the goyacc parser is the architectural prototype for the Rust parser, and its differential-testing methodology is the verification model for the whole migration.

The core strategic insight is that **TiDB SQL nodes are stateless**. Multiple TiDB versions already coexist against the same TiKV/PD cluster during rolling upgrades. A Rust TiDB (`tidb-rs`) can therefore join a live cluster as an additional SQL node and take traffic incrementally — no FFI embedding, no big-bang cutover. Migration happens at the cluster topology level, gated by differential testing, not inside a hybrid binary.

## Source-transit contract

The rewrite transits behavior from Go before it redesigns implementation. The
minimum transcreation unit is one complete upstream Go package or module. No
file, function, method, branch, SQL shape, or feature subset is a valid unit of
ownership or completion.

A package transcreation includes every production source file, generated file
and generator input, build-tag/platform variant, test and nested case,
external test package, benchmark, fuzz target, example, fixture, golden result,
embedded asset, testdata directory, failpoint, helper program, runner script,
and package build artifact. It preserves control-flow ordering, constants,
error identity and text, hash/equality framing, wire and SQL output, edge-case
arithmetic, cancellation, and state transitions before exposing Rust APIs.

The generated source/test ledgers remain the atomic evidence inventory, while
a checked package manifest groups every row and support artifact needed for a
single package receipt. A package cannot be called transcreated until that
inventory is closed and its focused, differential, fault, and live gates pass.
Existing partial ports remain frozen seed evidence, but they are explicitly
incomplete and cannot authorize new partial package work or stand in for
unported package branches and tests.

This is direct Go-to-Rust transcreation, not a second implementation invented
from this design document. The Go source and suite remain the behavioral oracle
until the complete Rust package and runtime replace them. Rust-specific
redesign is restricted to implementation mechanisms the language cannot carry
faithfully, such as GC pools, goroutine ownership, pointer identity, untyped
registries, and init-time globals; observable TiDB behavior does not change.

## Implementation status

Live implementation status is generated in [`rust/STATUS.md`](../../rust/STATUS.md).
Current architecture, package-level gaps, and migration steps are maintained in
[`rust/HANDOFF.md`](../../rust/HANDOFF.md). ExecPlans carry the validation
criteria for active work; this design does not duplicate their changing state.

## Motivation

Why rewrite, and why Rust specifically:

1. **Latency jitter from GC.** The Go GC works against large long-lived heaps: statistics caches, plan caches, the infoschema cache, chunk buffers. P99/P999 latency in large deployments is dominated by GC assist and sweep interference, and the standard mitigation (GOGC tuning, ballast, memory limiter) is a permanent tax on operations. Rust removes the collector entirely.
2. **Allocation cost is already the bottleneck in hot paths.** The `hparser-integration` branch exists because parsing was allocation-bound: replacing goyacc with a hand-written parser plus slab/arena allocation was worth a rewrite of 27k generated lines into ~40 hand-written files. The same profile shape (allocation-dominated) recurs in the planner (logical plan tree churn per statement) and in expression evaluation. Rust's ownership model makes arena allocation, zero-copy string handling, and buffer reuse the natural idiom instead of a fight against the runtime — the `hack.String`/`hack.Slice` unsafe workarounds and the parser's `Arena` become simply how the language works.
3. **Memory footprint.** Go interface headers, GC headroom (typically 2× live heap), and pointer-rich data structures (AST, plan trees, `Datum`) inflate resident memory. Rust enums-by-value, arenas, and precise layout typically halve footprint for this workload class, which directly cuts cloud COGS for TiDB Cloud.
4. **One language across the stack.** TiKV — including its coprocessor, which re-implements TiDB's expression and executor semantics — is already Rust. Today every pushdown function is written twice (Go in `pkg/expression`, Rust in TiKV `components/tidb_query_expr`) and kept in sync by hand. A Rust SQL layer shares those crates: **one implementation of MySQL semantics, used on both sides of the wire.** This eliminates an entire class of "coprocessor disagrees with TiDB" bugs and halves the cost of adding builtin functions.
5. **Compile-time data-race elimination.** Session state, statistics, and DDL have a long history of race bugs found by `-race` in CI or by users in production. Rust moves this class of bug to compile time.

### Non-goals

- **No cgo. No in-process Go↔Rust linkage of any kind.** This is a hard architectural rule, not a preference. Every cross-language boundary in the migration is one of the existing *serialized network protocols* (MySQL wire, kvproto/gRPC, tipb, MPP, etcd). There is never a moment where Go code calls Rust code or vice versa inside one address space — no cgo, no shared C ABI hot path, no FFI handles. The cluster-level strangler exists precisely so this rule can hold: a `tidb-rs` node is a standalone process. (The Phase-0 parser can still be exposed as a standalone Rust library for *other native/Rust* consumers, but it is never linked into the Go binary.)
- **No change to SQL semantics, the MySQL wire protocol, or KV encodings.** Bit-for-bit compatibility with the Go implementation is the acceptance criterion, verified differentially.
- **No optimizer redesign during migration.** The existing planner is ported faithfully (same rules, same cost model, same plan output). Migrating language and redesigning the optimizer at once makes regressions undiagnosable. One variable at a time.
- **TiKV and PD are out of scope** (TiKV is already Rust; PD stays Go — its clients speak gRPC and don't care).
- **BR / Lightning / Dumpling / DM / TiCDC stay Go initially.** They are separate binaries speaking stable protocols; they migrate (or don't) on their own schedule after the SQL layer proves out.

## Ground truth: what we are rewriting

Non-test Go LOC on `hparser-integration` after merging master (2026-07-11):

| Subsystem | LOC (non-test) | Responsibility | Key contracts |
|---|---|---|---|
| `pkg/planner` | 120k | AST → logical → physical plan, cost model, hints, bindings, cascades | `base.Plan`/`LogicalPlan`/`PhysicalPlan` (`pkg/planner/core/base/plan_base.go`), `Optimize()` (`pkg/planner/optimize.go`) |
| `pkg/executor` | 106k | Volcano/chunk execution of physical plans | `exec.Executor` (`pkg/executor/internal/exec/executor.go`), `ExecStmt` (`adapter.go`), `Compiler` (`compiler.go`) |
| `pkg/util` | 99k | chunk, memory tracker, collation, ranger, codecs, ~60 sub-packages | `chunk.Chunk`, `memory.Tracker`, `collate.Collator`, `ranger.Range`, `rowcodec` |
| `pkg/expression` | 87k | scalar/aggregate expression trees and vectorized evaluation | `Expression`, `ScalarFunction`, `EvalContext` (`pkg/expression/exprctx`) |
| `br/` | 71k | backup/restore (separate concern) | — |
| `pkg/ddl` | 63k | online schema change state machine, reorg/backfill | `DDL` (`ddl.go`), `Job` (`pkg/meta/model/job.go`) |
| `pkg/parser` | 60k | hand-written recursive-descent parser + AST (own Go module) | `Parser`, `ast.Node`/`StmtNode` |
| `pkg/store` | 33k | TiKV driver, coprocessor client, region cache glue | `TiKVDriver` → `kv.Storage` |
| `pkg/statistics` | 25k | histograms, CM sketch, TopN, stats lifecycle | `statistics.Table`, `handle.Handle` |
| `lightning/` | 21k | bulk import (separate concern) | — |
| `pkg/types` | 18k | `Datum`, `MyDecimal`, `Time`, `Duration`, JSON | `types.Datum` (`datum.go`) |
| `pkg/server` | 18k | MySQL wire protocol, connection lifecycle | `Server`, `clientConn`, `IDriver` |
| `pkg/sessionctx` | 17k | session variables (~1000), statement context | `SessionVars`, `StatementContext` |
| `pkg/infoschema` | 16k | schema snapshot cache, memory tables | `InfoSchema`, `InfoCache` |
| `pkg/session` | 14k | session lifecycle, txn state machine, bootstrap | `session.Session` (`sessionapi`) |
| `pkg/meta` | 13k | meta KV encoding, `TableInfo`/`DBInfo`, autoid | `meta.Mutator`, `autoid.Allocator` |
| `pkg/domain` | 12k | per-instance singleton wiring all of the above | `Domain` (`domain.go`) |
| `dumpling/` | 9k | logical dump (separate concern) | — |
| `pkg/kv` | 3.4k | storage abstraction interfaces | `kv.Storage`/`Transaction`/`Snapshot`/`Request` |
| `pkg/distsql` | 2.5k | coprocessor request building/result streaming | `SelectResult`, `RequestBuilder` |

Total: ~948k non-test LOC (~1.6M including tests). The SQL-layer core (excluding br/lightning/dumpling) is ~750k LOC. Tests are half the corpus and are **the asset that makes a rewrite feasible**: `tests/integrationtest` (SQL-in/result-out golden files) and the MySQL-protocol test suites are implementation-language-neutral.

### The serialized boundaries

Four boundaries in the current system are already language-neutral serialized protocols. They are where a rewrite can be cut into independently verifiable pieces:

1. **Top: MySQL wire protocol** (`pkg/server`: `clientConn.dispatch`, `PacketIO`, binary prepared-statement protocol, auth plugins incl. `caching_sha2_password`). Everything above the `Session` interface is protocol handling.
2. **Bottom: kvproto/gRPC** — all TiKV/PD communication (`pkg/store` wraps `tikv/client-go`; region metadata via `metapb`, txn RPC via `kvrpcpb`).
3. **Bottom: tipb** — coprocessor DAG requests (`distsql.RequestBuilder` → `tipb.DAGRequest`; each physical operator implements `ToPB`). Plans are serialized to protobuf executor trees + expression trees and evaluated **by Rust code inside TiKV** (`components/tidb_query_*`). The pushdown allowlist (`pkg/expression/infer_pushdown.go`, ~96 function cases) is the exact contract of what already exists in Rust.
4. **Bottom: MPP** — TiFlash query fragments (`mpp.DispatchTaskRequest`/`EstablishMPPConnection` via `pkg/store/copr/mpp.go`, exchange operators, `local_mpp_coordinator`). Also protobuf; TiFlash (C++) doesn't care what language the coordinator is written in.

There is also a **sideways** contract: etcd, for DDL owner election (`pkg/owner/manager.go`), schema-version sync and server-info registry (`pkg/domain/infosync`). Standard etcd v3 API — `etcd-client` crates exist.

Everything between (1) and (2)/(3)/(4) — session, parse, plan, execute — is in-memory Go structs with no serialization boundary. That is why FFI-based incremental replacement of individual subsystems is a dead end (see Alternatives), and why the unit of migration must be **the whole SQL node**.

### Existing Rust assets

| Asset | What it gives us | Gap |
|---|---|---|
| TiKV `components/tidb_query_datatype` | `Datum`-equivalents: Decimal (ports `MyDecimal`), Time, Duration, JSON, collations, charset handling — battle-tested in production for years | Written for coprocessor evaluation; needs extraction into a shared workspace and API generalization |
| TiKV `components/tidb_query_expr` | Vectorized implementations of several hundred pushdownable builtin functions with MySQL semantics | Covers the pushdownable subset (~60-70% of builtins); non-pushdownable functions (e.g. `LAST_INSERT_ID`, sequence/lock functions, some JSON/GIS) must be written new |
| TiKV `components/tidb_query_executors` | Batch (vectorized) table scan, index scan, selection, hash/stream aggregation, TopN, limit executors | Storage-side only; join, sort spill, window, CTE, apply, index-lookup are TiDB-side and must be written new |
| `tikv/client-rust` | Raw + transactional KV client skeleton, PD client, region cache | **Explicitly not production ready** (unstable API, untested at scale). Missing/immature vs client-go: pessimistic-lock robustness, async-commit/1PC parity, follower & stale reads, resource control, batch-coprocessor. This is a build, not a reuse — treat client-go as the spec |
| `pingcap/tipb`, `pingcap/kvproto` | Protobuf contracts | Generate with `prost`; zero semantic work |
| `hparser-integration` (this branch) | A complete, freshly-audited encoding of TiDB's grammar as recursive-descent code (~40 files, arena-allocated), with a differential test harness against the goyacc corpus | It's Go, but it is structurally 1:1 transliterable to Rust — grammar knowledge is the expensive part and it was just re-derived |

## Strategy

### Rejected: big-bang rewrite

Build `tidb-rs` to full parity in a silo, switch when done. Rejected: at ~750k LOC of semantics-dense code, parity is 3+ years away; the Go tree doesn't freeze meanwhile (this merge alone brought 16 grammar changes in ~3 weeks); nothing is verifiable in production until the end; and the project produces zero value until it produces all of it. This is how rewrites die.

### Rejected: in-process FFI strangler (cgo → Rust subsystems)

Replace one Go package at a time with a Rust implementation behind cgo. Rejected: the internal boundaries are chatty, pointer-rich, in-memory interfaces (`LogicalPlan`, `Expression`, `sessionctx.Context` with ~1000 session variables). cgo cannot share Go pointers with Rust, so every call means serialization or handle indirection; the hot paths cross these boundaries millions of times per second. Two schedulers (Go runtime + tokio) in one process fight over threads. Each intermediate state costs more than it returns and none of the glue survives to the end state. The one place FFI *is* cheap — behind a serialized contract — is already a process boundary in TiDB's architecture.

### Chosen: cluster-level strangler — the Rust SQL node

`tidb-rs` is a new, separately-deployed TiDB server binary that joins an **existing production cluster** alongside Go TiDB nodes, speaking the four protocols that are already stable: MySQL wire on top; kvproto, tipb and MPP below. Rollout is a routing decision, rollback is instant (drain the Rust nodes), and every phase runs against real workloads.

### Chosen implementation method: source-first structural transition

The unit of work is a whole Go package or module plus its complete original
test/support inventory, not a new Rust interpretation of SQL behavior. For
each package, port the Go control flow, data representation, error surface, and
test vectors directly. Automated and agent-assisted translation is encouraged
for the mechanical work, but it must traverse the complete package inventory;
it may not stop after the branches needed by the current connected feature.

Generated ledgers inventory every production source and every original test,
fixture, runner, and expected result. Checked package manifests add the missing
acceptance boundary: they group all ledger rows, generated inputs/outputs,
testdata, helper programs, and build metadata for one upstream package. The
manifest is invalid if any package artifact is unowned or multiply owned.

Parallel work follows the package dependency DAG. A team claims one package or
a dependency-closed package group, declares the entire Rust write set, and
owns the complete package receipt. Independent teams must be disjoint across
Go source, original test/support artifacts, and mutable Rust paths. A large
package still has one umbrella claim, one integration owner, and one completion
decision. Inside that umbrella, subagents may implement disjoint Rust crates or
crate-local leaves in parallel when their write sets and interfaces are frozen.
Those internal branches are package-staging inputs, not independently
promotable claims or package-completion evidence. New partial Go-package
implementations are not integrated into the shared branch.

Agents do not pay a workspace build for every translated file. Package teams
run focused checks against a reused checkout-specific target. One integration
steward batches a frozen dependency frontier through a reused 12-job Cargo
target and runs workspace tests, strict all-target Clippy, formatting,
generated-inventory checks, differential rings, and required live gates. This
makes build frequency proportional to accepted package frontiers rather than
agent or file count.

The physical workspace separates package-owned leaves from shared routing
seams. Crate roots contain contracts, dispatch, and public re-exports only;
they do not retain feature behavior or private compatibility aliases after an
owner transition. Stewards serialize AST/parser routing, datatype/evaluation
context, planner/executor/session dispatch, server connection lifecycle,
transaction/storage authority, and workspace/evidence generation. Rust crates
may split a Go package for compile-time or ownership reasons, but crate layout
never weakens package-level acceptance.

Translation stops at a real language/runtime boundary. GC-coupled pools,
goroutine/channel lifecycle, pointer-identity tricks, and untyped `any`
registries must be expressed using Rust ownership, typed state, and explicit
task supervision; carrying those shapes over verbatim would preserve the Go
runtime's costs and create unsafe or unmaintainable Rust. The rule is therefore
**translate the complete package contract directly; redesign only the runtime
mechanism Rust cannot faithfully carry**. Redesigning SQL, optimizer,
transaction, storage, or wire behavior during transcreation is prohibited.

The existing `schema = "1"` records under `rust/workstreams/slices/` are
frozen legacy evidence from the earlier feature-slice process. They remain
readable for exact anchors, paths, claims, and receipts, but they are not valid
templates, dispatch units, dependency-completion signals, or package-coverage
evidence. No new schema-1 feature slice may be created or extended. Migration
associates its preserved evidence with the owning package manifest without
changing the original record or promoting ledger/status state.

The migration ladder for a `tidb-rs` node inside a mixed cluster:

1. **Shadow** — receives mirrored traffic from a proxy, executes reads, results are compared with the Go node's answers, discarded. Zero risk; maximum signal.
2. **Read-only compute node** — serves reads for sessions routed to it; unsupported statements are rejected at parse/plan time with a distinguishable error and the proxy retries them on a Go node. (TiProxy already does connection migration; the "capability negotiation" here is a static statement-class list per release, not a per-query oracle.)
3. **Read-write node** — full DML with the ported transaction client; still no background ownership.
4. **Full peer** — eligible for DDL ownership, stats ownership, background jobs (GC, autoanalyze, bindinfo). Go nodes drain away.

Mixed-cluster protocol obligations (these make or break the design, and get their own compatibility test suite):
- **Schema lease protocol**: `tidb-rs` must implement the schema-version lease and `mdl` (metadata lock) reporting exactly, or online DDL from Go-owned workers corrupts it. This is required already at step 2.
- **DDL job queue encoding** (`pkg/meta/model/job.go` JSON): read-compatibility at step 2, write at step 3, ownership at step 4.
- **Statistics storage tables** (`mysql.stats_*`): read at step 2, feedback/writes at step 4.
- **System/bootstrap tables**: `tidb-rs` never bootstraps a cluster until step 4; it joins existing clusters and reads the bootstrap version, refusing versions it doesn't know.

### What ships value early (the pragmatism test)

Each phase must be independently worth its cost even if the project stopped there:

- The **Rust parser** (Phase 0) is immediately reusable by any *native* consumer — a future Rust TiProxy, SQL-aware routing, static analysis tooling — as a normal Rust crate. (It is deliberately **not** offered as a cgo library for the Go tree; per the no-cgo rule, Go consumers that need it would run it out-of-process.)
- The **transaction client** (Phase 1) turns `tikv/client-rust` into a production-grade client — a standalone deliverable for the whole TiKV ecosystem, upstreamed.
- The **read-only compute node** (Phase 2) is a sellable product on its own: a low-jitter, low-memory analytics/read-replica endpoint for existing clusters — the shadow/differential infrastructure doubles as the QA gate for every later phase.

## Target architecture (`tidb-rs`)

Cargo workspace; crates mirror the boundaries that proved stable in Go, with corrections where Go's layout is historical accident (`pkg/util`'s 60 sub-packages get homes; `sessionctx` splits from `session`; types unify with parser types — eliminating the `pkg/types` vs `pkg/parser/types` split, one of several places where the Go module boundary forced a duplicate).

```
tidb-rs/
├── crates/
│   ├── tidb-proto        # prost-generated kvproto + tipb (shared with TiKV where possible)
│   ├── tidb-datatype     # EXTRACTED FROM TIKV: Decimal, Time, Duration, JSON, Datum,
│   │                     #   collation, charset (tidb_query_datatype becomes a shared crate)
│   ├── tidb-parser       # 1:1 transliteration of hparser: LexerBridge, HandParser,
│   │                     #   arena AST; token tables generated from the Go tables
│   ├── tidb-ast          # AST node types + visitor + restore (SQL regeneration)
│   ├── tidb-expr         # scalar/agg expressions; wraps tidb_query_expr for the
│   │                     #   pushdownable set, native impls for the remainder
│   ├── tidb-chunk        # columnar batch (Arrow-compatible layout), null bitmaps,
│   │                     #   spill; replaces pkg/util/chunk
│   ├── tidb-codec        # tablecodec + rowcodec (row format v2), key encoding
│   ├── tidb-catalog      # infoschema snapshot cache, meta mutator, TableInfo model
│   ├── tidb-txnkv        # transactional KV client: 2PC, async commit, 1PC,
│   │                     #   pessimistic locks, lock resolution, region cache,
│   │                     #   follower/stale read, resource control
│   │                     #   (client-go is the reference spec; upstream to client-rust)
│   ├── tidb-distsql      # coprocessor DAG building, batch-cop, result streaming
│   ├── tidb-planner      # logical/physical plans, rules, cost model, hints, bindings
│   ├── tidb-exec         # executor tree: joins, sort, window, CTE, apply,
│   │                     #   index-lookup; storage executors delegate to pushdown
│   ├── tidb-session      # session state machine, ~1000 sysvars (macro-generated
│   │                     #   from a declarative table), txn lifecycle, privileges
│   ├── tidb-protocol     # MySQL wire protocol, auth (incl. plugins), TLS
│   ├── tidb-ddl          # job state machine, schema lease/MDL, backfill (last!)
│   ├── tidb-stats        # histogram/CMSketch/TopN, sync load, autoanalyze
│   └── tidb-server       # binary: config, domain wiring, HTTP status, telemetry
└── difftests/            # differential harness: corpus replay Go-vs-Rust
```

Key technical decisions, with the Go pain point each one eliminates:

- **Concurrency**: tokio for IO (connections, RPC fan-out); **synchronous pull-based `Next()` inside executor trees** running on a CPU worker pool. Async executor trees would box every future and poison the whole tree with `.await`; TiKV's coprocessor already proved sync-batch-on-thread-pool is the right shape. An inventory of the Go tree shows the executor model is already pull-based, with a small set of operators (copIterator, hash join, hash agg, parallel sort, index-lookup/index-merge, projection, shuffle, apply) internally backed by push pipelines built from one repeating idiom: *fetcher goroutine → bounded worker fleet → result channel, with chunk buffers recirculating through "give-back" channels as combined backpressure + ordering tokens, and worker panics converted to errors on the same result channel the consumer reads*. That idiom maps 1:1 to Rust: bounded `mpsc` + owned chunk buffers (ownership transfer replaces the give-back convention — the buffer *must* come back because the worker can't keep it), scoped threads per operator, and `Result`-carrying channels instead of panic-recovery wrappers. The ~30 long-lived Domain background loops (schema lease sync, stats workers, auto-analyze, DDL scheduler, GC) become cancellable tokio tasks under a supervisor with the same named-goroutine leak-detection discipline (`WaitGroupEnhancedWrapper` → task-tracker).
- **Memory**: statement-scoped bump arenas for AST and plan nodes (the hparser `Arena`, but with compiler enforcement instead of discipline); `bytes::Bytes`-style refcounted buffers for chunk data; a hierarchical memory tracker preserved as-is conceptually (`memory.Tracker` → RAII guards, so untracked allocation paths become type errors rather than audit findings).
- **AST ownership**: nodes live in the arena, cross-references are typed indexes (as in hparser's slab design) — this sidesteps the classic Rust AST borrow fight and is *already the design of the Go branch this builds on*.
- **Error handling**: `thiserror` enums per crate; MySQL error codes/classes preserved verbatim (terror's code tables generated into Rust).
- **Session variables**: today `SessionVars` is an 800-line struct + string-keyed registry consulted via scattered getters. Declare each variable once in a table (name, type, scope, default, validator) and macro-generate the struct, the registry, and `SHOW VARIABLES` — eliminating the three-places-per-variable special case.
- **Plugins**: Go's `plugin` system (audit/authn plugins) is replaced by static feature crates + an extension trait registry. Dynamic loading is not carried over (it barely works in Go across builds anyway).
- **failpoints**: `fail-rs`, same as TiKV — the failpoint-gated tests port with their semantics intact. (Scale note: the Go tree has ~3,400 `failpoint.Inject` sites across ~570 files; porting them is mechanical but budgeted work, and only the sites covering ported subsystems come along.)

### Go-runtime couplings: what disappears vs. what needs redesign

An audit of Go-specific idioms in the tree sorts into three buckets:

**Disappears entirely (the rewrite pays for itself here):**
- `pkg/util/gctuner` (dynamic GOGC), `servermemorylimit`'s force-GC watchdog, `memory.global_arbitrator`'s heap reconciliation, `runtime.SetFinalizer` lifecycle hooks — the whole layer that exists to negotiate with the Go GC has no reason to exist. This is the "eliminate the special case" outcome: today's OOM story is manual accounting (`memory.Tracker`) *reconciled against* GC heap statistics; in Rust the manual accounting **is** the ground truth (RAII tracker guards + jemalloc stats + cgroup pressure for the global budget), and query-kill/spill actions hang off it exactly as today.
- `pkg/util/hack` (unsafe string↔bytes aliasing), the join executor's tagged-pointer scheme (metadata stuffed in pointer high bits), `chunk`'s byte-buffer reinterpretation casts — these are Go fighting its runtime for layout control. In Rust they are `&str`/`bytemuck`-style safe casts or plain enum layout.
- cgo: exactly one site in the tree (`lightning/manual` malloc-backed buffers, which exists to escape the GC) — moot.

**Ports cleanly:**
- `Datum` is already a hand-rolled tagged union → a real `enum`. `Expression`/`ast.Node` interfaces → enums + visitor derive. Atomic copy-on-write globals (config, infoschema B-trees) → `arc-swap`. Reflection is not load-bearing anywhere in the query path. No goroutine-local-storage tricks exist.
- The terror/errno tables (exact MySQL error numbers — the wire compatibility contract) are mechanical constant tables.

**Needs real redesign:**
- `sync.Pool`-based recycling (chunks, columns, plan nodes) is GC-integrated; Rust replaces it with explicit pools/arenas — better behavior, but every call site is a decision.
- The type-unsafe grab-bags — `sessionctx.SetValue(fmt.Stringer, any)`, `kv.Transaction.SetOption(int, any)` — must be enumerated into typed structs up front. This is a forced design improvement but real up-front work.
- Go `plugin` (.so dlopen for audit/authn) → static feature crates (already the fallback path in Go) or a C-ABI extension boundary.
- `init()`-time global registration (the sysvar registry, `Domain`-per-storage map) → explicit wiring / `OnceLock` registries — see the sysvar macro-table decision above.

## Verification: differential everything

Correctness is the whole ballgame; the strategy is to never trust a port, only
a comparison. The four rings below are the target acceptance system. Existing
static-corpus checks are preserved as evidence, while package receipts require
the complete package inventory and every applicable ring; cluster shadowing,
full plan parity, and real-TiKV transaction testing remain future gates.

1. **Parser ring**: replay every statement in `tests/integrationtest/t/**`, the parser unit corpus, and a grammar-aware fuzzer through Go-hparser and tidb-parser; compare restored SQL text, AST digests, error code + message + position. The current checked oracle has zero Rust parse failures, restore mismatches, or false accepts; its one remaining actionable row is the pinned Go restore failure for `json_memberof()`, and the 99 dual rejections are explicit rejection parity.
2. **Plan ring**: same corpus + stats fixtures; compare `EXPLAIN` output and plan digests statement-by-statement. The zero-diff gate remains mandatory before traffic; existing planner seed evidence does not complete the `pkg/planner` package inventory or open the full optimizer ring.
3. **Result ring**: begin with static Go-backed query/expression/table corpora, then add shadow traffic in real clusters (step 1 of the ladder) and `copr-test`-style randomized differential (random schema + random queries, TiDB-Go vs tidb-rs vs MySQL as the 3-way oracle).
4. **Transaction ring**: begin with source-owned storage primitives and fault/error boundaries, then run the txnkv crate's Jepsen and client-go integration suite against real TiKV before any write path opens; error-injection (fail-rs) tests are ported from client-go's.

CI keeps both implementations honest during the multi-year overlap: every grammar/behavior change to Go TiDB must land with its corpus entry, and the corpus is the contract — exactly how this merge's 16 grammar features were caught and ported into the hand parser, which is the process working as designed at small scale.

## Phasing

Ordered by (value ÷ risk), each phase gated by its differential ring:

### Completion bar for TLS and transactions

The bounded plaintext read node and one normal optimistic 2PC path are stepping stones, not compatibility endpoints. Phase 2 includes a real MySQL TLS transport on the production listener: `CLIENT_SSL` negotiation upgrades the accepted socket before credentials are read, configured CA/certificate/key material is validated, TLS 1.2/1.3 policy and `require_secure_transport` match TiDB, certificate reload and AutoTLS have explicit lifecycle ownership, and stock MySQL clients prove plaintext rejection, encrypted authentication/query/prepared traffic, certificate verification, and reload. A parsed SSLRequest or an asserted `TransportKind::DirectTls` without a completed cryptographic handshake is not TLS support.

Phase 3 includes the complete concrete TiDB/client-go transaction and batch-KV behavior required by the original source/test inventory, not only the currently admitted autocommit optimistic 2PC subset. This includes region-aware BatchGet/scan/write batching, explicit transaction state and statement staging, optimistic retries and cleanup, lock resolution and TTL heartbeats, pessimistic locks/rollback, primary and secondary recovery, 1PC and async commit eligibility/fallback, savepoint/option semantics used by TiDB, cancellation and undetermined-result boundaries, and the relevant failpoint/fault cases. All modes reuse one PD, RegionCache, lock resolver, BatchCommands transport, retry authority, and shutdown lifecycle. A second transaction client, an in-memory backend, or a mock transport cannot satisfy implementation or acceptance. Real-TiKV differential, fault-injection, Jepsen, sysbench, and TPC-C gates remain mandatory before the write phase is complete.

| Phase | Deliverable | Reuses | New Rust LOC (est.) | Gate |
|---|---|---|---|---|
| 0 | Complete parser/AST/datatype package frontier | hparser design; TiKV datatype crate | 60-80k | Complete package receipts; parser zero-diff on accepted inputs; explicit rejection parity; documented oracle failures |
| 1 | Complete KV/codec/catalog read package frontier | client-rust skeleton; client-go as spec | 50-70k | Complete package receipts; transaction read ring; Jepsen for reads and stale reads |
| 2 | Read-only SQL-node package frontier: protocol, session, planner, executor, expression, and DistSQL | tidb_query_executors patterns; tipb | 250-350k | Complete package receipts; plan ring zero-diff; shadow traffic; sysbench-read and TPC-H performance at least Go |
| 3 | Read-write package frontier: full transaction lifecycle, DML, and statistics writes | Phase 1 client | 80-120k | Complete package receipts; Jepsen full; TPC-C parity; shadow-write comparison |
| 4 | Full-peer package frontier: DDL, metadata, domain, ownership, and bootstrap | — | 80-120k | Complete package receipts; mixed-cluster DDL suite; ownership handover drills; long-run canary |

Estimated total: 500-700k Rust LOC (Rust runs denser than Go for this code;
enum-based AST/plan nodes and macro-generated variable/function registries
remove much of Go's repetition). The staffing and calendar numbers are
planning estimates, not commitments. A calendar-parallel Go tree means the
corpus-sync CI above is not optional at any point. Current implementation and
ledger state live only in `rust/STATUS.md`, `rust/HANDOFF.md`, package
manifests, and active ExecPlans.

DDL is deliberately last: it is the only subsystem where a bug destroys user data through a background process (reorg backfill), it has the deepest coupling to cluster-wide invariants (schema lease, MDL), and it benefits most from the longest shadow period.

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Semantic drift in the long tail (sql_mode interactions, collation edge cases, zero-date/`DIV`/coercion quirks, ~1000 sysvars) | High — this is the rewrite-killer | Differential rings; port-don't-redesign; MySQL 3-way oracle; corpus-as-contract CI |
| `tidb-txnkv` maturity (client-rust is not production ready) | High | Treat as Phase 1 first-class deliverable with client-go as executable spec; Jepsen; upstream so TiKV org co-owns it |
| Plan regressions (cost model differences → customer-visible perf cliffs) | High | Plan-digest zero-diff gate; bindings/hints work day one, so escapes exist |
| Two codebases in flight for years (feature velocity tax) | Medium-High | Corpus-sync CI makes divergence a build failure, not a discovery; statement-class capability list keeps the proxy honest; org must accept the tax explicitly — this design makes it visible, not free |
| Mixed-cluster protocol subtleties (schema lease, MDL, DDL queue) | Medium | Dedicated mixed-cluster test suite from Phase 2 day one; `tidb-rs` refuses unknown bootstrap versions |
| Rust talent pool vs TiDB-internals talent pool intersection | Medium | Phase 0/1 are the training ground (bounded, spec-rich); TiKV team seeds reviews |
| Ecosystem tools assuming Go TiDB internals (BR backup of stats, Lightning checkpoint tables) | Medium | Tools speak SQL/KV protocols already; compatibility tests per tool in Phase 3 |

## Alternatives considered

- **Big-bang and in-process FFI**: rejected above (Strategy).
- **Build on Apache DataFusion** instead of porting the planner/executor: DataFusion is an excellent engine but its SQL semantics are not MySQL's, its optimizer is not TiDB's, and bending it to bug-for-bug MySQL compatibility plus TiKV pushdown plus TiDB's plan-stability surface (hints, bindings, plan digests) is a larger delta than porting. Selectively borrowing (Arrow memory layout for `tidb-chunk`, spill machinery) is in scope; adopting the framework is not.
- **Blind mechanical transpilation without contract gates or runtime adaptation**: rejected. It copies GC-shaped ownership graphs, goroutine lifecycle, and untyped registries into Rust without proving behavior. This does not weaken the required whole-package boundary. The chosen method is source-grounded complete-package transcreation: mechanically translate the entire package and original test/support inventory, adapt only Go-runtime mechanisms to idiomatic Rust, and require package inventory closure plus differential and live gates.
- **Rewrite only the hot subsystems, keep Go for the rest, permanently**: permanent FFI seams in-process (rejected above), or a permanent two-binary architecture whose operational complexity outlives the migration's benefits. Acceptable only as a fallback if Phase 3+ stalls: the Phase-2 read-only node is designed to be a stable stopping point.

## Appendix: the query path being ported

The verified end-to-end call path in today's Go tree, with the interface at each handoff — each arrow is a crate boundary in `tidb-rs`:

```
listener.Accept (pkg/server/server.go)                          [tidb-protocol]
  → clientConn.Run / PacketIO / dispatch(ComQuery)              [tidb-protocol]
  → TiDBContext.ExecuteStmt → sessionapi.Session.ExecuteStmt    [tidb-session]
  → Parser.ParseSQL → []ast.StmtNode                            [tidb-parser / tidb-ast]
  → executor.Compiler.Compile → planner.Optimize                [tidb-planner]
      logicalOptimize (≈35 ordered rules) → physicalOptimize
      → FindBestTask → PhysicalPlan
  → ExecStmt.Exec → exec.Executor.Open/Next(*chunk.Chunk)/Close [tidb-exec / tidb-chunk]
  → PhysicalPlan.ToPB → tipb.DAGRequest                         [tidb-distsql]
      + RequestBuilder.Build → kv.Request
  → kv.Client.Send → copr.CopClient → buildCopTasks             [tidb-txnkv]
      (region split) → copIteratorWorker → tikvrpc.CmdCop → TiKV
  ← selectResult.Next ← chunk decode ← tipb.SelectResponse      [tidb-distsql]
  ← recordSet.Next ← clientConn.writeChunks                     [tidb-protocol]

writes: DML executors → kv.MemBuffer (staging per statement)    [tidb-txnkv]
  → tikvTxn.Commit → client-go 2PC (prewrite/commit)            [tidb-txnkv]
```

The `exec.Executor` interface (`Open/Next/Close` over `*chunk.Chunk`) and `kv.Client.Send` are the two contracts that carry ~all of the runtime's data flow; their Rust equivalents are the first APIs to stabilize in Phase 2.

## Unresolved questions

- Whether `tidb_query_*` crate extraction lands in the TiKV repo, a new shared repo, or is vendored — needs TiKV maintainer buy-in early (it's the Phase 0 critical path).
- Proxy layer: TiProxy capability-based routing needs its own small design doc (statement-class negotiation, session pinning, retry-on-unsupported semantics).
- MySQL 9.x feature tracking during the migration window (who implements new features twice, and when does Go TiDB feature-freeze).
- Productionizing the Rust TiKV/PD client: upstream ownership, compatibility scope against client-go, and the first real-TiKV test environment.

## Resolved execution choices

- The implementation currently lives in the in-repo `rust/` workspace, with
  differential corpora, ledgers, and evidence alongside the crates. A future
  repository split must preserve that source/test ownership history and exact
  corpus snapshots.
- The migration unit is one complete upstream Go package or module plus its
  complete original test/support inventory; the deployment unit remains a
  standalone SQL node. Package-shaped translation enables dependency-DAG
  parallelism, while the serialized cluster boundary avoids an in-process FFI
  seam.
- Existing schema-1 feature slices are frozen legacy evidence. They are not
  templates, dispatch units, dependency-completion signals, or evidence of
  package coverage.

### Parallel execution contract

Parallelism is organized around the whole-package dependency DAG. One checked
package manifest joins every authoritative production source, every original
test/support artifact, all generated and build inputs, the complete Rust write
set, focused targets, differential/live gates, and explicit package
dependencies. Only an inventory-complete, dependency-ready package or
dependency-closed package group may be dispatched. Claims are atomic and reject
overlap across Go sources, test/support artifacts, and mutable Rust paths.

A package team owns all Rust implementation paths, mirrored package tests, and
owner-named evidence declared by its package manifest. Read-only inventory and
review audits may run in parallel, and write-disjoint Rust-crate subteams may
implement behind the package's single umbrella claim. Their work merges only
into the package staging branch and cannot be integrated independently. Crate
routing, test registration, manifests, generated inventories, and current
status are steward-owned integration products. Claims coordinate work but
cannot hide, waive, or promote a ledger obligation.

Validation has three scopes: focused package gates, a static merged-inventory
gate, and one full workspace test/Clippy/differential/live gate after a
dependency frontier freezes. A successful compilation, bounded query, or
differential sample is not package progress by itself. Progress is an exact
package inventory closed by an immutable receipt, with explicit remaining
package gaps. The executable protocol lives in `rust/PARALLEL.md` and
`rust/docs/operations/validation.md`.

Each package claim uses a `codex/<package-owner>` branch in a writable in-repo
worktree. The dispatcher acquires the complete claim before worktree creation;
package source, test/support, and Rust write sets are the isolation boundary.
The integrator alone runs the persistent 12-job shared gate and releases claims
only from an immutable successful package receipt.
