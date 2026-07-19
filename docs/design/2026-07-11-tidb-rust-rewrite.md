# TiDB Rust Rewrite: Design

- Author(s): qiliu
- Discussion PR: TBD
- Tracking Issue: TBD

## Abstract

This document proposes a design for rewriting the TiDB SQL layer in Rust. It is grounded in a study of the current codebase (~950k lines of non-test Go across `pkg/`, `br/`, `lightning/`, `dumpling/`, `cmd/`) and builds directly on the `hparser-integration` work: the hand-written, arena-allocated SQL parser that replaced the goyacc parser is the architectural prototype for the Rust parser, and its differential-testing methodology is the verification model for the whole migration.

The core strategic insight is that **TiDB SQL nodes are stateless**. Multiple TiDB versions already coexist against the same TiKV/PD cluster during rolling upgrades. A Rust TiDB (`tidb-rs`) can therefore join a live cluster as an additional SQL node and take traffic incrementally — no FFI embedding, no big-bang cutover. Migration happens at the cluster topology level, gated by differential testing, not inside a hybrid binary.

## Source-transit contract

The rewrite transits behavior from Go before it redesigns implementation. For
each leaf, the owning Go file and symbol are the contract: preserve branch
ordering, constants, error text, hash/equality framing, wire/SQL text, and
edge-case arithmetic first; then expose a Rust API that is dependency-closed
and easy to compose. Every leaf carries an exact original-test anchor and
source/test evidence row. The generated ledgers keep one owner per Go source
file and per test obligation, so fixture coverage cannot be mistaken for test
parity and parallel agents cannot silently claim the same behavior.

Pure policy, metadata, codec, and formatting leaves are migrated first. Live
session, transaction, catalog, storage, RPC, DDL, and cluster behavior is
attached only after its source contract is covered by a focused Rust test and
the corresponding integration seam exists. This is a staged source transit,
not a second implementation invented from the design document; the Go suite
remains the behavioral oracle until the Rust suite and runtime replace it.

## Implementation status

Live implementation status is generated in [`rust/STATUS.md`](../../rust/STATUS.md).
Current execution work, dependencies, and validation criteria are maintained in
the [active ExecPlan](../../rust/workstreams/plans/2026-07-read-path-25.md).

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

The unit of work is a bounded Go source domain plus its existing tests, not a
new Rust interpretation of SQL behavior. For each domain, port the normal Go
control flow, data representation, error surface, and test vectors directly;
the Go implementation is the specification and its tests are obligations.
Automated/agent-assisted translation is encouraged for this mechanical work,
especially parser branches, AST restore methods, builtin dispatch, and test
tables. Every translated leaf must then pass the relevant Go-oracle
differential ring before it is considered covered.

Two generated ledgers make that rule executable: one inventories every
production Go source owner and routes it to a target crate; the other
inventories every original test entry point, fixture, runner, and expected
result. Parallel work is dispatched only from the intersection of those
queues, so neither an implementation file nor a test obligation can disappear
behind package-level progress estimates.

Agents do not pay a workspace build for each translated leaf. They claim a
complete source/test envelope, declare every Rust output path, inspect and
translate against Go, and run only zero-build/static checks. Exact Go anchors,
test anchors, dependency capabilities, and Rust output paths are atomic claim
dimensions. A single integration steward then batches the accepted envelopes
through one reused 12-job Cargo target and runs workspace tests, strict
all-target Clippy, formatting, and the generated-ledger/differential gates
once. This makes build frequency proportional to accepted parallel waves, not
agent count or file count.

The physical workspace follows the same ownership unit. A source-domain
envelope owns its typed AST/data shape, implementation, mirrored original
tests, differential selector or corpus, and sparse ledger fragment together.
Crate roots contain contracts, dispatch, and public re-exports only; they do
not retain feature behavior or private compatibility aliases after an
extraction. Shared routing is serialized through four narrow steward seams:
AST/parser routing, executor state/dispatch, datatype/evaluation context, and
workspace/evidence generation. Parser-only, result, and transaction evidence
are separate Cargo packages so an agent can compile its source family while a
different behavior crate is temporarily changing. This is the directory
structure that turns source translation into real parallelism instead of
several agents contending on the same roots.

Translation stops at a real language/runtime boundary. GC-coupled pools,
goroutine/channel lifecycle, pointer-identity tricks, and untyped `any`
registries must be expressed using Rust ownership, typed state, and explicit
task supervision; carrying those shapes over verbatim would preserve the Go
runtime's costs and create unsafe or unmaintainable Rust. The rule is therefore
**translate contracts directly; redesign only the implementation mechanism
that Rust cannot faithfully carry**. Redesigning SQL, optimizer, transaction,
or wire behavior during a transition is prohibited.

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
a comparison. The four rings below are the target acceptance system, but they
are staged: the current workspace has source-owned leaf and static-corpus
gates, while cluster shadowing, full plan parity, and real-TiKV transaction
testing remain future gates.

1. **Parser ring**: replay every statement in `tests/integrationtest/t/**`, the parser unit corpus, and a grammar-aware fuzzer through Go-hparser and tidb-parser; compare restored SQL text, AST digests, error code + message + position. The current checked oracle has zero Rust parse failures, restore mismatches, or false accepts; its one remaining actionable row is the pinned Go restore failure for `json_memberof()`, and the 99 dual rejections are explicit rejection parity.
2. **Plan ring**: same corpus + stats fixtures; compare `EXPLAIN` output and plan digests statement-by-statement. The zero-diff gate remains mandatory before traffic, but the current implementation only covers narrow planner source leaves; the full optimizer ring is not yet open.
3. **Result ring**: begin with static Go-backed query/expression/table corpora, then add shadow traffic in real clusters (step 1 of the ladder) and `copr-test`-style randomized differential (random schema + random queries, TiDB-Go vs tidb-rs vs MySQL as the 3-way oracle).
4. **Transaction ring**: begin with source-owned storage primitives and fault/error boundaries, then run the txnkv crate's Jepsen and client-go integration suite against real TiKV before any write path opens; error-injection (fail-rs) tests are ported from client-go's.

CI keeps both implementations honest during the multi-year overlap: every grammar/behavior change to Go TiDB must land with its corpus entry, and the corpus is the contract — exactly how this merge's 16 grammar features were caught and ported into the hand parser, which is the process working as designed at small scale.

## Phasing

Ordered by (value ÷ risk), each phase gated by its differential ring:

| Phase | Deliverable | Reuses | New Rust LOC (est.) | Gate | Current status (2026-07-19) |
|---|---|---|---|---|---|
| 0 | `tidb-parser` + `tidb-ast` + `tidb-datatype` extraction | hparser design 1:1; TiKV datatype crate | 60-80k | Zero Rust regressions on accepted inputs, explicit rejection parity, and documented oracle failures | In progress: parser ring is clean except the pinned Go `json_memberof()` failure; source/test obligations remain |
| 1 | `tidb-txnkv` + `tidb-codec` + `tidb-catalog` (read) | client-rust skeleton; client-go as spec | 50-70k | Transaction ring (read path); Jepsen for reads/stale reads | A production bounded read client now owns real PD TSO/region/store discovery, RegionCache routing/recovery, BatchCommands-first TiKV Coprocessor dispatch, and table-key/row decoding. Dynamic catalog discovery plus broad read/stale-read and Jepsen parity remain open. |
| 2 | Read-only compute node: protocol + session (read subset) + planner + exec + distsql | tidb_query_executors patterns; tipb | 250-350k | Plan ring zero-diff; shadow → read traffic in staging; perf ≥ Go on sysbench read + TPC-H | The first topology-resilient bounded vertical slice is live: authenticated stock MySQL clients → Rust COM_QUERY → parser/planner/clustered signed-BIGINT range detachment/tipb `[TableScan, Selection]` → real PD TSO/RegionCache → BatchCommands-first real TiKV → exact clustered-key and stored-column rows. Direct projections, all six comparisons, reversed operands, conjunctions, signed extremes, split `!=`, contradictions, and access-plus-residual predicates are proven; zero ranges avoid TSO/transport. Eight sessions overlap through a fixed worker pool and one process authority; persistent sessions survive A→B→C→same-address-restarted-B leader churn, exact channel/stream identity is observable, and blocked-query SIGTERM shuts connections→RegionCache→TiKV→PD down fallibly. Phase 2 remains incomplete: the executable is loopback/plaintext and static-catalog with only bounded signed-BIGINT predicates; temporal SQL/Duration, decimal/enum/set/vector/native CHBlock, typed expression/nested FullSchema mappings, NULL/coercion/unsigned semantics, `OR`/functions/arithmetic, index/common-handle/partition ranges, joins/aggregation/order/limit, grants/TLS/prepared statements, plan-ring zero-diff, shadow traffic, and performance gates remain open. Campaigns 23 and 24 are Ready-integrated; Campaign 25 has two disjoint roots ready for parallel implementation. |
| 3 | Read-write: full txn lifecycle, DML, `tidb-stats` write path | Phase 1 client | 80-120k | Jepsen full; TPC-C parity; shadow-write comparison | Not started |
| 4 | Full peer: `tidb-ddl`, background ownership, bootstrap | — | 80-120k | Mixed-cluster DDL suite; ownership handover drills; long-run canary | Not started |

Estimated total: 500-700k Rust LOC (Rust runs denser than Go for this code; enum-based AST/plan nodes and macro-generated variable/function registries remove much of Go's repetition). The staffing and calendar numbers are planning estimates, not commitments; they must be re-estimated after the first connected read-only vertical slice. A calendar-parallel Go tree means the corpus-sync CI (above) is not optional at any point.

Campaign 24 is Ready-integrated: the shared gate and repository lint passed,
all six receipt-backed members were released, and generated status is current.
Campaign 25 should now dispatch its two ready roots in parallel, then unlock
relation binding, FullSchema join planning, join execution, and the live
two-relation proof through exact evidence prerequisites.

After that boundary, carry the bounded multi-relation catalog binding into
general typed expressions, FullSchema redundant-column mappings, typed
ON/USING semantics, and the broader uncompressed COM_QUERY → server dispatch
→ session → DistSQL → metadata/row/EOF statement flow. Attach response events
and status snapshots to the real session/error-context and wire writers;
complete typed default/columnar/CHBlock codecs, temporal/JSON/enum/set/vector
Datum, full charset/session formatting, and intermediate-output routing. Do
not treat another collection of isolated leaf ports as a substitute for the
end-to-end gate.

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
- **Blind whole-package transpilation without contract gates**: rejected. It copies GC-shaped ownership graphs, goroutine lifecycle, and untyped registries into Rust without proving behavior. This is distinct from the chosen source-first structural transition: agentic/mechanical translation of a bounded Go domain plus its tests is the default implementation accelerator, provided each leaf is differentially verified and runtime-only mechanisms are translated into their idiomatic Rust equivalent.
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
- The migration unit is a source-domain envelope; the deployment unit remains
  a standalone SQL node. These are complementary decisions: source-shaped
  translation enables parallel work, while the serialized cluster boundary
  avoids an in-process FFI seam.

### Parallel execution contract

Parallelism is organized around dependency-ready vertical slices, not isolated
helper methods, horizontal file types, or whichever Go file has the highest
raw queue score. One checked slice joins one or more authoritative Go source
owners, every directly owned original test/support obligation, the Rust leaf
and test destination, a focused target, its immediate consumer, and explicit
prerequisites. A whole-slice dependency must be `covered`; a capability inside
a broader partial family instead names one exact source/test ledger anchor,
its evidence owner, and the required `PARTIAL` or `COVERED` minimum. Readiness
must never be inferred from another row owned by the same agent. Only a
`ready` slice whose prerequisites are satisfied may be dispatched. Its
multi-source claim is atomic, must exactly match the checked slice, and must
reject every overlapping source or test anchor before either agent edits code.

Feature agents own only their domain leaves, focused tests, and owner-named
evidence fragments. Crate routing, test registration, generated inventories,
and current progress snapshots are deterministic integration products rather
than recurring feature-agent edits. The checked ledgers remain authoritative;
claims coordinate active work but cannot hide, waive, or mark an obligation
covered.

Validation has three scopes: a focused leaf gate, a static merged-evidence
gate, and one full workspace test/Clippy gate after a substantial multi-domain
batch freezes. A numbered wave, successful compilation, or a differential
sample is not progress by itself. Progress is the exact reduction of
untriaged/partial source and original-test obligations, with `COVERED` reserved
for a completely audited source family and its required differential ring.
The executable protocol and commands live in `rust/PARALLEL.md` and
`rust/docs/operations/validation.md`; checked dispatch records live under
`rust/workstreams/slices/` and are validated by
`rust/scripts/work-unit-queue.py check`.

The normal integration batch is a checked campaign: one or more three-agent
implementation/review rotations covering at least nine authoritative production
files or fifty original test/support obligations before the expensive shared
gate. The dispatcher keeps two rotations (six disjoint ready slices) prepared
ahead when dependencies permit, but a coherent three-slice campaign may freeze
once it meets the obligation floor. Agents translate directly from the owned Go
code and tests and run static/focused checks; the integrator alone runs the
persistent 12-job workspace gate after freeze.

Each claimed feature slice uses a `codex/<slice>` branch in a writable in-repo
worktree. The primary dispatcher acquires claims before worktree creation;
checked source, test, and Rust write sets remain the semantic isolation boundary.
Worktrees share dependency and build caches, shared runtime seams stay frozen
during each rotation, and claims are released only from an immutable successful
integration receipt.
