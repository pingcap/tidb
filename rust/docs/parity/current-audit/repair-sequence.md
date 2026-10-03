# Repair sequence for Go/Rust structural parity

This is the current work map for the [living ExecPlan](../../full-structural-parity-execplan.md), dated 2026-10-01. It assigns every one of the **77 known unresolved findings** to one primary workstream. A workstream groups related responsibilities; it is **not** a package acceptance unit or an instruction to port only the named functions. The [register](structural-findings.json) remains the finding/status authority and contains the precise Go and Rust evidence.

## Baseline and scope


Original planning started at TiDB integration `4285385fad20855487ec1d8ff113290d48f949a5`, freshly fetched Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, and native client-rust `6f663b396552eec6d1bfad76b65f813e317884a4`. The normative pins come from that Go master's go.mod: client-go `v2.0.8-0.20260928031501-8edb23f6c7ee`, PD client `v0.0.0-20260805103528-afa43111d149`, kvproto `v0.0.0-20260820070758-623e58e60fa9`, TiPB `v0.0.0-20260908093239-fed7bc47c39d`, etcd API `v3.5.15`, and Ristretto `v0.1.1`.

Current reconciliation: [review after removals](post-removal-structural-review.md)
at integration `68d6de685a5e58c559a861ec7b85d10bc8a2aa60`, unchanged Go master and
native master `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`. All 77 IDs below remain
open/partial. The old repartition, IMPORT and cluster-fixture implementations
are retired; their complete Go owners remain acceptance obligations. Use this
current review for live-versus-historical behavior and repair priorities.


The inventories currently enumerate 856 TiDB, 41 client-go, 41 kvproto, 24 PD-client and seven etcd-API package directories, plus 83 Rust crates. All 969 directories remain in the coverage process, including packages without a listed finding. Other external modules need the same inventory before acceptance. There is no claim that 77 is every possible semantic defect. The separate fresh-master range-count-variable delta belongs in the affected whole-package scopes even though it is not a new structural ID.

Read a selected package's doc.go first if present. Before implementation, take its complete artifact list from the coverage JSON, add its required external dependency closure, map every production entrypoint and original test/support/fixture/generated/build/platform artifact, and record an integration decision for every item. Preserve meaningful Rust ownership and memory management while reproducing Go's externally observable contracts. Original variants are obligations to implement or document as unaccepted, not permission to invent behavior.

## Complete finding assignment


The table gives primary ownership, not a complete Go import graph. W03, W05, W06, W07, W08, W09, W10 and W11 overlap broad packages such as planner/core, executor and Domain. Gather all obligations for the same Go package into **one atomic inventory, integration decision and acceptance receipt**. No parent-package certification follows from a completed leaf repair. Runtime activation dependencies below supplement, rather than replace, the actual source dependency graph.

| Workstream | Finding IDs | Count |
| --- | --- | --- |
| W01 Native PD ownership | P03, P06, P07 | 3 |
| W02 Native KV and TiDB storage consumers | T02, T04 | 2 |
| W03 Shared SQL, transaction and table execution | S01, S02, S03, S04, A01, E02, E03, K01, K03, T01 | 10 |
| W04 Schema, identity and GC safety | O01, O02, O03, O09, I04, K02 | 6 |
| W05 Durable DDL, TiFlash placement and affinity | D01, D02, D03, D04, D05, D06, D07, D08, D09, D10, D11, F01, F02, F03, O14 | 15 |
| W06 Account, wire, configuration and administration | A02, A03, A04, N01, N02, N03, N04, N05 | 8 |
| W07 Shared cache dependency and consumers | B01, B02, C02, C03, C04 | 5 |
| W08 Optimizer, typed expressions and executor workers | Q01, X01, E04, E06 | 4 |
| W09 MPP and TiFlash Compute | M01, M02, M03, M04, M05 | 5 |
| W10 Domain job, resource and bulk-operation services | O04, O05, O06, O07, O10, O15, E05, E07 | 8 |
| W11 Live runtime information and observability | I01, I02, I03, O08, O11, O13, O16, O17, O18, O19 | 10 |
| W12 Inference runtime | X02 | 1 |

The eight repaired register entries remain outside this queue: C01, E01, O12, P01, P02, P04, P05 and T03. Their existing regressions remain mandatory when affected. Broader routing work does not reopen the five already-repaired transaction review defects or the resolved cleanup-lifetime approval issue.

## W01 — Native PD ownership


**Complete source owners to scope:** Pinned PD client root, `clients/tso`, `servicediscovery`, and their complete dependency packages.

**Migration and removal:** Native `src/pd/client.rs`, `retry.rs`, `timestamp.rs`: own service discovery, request deadlines, bounded synchronization and retained/joined workers. Preserve Go fallback and reconnection contracts; remove network-duration global write locking and discarded worker ownership.

Current status is reconciled in the [post-removal review](post-removal-structural-review.md):
native timestamp deadlines and retained/joined stream retirement are implemented.
Keep those owners; public PD shutdown and complete parent ownership remain open.
Go default pick_first reconnects on demand after an established connection becomes
Idle. Initial dialing/retry and blocking readiness remain distinct contracts.

**Integration gate:** All root APIs and configuration variants remain functional; stalled streams time out, pending callers complete on close, reconnect cannot resurrect a closed owner, and unrelated metadata requests overlap without racing leader replacement.

## W02 — Native KV and TiDB storage consumers


**Complete source owners to scope:** Pinned client-go `internal/locate`, `internal/client`, `tikvrpc`, `tikv`, `txnkv` packages and their dependency closure; TiDB `pkg/store/driver` and `pkg/store/copr`.

**Migration and removal:** Native location/cache/transport owners replace the corresponding algorithms behind TiDB `tidb-txnkv/src/driver/client_bridge.rs::ClientPd`. Migrate transaction, snapshot, coprocessor and later MPP consumers, including latency/health/tick events. Keep TiDB distributed-task planning and SQL transaction policy.

**Integration gate:** Shared routing handles region splits, epoch changes, leader/store loss, health feedback contention, explicit retry exhaustion and cancellation. Delete each competing TiDB owner only after every consumer uses the complete native contract; MPP consumers in W09 gate final transport retirement.

## W03 — Shared SQL, transaction and table execution


**Complete source owners to scope:** TiDB `pkg/session`, `pkg/sessiontxn` and providers, `pkg/planner/core`, `pkg/executor`, `pkg/table/tables`, `pkg/meta/autoid`, and their full dependencies.

**Migration and removal:** Unify the configured one/two-table session routes with normal Session/compiler/storage adapters. Preserve resolved privilege/FK/handle metadata through `tidb-executor/src/driver/physical_builder.rs::execute_dml_source`. Move assertion, auto-ID and generated-value conversion decisions to table/mutation-context owners; remove the second SQL interpreter, static catalog and generic insertion policy after caller migration.

The concurrent configured-TopN commit 0b2cf64069 leaves two exact tie-order tests failing; `pd-retry-owner-repair.md` records isolation against the pre-merge file. Include those fixtures, the remaining heap ordinal and stable-sort policy in this complete owner migration rather than restoring Go-absent final ordering.

**Integration gate:** All selectable storage modes execute text/prepared/internal statements through shared owners, preserve warnings/errors/rollback and row identity, migrate session state, and use the shared historical timestamp/schema provider. Test eager/pessimistic versus lazy assertions separately. Historical reads require W04; complete expression/optimizer obligations also include W08.

## W04 — Schema, identity and GC safety


**Complete source owners to scope:** TiDB `pkg/domain`, `pkg/domain/infosync`, `pkg/infoschema`/`issyncer`, `pkg/session`, `pkg/store/driver`, `pkg/store/gcworker`, `pkg/table/tables` and their dependencies.

**Migration and removal:** Compose server-ID allocation/renewal/loss, versioned bootstrap/upgrade, versioned/lazy schema cache, cached-table read/write leases and min-start-TS reporting. Connect the server GC worker only after safe-point protection is complete. Replace boolean bootstrap and latest-only catalog assumptions in their callers.

**Integration gate:** Two nodes retain unique identity; upgrades resume safely; old-schema reads remain valid; leased cached reads cannot survive conflicting writes; active transactions/cursors and required internal sessions protect their timestamps. Validate reporting with a Go peer before enabling Rust GC. DDL delete-range registration and worker execution jointly gate cleanup.

## W05 — Durable DDL, TiFlash placement and affinity


**Complete source owners to scope:** TiDB `pkg/ddl`, `pkg/ddl/jobsubmit`, reorganization dependencies, `pkg/domain/infosync`, `pkg/domain/affinity`, and GC consumers.

**Migration and removal:** Replace non-CHECK direct publication in `cluster_session_node/ddl.rs::RealClusterDdl::execute` with typed durable job submission. Complete scheduling, state/error persistence, online backfill, dependency/rollback and schema barriers. Move rule creation to DDL and retired-rule cleanup to GC; remove poll-owned classic reconciliation, direct range deletion and incomplete live seed candidates.

**Integration gate:** Crash/restart, owner handoff, pause/resume/cancel, retry exhaustion, MDL on/off, multi-action rollback, partitions and reset semantics match Go. Keep D09/D10 disabled seeds until their whole owners qualify. NextGen refresh remains where Go has it. Online reorganization modes requiring DXF also require W10; SQL submission, worker and waiters migrate together.

## W06 — Account, wire, configuration and administration


**Complete source owners to scope:** TiDB `pkg/privilege/privileges`, `pkg/parser/charset`, `pkg/config`, `pkg/server`, `pkg/executor`, utility dependencies and `cmd/tidb-server`.

**Migration and removal:** Preserve the complete durable account/security image; enforce password history/reuse and verified TLS policy. Carry source bytes through charset decoding. Replace the private NodeConfig whitelist with the effective validated source configuration; connect command admission, remote KILL and source HTTP handlers.

**Integration gate:** Account reload/restart retains policy and epochs, anonymous accounts remain represented, TLS reload and certificate constraints work, Latin-1 bytes round-trip, command permits release on errors, and global KILL targets the correct peer. W04 identity/discovery gates remote KILL. Independent complete security/charset owners need not wait for unrelated cache or MPP work.

## W07 — Shared cache dependency and consumers

Current checkpoint: the [shared cache batch](shared-cache-batch-repair.md)
closes B01, B02, C03 and C04. The pinned root and LFU package obligations have
receipts; C02 and W12 inference remain unresolved. Broader bindinfo/copr/Domain
acceptance still requires their other package obligations. The original five-ID
ownership table remains the historical workstream allocation.


**Complete source owners to scope:** Pinned Ristretto root/dependencies, TiDB `pkg/statistics/handle/cache/internal/lfu` and parents, `pkg/bindinfo`, `pkg/store/copr`, `pkg/planner/core` and Domain.

**Migration and removal:** Implement the complete Ristretto contract once, with separate instances/budgets/lifetimes for bindings, LFU, coprocessor results and W12 inference. Migrate incremental binding reload/GC/usage persistence and effective coprocessor configuration with their owners. Compose the separate Go instance plan cache and per-execution cloning.

**Integration gate:** Both retained C04 dependency probes pass without suppression; admission, queue pressure, TTL, callbacks, metrics, Wait/Clear/Close and consumer configuration pass original cases. Remove Stretto and private FIFO stores at migration. Preserve the session LRU, instance cache contract, needed indexes and LFU fallback. See the shared-cache ownership review.

## W08 — Optimizer, typed expressions and executor workers


**Complete source owners to scope:** TiDB `pkg/planner/core`, Cascades/memo dependencies, `pkg/expression`, `pkg/executor` and their complete dependencies.

**Migration and removal:** Retain Go shared candidate selection for ordinary/merge planning and integrate Cascades selection with the real memo/rules/implementation lifecycle. Unify SQL/PB signature construction and typed scalar/vector contracts. Compose eligible parallel Apply with serial fallback and projection cancellation/join.

**Integration gate:** Original plan/result/error/warning cases pass under supported SQL modes and both optimizer selections. LIMIT, errors, cancellation, close/reopen and worker failure release resources. Integrate fresh master range-count-variable/skyline/upgrade propagation with its owning packages; a hardcoded threshold is insufficient.

## W09 — MPP and TiFlash Compute


**Complete source owners to scope:** TiDB `pkg/planner/core`, `pkg/executor/internal/mpp`, `pkg/store/copr`, `pkg/util/tiflashcompute` and dependencies.

**Migration and removal:** Replace scan-only task planning with source fragment/task ownership; preserve each key range and region continuation; stream tracked results with joined local/remote cancellation. Use W02 security/routing/recovery instead of the private plaintext transport. Compose configured Compute topology and dispatch/recovery.

**Integration gate:** Disjoint ranges, more than one region page, multi-fragment/task execution, stale regions, TLS, topology changes and early close match Go. First-row delivery does not wait for the whole result and memory follows bounded streaming. W04 identity and W08 plan/expression contracts are prerequisites for full integration.

## W10 — Domain job, resource and bulk-operation services


**Complete source owners to scope:** TiDB `pkg/ttl/ttlworker`, resource/runaway dependencies, `pkg/domain`/`crossks`, statistics owners, `pkg/dxf/framework` managers, `pkg/executor/importer`, `pkg/dxf/importinto`, BR and native PD resource control.

**Migration and removal:** Compose existing source lifecycle owners for TTL, resource control/runaway, RU history, statistics GC, DXF and cross-keyspace runtimes. The private CSV/INSERT IMPORT pipeline is already withdrawn; implement the complete import and BRIE task, option, progress and cancellation/recovery owners before exposing those operations.

**Integration gate:** Role/configuration gates, periodic work, owner transfer, recovery, task cancellation and joined shutdown work through production startup. Reuse repaired BR protocol/range helpers. RU history consumes the resource controller; cross-keyspace runtime requires W04 protection per runtime; IMPORT/reorganization consume DXF where Go does.

## W11 — Live runtime information and observability


**Complete source owners to scope:** TiDB `pkg/infoschema`, peer retrievers/executors, `pkg/domain`, plan-replayer owners, `pkg/util/topsql`, `pkg/workloadlearning`, `pkg/telemetry`, statement-summary v1/v2, statistics load and `pkg/metrics`.

**Migration and removal:** Replace remaining runtime constant providers with the source retrievers and cluster fanout. The captured cluster config/topology paths are already withdrawn; their missing retrievers remain open. Wire statement-summary and metrics producers, plan replay, profiling/reporting, learning, telemetry and AZ adjustment through their actual owners and lifetimes. Keep genuine static metadata definitions.

**Integration gate:** Real SQL produces summary records in both modes and readers see them; runtime rows reflect peers and errors propagate; load events update the existing registry; replay archives and background tasks have complete lifetimes. Keep the existing workload-repository worker distinct from workload learning. Go telemetry currently logs; do not introduce an uploader.

## W12 — Inference runtime


**Complete source owners to scope:** TiDB `pkg/inference`, pinned provider/batcher dependencies, `pkg/domain`, and `pkg/expression`.

**Migration and removal:** Compose the source provider/batcher, Domain-owned embedding runtime and fourth Ristretto consumer; replace the current boundary only as complete packages. Reuse W07 dependency contracts and W08 expression contexts.

**Integration gate:** Source configuration, batching, cache identity/cost, errors, cancellation and owned close pass deterministic provider tests and applicable integration cases. Do not invent provider modes or require production credentials to prove local lifecycle behavior.

## W01 prerequisite progress — 2026-10-01


The complete PD `pkg/deadline` leaf and its native batch call sites are implemented
in client-rust `5928b6e480b441496f9a3cd9bed6a7e8d56215a1`; see the
[repair receipt](pd-deadline-owner-repair.md). This supplies deadline admission,
completion and stream retirement, with a correction to the shared cancellation
adapter. W01 remains open for public PD close propagation, discovery, root APIs,
metadata concurrency and complete parent-package tests/variants. No known finding
is fully closed by this prerequisite; the primary 77-ID assignment is unchanged.

The next complete dependency, `pkg/connectionctx`, is implemented in native
`4e3169ed93e433eab38638a1dd92a24f25f7caaf`; see its
[repair receipt](pd-connectionctx-owner-repair.md). Native single-leader TSO now
uses this shared URL/cancellation owner, retains healthy same-URL streams and
replaces canceled or stale streams. Store rejection preserves caller ownership;
retirement retains and joins old workers. Full parent/source-mode/retry ownership
and public close remain W01 obligations. All 77 IDs retain their assignment.

The complete `pkg/batch` prerequisite is implemented in native
`bcf74b7282b01372f93fb814ba601eb4c22b5d12`; its
[repair receipt](pd-batch-owner-repair.md) covers all controller operations,
original cases and Rust ownership adapters. Native TSO now uses source default
20,000-entry queue/collector and one RPC token, returns tokens before completion
and recycles buffers. Dynamic concurrency/pacing, full request/error/options and
router/PD parent ownership remain open. All 77 IDs retain their assignment.

The complete `pkg/retry` prerequisite is implemented in native
`2fd0ecebadf0e8274a2b10ebfed729b03284a52b`; see its
[repair receipt](pd-retry-owner-repair.md). Default initialization uses source
100-attempt/one-second ticker policy and membership probes share an absolute
deadline. Exponential, fixed-interval, cancellation/error, context and reset
semantics are covered across the complete package. Root per-RPC retries,
configurable options, full discovery and close still require their owners.

The complete `opt` dependency is published in native `df0d4ccc5b595959f496b3cf6e6b87f22b337bf4`
and synchronized into TiDB; see its [repair receipt](pd-opt-owner-repair.md). It centralizes all static/dynamic/request
options and removes the scan-only policy declaration. Existing native/TiDB
callers migrate together. Actual follower/router/proxy/concurrency behavior and
full configurable construction remain part of the open parent owners.

## Activation and removal order


Start with W01's complete PD root/TSO/discovery closure, then native routing and TiDB consumers in W02. Request timeout, cancellation, connection replacement and joined shutdown are one lifecycle; independent fixes to a mutex or spawned task do not complete it. Inventory currently lists 13 PD-root artifacts, seven TSO artifacts and nine service-discovery artifacts; this bounds the starting review, not the full dependency closure. Other PD APIs in the root package must also be reviewed. Native changes publish to client-rust master before TiDB synchronization.

Develop the shared SQL/table, schema and DDL owners as a coupled integration sequence: W03 needs W04 for historical schemas; W05 needs their transaction, schema and lease contracts; W04 GC deletion needs W05 durable delete-range registration. Min-active-start-TS reporting is required even when only a Go peer runs GC. Bring up reporting and prove timestamp protection before activating collection. Establish durable job scheduling and shared state/error transitions before enabling action/reorganization paths or retiring the direct SQL publisher. DXF-backed modes also require W10. Do not use the cyclic runtime dependencies as a reason to add another temporary production owner.

Complete independent account, charset, table-policy and cache dependency packages whenever their prerequisite closure is ready. They need not wait for the entire core migration. W07's full Ristretto root precedes its four consumers; C02's instance plan cache has its own Go design and does not depend on replacing it with Ristretto. The detailed [cache design](shared-cache-owner-review.md) remains the specification for this dependency.

Finish W08/W09 planning and streaming integration through the shared session/storage owners. Compose W10 and W11 source services as their prerequisites become available, not as one last startup-only patch. Their parent packages remain unaccepted until every service, producer, consumer, role/configuration gate and shutdown branch is accounted for. W12 requires its complete provider closure, shared cache contract and expression integration. Reuse existing successful implementations and test receipts throughout.

For each replacement, trace construction, all public calls, state transitions, errors, retry owner/budget, cancellation, completion and close. Remove the displaced owner and its configuration/callers/tests that assert Go-absent behavior only when that trace and original-source regressions pass. Preserve tests which prove valid behavior. Remove unused dependencies once their last legitimate consumer migrates. Legitimate separate SQL transaction managers, native KV transactions, table assertions, distributed-task planners and consumer caches must remain separate as in Go.

## Completion and performance


The ExecPlan supplies exact validation/publication commands and benchmark procedures. Each complete package must have original-case equivalence, regression evidence, caller integration, required checks and a current receipt before commit/push is described as a completed package. Known baseline failures are tracked by test and source owner; unchanged failures do not become accepted behavior. Broad packages remain open until those obligations close.

Establish matched Go/previous-Rust baselines before each applicable runtime change and measure the new Rust owner after correctness passes. Preserve SQL results, warnings/errors, isolation, key distribution, tool/input versions, storage topology, configuration and resource limits. Performance improvements must arise from source-compatible ownership, batching, streaming, vectorization and concurrency. Run sysbench, TPC-C, all 22 TPC-H queries and SQL-bound YCSB where supported; an unsupported query or invalid sample is an open result, never silently omitted. No improvement is claimed by this planning document.

Closing all 77 findings is a checkpoint. Complete the remaining package inventory, original tests and platform/build/generated variants before claiming full parity. Every new upstream revision invalidates the affected package/dependency receipts until its delta is reviewed; unchanged receipts retain their exact source pins.


The complete PD `metrics` and `resource_group/controller/metrics` prerequisites
are published in native `61e9a86b9261aff9588597a5899941df57f15b38`; formatting
follow-up `e3e8de80f2791ed725b6f06c25dfe4ed339ce564` is synchronized.
The [metrics owner receipt](pd-metrics-owner-repair.md) records both complete
inventories, independent Go runtime oracles, consumer rebinding and removal of
six native plus six TiDB collector definitions. Integration validation and the
mandatory publication gates are tracked there. Circuit breaker/grpcutil and the
broader W01 root/TSO/discovery owners remain open; finding counts are unchanged.


The complete PD `pkg/circuitbreaker` prerequisite is published in native
`44afb53ffadfb5a711fa9c459d95fd197a3adb1a` and synchronized; see the
[shared circuit-breaker receipt](pd-circuitbreaker-owner-repair.md). It removes
the private region-cache state machine and restores the shared execution,
settings, context and metric owner. Source, regression, complete-library and
TiDB integration evidence and publication gates are recorded there. Full grpcutil per-RPC placement, root/TSO/discovery and
parent-package acceptance remain open; finding counts are unchanged.


The [retry value-ownership re-review](pd-retry-value-ownership-repair.md) corrects
the earlier unrestricted retry acceptance. Go copies backoffers for each RPC;
Rust needed shared callback identity with independent scalar state, signed timing
arithmetic and reusable constructor options. The entire retry package and source
support are revalidated before implementing grpcutil. Parent ownership and
finding counts remain open and unchanged.


The [complete PD error owner](pd-errors-owner-repair.md) supplies all source
definitions, codes, cause chains and classification/logging helpers before the
grpcutil migration. It removes the uncoded breaker enum and duplicate native TSO
EOF/count diagnostics. Parent transport/lifecycle and all 77 findings remain open.
