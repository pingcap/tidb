# Structural execution batches for the remaining findings

Current evidence and cleanup receipts are indexed in [README.md](README.md). The [JSON register](structural-findings.json) owns finding counts and dispositions; dated receipts retain their original verification limits.

Latest connected repair: [view admission and publication](view-owner-batch-validation.json) joins B02/A01 privilege collection and B04/D01 query/target resolution. Persistent target publication preserves local overlays and refuses base-table replacement; unparsed ALTER VIEW execution is removed. Nine Rust regressions and nine live assertions fail before repair; the receipt records grouped after-checks. Counts remain 86 tracked / 30 repaired / 56 unresolved (27 open, 29 partial). These are existing-owner repairs; other 54 roots retain carried evidence.

A batch groups a shared production lifecycle. Complete Go packages remain the atomic acceptance unit, including original tests, support, generated/platform/build variants and fixtures. Broad shared packages such as Domain, planner and executor retain one inventory and receipt across contributing batches. Dependencies below are integration gates; they do not prevent implementing independent prerequisite packages.

| Batch | Shared owner | Findings | Count |
| --- | --- | --- | ---: |
| B01 | Native PD and shared transport | P03, P06, T02, M04 | 4 |
| B02 | Shared SQL session, planner and table execution | A01, E02, E03, T01, K01, K03, S03, S04 | 8 |
| B03 | Schema, identity and timestamp protection | O02, O03, O09, I04, K02 | 5 |
| B04 | Durable DDL and placement recovery | D01, D02, D03, D05, D08, D09, D10, D11, F01, F02, O14 | 11 |
| B05 | Account security, wire bytes and administration | A02, A03, N01, N03, N04, N05 | 6 |
| B06 | Typed expression and optimizer execution | Q01, X01, E04 | 3 |
| B07 | MPP fragments and compute topology | M01, M05 | 2 |
| B08 | Owned job, resource and bulk-operation runtimes | O04, O05, O06, O10, O15, E05, E07 | 7 |
| B09 | Live information and statement observability | I01, I02, I03, O08, O11, O13, O16, O17, O18 | 9 |
| B10 | Inference provider, batching and cache runtime | X02 | 1 |

Current connected maintenance: [calendar conversion](calendar-owner-batch-validation.json) links B02/K03 and B06/X01. Shared datum diagnostics and table completion preserve conversion warnings, DST error distinctions, DATE fields and unsigned fallback. Typed JSON calendar/duration signatures retain their distinct Go dispatch. Broader generated/ANALYZE/vector and package obligations remain open.

## Execution and validation cadence

Start with B01 native ownership, then move shared SQL/table and schema/durable DDL through their coupled gates. B03 timestamp reporting precedes GC; B04 delete-range registration gates collection. B05 security/charset packages can progress independently. Resource/DXF owners precede their history/import consumers. No worker is accepted just because startup calls a seed helper.

Before editing a selected unit, refresh affected sources and inventory its whole ownership boundary. Implement connected producers, callers, configuration, state/error transitions, cancellation and removal together. Capture regressions together before fixes and run them together afterward. Combine filters for a Rust target; do not start Cargo separately for each case. Build compatible targets together and keep failures visible. Lint, all-target checking and the required locked server build occur at the batch boundary. Actual precommit hooks remain mandatory; publication policy and access status live in [README.md](README.md).

Tests that preserve Go contracts remain. Empty shells and assertions of removed Go-absent behavior can be retired only with their original obligations retained and every production caller migrated. Do not discard useful regressions to make a batch pass.

## B01: Native PD and shared transport

Source owners: Pinned PD root, clients/tso, servicediscovery and grpcutil dependency closure; client-go internal/locate, internal/client and TiDB routing/MPP consumers.

Completion: One discovery/TSO/region/channel lifecycle; service-mode switches, options, deadlines, security, cancellation and joined close. Retire competing TiDB routing only after every ordinary/coprocessor/MPP consumer migrates.

Integration dependencies: none; external prerequisite packages still require complete acceptance.

The connected replica-routing batch removes ClientPd's inherited leader-only fallback and private address cache. Ordinary readers and the native PD client share ReplicaRouting and native retry state. Coprocessor candidate scoring and store health remain shared, but RequestSelector and full cache/recovery lifecycles still require migration before either cache can be retired. See [routing validation](replica-routing-batch-validation.json). Ordinary forwarding now consumes the native request state and shared canonical proxy feedback across batch/unary commands; see [forwarding validation](ordinary-forwarding-batch-validation.json). Configured health timeouts, zero/no-I/O semantics and periodic shared flow reporting now have connected production consumers; see [store maintenance validation](store-maintenance-batch-validation.json). Complete health policy, RequestSelector migration and cache consolidation remain open.

## B02: Shared SQL session, planner and table execution

Current connected probe maintenance shares logical alias selection and reader policy with O13 in B09; direct GROUP BY eligibility follows Go7a3. See [index-probe validation](index-probe-batch-validation.json). Parent/package boundaries remain unchanged.

Current maintenance: [shared DML contracts](shared-dml-contract-batch-validation.json) repairs heap SQL scope, partitioned-heap eligibility and FK-before-NULL ordering after the [DML writable-row batch](dml-identity-batch-validation.json). E02/E03/K03 remain partial for their retained broader boundaries; validate connected callers together.

Source owners: pkg/session and sessiontxn providers; pkg/planner/core, pkg/executor, pkg/table/tables, pkg/meta/autoid.

Completion: One resolved privilege/FK/handle handoff and transaction-aware table policy, with prepared/migrated sessions and historical reads. Matrix read interpreters are retired; complete chunk writes and indexed FK/cascade owners remain. Existing-owner maintenance does not accept this batch.

Integration dependencies: B03.

## B03: Schema, identity and timestamp protection

Source owners: pkg/domain, domain/infosync, infoschema and issyncer; pkg/session, table/tables, store/gcworker.

Completion: Versioned schema/upgrade, cluster identity/leases, cached-table leases and all active timestamp producers/reporters. Reporting precedes GC activation; GC also requires B04 durable delete-range work. Safe submilestones may proceed before the final cyclic integration gate.

Integration dependencies: B01, B04.

## B04: Durable DDL and placement recovery

Current [cluster column batch](cluster-column-batch-validation.json) retains every supported column sibling and shares original-schema admission, conflict ownership, stable-ID application and notifier/index consumers. Metadata-only type safety is enforced. Previous [metadata admission](alter-admission-batch-validation.json) owns local options/charset/TTL preparation. The next boundary remains complete durable multi-schema/column worker integration, alongside partition/cache admission and cluster option lowering; these are not discharged by the transaction planner.

Current index lifecycle maintenance connects B02 K03 with B04 D02/D11: submitting evaluation policy, typed errors, physical-range deletion and implicit column-index cleanup. See [validation](index-lifecycle-batch-validation.json). Durable workers/recovery and parent dispositions remain unchanged.

Current maintenance: [constraint admission/index backfill](constraint-admission-batch-validation.json) connects local and persisted CHECK metadata, original-schema FK admission and ordinary implicit-index backfill. D01/D02 remain open and D11 partial; this is not durable DDL package acceptance.

Source owners: pkg/ddl, ddl/jobsubmit and complete reorganization owners; domain/infosync, domain/affinity and GC consumers.

Completion: Submit, schedule, execute, recover and wait on the same persisted job and schema lifecycle. Placement and delete ranges follow durable phases. Keep materialized-view/import seeds disabled until complete owners pass. B08 is required only for source modes that use DXF.

Integration dependencies: B01, B03, B08.

## B05: Account security, wire bytes and administration

Source owners: pkg/privilege/privileges, parser/charset and config; pkg/server, pkg/executor and cmd/tidb-server.

Completion: Durable policy load/publication/authentication/writeback, verified TLS reload, byte-authoritative ingress and shared configuration consumers. Cross-node KILL requires B03 identity; independent security and charset packages can proceed without that gate.

Integration dependencies: B03.

## B06: Typed expression and optimizer execution

Source owners: pkg/expression and complete signature dependencies; pkg/planner/core, Cascades/memo, pkg/executor.

Completion: Shared SQL/PB typed scalar/vector construction, real memo optimization and eligible cloned Apply/CTE/shuffle execution with shared candidates, errors, cancellation and joined close.

Integration dependencies: B02.

## B07: MPP fragments and compute topology

Source owners: pkg/planner/core, executor/internal/mpp and store/copr; pkg/util/tiflashcompute.

Completion: Physical fragment/task graph, replica/topology selection and configured compute cache/dispatch/recovery. Transport M04 is primarily tracked in B01; this batch supplies its final MPP consumer acceptance.

Integration dependencies: B01, B03, B06.

## B08: Owned job, resource and bulk-operation runtimes

Source owners: TTL, resource/runaway, domain/crossks and DXF manager packages; executor/importer, dxf/importinto and BRIE owners.

Completion: Compose controller before RU history and DXF before dependent import/reorg work. Share startup gates, durable task state, transfer/recovery, cancellation and shutdown; keyspace holders must protect their own timestamps.

Integration dependencies: B01, B03.

## B09: Live information and statement observability

Current B05/B09 maintenance shares ordinary/probe/merge table-request policy and actual handle-order restoration, now including routed physical partitions. Small-batch/partition-only bypasses are retired. Dirty fallback, unrouted point probes and broader configuration/estimate/live-cluster/package obligations remain. See [partition reader validation](partition-reader-batch-validation.json); N03/O13 stay partial.

Source owners: pkg/infoschema and remote executor retrievers; domain, plan replay, topsql, workloadlearning, telemetry and summary v1/v2.

Completion: Real peers and SQL events feed owned providers, summaries, profiles/reports, replay archives and periodic learning/telemetry. Reuse process/session lifetimes and avoid synthetic rows or zero-field acceptance. Workload repository remains separate from learning. Dependencies gate relevant consumers, not every leaf prerequisite.

Integration dependencies: B01, B02, B03, B08.

## B10: Inference provider, batching and cache runtime

Source owners: pkg/inference and full provider/batcher packages; Domain runtime, shared Ristretto dependency and expressions.

Completion: One provider/batcher/Domain/cache lifetime, configuration, errors, cancellation and joined close. One finding here represents a whole structural runtime, not a one-function fix.

Integration dependencies: B02, B06.

## Current evidence boundary

P03/P06 provider/bootstrap residuals are repaired together: requested-keyspace initialization precedes TSO discovery, minimum timestamps select Go's provider/fallback policy, and optional metadata headers preserve response payloads without panics. The [current receipt](pd-provider-bootstrap-batch-validation.json) records four failures before and 207 native cases after. These broader findings remain partial; follower/forwarding and full package obligations remain. This batch does not freshly reproduce the other 54 unresolved roots.

The shared snapshot batch advances B02/B03 together: S04 ordinary-transaction
SnapshotTS switching, I04 publication/resize retention and O09 result/cursor
protection share one tested path. See [validation](schema-snapshot-batch-validation.json).
No parent finding or complete package is closed; lazy V2, MVCC schema timestamp
ranges, DDL exclusion and live Go-peer GC remain explicit prerequisites.

B02/B05 now share the deferred pessimistic uniqueness consumer and its safety lifecycle. See [validation](deferred-uniqueness-batch-validation.json). T01/N03 remain partial; the other 54 roots were not freshly reproduced.

B01 channel ownership now shares native/adapter membership, keyspace, discovery and TSO connections, including bootstrap/refresh and joined runtime shutdown. See [validation](pd-channel-batch-validation.json). Forwarding/follower consumers, health and full-package acceptance remain open.

B09/B05 coprocessor policy now connects O13/N03 settings, reader estimates, adaptive routing, busy duration and deadlines through the shared request builder. See [validation](cop-read-policy-batch-validation.json). Point/batch snapshot routing and remaining reader/scope producers remain separate open obligations.

B02 shared write policy now retains binary/temporal numeric diagnostics, charset error precedence and UPDATE source-row ordinals. The obsolete executor replay harness was migrated to the session rollback suite. See [validation](write-diagnostics-batch-validation.json). K03/E03 remain partial for the explicitly retained structural and conversion obligations.

The K03/X01 [JSON numeric batch](json-numeric-batch-validation.json) connects the shared datatype source owner to table writes, scalar casts, bitwise operands and numeric JSON cast batches. Both parents remain partial; broader temporal/generated/ANALYZE and complete SQL/PB signature/kernel obligations remain.

B03/B05/B09 share the [internal-process repair](internal-process-batch-validation.json): one live internal-session entry supplies tracked-task visibility, local cancellation and internal timestamp diagnostics. N04, I01 and O09 remain partial; remote dispatch, other providers, lazy schema V2 and full exclusion/package obligations remain. Physical timestamp holds remain conservative.

B02/B06 share the [typed user-variable repair](user-variable-batch-validation.json): one value/type owner serves planning, SET, inline expression execution, prepared statements and session migration. S03/X01 remain partial because their other package obligations are not accepted. Other54 unresolved roots retain earlier evidence.

## Connected B02/B06 JSON checkpoint — 2026-10-06

X01/K03 now share JSON result typing, typed charset errors and expression-index
hidden-column admission across SQL and cluster metadata creation. The duplicate
result-type resolver and hardcoded BIGINT path are removed. See
[validation](json-result-batch-validation.json): 211 passing Rust cases, fourteen
passing TCP checks and five unchanged historical DDL failures. Both roots stay
partial; this is existing-owner maintenance, not whole-package acceptance.


## Current-Go statistics follow-up — 2026-10-06


The previously unimplemented uniqueness and FM-sketch deltas now share collection, blocking/asynchronous/in-process merging, canonical conversion, JSON and storage owners. [Validation](statistics-ndv-batch-validation.json) records 220 Rust tests and 15 TCP checks passing. This maintenance retires duplicate global sketch merging; broad B01–B10 assignments and finding dispositions remain unchanged.


## PD region lifecycle checkpoint — 2026-10-06

The [PD region receipt](pd-region-batch-validation.json) connects B01 discovery and cache request options with N03 live global policy. Selection, metadata, same-deadline leader fallback, cache stale-response fallback and ID/scan caller distinctions share one batch. P03/T02/N03 remain partial; all 56 broader unresolved assignments are retained.


## PD service availability checkpoint — 2026-10-06

The [availability receipt](pd-availability-batch-validation.json) advances
B01 P03/P06/T02 through shared member health, region API cooldown, topology
publication and independently polled, joined maintenance in both clients.
Duplicate health wire code is retired; forwarding, TSO proxy and shared cache
consolidation remain open. The other 53 unresolved roots retain carried evidence.


## PD unary forwarding checkpoint — 2026-10-06

Shared native/adapter selection, startup policy and metadata connect B01 with N03. Three duplicated failover loops are removed. [Validation](pd-forwarding-batch-validation.json) records four baseline failures and 336 selected passes. P03/T02/N03 stay partial; TSO proxy/router and broader package obligations remain open.


## Shared TSO proxy checkpoint — 2026-10-06

B01 discovery/stream ownership and B05 process policy advance P03/P06/N03 together. The [receipt](tso-proxy-batch-validation.json) records three baseline failures, shared transport removal, one native dispatcher and grouped validation. Automatic forwarding/bootstrap and full package obligations remain open; all 56 unresolved roots retain their assignments.


## PD bootstrap and provider checkpoint — 2026-10-06

P03/P06/N03 now share nonblocking configured connections, accepted member publication and forced-PD timestamp provider selection. Eight regressions failed before repair; 364 selected tests, affected all-target checks and lint pass. The separate TSO primary may be unreachable while an explicitly enabled healthy proxy serves requests. Automatic network-failure forwarding and full-package obligations remain open. See [validation](pd-bootstrap-policy-batch-validation.json).


## TSO failure/recovery checkpoint — 2026-10-07

P03/P06/N03 now share construction-failure feedback, automatic healthy-backup routing, recovery and accepted-provider retention. Response-phase errors remain distinct from construction failures. See [validation](tso-failure-batch-validation.json); stream-readiness/prewarm, exact idle timing and complete packages remain open. No batch assignment or finding disposition changes.


## Shared common-handle readers — 2026-10-07

Latest connected repair: [partitioned shared table readers](partition-reader-batch-validation.json). Ordinary lookup, index join and index merge now carry physical partition identity through the shared request owner, ordered row/chunk completion, projections and fallback. Three baseline policy failures precede 221 passing Rust cases and nine passing real MySQL controls. N03/O13 remain partial; counts stay 86 tracked/30 repaired/56 unresolved (27 open,29 partial). Other 54 roots retain carried evidence.

Current connected B02/B05/B04 maintenance: [partition locking and reorganization safety](partition-lock-batch-validation.json). Shared locking readers now retain physical partition IDs for SELECT/UPDATE/DELETE. The local metadata-only REORGANIZE path is removed after a live data-disappearance reproduction; preserve its durable-owner admission gate until the full job/data-movement lifecycle is available. E03/N03/D11 remain partial.


## Shared FK access checkpoint — 2026-10-07

Latest connected repair: [shared foreign-key access](fk-access-batch-validation.json). Child checks, parent restrictions, cascades and existing-row ALTER validation now use the selected canonical record/index keys. Cascades retain handles/preimages; typed read errors and submitting decode context survive all callers. Five baseline Rust failures precede 203 passing grouped cases and five supported real MySQL/unistore scenarios. Cluster ALTER-FK remains explicitly unsupported. E02/K03/D11 remain partial; counts stay 86 tracked/30 repaired/56 unresolved (27 open,29 partial). Other 53 roots retain carried evidence.
