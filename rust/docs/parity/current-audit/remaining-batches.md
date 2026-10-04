# Structural execution batches for the remaining findings

The [DML metadata batch](dml-owner-batch-repair.md) advances **A01/E02/E03 together**: metadata-only joined sources, shared write-target authorization and per-table FK plans. Retained failures also repair synchronous FK-ID rollback and strict single-statement parsing. **86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial)** remain; executable FK objects, row identities and matrix execution are still open. See the validation receipt for actual grouped checks. No complete package acceptance or push.


Latest B05 maintenance: [admission-policy batch](admission-policy-batch-repair.md) repairs retained-password and specified certificate consumers together. Counts stay 57 unresolved; remaining durable login-counter/GRANT/cache and certificate reload/rotation/no-CA/platform owners stay explicit. Other 55 IDs retain earlier evidence.

The current register has **57 unresolved findings: 39 open and 18 partial**, plus 29 repaired. Every unresolved ID is assigned exactly once below. Go master is `93a01d31f6da205ae4bf376825293903a6899fdb`. This replaces symptom-by-symptom scheduling; it does not freshly reproduce every recorded finding or certify any package.

A batch groups a shared production lifecycle. Complete Go packages remain the atomic acceptance unit, including original tests, support, generated/platform/build variants and fixtures. Broad shared packages such as Domain, planner and executor retain one inventory and receipt across contributing batches. Dependencies below are integration gates; they do not prevent implementing independent prerequisite packages.

| Batch | Shared owner | Findings | Count |
| --- | --- | --- | ---: |
| B01 | Native PD and shared transport | P03, P06, T02, M04 | 4 |
| B02 | Shared SQL session, planner and table execution | A01, E02, E03, T01, K01, K03, S03, S04 | 8 |
| B03 | Schema, identity and timestamp protection | O01, O02, O03, O09, I04, K02 | 6 |
| B04 | Durable DDL and placement recovery | D01, D02, D03, D05, D08, D09, D10, D11, F01, F02, O14 | 11 |
| B05 | Account security, wire bytes and administration | A02, A03, N01, N03, N04, N05 | 6 |
| B06 | Typed expression and optimizer execution | Q01, X01, E04 | 3 |
| B07 | MPP fragments and compute topology | M01, M05 | 2 |
| B08 | Owned job, resource and bulk-operation runtimes | O04, O05, O06, O10, O15, E05, E07 | 7 |
| B09 | Live information and statement observability | I01, I02, I03, O08, O11, O13, O16, O17, O18 | 9 |
| B10 | Inference provider, batching and cache runtime | X02 | 1 |

## Execution and validation cadence

Start with B01 native ownership, then move shared SQL/table and schema/durable DDL through their coupled gates. B03 timestamp reporting precedes GC; B04 delete-range registration gates collection. B05 security/charset packages can progress independently. Resource/DXF owners precede their history/import consumers. No worker is accepted just because startup calls a seed helper.

Before editing a selected unit, refresh affected sources and inventory its whole ownership boundary. Implement connected producers, callers, configuration, state/error transitions, cancellation and removal together. Capture regressions together before fixes and run them together afterward. Combine filters for a Rust target; do not start Cargo separately for each case. Build compatible targets together and keep failures visible. Lint, all-target checking and the required locked server build occur at the batch boundary. Actual precommit hooks remain mandatory; no push is authorized.

Tests that preserve Go contracts remain. Empty shells and assertions of removed Go-absent behavior can be retired only with their original obligations retained and every production caller migrated. Do not discard useful regressions to make a batch pass.

## B01: Native PD and shared transport

Source owners: Pinned PD root, clients/tso, servicediscovery and grpcutil dependency closure; client-go internal/locate, internal/client and TiDB routing/MPP consumers.

Completion: One discovery/TSO/region/channel lifecycle; service-mode switches, options, deadlines, security, cancellation and joined close. Retire competing TiDB routing only after every ordinary/coprocessor/MPP consumer migrates.

Integration dependencies: none; external prerequisite packages still require complete acceptance.

## B02: Shared SQL session, planner and table execution

Source owners: pkg/session and sessiontxn providers; pkg/planner/core, pkg/executor, pkg/table/tables, pkg/meta/autoid.

Completion: One resolved privilege/FK/handle handoff and transaction-aware table policy, with prepared/migrated sessions and historical reads. Delete the matrix interpreter only after all its callers use the shared executor. T01/K03 maintenance is evidence inside this batch, not acceptance of it.

Integration dependencies: B03.

## B03: Schema, identity and timestamp protection

Source owners: pkg/domain, domain/infosync, infoschema and issyncer; pkg/session, table/tables, store/gcworker.

Completion: Versioned schema/upgrade, cluster identity/leases, cached-table leases and all active timestamp producers/reporters. Reporting precedes GC activation; GC also requires B04 durable delete-range work. Safe submilestones may proceed before the final cyclic integration gate.

Integration dependencies: B01, B04.

## B04: Durable DDL and placement recovery

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

Source owners: pkg/infoschema and remote executor retrievers; domain, plan replay, topsql, workloadlearning, telemetry and summary v1/v2.

Completion: Real peers and SQL events feed owned providers, summaries, profiles/reports, replay archives and periodic learning/telemetry. Reuse process/session lifetimes and avoid synthetic rows or zero-field acceptance. Workload repository remains separate from learning. Dependencies gate relevant consumers, not every leaf prerequisite.

Integration dependencies: B01, B02, B03, B08.

## B10: Inference provider, batching and cache runtime

Source owners: pkg/inference and full provider/batcher packages; Domain runtime, shared Ristretto dependency and expressions.

Completion: One provider/batcher/Domain/cache lifetime, configuration, errors, cancellation and joined close. One finding here represents a whole structural runtime, not a one-function fix.

Integration dependencies: B02, B06.

## Current evidence boundary

The T01/K03 table maintenance repairs six reproduced contracts in B02. It does not complete B02 or either parent finding. The other 55 unresolved findings retain their prior evidence. Exact commands and source identities belong in the linked repair receipt, not in a claim that the entire register was retested. The machine-readable map is [remaining-batches.json](remaining-batches.json); the status authority is [structural-findings.json](structural-findings.json).
