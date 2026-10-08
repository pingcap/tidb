# Structural execution batches for the remaining findings

The [JSON batch map](remaining-batches.json) owns the allocation below; the
[finding register](structural-findings.json) owns dispositions and detailed
residuals. Current counts are 86 tracked, 30 repaired and 56 unresolved
(27 open, 29 partial). Read [README.md](README.md) for the latest repair and
validation receipts. Older maintenance summaries are preserved in the
[planning archive](https://github.com/pingcap/tidb/blob/9a319a5d6d78593e623a9db8e8aed1f750380507/rust/docs/parity/current-audit/remaining-batches.md).

A batch groups a shared production lifecycle. One complete Go package remains
the acceptance unit, including original tests, support, generated/platform/build
variants and fixtures. Broad shared packages retain one inventory and acceptance
receipt across contributing batches. Dependencies below gate integration;
independent prerequisite packages can proceed before a parent is complete.

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

## Execution and validation cadence

Start with native ownership, then shared SQL/table and the coupled schema/DDL
lifecycles. Timestamp protection precedes GC; durable delete-range registration
gates collection. Account security and charset prerequisites can progress
independently. Resource/DXF owners precede their history/import consumers.

Inventory the complete owner/dependency boundary before selecting edits. Capture
related regressions in one baseline run, migrate producers and consumers, then
validate the completed batch with grouped filters and compatible targets.
Repeat compilation only after a meaningful change, failure or unresolved concern.
Follow [the living plan](../../full-structural-parity-execplan.md) and root
AGENTS.md for scoped checks, actual hook and fresh locked pre-push build.

Retain meaningful Go and Rust correctness tests. Remove empty fixtures,
source-shape assertions and obsolete adapters only with caller/coverage evidence.
A startup call to a seed helper does not establish its full runtime lifecycle.

## B01: Native PD and shared transport

Source owners: Pinned PD root, clients/tso, servicediscovery and grpcutil dependency closure; client-go internal/locate, internal/client and TiDB routing/MPP consumers.

Completion: One discovery/TSO/region/channel lifecycle; service-mode switches, options, deadlines, security, cancellation and joined close. Retire competing TiDB routing only after every ordinary/coprocessor/MPP consumer migrates.

Integration dependencies: none; external prerequisites still need complete acceptance.

## B02: Shared SQL session, planner and table execution

Source owners: pkg/session and sessiontxn providers; pkg/planner/core, pkg/executor, pkg/table/tables, pkg/meta/autoid.

Completion: One resolved privilege/FK/handle handoff and transaction-aware table policy, with prepared/migrated sessions and historical reads. Matrix read interpreters are retired; complete chunk writes and indexed FK/cascade owners remain. Existing-owner maintenance does not accept this batch.

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
