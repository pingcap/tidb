# Share live cluster diagnostic readers

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Expose live running transactions, deadlock history, memory usage/history and
index usage through the existing cluster SQL and generated peer boundary.
I01/I03/N05 share these missing readers. Use live process, catalog, privilege
and usage owners; do not manufacture rows or register a synthetic SQL client.
This is connected consumer repair, not acceptance of complete upstream packages.

## Progress


- [x] Inspect baseline8a14f89b5e and freshly fetched Go1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d.
- [x] Capture five baseline failures in before.log (0passed,5failed).
- [x] Compose five diagnostic schemas, shared local readers and live peer catalog/collector.
- [x] Validate16 grouped Rust cases, real SQL/peer requests, affected all-target checks and make lint.
- [ ] Run actual hook, fresh locked pre-push build, remote verification and checkpoint.

## Context and Orientation


Executor driver/infoschema_meta.rs maps cluster tables to existing local schemas.
Session dispatch.rs owns local rows and process_arm.rs owns shared peer fanout.
Server peer_rpc.rs owns generated receiving requests but creates a bare Session.
Go infoschema/cluster.go maps these five cluster tables; executor/infoschema_reader.go
uses local process visibility, PROCESS admission for deadlocks, shared memory
owners and live Domain index counters. Deadlock key decoding needs current schema.
Rust ClusterSessionFactory owns SharedClusterCatalog and StatsUsageHandle; borrow
those owners through a request-time Session factory instead of copying a stale
catalog or retaining an entire SQL factory.

## Milestones and Plan of Work


First add grouped baseline regressions in the existing server peer tests. Then
extend CLUSTER_TABLES and the local-only reader. Refactor running-transaction
rendering to accept the receiving node registry. Share memory/history rendering
and deadlock admission across local and cluster consumers. Peer construction
borrows live catalog snapshots and the process index collector, enabling real
key/table/index names. Keep current SQL digest lookup limitations explicit.
Finally verify production paths and retire duplicated branches after migration.

## Concrete Steps and Acceptance


Use /workspace/tidb hparser-integration and unchanged /workspace/client-rust master.
Source /workspace/.cloud-setup/env.sh in every shell, export CARGO_BUILD_JOBS=1,
and run Cargo from rust/. Run cargo test --locked -p tidb-server --lib
cluster_diagnostics_batch -- --test-threads=1 before and after implementation.
Before repair the five cases must fail on missing tables/receiver/admission.
Afterward cover live transactions/visibility, deadlock keys/privileges, current
catalog/index counters and shared memory owners with real generated transport.
Run existing cluster-summary/peer cases, affected all-target checks and make lint.
The actual precommit hook and every push must pass cargo build --locked -p
tidb-server from rust/. Normal push only, verify exact remote SHA.

## Surprises & Discoveries


The existing peer Session has neither the production schema image nor its index
usage collector. Registering table names alone would return misleading empty rows.

## Decision Log


- Decision: borrow only the metadata and usage owners through a per-request factory.
  Rationale: preserve fresh schema names without synthetic client/transaction state
  or a factory-retention cycle.
  Date/Author: 2026-10-08, Codex.

## Outcomes & Retrospective


Five diagnostic tables now share local/peer readers and production process/catalog/collector owners. All16 grouped Rust cases and 21 live MySQL/gRPC assertions pass; affected all-target checks, make lint and locked production build pass. Duplicate local dispatch branches were removed. Parent findings remain partial and all54 unresolved counts remain unchanged. Full Go packages,
other tables/operators, global SQL-digest resolution and mixed-node validation
remain separate obligations.

## Idempotence and Recovery


Preserve concurrent work and exact remotes; never force push or bypass hooks.
Logs live under /workspace/.cloud-setup/cluster-diagnostics-batch. Retire only
owned completed binaries with hashes/process checks when disk space requires it.
Keep env.sh's RUST_MIN_STACK=33554432 for debug binary execution.

## Interfaces and Dependencies


Reuse Session::local_cluster_table_rows, ProcessRegistry, existing catalog
conversion, index collector, deadlock and memory managers and ClusterPeerClient.
No new protocol, generated code edits or dependency changes are required.

Publication gates and checkpoint identity are recorded externally in /workspace/.cloud-setup/cluster-diagnostics-batch/final-handoff.json after normal commit/push. Exact commands and log/source hashes are in parity/current-audit/cluster-diagnostics-validation.json.
