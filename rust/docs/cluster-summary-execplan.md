# Share local and distributed summary readers

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

Queries of cluster statement current/history/cumulative/evicted summaries and
transaction history should consume the existing live local owners on every
discovered TiDB. I01, I03 and O18 share this missing boundary. Preserve user
filtering, PROCESS admission, persistent v2 refusal for cumulative data, encoding
and local identity. This is a connected repair, not acceptance of complete Go
executor, infoschema or statement-summary packages.

## Progress


- [x] Refresh Go master and verify clean integration/native checkouts.
- [x] Inspect Go infoschema/cluster.go, executor/stmtsummary.go and transaction reader.
- [x] Capture grouped SQL/admission/peer regressions: all3 fail on missing paths (before-final.log).
- [x] Compose common cluster schemas, local readers and outgoing/incoming consumers.
- [x] Validate10 grouped Rust cases, real SQL/peer behavior, affected all-target checks and make lint.
- [ ] Run actual hook, fresh locked build before authorized push, verify remote and save checkpoint.

## Context and Orientation

Baseline 04853ff952 has generated peer transport for KILL and CLUSTER_PROCESSLIST.
Session dispatch.rs owns statement and transaction summary rows; process_arm.rs
owns process-only fanout. Executor driver/infoschema_meta.rs registers schemas.
Go cluster.go prepends INSTANCE to local schemas; executor/stmtsummary.go uses
the same summary reader for cluster and local tables. Transaction history hides
rows without PROCESS; eviction counters instead raise access denied.

## Milestones and Plan of Work

First add failing regressions in the existing server peer test module. Then
register cluster schemas from local column declarations, removing duplicate
process/transaction cluster declarations. Extract common discovered-peer fanout
and local cluster row dispatch. Extend generated receiver to these table IDs,
using its privilege registry and the shared session readers without recursive
fanout. Route SQL through that same local reader before outgoing requests.
Finally test the whole batch, review errors/identity/persistent selection and
update both finding registers and this plan. No new transport or synthetic data.

## Concrete Steps and Acceptance

In /workspace/tidb source /workspace/.cloud-setup/env.sh and export
CARGO_BUILD_JOBS=1. From rust run cargo test --locked -p tidb-server --lib
cluster_summary_batch. Before repair all three cases must fail on missing query,
wrong admission and rejected peer table. After repair they and expanded protocol
coverage must pass. Run relevant peer/status tests and affected all-target
checks. From root run make lint. The actual hooks/pre-commit must run cd rust &&
cargo build --locked -p tidb-server; repeat that locked build immediately before
normal push and verify remote HEAD. Store logs under
/workspace/.cloud-setup/cluster-summary-batch.

## Surprises & Discoveries

The eviction rollups already exist in both v1 and v2; only SQL and peer consumers
are absent. Preserve these owners instead of implementing new counters.

## Decision Log

- Decision: group five cluster summary readers with shared schema/fanout ownership.
  Rationale: repairs multiple recorded consumer gaps with one production boundary.
  Date/Author: 2026-10-08, Codex.

## Outcomes & Retrospective

Five cluster summary tables now share local schema declarations, fanout and the generated receiver. Ten distinct Rust cases and 19 live MySQL/gRPC assertions pass; affected all-target checks and make lint pass. Duplicate process/transaction cluster column declarations and the stale refusal assertion are retired. Parent findings I01/I03/O18/N05 remain partial and counts remain54 unresolved. Other roots retain carried evidence;
no complete package, live Go/multi-node TiKV or performance claim is authorized
by these scoped checks alone.

## Idempotence and Recovery

Preserve concurrent changes; never force push or bypass hooks. Retire only owned
completed artifacts with receipts if storage is insufficient. Do not purge caches.

## Interfaces and Dependencies

Use existing Session, ProcessRegistry, StmtSummaryReader/v2 readers and
ClusterPeerClient. Keep generated protocol, codecs and process runtime unchanged.
Expose a local-only cluster table reader for the peer receiver and share schema
mapping with outgoing SQL. No new dependencies are needed.

Final gates and checkpoint identity are recorded externally in /workspace/.cloud-setup/cluster-summary-batch/final-handoff.json after normal commit/push, avoiding a self-referential commit loop. See parity/current-audit/cluster-summary-validation.json for exact commands, log hashes, intermediate test corrections and verification limits.

The initial live probe omitted env.sh and its RUST_MIN_STACK=33554432 setting, passed six checks then overflowed the default development thread stack. The preserved failure is in missing-stack-env-live.json. Sourcing the saved environment yields19 passing checks and clean exit without source changes. This setup prerequisite is not a claim that default-stack debug deployment is fixed.
