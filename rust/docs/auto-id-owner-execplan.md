# Complete cluster auto-ID selection and shared service ownership

This living ExecPlan follows repository `PLANS.md`.

## Purpose / Big Picture

Tables created with AUTO_ID_CACHE=1 must allocate AUTO_INCREMENT values through Go's shared AutoID service, while hidden row IDs retain their separate ordinary allocator. A second SQL node must observe the same service authority. Schema reload, explicit values, UPDATE, SHOW and DDL must consume the same owners. The acceptance target is finding K01; broader table and DDL findings remain open unless their recorded obligations are met.

## Progress

- [x] Inspect clean Cloud checkouts and refresh Go master to 1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d. Selected client-go remains 8edb23f6c7ee and PD afa43111d149.
- [x] Inventory source package and add grouped regressions; demonstrate failures before production edits.
- [x] Compose process discovery/transport and table allocation, separate hidden-row counters, DML cancellation, SHOW CREATE/NEXT_ROW_ID, DDL rebase and rename identity.
- [x] Run grouped regressions, affected all-target checks, lint, self-review and the locked server build. Normal commit-hook and fresh pre-push gates are mandatory; their exact final outcomes are in the external final handoff.
- [x] Update both finding registers and validation receipt; prepare the reusable Cloud checkpoint with accurate acceptance limits. Save/readback pins the final commit after publication.

## Context and Orientation

`rust/crates/tidb-exec/src/cluster_auto_id.rs` has the service allocation/retry policy but no production transport. `rust/crates/tidb-server/src/cluster_auto_id_seam.rs` always selects a metadata-backed cached allocator. `tidb-executor/src/kv_table/auto_id.rs` holds table allocation policy; `kv_table.rs` also uses that allocator for hidden record handles. Go `pkg/meta/autoid` selects singlePointAlloc only for version-five AUTO_INCREMENT with a custom cache of one. Go `TableInfo.SepAutoInc` then separates IID from TID metadata. The shared discovery owner reads the earliest-created election entry and retains a generation-bound gRPC client. Errors, deadlines and shutdown must not create independent per-table transport lifecycles.

## Plan of Work

First extend existing allocator/table suites with separate AUTO_INCREMENT and hidden-row allocation, explicit-value rebasing, SHOW and reload cases. Add socket-backed service coverage using the existing generated native autoid protocol; do not invent protocol messages or a private HTTP endpoint. Introduce the service allocator at the executor boundary without a dependency cycle. Keep ordinary allocation for legacy metadata, hidden rows and AUTO_RANDOM. Wire the process registry to existing etcd and cluster TLS capabilities, then migrate registry reload and table consumers together. Remove superseded assumptions only after migration.

## Milestones

The first milestone establishes failing behavioral examples. The second makes the production allocator selection and all table consumers share the correct authorities. The final milestone records validation and determines whether the complete K01 gap is closed. A source inventory does not itself establish whole-package transcreation; no such claim is made without every production/platform/generated/test obligation.

## Concrete Steps

All work runs in `/workspace/tidb`. Activate `/workspace/.cloud-setup/env.sh`, set CARGO_BUILD_JOBS=1, and run Cargo from `rust/`. Use existing allocator and table test modules for focused before/after checks, followed by affected all-target checking. Run `make lint` at repository root. For Rust commits, the actual hook must run `cd rust && cargo build --locked -p tidb-server`; repeat that command immediately before an authorized push and verify the remote SHA. Never bypass hooks or force-push.

## Validation and Acceptance

Require service-backed allocations across two independently constructed table registries, no hidden-row consumption of service IDs, correct signed/unsigned and increment/offset requests, explicit/forced rebasing, reload identity, leader recovery, terminal application errors and bounded shutdown. Retain existing ordinary allocation/DDL regressions. Record exact command outcomes and distinguish simulated sockets from live TiKV/Go peers. A missing production dependency or unverified cross-node behavior must remain explicit.

## Idempotence and Recovery

Preserve concurrent changes and existing checkouts. Re-run only affected failed gates. Disk is constrained; retire only owned completed executable outputs after recording hashes and confirming they are inactive. Preserve libraries and compiler caches. Revert only this batch's own changes if needed. Native changes, if required, must use the maintained synchronization workflow.

## Interfaces and Dependencies

Keep table policy in tidb-executor, service retry/binding in tidb-exec, and process transport composition in tidb-server. Reuse tidb-pd-client EtcdClient and ClusterSecurity and generated native autoid messages. Service discovery is process-owned and shared across tables; allocation bindings remain table-owned.

## Surprises & Discoveries

The baseline table also routes hidden handles through its auto-increment allocator, so selecting the service alone would be incorrect. Existing service generation state is per allocator, whereas Go shares the discovery generation across all tables; integration must address this boundary.

## Decision Log

- Decision: repair the complete auto-ID segment instead of another MPP maintenance slice. Rationale: K01 has a concrete production-selection gap with observable multi-node behavior; T02/M04 encompass much larger incomplete owners. Author: Codex.

## Outcomes & Retrospective

K01's recorded selection defect is repaired across the connected table/transport/DML/DDL/SHOW batch. The register now has 31 repaired and 55 unresolved findings; T02/M04 remain partial. Service hosting/election remains explicitly unresolved under N05. Validation and publication results are recorded in the receipt and external final handoff.

### Validation discoveries

The three new executor regressions failed together before production changes and pass afterward. The grouped 20-case allocator run initially exposed two stale expectations: Go TestIssue40584 reads Base, not the reserved End, and TestInMemoryAlloc uses a one-ID allocator. Both tests now exercise those source contracts instead of weakening assertions. Six generated-protocol socket cases and ten existing allocation/retry/transfer cases pass; affected all-target checking and make lint pass.

Go's NullspaceID is 0xFFFFFFFF. The old uncomposed leader-path helper used zero; production requests now use the correct value. Cancellation must observe StatementMemory's actual SQL killer; its coprocessor cancellation handle alone does not track KILL QUERY. A canceled discovery waiter retains at most one bounded etcd lookup rather than spawning detached lookups repeatedly. Service connection generation now belongs to process discovery; obsolete per-table reset generations and their mock assertions are removed.

Cross-database rename retains AutoIDSchemaID. Both table construction and DDL metadata counter keys now respect that schema, while DDL service rebasing reads consumed service IDs rather than a stored reserved end. Rebase rebuilds the table-local service Base without replacing shared discovery. Ordinary hidden-row and AUTO_RANDOM counters remain independent.

### Explicit acceptance boundary

This repairs K01's recorded allocator selection/composition defect, verified by the final registry and SQL validation. It does not accept pkg/meta/autoid as a complete transcreated package. Rust does not yet host/elect the AutoID gRPC service on its status server (Go pkg/autoid_service and pkg/server/http_status.go); TiKV AUTO_ID_CACHE=1 requires an existing compatible leader. That server lifecycle remains under N05. API-v2/keyspace hosting, live Go/TiKV multi-node interoperability, full Go suites and performance are unverified. T02/M04 and broad K03/N03/D01 obligations are not closed. No claim that all 56 original roots were re-audited is made.

Final source review retains Go background contexts for ordinary service reads and non-force DDL rebasing; forced internal writes keep the 30-second bound. The first server artifact passes 19 MySQL/unistore checks, including both counters across explicit values, UPDATE, rename, force rebase and truncate. The final rebuilt artifact also passes all 19 checks and shuts down successfully. The grouped total is 37 Rust cases, zero failures/ignored; affected all-target checking and make lint pass. No speedup is claimed.
