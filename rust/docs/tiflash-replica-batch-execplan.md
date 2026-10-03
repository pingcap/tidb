# Repair shared TiFlash replica state and polling

This living ExecPlan follows `PLANS.md`. Changing replica count must preserve an already usable replica, and partition readiness must be published against the physical partition while the containing table becomes ready only when every partition is ready. Polling must use Go's retained progress, exponential tick backoff and five-tick store refresh rather than rediscovering stores on every pass.

## Progress

- [x] Refresh integration and master; Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb, integration remote 7b991676da79f044774caf6da4dfffe247160feb; clean local 80af2d654e.
- [x] Review F02/F03 and Go table.go, ddl_tiflash_api.go and infosync/tiflash_manager.go; retain F01's separate DDL/GC prerequisite.
- [x] Capture six failing metadata/cadence regressions before their corresponding production edits.
- [x] Repair shared count/reset/physical status, DROP/TRUNCATE readiness and retained polling; migrate HTTP/security consumers; remove eight empty ignored shells while preserving their obligations.
- [x] Validate 29 distinct targeted Rust cases, a fifty-tick independent Go source oracle, four affected crates/all targets and root lint.
- [x] Source commit 22aa8ac530 passes the actual hook and fresh immediate-pre-push locked build; real unistore wire smoke passes. Exact requested push is denied with 403 and remote remains 7b991676da.
- [ ] Update both finding registers, receipts and reusable cloud draft; verify exact push destination and outcome.

## Context and Orientation

Work in `/workspace/tidb`, branch hparser-integration. Source comparison is `/workspace/.cloud-setup/go-master`. Native `/workspace/client-rust` remains master and is not edited. Physical IDs identify storage partitions; a logical ID identifies their containing SQL table. `rust/crates/tidb-exec/src/cluster_ddl.rs` currently replaces replica state and resolves status only by logical ID. `tiflash_replica_manager.rs` currently polls logical IDs with fresh gRPC stores on every pass. Production construction is `rust/crates/tidb-server/src/cluster_session_node/mod.rs::build_tiflash_replica_poll`.

## Milestones and Plan of Work

First extend existing catalog-writer and HTTP fixtures with fail-before regressions for preserved availability, physical partition publication and five-tick discovery. Then repair these existing owners in one batch: carry the explicit reset flag, preserve Go's count-change availability policy, update containing metadata for physical IDs, collect normal and adding partition IDs, retain polling state and progress, and obtain write-store metadata from PD HTTP. Keep the legacy rule reconciliation until its DDL and GC replacement is safe; do not claim F01 or complete pkg/ddl/infosync package transcreation. Finish with source review, targeted runtime fixtures and mandatory publication gates.

## Concrete Steps and Acceptance

From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh` and run `cargo test --locked -p tidb-exec tiflash_batch`, followed by `cargo test --locked -p tidb-exec tiflash_replica_manager`. New regressions must fail on original production source and pass after repair. Run `cargo check --locked -p tidb-exec -p tidb-server --all-targets`, then `make lint` from repository root, inspecting the entire log. Commit through executable `hooks/pre-commit`, whose actual command must pass `cd rust && cargo build --locked -p tidb-server`. Repeat that locked build immediately before normal push to origin hparser-integration and verify remote SHA. Preserve any concurrent changes and never force push.

## Surprises & Discoveries

Go preserves Available even when count or labels change, but constructs a new replica record and does not carry old AvailablePartitionIDs. The earlier Rust comments that assigned placement creation to the classic poller contradict current Go; F01 remains open and its old tests are not completion evidence.

## Decision Log

Decision: maintain the existing executable metadata/polling owners together rather than introducing another implementation. Rationale: both findings share their catalog and publication boundary. Date: 2026-10-03.

Decision: no whole-package acceptance or new seed dispatch. Rationale: broader durable DDL and infosync obligations remain unresolved; these are repairs of maintained production owners. Date: 2026-10-03.

## Idempotence and Recovery

Tests use ephemeral metadata and joined HTTP fixtures. Fetch is safe; merge remote integration only after inspecting concurrent changes. Do not delete source or valid dependency artifacts to recover disk. Preserve validated local commits in the existing unpublished bundle if GitHub write remains denied.

## Interfaces and Dependencies

Retain `TiFlashReplicaControl` for owner gating and metadata publication. Add source-shaped poll state inside `TiFlashReplicaManager`; its worker remains stopped and joined before PD/DDL teardown. Reuse reqwest, existing metadata serialization and model partition types; no new external dependency is needed.

## Outcomes & Retrospective

F03's recorded classic polling gap is repaired; F01/F02 remain partial for placement/GC and durable partition phases, and N03 remains partial after connecting the secure HTTP consumer. The register has 60 unresolved findings (49 open, eleven partial), 26 repaired. All targeted checks and both source publication build gates pass. GitHub denied the requested push with 403; remote remains unchanged. The receipt-only follow-up also must use the normal hook. No whole pkg/ddl/infosync/security acceptance, live multi-node result or workload speedup is claimed.

Revision (2026-10-03): the same-owner review also repaired DROP/TRUNCATE identity readiness and physical desired-rule scope, reproduced before edits. Secure HTTP consumes the existing cluster security owner. Eight empty placeholders were removed without removing original-source obligations. See `parity/current-audit/tiflash-replica-batch-repair.md` and its validation JSON for precise evidence and remaining scope.
