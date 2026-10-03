# Repair exact MPP ranges and incremental results

This living ExecPlan follows root PLANS.md. Disjoint TiFlash scan ranges must not read the gaps between them, and a slow remote stream must return its first packet without buffering its entire answer. Work stays in Codex Cloud; the user forbids pushes to both repositories.

## Progress

- [x] Recheck M02/M03/M04 against fetched master 93a01d31f6da205ae4bf376825293903a6899fdb and local integration 02f3ba2e620dedde4e8312fa504b3c78004e0add.
- [x] Capture three fail-before exact-range/unrelated-region/delayed-stream regressions; they pass after repair.
- [x] Compose canonical process PD/cache capabilities, shared traversal with I/O outside the cache lock, exact intersections, query lease, live pull stream, shared cancellation/quota and bounded cleanup.
- [x] Validate 37 targeted Rust cases, five affected crates/all targets and root lint.
- [ ] Pass the actual locked-server precommit hook and run the new binary wire smoke.
- [ ] Update both finding registers and durable receipts, commit locally and preserve the recovery bundle/startup draft.

## Context and Orientation

The live source is rust/crates/tidb-exec/src/tiflash_mpp_scan.rs. CopScanSource dispatches TiFlash requests there; the server constructs one source per node. Its old table_regions queries one min/max envelope once with a 10,000-region cap. open_mpp drains the result stream into VecDeque. RegionCache and BackgroundRegionCache in tidb-txnkv already own exact batch lookup, pagination, coverage validation and invalidation. ProductionReadProcessAuthority passes their existing request-opening capability; MPP opens one foreground lease and releases it only after cleanup. StatementMemory in tidb-executor owns SQL cancellation and quota errors. MPP retains an Arc runtime so each QueryResponse::next advances only one live packet; no packet-draining worker or queue is created. PushdownStatementContext must carry that existing owner to MPP, not create a separate cancellation flag. Normative sources are pkg/store/copr/mpp.go, batch_coprocessor.go and pkg/executor/internal/mpp/local_mpp_coordinator.go in /workspace/.cloud-setup/go-master.

## Milestones and Plan of Work

First expose the existing region-info and connection behavior to focused tests without changing semantics. Capture failures for disjoint ranges and a delayed terminal stream. Then replace envelope discovery with the maintained RegionCache and preserve each requested range when projecting region infos. Retain the live gRPC stream in QueryResponse so its consumer pulls one packet at a time. Check the statement killer while waiting, account each retained response, and send a bounded best-effort cancel once on early close/error/drop. Apply dispatch RetryRegions to the same retained cache. Preserve natural completion without unnecessary cancellation. Finally self-review, validate and record precise residual scope. This repairs existing executable owners; it grants no whole-package transcreation acceptance and adds no new supported MPP plan shape.

## Concrete Steps and Validation

Source /workspace/.cloud-setup/env.sh for every Rust command. From /workspace/tidb/rust run cargo test --locked -p tidb-exec tiflash_mpp_scan, followed by cargo check --locked -p tidb-exec -p tidb-executor -p tidb-server --all-targets. From /workspace/tidb run make lint, inspecting the complete output. Commit normally: hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server. Never bypass hooks or push. Capture logs under /workspace/.cloud-setup/mpp-batch and hashes in the committed validation receipt. Real multi-node TiFlash behavior and broader MPP planner/transport policy remain unverified unless independently exercised.

## Surprises & Discoveries

The maintained RegionCache already implements pagination and coverage checks; a second MPP-specific pagination algorithm would duplicate ownership. PD raw metadata keys and SQL record keys have different encoding boundaries; PdRegionLoader owns that conversion.

## Decision Log

Decision: repair M02/M03 together and M04's stale-region invalidation through their shared scan owner. Rationale: range lookup, dispatch, stream and cleanup share one request lifetime; separate leaf fixes leave correctness holes. Date: 2026-10-03.

Decision: retain honest partial status for any missing coordinator, shared transport or complete source obligations. Rationale: this is maintenance of the existing scan-only implementation, not acceptance of the whole Go copr or MPP package. Date: 2026-10-03.

## Idempotence and Recovery

Preserve concurrent repository changes. Tests use ephemeral joined gRPC fixtures. Only remove verified obsolete build outputs if disk is insufficient. Retain all source and validation logs. Local commits and /workspace/.cloud-setup/tidb-unpublished.bundle preserve delivery; fresh-task restoration remains unverified.

## Outcomes & Retrospective

The three initial regressions now pass along with quota, kill, cleanup, EOF, shared-cache pagination/invalidation and identity tests. Five affected crates/all targets pass. Shared traversal and shutdown tests pass (fourteen plus three), and four executable MPP planner cases remain passing. Lint passes. Completion still requires the actual locked-server commit hook and final register/receipt delivery. No full package or live multi-node acceptance is claimed.
