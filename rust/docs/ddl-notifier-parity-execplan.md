# Complete pinned-Go DDL notifier parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned source of truth is Go commit `e2788410d8d696605e8cb002585877a063ccc909`, package `pkg/ddl/notifier`, together with its DDL publisher and Domain/statistics-owner integration call sites.

## Purpose / Big Picture

After this work, a successful non-system DDL in the Rust server durably records the same `SchemaChangeEvent` as pinned Go in `mysql.tidb_ddl_notifier`, in the same transaction as the catalog change. The elected statistics owner then delivers each event in DDL order to the stats-meta and priority-queue handlers, commits each handler's work together with its processed bit, retries failures without advancing that handler, and deletes an event only after every registered handler processed it. A user can observe the result by executing DDL, seeing the expected statistics changes, and verifying that processed notifier rows are cleaned up.

## Progress

- [x] (2026-08-31 16:16Z) Inventoried every production, test, support, and build file in pinned Go `pkg/ddl/notifier`.
- [x] (2026-08-31 16:16Z) Added the Rust event model, durable store interfaces, ordered subscriber, owner listener, bootstrap table version 4, SQL table-store adapter, and focused unit/integration tests.
- [x] (2026-08-31 16:16Z) Fixed remote residual-filter and composite unique-prefix PointGet bugs exposed by the notifier's paginated list and processed-bit CAS.
- [ ] Stage the notifier row in the same optimistic transaction as the DDL catalog mutations, using a positive cluster-global DDL job ID and Go's sub-job IDs.
- [ ] Replace synchronous post-DDL statistics mutation with notifier handlers for stats-meta and auto-analyze priority queue behavior.
- [ ] Attach the notifier listener to the statistics owner before campaigning and stop it on retirement/process shutdown.
- [ ] Port or map every pinned package test scenario: publish, basic pub/sub, delivery order, concurrent owners, pagination, pessimistic transaction failure, commit failure, event formatting, and decode reuse/forward compatibility.
- [ ] Produce a package inventory receipt and run WIP then Ready validation, including `make lint` before a completion claim.

## Surprises & Discoveries

- Observation: the notifier's tuple pagination exposed a remote execution bug, not a notifier-store bug. A backend receipt said every described predicate ran even when the logical residual could not be described, and a lookup chunk bypassed local filtering.
  Evidence: the second page initially returned `(2,-1)` again; requiring `PushedScanFilter::fully_described` and disabling remote lookup chunks for incomplete filters made the page return only `(3,-1)`.

- Observation: the processed-bit CAS exposed a missing pinned-Go planner guard. Rust converted a one-column prefix range into a PointGet on a two-column unique primary index.
  Evidence: pinned Go `AccessPath.OnlyPointRange` checks `len(ran.HighVal) == len(path.Index.Columns)`; adding the same condition made the notifier end-to-end test pass.

- Observation: starting a production notifier with no handlers is destructive by design because pinned Go production deletes rows when the registered-handler bitmap is zero. Rust must register real handlers before the statistics-owner campaign.

- Observation: Rust currently applies statistics DDL effects synchronously after `ClusterDdl::execute`. Pinned Go publishes an event inside the DDL transaction and lets the stats owner process it later. Keeping both paths would duplicate mutations; merely publishing after commit would lose atomicity.

## Decision Log

- Decision: do not implement post-commit event insertion as an intermediate production path.
  Rationale: pinned Go calls `PubSchemeChangeToStore` through the DDL session before the job transaction commits. A second transaction can lose an event after a committed schema change and is a behavioral workaround, not parity.
  Date/Author: 2026-08-31 / Codex

- Decision: use the existing statistics owner and `tidb_owner::ListenersWrapper`, registering both production subscribers before `campaign_owner`.
  Rationale: pinned Go intentionally ties notifier delivery and in-memory priority-queue mutation to one statistics owner.
  Date/Author: 2026-08-31 / Codex

- Decision: remove the synchronous Rust statistics DDL path only after notifier publication and both handlers are proven end to end.
  Rationale: this prevents a temporary correctness hole while ensuring the final implementation has one Go-shaped path rather than duplicate behavior.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

The notifier package core and table adapter work in isolation, and their integration test now passes. Production DDL publication and production subscriber wiring remain incomplete, so no package-parity claim is valid yet.

## Context and Orientation

`rust/crates/tidb-ddl-notifier` is the Rust package corresponding to pinned Go `pkg/ddl/notifier`. `SchemaChangeEvent` is the JSON payload persisted for one DDL change. `processed_by_flag` is a 64-bit bitmap: each stable handler ID owns one bit. The subscriber starts a pessimistic transaction, runs one handler, compare-and-swaps its bit, and commits both together, providing at-least-once invocation with exactly-once durable SQL effects.

`rust/crates/tidb-exec/src/cluster_ddl.rs` plans one catalog change into `DdlWrite`. `rust/crates/tidb-exec/src/real_tikv_ddl.rs` commits that write and optional index backfill in one optimistic transaction. This is where the notifier row must be staged before commit. `rust/crates/tidb-server/src/cluster_session_node/ddl_notifier.rs` adapts internal SQL sessions to the notifier store. `rust/crates/tidb-server/src/cluster_session_node/boot.rs` creates and campaigns the statistics owner. `rust/crates/tidb-server/src/cluster_session_node/mod.rs` currently contains the synchronous stats DDL code that must be replaced by subscribers.

Pinned Go production publication is `pkg/ddl/ddl.go:asyncNotifyEvent`; construction and owner wiring are in `pkg/domain/domain.go`; stats-meta registration is in `pkg/statistics/handle/handle.go`; priority-queue registration is in `pkg/statistics/handle/autoanalyze/refresher/refresher.go`.

## Plan of Work

First extend `DdlWrite` with the positive DDL job ID, sub-job identity, notifier-table metadata needed to encode the row, and the optional `SchemaChangeEvent`. Construct events only at the same source points represented by pinned Go constructors. Allocate the job ID from the same global metadata allocator before object IDs, but do not spend it for an `AlreadySatisfied` plan because the rolled-back attempt publishes nothing.

Next add a transaction-staging helper in `real_tikv_ddl.rs`. It must build the bootstrapped notifier table through the ordinary `KvTable` row writer over the DDL transaction's snapshot and `MutationBuffer`, insert `(job_id, sub_job_id, JSON event, 0)`, and commit it with schema metadata and backfill entries. System/memory schemas must skip publication exactly as pinned Go does. Duplicate notifier keys must fail the DDL transaction rather than be ignored.

Then expose stats-meta and priority-queue handler adapters over the notifier session. Translate each event with the same getter/action switch as pinned Go. Reuse the existing statistics write implementations, but execute their SQL mutations through the handler's already-open pessimistic transaction so the processed bit and stats changes commit together. Return `NotReadyRetryLater` from the priority handler while its queue is not initialized.

Finally create one `DdlNotifier` during boot, register stable handler IDs 1 and 2, wrap it with other statistics-owner listeners, and campaign only afterward. Remove the synchronous post-DDL stats and priority-queue calls after equivalent notifier tests pass. Keep no feature flag or legacy fallback.

## Concrete Steps

Run source inventories from repository root:

    git ls-tree -r --name-only e2788410d8d696605e8cb002585877a063ccc909 pkg/ddl/notifier
    git grep -n 'PubSchemeChangeToStore\|RegisterHandler' e2788410d8d696605e8cb002585877a063ccc909 -- pkg/ddl pkg/domain pkg/statistics

Use WIP checks from `rust/` during implementation:

    cargo fmt --all -- --check
    cargo test --locked --offline -p tidb-ddl-notifier --lib
    cargo test --locked --offline -p tidb-executor --lib composite_unique_prefix_is_not_a_point_get
    cargo test --locked --offline -p tidb-server --lib ddl_notifier_table_store_delivers_in_order_and_cleans_up

Before claiming completion, follow `.agents/skills/tidb-verify-profile/SKILL.md` Ready profile, including `make lint`, and record every exact command and result here.

## Validation and Acceptance

Acceptance requires a regression that begins a real cluster-backed DDL, observes the notifier row as part of the committed state, campaigns the statistics owner, and observes both stats behavior and row cleanup. A forced DDL commit failure must leave neither catalog change nor notifier row. A forced handler/commit failure must retain the row and bit, and a later pass must retry it before later events for that handler. System-database DDL must publish no event. Pagination must preserve strict `(ddl_job_id, sub_job_id)` order across pages.

The complete pinned package inventory must map every Go source/test/support/build artifact to Rust implementation, an explicit integration decision, or a justified platform-only exclusion. Partial function coverage is not a completed package.

## Idempotence and Recovery

All focused tests are safe to rerun. Failed optimistic DDL attempts must roll back their mutation buffer and re-plan from a fresh snapshot; never reuse a serialized event whose table IDs came from the failed attempt. If production handler wiring fails midway, keep the notifier listener out of the owner wrapper so no zero-handler worker can delete stored events. Do not restore the synchronous path once it is removed; fix the notifier transaction or handler instead.

## Artifacts and Notes

Fail-before evidence captured on 2026-08-31:

    second notifier page: left [(2, -1), (3, -1)], expected [(3, -1)]
    processed-bit CAS: a retained point-get key has the wrong unique-index width

Pass-after evidence:

    cargo test --locked --offline -p tidb-server --lib ddl_notifier_table_store_delivers_in_order_and_cleans_up
    test ... ok. 1 passed; 0 failed

## Interfaces and Dependencies

`tidb-ddl-notifier` remains the owner of event JSON, store/subscriber contracts, handler IDs, and owner-listener behavior. `tidb-exec` may depend on it to carry and serialize DDL events, but the notifier crate must not depend on executor/server crates. `tidb-server` owns SQL-session and statistics-handler adapters. `tidb-owner::ListenersWrapper` broadcasts the one statistics election lifecycle. The final path must contain no cache-only runner, synchronous stats-DDL fallback, or post-commit event insertion.

Revision note (2026-08-31): initial plan created after the notifier table-store integration exposed two executor/planner correctness gaps and before production DDL publication work began.
