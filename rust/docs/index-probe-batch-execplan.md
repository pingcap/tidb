# Preserve index-join probe identity and reader policy

This living ExecPlan follows root `PLANS.md`.

## Purpose / Big Picture

Repair three connected existing-owner gaps: GROUP BY expressions must not split one group across probes; inner hash joins must distinguish aliases of one table; rebuilt readers must retain network estimates for adaptive replica reads. No whole package acceptance is claimed.

## Progress

- [x] Confirm clean integration 8125137b88, refresh unchanged Go master 7a3dacb52e, inspect source and actual hook.
- [x] Add grouped regressions to existing planner/executor/session suites.
- [x] Three distinct regressions fail before production changes; four selected session controls and six real-wire assertions already pass.
- [x] Repair admission, logical-key branch selection and shared reader policy together.
- [x] Grouped tests, all-target checking, lint, locked server build and self-review.
- [ ] Update both registers and receipt, commit through hook, fresh build before push, verify remote and checkpoint.

## Context and Orientation

Go `pkg/planner/core/exhaust_physical_plans.go::checkIndexJoinInnerTaskWithAgg` counts only direct GroupByItems columns. Rust `tidb-planner/src/find_best_task/dispatch.rs` recursively extracts nested columns. Go `pkg/executor/builder.go::indexJoinLookupChildIdx` selects a child by membership of every retained join-key UniqueID. Rust `tidb-executor/src/driver/physical_builder.rs` searches table IDs, ambiguous for self joins. Go builds ordinary no-range readers for inner tasks, retaining GetNetDataSize and GetAvgTableRowSize. Rust `IndexJoinLookupExec` recreates default policy without these estimates.

A probe restricts an inner read to outer keys. UniqueID distinguishes SQL aliases of the same physical table. Estimated bytes feed replica selection; they are not a performance measurement.

## Plan of Work

Extend existing suites first. Admit direct grouping columns only. Replace table-ID lookup-branch search with schema membership of all retained inner keys. Supply the selected reader's PushdownStatementContext to IndexJoinLookupExec, retaining embedded plan estimates and separate table-row width for double-read batches. Existing worker cloning must preserve that statement.

## Milestones

Capture the distinct failures together; migrate the connected production handoff and retire the obsolete selector; run one combined regression set and boundary gates before recording publication.

## Concrete Steps

From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh` and export CARGO_BUILD_JOBS=1:

    cargo test --locked --no-fail-fast -p tidb-planner -p tidb-executor -p tidb-session --lib -- index_probe_ admits_index_join_inner_child_pattern_matches_go --test-threads=1
    cargo check --locked --all-targets -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run `make lint` from root. The actual commit hook must build the locked server. Repeat this build immediately before every normal push and verify the remote SHA.

## Validation and Acceptance

Nested grouping-key membership fails admission, while direct columns remain valid. Inner hash joins select exactly one alias. Actual requests retain positive network estimates; double reads scale table widths by handle count. The Go regression returns one count of two under index/hash hints. Separate assertions must fail before fixes and pass afterward. Live multi-node TiKV, performance and whole packages remain unverified.

## Idempotence and Recovery

Preserve concurrent work, existing checkout and exact remotes. Do not create a worktree. Preserve dependency caches; do not cargo clean. Only verified obsolete build artifacts may be retired. Keep external logs in `/workspace/.cloud-setup/index-probe-batch`; never replay historical cleanup helpers.

## Surprises & Discoveries

The aggregation comment already promises bare columns, but code extracts nested expressions. Table-ID branch selection cannot distinguish self joins although Go uses logical identity.

## Decision Log

Repair one shared probe lifecycle with existing owners. No duplicate lookup engine or dependency is needed. Keep parent dispositions open until their full acceptance boundaries are satisfied.

## Outcomes & Retrospective

Three baseline failures are repaired; 21 selected Rust tests and six real MySQL controls pass. Lint, affected all-target checks and locked production build pass. Counts remain30 repaired/56 unresolved; unexamined findings retain prior evidence. Full Go packages, live TiKV replica placement and performance remain unverified.

## Artifacts and Notes

Logs and final postcommit evidence live under `/workspace/.cloud-setup/index-probe-batch`. A committed receipt will record actual counts, commands and limitations.

## Interfaces and Dependencies

Reuse PhysicalIndexJoin.inner_join_keys, Schema::contains, physical reader estimates, PushdownStatementContext and for_lookup_batch. Native client changes are not planned.

Validation discovery: the wider retained suite exposed three stale assertions that also fail in the preserved before-edit executable. Current Go infer_pushdown.go admits nonbinary REGEXP to TiKV, so its cop Selection does not require the logical inner multi-pattern switch. The dedup test now checks both stream aggregation stages, ordered access and actual deduplicated rows instead of IDs/cardinalities copied from an older source revision. The multiway Cartesian query has no ORDER BY and now compares its complete row multiset. The obsolete table-ID helper test was retired only after its distinct-table case migrated into the production builder regression. That new physical fixture needed consistent scan cost_columns and joined output coordinates; the final affected executor selection passes all six tests. Planner (one) and session (fourteen) selections passed on unchanged production code before the final fixture-only correction.

Workflow lesson: choose the same retained regression filters for baseline and final validation before editing, so stale fixture assumptions are discovered in the first combined compilation. Keep source edits grouped; after test-only fixture corrections, rerun the affected crate selection and retain already-passing unrelated suites. Do not accept a zero-test run or hide intermediate failures.

Review note: Go initializes both double-read index estimate and table average from GetAvgTableRowSize. Table requests scale by actual handles. An intermediate compile was stopped before tests to correct this source detail. The Go wire GROUP BY fixture passed before repair; only planner admission reproduced that gap. Publication remains pending until the external final-handoff receipt records the actual hook, fresh prepush build and remote verification.
