# Preserve index evaluation and remove row-based index deletion


This living ExecPlan follows `PLANS.md`.

## Purpose / Big Picture


Creating an index over existing generated values must use the submitting session's SQL mode and time zone. Removing an index must delete its physical key ranges without evaluating current row values. These are connected repairs to the existing D02/K03 owners, not completion of the Go DDL package or its durable reorganization lifecycle.

## Progress


- [x] Read current owners and Go master 3ca96b1d5df8da123e7a650512654eedab12c861.
- [x] Captured five real MySQL failures and two storage/catalog failures against the pre-edit source.
- [x] Connected all index-add producers, typed DDL errors, implicit column-index cleanup and physical-range deletion; migrated legacy adapter callers.
- [x] Run grouped regressions, affected all-target checking, lint, locked server build, and real wire checks.
- [ ] Update audit evidence, review, commit with actual hook, rebuild immediately before authorized push, verify remote SHA and save reusable Cloud checkpoint.

## Context and Orientation


`rust/crates/tidb-exec/src/cluster_ddl.rs` lowers SQL and plans catalog mutations and index work. `rust/crates/tidb-server/src/cluster_session_node/ddl.rs::KvTableIndexBackfiller` performs that work through `tidb-executor::KvTable`. `kv_table.rs` currently rebuilds deletion keys by decoding rows; this can fail under new SQL modes and miss orphaned or differently evaluated entries. Go `pkg/ddl/reorg.go::newReorgExprCtxWithReorgMeta` captures SQLMode/Location, and `pkg/ddl/delete_range.go` deletes index ranges without expressions. The existing direct Rust DDL transaction remains responsible for atomicity; durable Go schema phases and asynchronous GC remain unimplemented prerequisites.

## Plan of Work


Preserve the input statement context in every index-add producer. Resolve reorganization error levels in the shared statement/row decoder owner. Distinguish add work, which needs evaluation context, from removal, which needs only index/table identity. Delete physical index prefixes for ordinary, partition-local and global indexes. Migrate every old adapter caller before removing the adapters. Retain useful source-contract regressions and avoid a new test harness.

## Validation and Acceptance


From `/workspace/tidb/rust`, activate `/workspace/.cloud-setup/env.sh` and set `CARGO_BUILD_JOBS=1`. Run focused index/generated-column/session and catalog tests together, then `cargo check --locked --all-targets -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server`. Run `make lint` from the repository root and the required `cargo build --locked -p tidb-server` from `rust`. Real unistore SQL must reject strict division-by-zero index backfill and permit index removal irrespective of generated expression evaluation. Storage regressions must remove orphan keys and preserve other indexes/rows. Logs live in `/workspace/.cloud-setup/index-lifecycle-batch`; the committed receipt will retain counts and limitations.

## Idempotence and Recovery


Use the existing Cloud checkouts, preserve unrelated changes, never force-push. Tests own their temporary stores/processes. Do not remove shared build caches. A failed transaction must leave metadata unchanged; standalone memory storage needs rollback for partial deletion failures. No native source/dependency change is planned.

## Surprises & Discoveries


Index removal currently removes metadata before row decoding succeeds. Its zone-only wrapper remains used by the server although local DDL already takes a statement context. Both adapters become unnecessary when deletion uses key identity.

## Decision Log


- Decision: Repair creation and removal together in the shared table owner, retaining direct-transaction limitations explicitly. Rationale: one lifecycle supplies every local/cluster caller and allows obsolete row decoding paths to be retired safely. Date: 2026-10-07.

## Outcomes & Retrospective


All-target checking, make lint and locked server build pass. 107 grouped Rust tests and 6 real MySQL assertions pass. Parent findings remain open/partial until their distributed owners and package obligations are complete.


The implementation additionally repairs generic 1105 error flattening: index backfill now retains the shared SQL error triple. The original unique-index probe returned1105 instead of1062. Grouped DROP COLUMN also lacked index-key cleanup, so both single and grouped producers now schedule physical removal. Under disk pressure, four obsolete setup download extractions were verified file-for-file against retained ZIP archives before retirement; pre-edit test binaries were temporarily preserved in verified gzip archives, then retired after the current107 tests and six wire assertions passed. Hash receipts remain; current binaries are retained. Current compilers, dependency caches, native tests and production binaries remain available. Exact external preservation manifests live beside the batch logs.


Final comparison refresh uses Go7a3dacb52efe58d28db360ae8639d8838c376544. A complete git-archive refresh corrected the stale comparison SHA stamp and retired an untracked historical cloudhintprobe. Only the new index-join grouping source and fixtures changed relative to3ca96b1d5d; they remain a separate unverified planner follow-up. The DDL owners and module pins are unchanged.
