# Validate snapshot selection and share historical schema ownership

## Purpose / Big Picture


A successful SET tidb_snapshot must mean that its timestamp and historical schema were accepted by the storage authority. A failed SET must leave the previous effective timestamp and schema usable. A historical lookup made between a schema-version reservation and its diff publication must never reuse the later schema under that reserved version. This connected batch advances S04, I04 and N03; it does not accept complete upstream packages.

## Context and Orientation


Work in /workspace/tidb on hparser-integration, starting at 0605c85276c8228153b1d0f31aaa578ec3146b10. Current Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb after read-only verification. /workspace/client-rust stays at 19a56ccda1e128218cd33c69709038219aced9bc. Preserve remote integration 86e41faa92c1a7bb244e6793d90d88c8ce49f44b unmerged. No push, dry run, worktree or hook bypass.

Session SET currently resolves only a timestamp. The server HistoricalReadProvider loads metadata and binds a row snapshot when a query begins. SharedCatalog caches schemas by raw version, while catalog_reload already implements Go's missing-diff version adjustment. tidb-gcutil already owns mysql.tidb safe-point parsing and error identity. Compose these existing owners rather than duplicating their rules. A safe point is the oldest time the database promises to retain; a schema diff is the committed description of a schema change.

## Progress


- [x] Read instructions, review selected Go owners and verify remote reads.
- [x] Capture baseline failures and implement the connected SET/schema changes.
- [x] Run grouped tests, real unistore SQL checks, affected checks, lint and locked build.
- [x] Update both registers and evidence; normal local commit, recovery and draft identities follow in external final-handoff.json. No push.

## Plan of Work


Expose the existing nonempty-diff version resolver to historical lookup and label a full historical load with that effective version. Add a session schema-selection callback separate from row-snapshot binding. At SET, validate future time through the existing storage opener, read the GC safe point through an independent current snapshot using tidb-gcutil, and retain the selected schema. Do not replace an ordinary transaction's snapshot or retain an idle native timestamp guard. At read, reuse the retained schema with a fresh snapshot at the selected timestamp. Preserve Go's stale-transaction refusal and typed timestamp rollback. Clearing/default releases the schema. Keep standalone sessions without a store provider explicit in the acceptance limits.

## Concrete Steps


Activate source /workspace/.cloud-setup/env.sh. Run Cargo from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Keep logs and one-time probes under /workspace/.cloud-setup/snapshot-validation-batch. Run its wire.py before/after with PYTHONPATH=/workspace/.cloud-setup/python and rust/target/debug/tidb-server. Extend existing catalog_watch, session transaction and unistore_cop suites; do not add a harness. Freeze source before grouped Cargo runs. Finish with affected cargo check --locked --all-targets, make lint at the repository root, git diff --check and cargo build --locked -p tidb-server. A normal git commit must execute hooks/pre-commit and its locked build.

## Validation and Acceptance


Missing/malformed/too-new GC safe points and future timestamps must fail at SET, retaining the old effective read on failure. Snapshot SET/DEFAULT during a stale transaction must return 1568. Loading a schema must preserve an ordinary transaction's uncommitted writes. Reads must keep historical columns and values, while DEFAULT restores current columns. Empty topmost diffs must use the previous version, propagate malformed-diff failures, and never return a later cached schema. No lazy InfoSchema V2, complete timestamp-range cache, ordinary-transaction snapshot provider switching, external/one-shot read timestamp, real multi-node or performance acceptance is claimed.

## Idempotence and Recovery


Preserve current work and build artifacts; never clean Cargo wholesale. Probes own and stop their servers. Failures keep their real exit status. Local commit, bundle verification and Cloud draft readback follow successful gates; post-commit identities belong in the external final-handoff.json. Saving a draft is not publication or a fresh-restore test.

## Interfaces and Dependencies


Use Session snapshot schema/read callbacks, ClusterTransactions, ClusterTableStorage and SwappableSnapshot, SharedCatalog, catalog_reload's effective-version resolver, and tidb_gcutil. A SET-only current snapshot must be independent of the connection's row slot. Keep resource-group and native snapshot lifetimes explicit.

## Decision Log


2026-10-04: select SET validation, retained schema selection and unpublished-diff handling together because all govern whether a timestamp names a valid schema. Leave broader parent findings partial and preserve useful failing tests.

## Surprises & Discoveries


The live reload path already adjusts missing diffs, but historical lookup bypasses it. Go also requires mysql.tidb's GC row at SET; mock-store tests explicitly seed that row. Existing Rust historical tests omitted this prerequisite and must add it when SET validation is composed.

## Outcomes & Retrospective


155 distinct grouped Rust cases, nineteen real MySQL/unistore checks, affected all-target checking, lint and locked server build pass. Baseline evidence reproduced ten wire failures and the cached version 11 versus expected 10 failure. Existing source helpers are shared; no test harness, dependency or vendored code is added. S04/I04/N03 remain partial for the named broader obligations. See parity/current-audit/snapshot-validation-batch-validation.json for commands, log hashes and limits.

SET failure restores the typed timestamp as Go does, while its raw system-variable string remains assigned. All runtime snapshot policy consumers now use the typed field. The selected schema stays pinned without retaining an idle native timestamp. A first grouped invocation was interrupted after an external edit-script working-directory error; a later compile found two test assertions requiring Debug on QueryResult. Correcting those assertions left runtime expectations intact, and the final grouped suite passes.
