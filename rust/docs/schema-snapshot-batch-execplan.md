# Shared snapshot reads and catalog retention

## Purpose


Allow tidb_snapshot SELECTs inside an existing transaction to read historical
schema and rows without committing, discarding or exposing its buffered writes.
Keep ordinary and historical timestamps protected through result/cursor close.
Retain published schemas in the shared version cache and apply capacity before
lookup, following Go InfoCache. This is connected maintenance for S04, I04 and
O09; full Go packages, lazy InfoSchema V2 and GC remain unaccepted.

## Context and source


Cloud base 57f8c6edd2153cf0c376136534bc552217950c2e; freshly fetched master
93a01d31f6da205ae4bf376825293903a6899fdb. Native client is unchanged at
bc8cca3fea4f741e68a41b124f28164604a57ee2. Read AGENTS.md and PLANS.md.
Go pkg/sessiontxn/isolation/base.go GetStmtReadTS and snapshot tests in
optimistic_test.go/readcommitted_test.go own statement overrides. Go
pkg/infoschema/cache.go owns descending version retention and resize; its
recentMinTS is specifically a lazy V2 API contract and must not be imitated
by an eager catalog. SnapshotTS must not replace the ordinary transaction.

## Progress


- [x] Refresh master/integration and trace shared session, result, storage and cache owners.
- [x] Four grouped Rust cases and ten TCP assertions fail before repair.
- [x] Implement statement snapshot lifetime, isolated physical/evaluation buffers, cursor handoff and cache retention together.
- [x] 32 distinct Rust cases, 18 TCP assertions, affected all-target checks, locked server build and lint pass.
- [x] Update both registers and evidence; prepare normal publication through mandatory gates. Exact hook/push outcomes belong in Cloud final-handoff.json after execution.

## Milestones


First add cases to existing registered catalog_watch, session transactions and
server unistore suites. Use one filter snapshot_provider_batch across compatible
library targets. Baseline must show assertion failures, not compilation errors.

Then compose a statement-specific historical catalog in Session without replacing
Transaction. The shared result close owner retires that catalog and timestamp on
success, errors and early close. Existing explicit physical transaction and staged
writes survive. Keep temporary-table attachment in the same catalog boundary.
Use the existing HistoricalReadProvider and ACTIVE_START_TS guard, and preserve
cursor materialization's timestamp hold. Remove the blanket refusal only after
all consumers use that state. Retain every publication in SharedCatalog with one
bounded version cache; capacity applies before cached lookup.

Finally run grouped tests with CARGO_BUILD_JOBS=1 from rust/ after sourcing
/workspace/.cloud-setup/env.sh, affected all-target checks, root make lint and
git diff --check. Preserve useful tests. Actual hooks/pre-commit must execute
cargo build --locked -p tidb-server; repeat immediately before each authorized
normal push to pingcap/tidb hparser-integration and verify SHA. Never force push.

## Surprises & Discoveries


The initial broad timestamp-cache candidate needs schema-diff MVCC commit timestamps
and lazy V2 ownership, which are not composed. Those remain open; read timestamps
must never be substituted for schema commit timestamps. The current concrete
statement override is refused even though the existing historical provider can
supply the required schema, storage snapshot and retention guard.

## Decision Log


Keep the active transaction intact and attach historical state only to its query
lifetime. Do not create another transaction manager, fake recent-V2 reporting,
or claim complete package acceptance. Preserve original Go tests and mixed-node
obligations. All validation is grouped at the completed batch boundary except the
required fail-before run and publication gates.

## Outcomes & Retrospective


Statement snapshots now preserve ordinary transaction identity, writes and savepoints;
historical tables exclude buffered mutations. Result/cursor guards retire independently.
Published catalog retention and resize-before-lookup match the selected Go contracts.
Both modes pass scan/point/prepared/schema-error and rollback checks. S04, I04 and
O09 remain partial for the explicitly listed wider owners. Full package/lazy V2/GC
acceptance and performance remain unverified.

Recovery is per-file git show from the base above;
never reset concurrent changes. Logs live in /workspace/.cloud-setup/schema-snapshot-batch.
