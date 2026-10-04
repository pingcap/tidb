# Share historical timestamp, schema and read ownership


This living ExecPlan follows root PLANS.md. The Cloud branch is hparser-integration at 796762dedd1662adc267daf74f7dfdc5f433c1cf. Fetched Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. Remote integration 86e41faa92c1a7bb244e6793d90d88c8ce49f44b remains unmerged; no push or dry run.

## Purpose / Big Picture


Connect S04 historical reads with I04 schema selection, O09 timestamp protection and N03 setting consumers. A historical read must use the same timestamp for persisted schema and row data, through existing store snapshots. AS OF, explicit stale transactions and pinned snapshot settings must not read the latest schema or the session's local catalog-history approximation. Complete lazy InfoSchema V2, full package acceptance and GC activation remain separate obligations.

## Progress


- [x] Read instructions, current registers and Go ownership; refresh comparison refs.
- [x] Capture grouped baseline failures on the unchanged server.
- [x] Compose historical snapshot/catalog owner and all selected session/read callers.
- [x] Connect retained timestamps and historical schema cache to existing lifetime.
- [x] Run grouped regressions, real unistore checks and required final gates.
- [x] Update registers/evidence and prepare the normal local commit; operational commit/bundle/draft identities are recorded in the external final handoff. No push.

## Context and Orientation


Session dispatch currently refuses tidb_snapshot and tidb_read_staleness. Its AS OF path uses Catalog.state_as_of, a local commit-history image which is not the persistent store. ClusterTransactions opens current snapshots only, although the native opener can open read-only transactions at an explicit timestamp. SharedCatalog publishes only the latest ClusterCatalog. The server's slot binds a snapshot to all table scans for one statement. ACTIVE_START_TS already retains native transaction timestamps; server-info combines them with process publications.

## Milestones


First establish failing real SQL cases for explicit AS OF, snapshot settings and stale transactions, with a passing current-data control. Second add explicit-timestamp storage and historical schema loading through one retained snapshot, and migrate session calls to that owner. Third exercise current/historical transitions, read-only rejection, schema changes, failure cleanup and active timestamp lifetime, then run final gates together. Each checkpoint must preserve fail-closed behavior for any uncomposed mode.

## Plan of Work


Extend the existing StatementSnapshot and ClusterTransactions owners, then install a session historical-read provider from ClusterSessionFactory. Resolve metadata from the same store timestamp as rows and retain its schema image independently of the latest catalog. Keep selected historical schema versions bounded and preserve the latest publication. Ensure explicit stale transactions never open a fresh writable transaction; query completion and rollback release their retained state. Source references are pkg/sessiontxn/staleread, pkg/domain snapshot schema lookup, pkg/infoschema/cache.go and pkg/domain/infosync minimum timestamp reporting.

## Concrete Steps


Activate source /workspace/.cloud-setup/env.sh and use CARGO_BUILD_JOBS=1. Cargo runs in /workspace/tidb/rust. Store current-instance receipts in /workspace/.cloud-setup/historical-read-batch. Run the existing probe with PYTHONPATH=/workspace/.cloud-setup/python python3 /workspace/.cloud-setup/historical-read-batch/wire.py before /workspace/tidb/rust/target/debug/tidb-server. Extend existing transaction/catalog suites; combine their filters. Required final commands include affected cargo check --locked --all-targets, make lint at repository root, git diff --check and cargo build --locked -p tidb-server through the actual precommit hook.

## Validation and Acceptance


Historical schema and values must survive a later UPDATE and ALTER, and unpinning must restore the latest state. Read-only transactions reject writes; close/error paths release snapshots and timestamp holds. Regressions must fail before and pass after. A passing targeted batch does not accept complete upstream packages, lazy table-loading behavior, real multi-node execution or performance.

## Idempotence and Recovery


Preserve concurrent work, native checkout and current build outputs. No reset, force push or hook bypass. Keep external probe logs and exact exit statuses. Use the existing verified recovery bundle workflow after a validated local commit.

## Interfaces and Dependencies


Use the existing ClusterSnapshot, SwappableSnapshot, ClusterCatalog and native read-only opener. A Session provider supplies a historical Catalog from those authorities; it does not invent a second storage engine. Reuse existing cancellation/resource-group and active timestamp ownership.

## Surprises & Discoveries


AS OF already has a logical local-history path, but the real server's transaction classifier rejects explicit historical BEGIN before reaching it. Enabling only the variable would therefore leave both row and schema authority incorrect.

Four of five expanded baseline wire checks failed; raw numeric snapshot SET also failed before execution. Intermediate grouped runs exposed a stale refusal assertion and a probe that discarded TSO logical bits by formatting a datetime. Go accepts raw TSO first for tidb_snapshot, but datetime first for AS OF. SnapshotTS now resolves in the SET-time timezone and retains that instant across later timezone changes. Existing transaction tests passed after correcting the obsolete table-history assertion. A wrong-working-directory Cargo attempt was stopped and is not validation. Final gates run from rust/ with the repository build profile.

## Decision Log


Decision: select the historical read/schema/timestamp segment as a connected batch. Rationale: each selected finding crosses the same timestamp and metadata boundary. Date:2026-10-04.

## Outcomes & Retrospective


Implementation is composed across storage, versioned catalog lookup, session settings, text/prepared execution, routed write admission and timestamp lifetime. Final grouped validation passes: 89 distinct Rust cases, 16 wire checks, affected all-target check, lint and locked build. See parity/current-audit/historical-read-batch-validation.json for exact commands, source hashes, failures and limits. S04/I04 move to partial, not repaired: complete provider transitions, SafeTS topology and lazy InfoSchema V2 remain incomplete. O09/N03 remain partial. A pinned snapshot inside an already active ordinary transaction fails closed until its provider transition is composed; autocommit-off snapshot reads remain statement scoped. No GC worker is enabled.

Decision: retain full-package and named residual finding obligations. Version capacity is applied during historical lookup; this does not implement the lazy schema byte cache or the full InfoCache timestamp/empty-diff algorithms. The existing local-history fallback remains only for standalone sessions without a store provider. Both production stores install the provider.

Validation outcome: all selected checks above pass; native source and remotes remain unchanged. The normal commit hook, recovery bundle and draft are the final operational steps; their exact identities belong to /workspace/.cloud-setup/historical-read-batch/final-handoff.json. No complete package acceptance follows from these tests.

Remaining provider boundary: unlike Go SetExecutor, snapshot schema/GC/future validation currently happens on first read rather than at SET. This is recorded under S04; the typed SET-time timezone conversion alone does not close that obligation.
