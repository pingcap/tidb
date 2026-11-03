# `mysql.*` writes as Go's statements on Go's sessions

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

Go writes its system tables with SQL: a session from a pool runs the
statement, usually inside a transaction the caller opens
(`util.CallWithSCtx(pool, f, util.FlagWrapTxn)` in the statistics handle,
`ExecRestrictedSQL` / `ExecuteInternal` elsewhere). The statement goes
through the session's table layer: its row format and checksum, unique and
index checks, `TableDeltaMap` (the system table's own `stats_meta` delta),
and whatever column layout the bootstrap or an upgrade gave the table.

The Rust rewrite instead encodes many of those rows by hand
(`tidb_syssession::system_row_write`). The DDL owner's rows moved to Go's
statements in `docs/ddl-step-session-execplan.md`; this plan covers the
rest. After it, each site below runs the statements Go runs, on a session
as Go's does, and its raw writer is gone.

## Inventory (2026-11-03, `system_row_write` callers outside the DDL owner)

Counted by raw-write call sites; each is one Go owner.

1. `tidb-stats-handle-storage/src/cluster_stats_write.rs` (22 sites, 2,487
   lines). Go `pkg/statistics/handle/storage/save.go`
   (`SaveAnalyzeResultToStorage`, `SaveColOrIdxStatsToStorage`,
   `SaveMetaToStorage`, `InsertColStats2KV`, `InsertTableStats2KV`,
   `saveTopNToStorage`, `saveBucketsToStorage`) plus `update.go` /
   `gc.go`'s statements. Rust already builds Go's statement text
   (`save_topn_sql`, `save_buckets_sql`, `save_stats_item_sql`) and models
   each statement (`LoadedStatsItemStatement`), but plans every one into row
   mutations itself instead of executing it.
2. `tidb-syssession/src/stats_table_rows.rs` (5): shared row encoders of
   the statistics tables, used by the sites above.
3. `tidb-stats-handle-lockstats/src/cluster_stats_lock.rs` (5): Go
   `lockstats` `INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?)
   ON DUPLICATE KEY UPDATE table_id = %?` and `DELETE FROM
   mysql.stats_table_locked WHERE table_id = %?`.
4. `tidb-syssession/src/cluster_sysvar_write.rs` (5): Go
   `SetGlobalSysVar` -> `REPLACE mysql.global_variables VALUES (%?, %?)`.
   Rust diffs a whole overrides image against the stored rows. Behavior
   checked equal for `SET GLOBAL x = DEFAULT` (the row keeps the default,
   as gorun shows).
5. `tidb-privilege/src/cluster_account_write.rs` (5): Go's account
   statements (`CREATE USER`, `GRANT`, ...) execute SQL against
   `mysql.user`, `mysql.db`, `mysql.tables_priv`, ...; Rust diffs a whole
   privilege image.
6. `tidb-syssession/src/advisory_locks.rs` (key computation for a raw
   pessimistic lock): Go `GET_LOCK` runs
   `INSERT INTO mysql.advisory_locks (lock_name) VALUES (%?)` in a
   dedicated session's pessimistic transaction it keeps open.
7. `tidb-stats-handle-autoanalyze/src/analyze_jobs.rs` (2):
   `mysql.analyze_jobs`.
8. `tidb-stats-handle-history/src/history_stats.rs` (1):
   `mysql.stats_history` / `stats_meta_history`.
9. `tidb-stats-handle-usage/src/session_stats_collect.rs` (1):
   `mysql.column_stats_usage` / predicate usage.
10. `tidb-executor/src/real_tikv_analyze.rs` (1).
11. `tidb-session/src/mysql_bootstrap*.rs` (4): Go's bootstrap also runs
    SQL (`doDDLWorks`, `doDMLWorks`); the Rust bootstrap stages rows before
    a session exists, which is the one place that may legitimately stay
    raw until the bootstrap itself runs on a session.

## Plan of work

- M1: the statistics handle's session pool (Go `statsHandle.SPool()`) and
  `util.CallWithSCtx(..., FlagWrapTxn)`: a pooled node session in one
  pessimistic transaction, over the store's transactions -- the seam the
  DDL work built (`DdlSessionPool`, `StepSqlSession`) is the model.
- M2: `cluster_stats_write.rs` executes its `LoadedStatsItemStatement`s and
  the analyze-result statements as SQL in that transaction; delete the row
  planners and `stats_table_rows.rs`'s encoders as their callers go.
- M3: lock stats, history, usage, analyze jobs (items 3, 7-10).
- M4: global sysvars and accounts (items 4-5): one statement per changed
  row, as Go, instead of the image diff.
- M5: advisory locks on a dedicated session (item 6).

Each milestone: the tests that observe the tables (and the integration
corpus) pass unchanged, a mutation of each statement fails one of them, and
a test pins anything a statement changes that raw rows did not (deltas,
checksums).

## Progress

- [x] (2026-11-03) Lock stats (item 3, part of M3): `statsLockImpl` is
  `session_stats_lock::execute_stats_lock` -- Go's statements
  (`selectSQL`, `insertSQL` + `updateMetaVersionSQL`, `selectDeltaSQL`,
  `updateDeltaSQL`, `DeleteLockSQL`) over `lock_stats`' policy, in
  `call_with_sctx(pool, ..., FLAG_WRAP_TXN)` on the domain's system-session
  pool (Go `statsHandle.SPool()`). The `ClusterStatsLock` seam, its raw
  committer (`real_tikv_stats_lock.rs`) and row planners are gone; a
  statistics session's statement error now keeps its MySQL code
  (`StatsSessionStatementError`). Tests
  `lock_stats_runs_in_its_own_session_transaction_as_go`,
  `lock_stats_checks_privileges_before_its_transaction_as_go`.
- [x] (2026-11-03) Analyze jobs (item 7): the session's job lifecycle and
  the domain's delete / corrupted-job cleanup run Go's statements on the
  domain's system-session pool. The raw cleanup had kept `update_time`;
  Go's UPDATE refreshes it (`ON UPDATE CURRENT_TIMESTAMP`, gorun). The row
  planners remain only as seeding fixtures of three session tests and the
  executor's planner tests.
- [x] (2026-11-03) Advisory locks (item 6, M5): `GET_LOCK` /
  `IS_USED_LOCK` are Go's `advisoryLock` -- a domain internal session per
  lock running `SET innodb_lock_wait_timeout`, `BEGIN PESSIMISTIC` and
  `INSERT INTO mysql.advisory_locks`, released by `ROLLBACK`
  (`cluster_session_node/advisory_locks.rs`). The raw pessimistic key lock
  (`ClusterTransactions::acquire_advisory_lock`, `AdvisoryLockTable`) is
  gone. `GET_LOCK(x, 0)` now waits Go's one second (the variable's minimum)
  before answering 0.
- [x] (2026-11-03) ANALYZE saves (item 10, M1 + the analyze half of M2):
  `statsReadWriter.SaveAnalyzeResultToStorage` /
  `SaveColOrIdxStatsToStorage` / `handleSlowStatsSaving` are Go's
  `storage` statements (`tidb-stats-handle-storage/src/save.rs`,
  `update.rs`), each Go `statsReadWriter` save one
  `call_with_sctx(..., FLAG_WRAP_TXN)` over a `StatsSessionPool`
  (`stats_read_writer.rs`; the session's `SessionStatsPool` is the domain's
  system-session pool). Table, partition,
  multi-valued / global-index and global-stats saves all go through it; the
  index-only analysis now reads in a read-only transaction and saves
  separately, as Go. `plan_analyze_stats_write`,
  `plan_independent_index_stats_write`, `plan_partial_stats_write` and their
  tests are gone. Go keeps histograms an analysis did not produce (a dropped
  index's rows stay until statistics GC; gorun); the raw full-table plan
  had retracted them. ANALYZE now stamps
  `column_stats_usage.last_analyzed_at` (table and global saves), as Go; the
  raw path never did. Tests
  `a_reanalyze_keeps_a_dropped_index_histogram_as_go`,
  `analyze_results_save_stats_meta_as_go`.
- [x] (2026-11-03) LOAD STATS / DROP STATS (M2): Go
  `loadStatsFromJSON` (`SaveColOrIdxStatsToStorage`,
  `SaveColumnStatsUsageToStorage`, `SaveMetaToStorage`) and
  `DeleteTableStatsFromKV(soft)` through the `StatsSessionPool`;
  `handleSlowStatsSaving` lives in `stats_read_writer.rs` for every save.
  LOAD STATS no longer stores the dump's FM sketches (Go only deletes them;
  `load_stats_stores_no_fm_sketch_as_go`). The `LoadedStatsItemStatement`
  planners are gone.
- [x] (2026-11-03) Statistics GC (M2) and historical stats (item 8):
  Go `storage.GCStats` / `ClearOutdatedHistoryStats` (`gc.rs`) and
  `history.RecordHistoricalStatsMeta` / `RecordHistoricalStatsToStorage`
  (`history_stats.rs`) on the domain's system-session pool. The raw history
  planners had written `version` unsigned into Go's signed BIGINT columns,
  so SQL deletes missed their keys. Internal statements now reach this
  tier's text parser through `escape_sql_text` (byte arguments as
  `_binary X'...'`); the DDL session pools had replaced non-UTF-8 bytes.
- [x] (2026-11-03) Usage (item 9): Go `DumpStatsDeltaToKV` (one wrapped
  transaction per batch: `QueryLockedTables` on its own session,
  `storage.UpdateStatsMeta`, then "flush stats" history) and
  `DumpColStatsUsageEntries` on the pool; `sys_session::StatsPool` is the
  one statistics-pool seam for the domain and the session.
- [x] (2026-11-03) The DDL statistics subscriber: Go
  `pkg/statistics/handle/ddl` as `tidb-stats-handle-ddl`, its storage
  statements (`InsertTableStats2KV`, `InsertColStats2KV`, `UpdateStatsVersion`,
  `ChangeGlobalStatsID`, `StatsMetaCountAndModifyCount`) on the notifier's
  session in its transaction; `StatsSessionContext::affected_rows` is Go
  `StmtCtx.AffectedRows`. The auto-analyze queue reads locked tables with
  `QueryLockedTables`.
- [x] (2026-11-03) Global sysvars (item 4, M4): Go `SetGlobalSysVar` --
  the assignment validates and runs its `SetGlobal` hooks on a copy of the
  cache, then `replaceGlobalVariablesTableValue` (`REPLACE INTO
  mysql.global_variables`) for the name and every alias a hook moved (Go
  `SetGlobalSysVarOnly`: `transaction_isolation` also writes
  `tx_isolation`), on the domain's system-session pool, then
  `NotifyUpdateSysVarCache(true)`. `tidb_enable_metadata_lock`'s hook is
  Go `ddl.SwitchMDL` (refused while a DDL job is queued; the process flag
  and the meta key follow); the image diff wrote the meta key from the
  stored value instead. Go writes a row for an instance-scoped variable
  too and keeps it out of its caches (`rebuildSysVarCache`); the loader now
  filters the same way. Test `set_global_replaces_the_variables_row_as_go`.
  `plan_sysvar_write` remains as a seeding fixture of two tests.
- [x] (2026-11-03) Accounts (item 5, M4) were already Go's: every account
  statement runs `tidb_executor::user_table`'s statements on a system
  session and then `NotifyUpdatePrivilege`. `plan_account_write` is left
  only in `seed_cluster_accounts`, which writes a unistore node's
  `--auth-file` identity at boot -- the bootstrap's category (Go has no
  such flag).
- [x] (2026-11-03) ANALYZE options and predicate columns: two statement-time
  raw writers the first inventory missed (they went through `StatsRows`, not
  `system_row_write`). Go `AnalyzeExec.saveAnalyzeOptions` is one `REPLACE
  INTO mysql.analyze_options` on a restricted session after the analysis;
  `PlanBuilder.getSavedAnalyzeOpts` its SELECT; `statsHandle.GetPredicateColumns`
  (`cleanupDroppedColumnStatsUsage`'s DELETE, then the SELECT) a wrapped
  transaction on the pool at plan time. The raw cleanup had ridden in the
  sampling transaction. `StatsSessionContext::latest_table_meta_by_id` is
  Go `sctx.GetLatestInfoSchema().TableByID`. Test
  `analyze_predicate_columns_forget_a_dropped_column_as_go` (gorun).
- [x] (2026-11-03) Readers: SHOW ANALYZE STATUS is Go
  `dataForAnalyzeStatusHelper`'s restricted SELECT and SHOW
  COLUMN_STATS_USAGE Go `statsUsageImpl.LoadColumnStatsUsage`, both on the
  domain's system-session pool (the providers took a resource group Go's
  calls do not have). Test
  `show_column_stats_usage_follows_the_session_time_zone_as_go` (gorun).
- [x] (2026-11-03) The statistics dump: Go `TableStatsToJSON` (the domain's
  `table_json`: `TableStatsFromStorage(loadAll)` in a wrapped transaction,
  then `StatsMetaByTableIDFromStorage` and `LoadColumnStatsUsageForTable`)
  and `TableHistoricalStatsToJSON` (`json.rs`, four SELECTs) on the pool.
  `storage/read.rs` now holds Go `read.go`'s `TableStatsFromStorage`,
  `indexStatsFromStorage`, `columnStatsFromStorage`,
  `HistogramFromStorageWithPriority`, `CMSketchAndTopNFromStorageWithHighPriority`,
  `TopNFromStorage`, `FMSketchFromStorage`, `StatsMetaByTableIDFromStorage`
  and `convertBoundFromBlob` (a BIT bound longer than the column fails the
  load, as Go's `ErrDataTooLong`; the raw loader saturated it). The raw dump
  module (`cluster_stats_dump.rs`) is gone. Test
  `latest_stats_dump_reads_the_analyzed_histograms_as_go`.
- [x] (2026-11-04) Groundwork for the DDL readers: `tidb-ddl-session` (Go
  `pkg/ddl/session`) takes `dyn SessionContext`; the node's pooled system
  session implements it over the session's transaction API; the DDL pool
  trait offers Go's `ddl.sessPool` (`DdlSessionPool::sess_pool`). Test
  `the_ddl_session_pool_is_gos_sess_pool_over_the_node`.
- [x] (2026-11-04) S1, sysvar half: the reload loop (Go
  `LoadSysVarCacheLoop`) starts after the domain and reads
  `mysql.global_variables` through its system sessions (Go
  `fetchTableValues`); a SET GLOBAL's cache rebuild reads through the
  commit's own pool; the metadata-lock switch at startup is Go
  `InitMDLVariable` -- its meta key, stored `true` when missing -- not the
  sysvar row (the server also re-published it from the row; removed). The
  raw sysvar reload chain is gone. Left of S1 for sysvars: the first load
  before the domain exists (`load_startup_variables_from_cluster`, which
  also reads `mysql.tidb`'s system time zone). Tests
  `the_sysvar_reload_reads_a_peers_row_through_the_domains_sessions_as_go`,
  `startup_mdl_state_is_the_metadata_lock_meta_key_as_go`.
- [x] (2026-11-04) S1, privilege half: the node's account table starts empty
  and is filled after the domain by Go `LoadPrivilegeLoop` -> `UpdateAll` ->
  `MySQLPrivilege.LoadAll`, a `SELECT HIGH_PRIORITY` per `mysql.*` table on
  the domain's system sessions (`cluster_privileges::update_all`; a missing
  table is skipped as Go `noSuchTable`; TIMESTAMPs are Go
  `GoTime(time.Local)`); the reload thread reads the same way, and its
  "unbootstrapped snapshot" arm is gone (Go has none: a missing
  `mysql.user` is the load's error). Go loads lazily per active user
  (`ensureActiveUser`); this node still loads every account, a registry
  shape left for its own finding. Test
  `the_privilege_load_reads_a_peers_rows_through_the_domains_sessions_as_go`.
- [x] (2026-11-04) S1, binding half: Go `InitBindingHandle` after the
  domain and its sysvar loop -- `NewBindingHandle(sysSessionPool)` loads
  `mysql.bind_info` on the domain's sessions from the first load on (the
  raw record-range fallback for a handle without sessions is gone), the
  domain stores the handle once (`Domain::init_binding_handle`, Go
  `bindHandle.Store`) and commit hooks read it at commit, so a session
  opened earlier sees it. Covered by the binding suites (no behavior a
  test could see changed: the attached sessions already served every load
  after startup); a mutation that skips the store fails six of them.
- [x] (2026-11-04) Sessions take globals the Go way: creation installs the
  accessor (Go `createSession`'s `GlobalVarsAccessor`) and the copy
  (`loadCommonGlobalVariablesIfNeeded`, from `GetSessionCache`) is taken at
  the first statement -- the statement-boundary parse (Go `Parse`), the
  prepare door (`PrepareStmt`) and the statement context
  (`executeStmtImpl` after `PrepareTxnCtx`) -- keeping any variable the
  session already set, and applying `CLIENT_INTERACTIVE`'s
  `wait_timeout = interactive_timeout` there. Three tests that encoded the
  copy-at-open rule now state Go's. Test
  `a_session_copies_the_global_tier_at_its_first_statement_as_go`.
- [x] (2026-11-04) S1, sysvar first load: `SysvarReloader::spawn` runs Go
  `LoadSysVarCacheLoop`'s first `rebuildSysVarCache` on the domain's
  sessions before the loop starts (its failure fails startup). Before the
  domain only Go's own pre-domain reads remain (`real_tikv_privileges.rs`):
  `InitMDLVariable`, `initGlobalVarFromSystemDB`'s system time zone, and the
  bootstrap flag; the raw account and sysvar snapshot loads are gone. The
  builders run, after the domain and its services: the sysvar cache, the
  privilege load, the binding handle (Go loads accounts at first use, so
  its pooled sessions never copy the global tier before the cache is
  built; here the full load follows the cache for the same reason). Test
  `the_sysvar_loop_builds_the_cache_before_it_starts_as_go`.
- [x] (2026-11-04) S1, statistics refresh: Go `StatsCacheImpl.Update` runs
  on the statistics handle's sessions -- the ordered `mysql.stats_meta`
  SELECT and `statsReadWriter.TableStatsFromStorage` (`storage/read.rs`,
  wrapped transaction) behind the cache's existing `StatsRefreshSource`
  (`real_tikv_stats::update_stats_cache`); the raw refresh source is gone.
  The lease loop starts after the binding handle (Go `UpdateTableStatsLoop`),
  and ANALYZE / LOAD STATS / DROP STATS refresh through the statement's
  pool (`ClusterAnalyze::update_stats_cache` takes it). Covered by the
  statistics suites (same data, read by SQL); a mutation that drops the
  `stats_meta` rows fails 85 of them.
- [x] (2026-11-04) `information_schema` size columns: Go
  `cache.TableRowStatsCache` is one process-wide cache
  (`TABLE_ROW_STATS_CACHE`, was one per session) whose `UpdateByID` runs
  `getRowCountTables` and `getColLengthTables` as two SELECTs on the
  domain's sessions (was the raw record ranges), always both: the
  `needs_column_lengths` skip came from a branch-local Go file
  (`storage/table_size_stats.go`, not in master, no caller) and is gone with
  the unused resource-group parameter. Test
  `the_table_size_cache_is_gos_process_wide_cache_over_both_reads`.
- [x] (2026-11-04) Sync load: Go `statsSyncLoad.handleOneItemTask` reads an
  item on the statistics handle's sessions (`readStatsForOneItem` over
  `read.rs`'s new `hist_meta_from_storage_with_high_priority`, histogram,
  CM sketch and TopN), not the raw `ClusterStatsLoader`; an index keeps its
  stored `tot_col_size`/`correlation` as Go's `histMetaFromStorage` does.
  Two `stats_watch` tests that read the LFU cache before its asynchronous
  admission settled now wait as Go's cache tests do. Test
  `a_sync_load_reads_an_evicted_column_on_the_domains_sessions_as_go`.
- [ ] S1, statistics left: `bootstrap.go`'s `initStats*` (the first pass,
  still `load_stats_snapshot_and_loader`), the async `LoadNeededHistograms`
  (`load_needed_histograms`), analyze's base-stats reads
  (`real_tikv_analyze.rs`), and the sync-load request API's unused
  `resource_group`; then `cluster_stats_load.rs` goes.
- [ ] Readers (below), in the order the remaining milestones set.

### Remaining milestones (Y01)

Every open item is downstream of one fact: parts of the node run without a
session where Go always has one. Done in this order, each removes a class
of raw paths instead of adding a second path next to them.

- S1, startup order (Go `bootstrapSessionImpl`): build the domain and its
  session pools over the empty live tables first; then the sysvar cache
  (`LoadSysVarCacheLoop` / `rebuildSysVarCache`), privileges
  (`LoadPrivilegeLoop`), bindings (`InitBindingHandle`, already attached
  after the domain) and statistics (`initStats`, `UpdateTableStatsLoop` over
  `storage/read.rs`) load through those sessions before the node serves; the
  system time zone and collation flag come from a session over a temporary
  system-DB-only domain (`initGlobalVarFromSystemDB`). Builders:
  `tidb-server/src/cluster_stack.rs`, `tidb-session/src/domain/unistore_node.rs`.
- S2, the bootstrap on a session (Go `runInBootstrapSession`, `doDDLWorks`,
  `doDMLWorks`): the bootstrap's objects are DDL jobs the owner runs with
  step sessions and its rows are SQL, one commit per job as Go; the store
  image the tests start from is taken after it, as Go's `tryMakeImage`.
  `seed_cluster_accounts` becomes the `--auth-file` identity's SQL.
- S3, with every DDL step on a session: delete the no-session fallbacks
  (`has_step_sessions`, `StagedDelRanges`, the raw job/history/MDL/notifier/
  submission rows), port `systable.Manager`, the scheduler's `getJob`, the
  history and MDL reads and `SwitchMDL`'s job check to SQL on
  `DdlSessionPool::sess_pool`, and move the `tidb-ddl` unit tests that drive
  an unbound pool onto a node.
- [ ] The bootstrap (item 11).

## Remaining inventory (2026-11-03)

Writes. Every statement-time writer now runs Go's SQL. What is left:

- The bootstrap (`mysql_bootstrap*.rs`) stages the system tables' rows and
  submits its DDL without a session, so the DDL owner keeps a raw path for
  a run without step sessions (`DdlSessionPool::has_step_sessions`): the
  job, history, MDL, notifier, delete-range (`StagedDelRanges`) and job
  submission rows (`tidb-ddl-jobsubmit`). Go's bootstrap runs `doDDLWorks`
  / `doDMLWorks` on a session, and those rows then take the session path.
  Fixing the bootstrap removes every one of these fallbacks.
- `seed_cluster_accounts` (above).
- `tidb-executor/src/cluster_analyze.rs` `analyze_table` /
  `analyze_physical_table`: a second, raw-scan sampler that only tests call
  (production samples through `cluster_analyze::sampling`, Go's
  coprocessor analyze requests). It should go with its four tests.
- Seeding fixtures used only by tests: `plan_stats_write`,
  `plan_partition_stats_write`, `plan_column_stats_usage_write`, the
  insert-table-stats, delta and history-meta planners,
  `plan_historical_stats_data_block`, `plan_sysvar_write`, the analyze-job
  planners.

The startup readers (sysvars, privileges, bindings, initial statistics)
run before the node has a session: Go builds the domain and its
`sysSessionPool` first (`BootstrapSession`) and loads each cache through a
session. Porting the startup order removes those scans with the bootstrap's
raw rows. Go's order (`bootstrapSessionImpl`):

1. `initGlobalVarFromSystemDB`: system time zone and new-collation flag from
   `mysql.tidb`, through a session over a temporary system-DB-only domain
   (Rust: `load_startup_variables_from_cluster`, raw).
2. The domain, its session pools, `dom.Start`.
3. `LoadPrivilegeLoop(ses[3])`, `LoadSysVarCacheLoop(ses[4])`,
   `InitBindingHandle` (after the sysvar cache), then the statistics loops
   (`UpdateTableStatsLoop`, `initStats` on `SPool`).

The Rust builders (`tidb-server/src/cluster_stack.rs`,
`tidb-session/src/domain/unistore_node.rs`) build the privilege, sysvar and
statistics loaders before `Domain::new`. The binding cache already follows
Go (`attach_binding_sessions` after the domain); the other loaders take the
same shape: the domain over the empty live tables, then each loop attached
to its session before the node serves.

Reads. Go reads every one of these tables with a statement on a session
(`ExecRestrictedSQL` / `util.Exec`); these scan the table's records
directly (`mysql_system_tables::scan_system_table*`):

| Rust reader | Go owner |
| --- | --- |
| `tidb-syssession/src/cluster_sysvar_load.rs` (the first load only, before the domain) | `Domain.fetchTableValues` (`rebuildSysVarCache`) |
| `tidb-privilege/src/cluster_privilege_load.rs`, `tidb-session/src/privilege_tables.rs` | `privileges.MySQLPrivilege.LoadAll` (`loadTable`) |
| `tidb-bindinfo/src/cluster_binding_seam.rs` (only before its session pool is attached; afterwards Go's SELECT) | `bindinfo` `readBindingsFromStorage` |
| `tidb-stats-handle-storage/src/cluster_stats_load.rs` (cache refresh, sync/async load, init stats, ANALYZE's base counts; `read.rs` has Go's statements for them) | `storage/read.go` and `initstats` statements |
| `tidb-infoschema-issyncer/src/mdl_info_load.rs`, `tidb-ddl-systable/src/lib.rs` | MDL `select ... from mysql.tidb_mdl_info` |
| `tidb-ddl-systable/src/ddl_job_table.rs` | job scheduler `getJob` / `GetAllDDLJobsInQueue` |
| `tidb-ddl/src/ddl_history_table.rs` | `GetHistoryJobByID` / `GetLastNHistoryDDLJobs` |
| `tidb-ddl/src/ddl_reorg_table.rs`, `ddl_delete_range.rs`, `cluster_ddl.rs` (MDL clean fallback) | reorg / delete-range / `cleanMDLInfo` statements |
| `tidb-session/src/cluster_sysvar_writer.rs` `switch_mdl`'s job check | `ddl.SwitchMDL`'s `select 1 from mysql.tidb_ddl_job` on `d.sessPool` |

The DDL readers share one layering mismatch. Go `systable.Manager` (and
the job scheduler, `SwitchMDL`) run their SELECTs on `pkg/ddl/session.Pool`,
the lowest DDL package. In Rust that pool is the `DdlSessionPool` trait in
`tidb-ddl-build`, above `tidb-ddl-systable` and `tidb-ddl-jobsubmit`, so the
manager cannot reach it and reads a `MetaSnapshot` instead; the port of Go's
`pkg/ddl/session` (`tidb-ddl-session`) has no dependents. The DDL readers'
unit starts by making `tidb-ddl-session` the pool those crates hold.

## Surprises & Discoveries

(None yet.)

## Decision Log

- 2026-11-03: the bootstrap's raw rows stay until the bootstrap runs on a
  session; every other site follows its Go owner.

## Outcomes & Retrospective

(Pending.)
