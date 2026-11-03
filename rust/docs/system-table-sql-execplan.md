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
- [ ] Readers (below).
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
| `tidb-syssession/src/cluster_sysvar_load.rs` | `Domain.fetchTableValues` (`rebuildSysVarCache`) |
| `tidb-privilege/src/cluster_privilege_load.rs`, `tidb-session/src/privilege_tables.rs` | `privileges.MySQLPrivilege.LoadAll` (`loadTable`) |
| `tidb-bindinfo/src/cluster_binding_seam.rs` (only before its session pool is attached; afterwards Go's SELECT) | `bindinfo` `readBindingsFromStorage` |
| `tidb-stats-handle-storage/src/cluster_stats_load.rs` | `storage/read.go` and `initstats` statements |
| `tidb-stats-handle-storage/src/cluster_stats_dump.rs` | `storage` `TableHistoricalStatsToJSON` / `getStatsVersion` |
| `tidb-stats-handle-usage-predicatecolumn/src/cluster_predicate_column.rs` `load_column_stats_usage_for_table` (the stats dump) | `predicatecolumn` `LoadColumnStatsUsageForTable` |
| `tidb-infoschema-issyncer/src/mdl_info_load.rs`, `tidb-ddl-systable/src/lib.rs` | MDL `select ... from mysql.tidb_mdl_info` |
| `tidb-ddl-systable/src/ddl_job_table.rs` | job scheduler `getJob` / `GetAllDDLJobsInQueue` |
| `tidb-ddl/src/ddl_history_table.rs` | `GetHistoryJobByID` / `GetLastNHistoryDDLJobs` |
| `tidb-ddl/src/ddl_reorg_table.rs`, `ddl_delete_range.rs`, `cluster_ddl.rs` (MDL clean fallback) | reorg / delete-range / `cleanMDLInfo` statements |

## Surprises & Discoveries

(None yet.)

## Decision Log

- 2026-11-03: the bootstrap's raw rows stay until the bootstrap runs on a
  session; every other site follows its Go owner.

## Outcomes & Retrospective

(Pending.)
