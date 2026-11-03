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
- [ ] M1 (the statistics handle's pool for the storage writes), M2, the rest
  of M3, M4, M5.

## Surprises & Discoveries

(None yet.)

## Decision Log

- 2026-11-03: the bootstrap's raw rows stay until the bootstrap runs on a
  session; every other site follows its Go owner.

## Outcomes & Retrospective

(Pending.)
