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
statements on step sessions; this plan covers the rest. After it, each site below runs the statements Go runs, on a session
as Go's does, and its raw writer is gone.

## Remaining work

Writes. Every statement-time writer now runs Go's SQL. What is left:

- The bootstrap (`mysql_bootstrap*.rs`) stages the system tables' rows and
  submits its DDL without a session, so the DDL owner keeps a raw path for
  a run without step sessions (`DdlSessionPool::has_step_sessions`): the
  job, history, MDL, notifier, delete-range (`StagedDelRanges`) and job
  submission rows (`tidb-ddl-jobsubmit`). Go's bootstrap runs `doDDLWorks`
  / `doDMLWorks` on a session, and those rows then take the session path.
  Fixing the bootstrap removes every one of these fallbacks.
- `seed_cluster_accounts` writes a unistore node's `--auth-file` identity
  with `plan_account_write` at boot (Go has no such flag); it becomes that
  identity's SQL once the bootstrap runs on a session.
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
| `tidb-executor/src/real_tikv_analyze.rs` global-stats merge (`ClusterStatsLoader`); the unused `read_column_distribution_stats` | `globalstats` (`statsReadWriter.LoadTablePartitionStats`, the async merge's `storage/read.go` statements) |
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

## Progress

- [ ] Global-stats merge on a statistics session; delete `cluster_stats_load.rs`.
- [ ] DDL readers on `tidb-ddl-session`'s pool.
- [ ] Startup loaders after the domain, each on its session.
- [ ] The bootstrap on a session (removes the DDL owner's no-session fallbacks).

## Surprises & Discoveries

(None.)

## Decision Log

- 2026-11-03: the bootstrap's raw rows stay until the bootstrap runs on a
  session; every other site follows its Go owner.

## Outcomes & Retrospective

(Pending.)
