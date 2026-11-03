# DDL steps inside the worker session's transaction, as Go's

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

Go's DDL job worker runs each step inside one SQL session (`w.sess`, a
`sess.Session` from the worker's pool): `w.sess.Begin`, a meta mutator over
that session's transaction (`jobCtx.metaMut = meta.NewMutator(txn)`), the
step, the job-table update, then `w.sess.Commit`. Every system-table row the
step writes is an internal SQL statement executed in that same transaction:
the DDL notifier's event, the job row, the history row, the MDL info, the
reorg handle, `mysql.table_cache_meta`, `mysql.gc_delete_range`.

The Rust worker instead stages each step's writes in `JobTxn` (a write buffer
over a snapshot) and commits them through a store-level `SessionTransaction`.
The system-table rows are encoded by hand (`tidb_syssession::system_row_write`)
in five modules:

- `tidb-ddl-systable/src/ddl_job_table.rs` (Go `job_scheduler.go`
  `updateDDLJob` / `deleteDDLJob` / `getJobsBySQL`);
- `tidb-ddl/src/ddl_history_table.rs` (Go `ddl_history.go`
  `addHistoryDDLJob`);
- `tidb-ddl/src/ddl_reorg_table.rs` (Go `job_scheduler.go`
  `getDDLReorgHandle` / `updateDDLReorgHandle` / `initDDLReorgHandle` /
  `removeDDLReorgHandle` / `cleanDDLReorgHandles`);
- `tidb-ddl/src/ddl_delete_range.rs` (Go `delete_range.go`
  `insertJobIntoDeleteRangeTable`);
- `tidb-ddl/src/cluster_ddl.rs` `append_schema_change_mutations` (Go
  `notifier/store.go` `tableStore.Insert`) and `write_table_cache_meta_row`
  (Go `AlterTableCache`'s `replace into mysql.table_cache_meta`).

The rows are the same, so ordinary results agree. The structure does not,
and its consequences are real where the SQL layer does work the raw writer
skips: per-table statistics deltas for these system tables
(`TxnCtx.TableDeltaMap`, dumped to `mysql.stats_meta`), the row format and
checksum settings of the writing session, unique and index checks, and the
schema of the system table itself (a raw writer is bound to one column
layout; Go's statement follows whatever the bootstrap or upgrade created).

After this plan, a persisted DDL step runs inside a pooled internal session's
transaction: its meta writes go through that transaction, and its
system-table rows are the SQL statements Go executes, in the same commit.

## The Go code to port (the source of truth)

- `pkg/ddl/job_worker.go`: `runOneJobStep`, `HandleDDLJobTable` /
  `transitOneJobStep` (`w.sess.Begin`, `jobCtx.metaMut`,
  `updateDDLJob(w.sess, ...)`, `w.sess.Commit`), `finishDDLJob`
  (`deleteDDLJob`, `addHistoryDDLJob`), `registerMDLInfo`.
- `pkg/ddl/job_scheduler.go`: the job-table SQL listed above.
- `pkg/ddl/ddl_history.go`: `addHistoryDDLJob`.
- `pkg/ddl/delete_range.go`: `insertJobIntoDeleteRangeTable`.
- `pkg/ddl/notifier/store.go`, `publish.go`: `PubSchemeChangeToStore`.
- `pkg/ddl/session/session.go`: `sess.Session` (`Begin`, `Txn`, `Execute`,
  `Commit`, `Rollback`).

## Rust pieces that exist

- `tidb_ddl_build::session_pool::DdlSessionPool`: pooled internal sessions
  with `exec_restricted_sql_with` (each in its own transaction today).
- `crate::job_txn::JobTxn`: the step's meta reads and writes
  (`MetaTransaction`), which the meta `Mutator` already runs over.
- `real_tikv_ddl::run_persisted_ddl_job`: the owner loop and its commit
  (`commit_cluster_ddl_with_backfill_once`, `SessionTransaction::commit_with`).

## Plan of work

- M1: a session-transaction seam. A pooled internal session can begin a
  transaction, expose it as the `MetaTransaction` a meta `Mutator` writes
  through (Go `sess.Txn()`), run internal SQL in it, and commit or roll
  back. Prove it with one test: a meta write and an `INSERT` in one session
  transaction commit together and roll back together.
- M2: the notifier event through the seam (Go `PubSchemeChangeToStore`):
  `append_schema_change_mutations` becomes the INSERT. Test: the event row
  and the step's meta write share one commit; `mysql.stats_meta` counts the
  notifier table's row as Go's does.
- M3: the job row, history row and MDL info (Go `updateDDLJob`,
  `addHistoryDDLJob`, `registerMDLInfo`) through the seam; the step commit
  becomes the session's commit.
- M4: reorg handles, delete ranges and `table_cache_meta` through the seam;
  delete the raw writers (`system_row_write` callers in the DDL crates).

## Progress

- [ ] M1: session-transaction seam.
- [ ] M2: notifier event.
- [ ] M3: job, history and MDL rows; the step commit.
- [ ] M4: reorg handles, delete ranges, table cache meta; raw writers gone.

## Surprises & Discoveries

(None yet.)

## Decision Log

- Port by milestone over the existing commit path rather than rewriting
  the owner loop at once: each system table moves when the seam can carry
  it, and the raw writer for that table is deleted in the same change.

## Outcomes & Retrospective

(Pending.)
