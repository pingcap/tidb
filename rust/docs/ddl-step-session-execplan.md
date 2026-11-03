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

- M1: a session-transaction seam (see the Decision Log for its shape):
  `DdlSessionPool::exec_in_step_txn(start_ts, buffer, sql, args)` runs one
  statement on a system session over the step's buffer and a step
  `ClusterTransactions`. Test: the row stages without publishing, commits
  with the step's buffer, and a duplicate is refused at the step's commit.
- M2: the notifier event through the seam (Go `PubSchemeChangeToStore`):
  `append_schema_change_mutations` runs Go's `tableStore.Insert` statement
  through the step's session (`JobTxn::execute`).
- M3: the job row, history row and MDL info (Go `updateDDLJob`,
  `addHistoryDDLJob`, `registerMDLInfo`) through the seam; the step commit
  becomes the session's commit.
- M4: reorg handles, delete ranges and `table_cache_meta` through the seam;
  delete the raw writers (`system_row_write` callers in the DDL crates).

## Progress

- [x] M1 (2026-11-03): session-transaction seam.
  `DomainSessions::open_system_session_over(transactions, buffer)`;
  `ddl_session_pool::StepTransactions` / `StepTransaction` (reads at the
  step's start timestamp; autocommit and direct commits refused; the
  transaction's commit publishes nothing). Test
  `a_step_session_statement_commits_with_its_step_as_go`.
- [x] M2 (2026-11-03): notifier event. The step's buffer is created before
  planning (`commit_cluster_ddl_with_backfill_once`) and reaches the
  handlers as `DdlStepWorker.step_buffer` -> `JobTxn`'s `StepSession`
  (Go `jobCtx` with `w.sess`); a sub-job's nested `JobTxn` inherits it.
  The 17 tests that consume DDL events (stats handler, notifier store)
  pass over it and fail when the statement is skipped. A run without a
  session pool still stages the row itself until M4.
- [x] M3: job, history and MDL rows; the step commit (the step's statements
  run on Go's `w.sess`; the commit itself stays the worker's `commit_with`).
  - [x] (2026-11-03) The job row (Go `updateDDLJob2Table`: the pause, step
    and failure-checkpoint updates), `deleteDDLJob` and
    `addHistoryDDLJob2Table` run on the step's session
    (`update_ddl_job_2_table`, `finish_persisted_ddl_job`,
    `add_history_ddl_job`); test
    `ddl_history_row_is_gos_insert_on_the_step_session` (create_time is
    the start rounded to the second, as Go). A step's statements share ONE
    session (`StepSession::sql`, Go `w.sess`): a second session's `BEGIN`
    first commits the non-empty buffer as leftover autocommit work, which
    the step transactions refuse, and the refusal resets the buffer --
    every statement before it was lost (M2 shipped with that: a step
    publishing several events kept only the last). A failed step's
    discard (`run_error`) now resets the step buffer too, as Go rolls
    `w.sess` back. Test `a_step_sessions_statements_all_commit_with_the_step_as_go`.
    A pool that cannot open step sessions yet (a node's pool before its
    domain is bound, as the `tidb-ddl` worker tests run it) leaves the step
    without a session (`DdlSessionPool::has_step_sessions`).
  - [x] (2026-11-03) MDL info: `registerMDLInfo` (`MdlInfoUpdate::register`)
    runs on the step's session, which the commit function keeps until the
    commit; `cleanMDLInfo` (`MdlInfoUpdate::clean_sql`) runs on a pooled
    session in its own transaction. Test
    `ddl_mdl_info_row_is_gos_replace_on_the_step_session` (an ALTER blocked
    by a reader's MDL publishes the row; it is gone once the DDL ends).
  Found 2026-11-03:
  `tidb-ddl-session` is a complete port of Go `pkg/ddl/session`
  (`Session::{begin, execute, commit, txn, run_in_txn}`, `Pool`) that NO
  crate depends on. M3 should make the step run in that `Session` (a
  `SessionContext` over a node system session whose transaction is the
  step's, with `txn()` the meta mutator's transaction), so the step commit
  is the session's commit as Go's is, instead of widening the M1 seam.
  Constraint for any partial step: the session reads the step's buffer, but
  raw rows in `write.mutations` are invisible to it, so every write to one
  system table within a step must take the same path. The job-row sites in
  steps: the pause update, the ordinary step update, the failure checkpoint
  update (`plan_persisted_ddl_job_failure`), and `finish_persisted_ddl_job`
  (delete-range rows, then Go's `deleteDDLJob`, then `AddHistoryDDLJob`).
- [ ] M4: reorg handles, delete ranges, table cache meta; raw writers gone.
  - [x] (2026-11-03) Delete ranges. Go's `addDelRangeJob` does NOT run on
    `w.sess`: it takes a pooled session (`dr.sessPool.Get()`) and runs one
    `INSERT IGNORE INTO mysql.gc_delete_range` per batch in that session's
    own transaction, stamped with a fresh TSO (`getNowTSO`). Ported as Go's
    `DelRangeExecWrapper` / `sessionDelRangeExecWrapper`
    (`add_del_range_job`, `DdlSessionPool::current_version`); a pool
    without sessions stages the rows through `StagedDelRanges`. Test
    `ddl_delete_range_row_is_gos_insert_on_a_pooled_session` (the row's
    `ts` follows the history job's `FinishedTS`; gorun answers
    `FinishedTS + 1`).
  - [x] (2026-11-03) `table_cache_meta`: CACHE's `replace into` runs on a
    pooled session before the job (Go `AlterTableCache` uses the
    statement's restricted executor); NOCACHE's `finishDDLJob` delete runs
    on the step's session. Rust never deleted the row before; the CACHE /
    NOCACHE test now checks it is gone.
  - [ ] Reorg handles: Rust's add-index reorg advances the handle inside
    each batch transaction; Go's `UpdateReorgMeta` is a separate
    transaction on a pooled session. That is part of the backfill
    structure, not a statement swap.
  - [ ] Job submission insert (`insertDDLJobs2Table`).
  - The raw job-row writes in `worker.rs` are test fixtures
    (`#[cfg(test)]`), not admin paths. Production ADMIN CANCEL / PAUSE /
    RESUME DDL JOBS is a stub answering not-found; Go's `processJobs` is
    the next port.

## Surprises & Discoveries

- A session resets its staged buffer when it closes (`TxnManager::rollback`
  resets unconditionally), and a step session's buffer is the step's: the
  session hands it back before closing (`release_step_buffer`). Go's
  `w.sess` is never closed between steps, so the question does not arise
  there.

## Decision Log

- 2026-11-03, M1's seam: Go has ONE membuffer for the step (`w.sess`'s
  transaction). The Rust worker already commits a `MutationBuffer` plus the
  step's meta mutations in one `SessionTransaction::commit_with`. So a
  system session is opened over the step's buffer (sessions take their
  buffer at construction, `open_storage_session`) and over a
  `ClusterTransactions` whose `begin` returns a transaction that reads at the
  step's start timestamp (`open_snapshot_at`) and whose commit publishes
  nothing: the statement's rows, with their key flags (presume-not-exist,
  assertions), land in the step's buffer and commit with it. Extracting
  rows from a session buffer instead would drop those flags.
- The session publishes its table delta when its (no-op) commit returns,
  i.e. before the worker's commit; Go dumps `w.sess`'s delta only after the
  step committed. A failed step therefore still counts the system table's
  delta here. Recorded rather than plumbed: the delta is advisory
  (`stats_meta.modify_count`) and the step retries.

- Port by milestone over the existing commit path rather than rewriting
  the owner loop at once: each system table moves when the seam can carry
  it, and the raw writer for that table is deleted in the same change.

## Outcomes & Retrospective

(Pending.)
