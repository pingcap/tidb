// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Documented go-parity-gap ports of the `pkg/ddl/multi_schema_change_test.go`
//! halves that observe the online-DDL job queue itself: `newCancelJobHook`
//! cancellation at a sub-job schema state, `testfailpoint` hooks
//! (`beforeRunOneJobStep`, `afterWaitSchemaSynced`, `beforeWaitSchemaSynced`),
//! `admin show ddl jobs` visibility of running sub-jobs, and the global
//! variable gates. The success/SQL-observable halves of the same Go tests are
//! ported running in `tests_ddl_multi_schema_change_sql`. This tier applies
//! one ALTER TABLE's action list synchronously (`ddl/alter_table.rs`), so
//! there is no job to cancel and no state machine to interleave with.

/// Go `multi_schema_change_test.go:738-748::TestMultiSchemaChangeNoSubJobs`.
/// `add column if not exists a int, add column if not exists b int` over a
/// table that has both columns completes with two
/// `Note 1060 Duplicate column name 'a'/'b'` warnings and no error; the
/// resulting no-op job is not enqueued and the last history job stays
/// `create table`. This tier's `add_column_action` matches the parsed
/// `IF NOT EXISTS` flag away (`alter_table.rs:150`) and answers 1060, so
/// the notes-not-errors contract has no counterpart.
// go-parity-gap: ADD COLUMN IF NOT EXISTS demotes duplicates to notes only
// in the job-submission path this tier does not build.
#[test]
#[ignore]
fn multi_schema_change_no_sub_jobs_files_duplicate_notes() {
}

/// Go `multi_schema_change_test.go:125::TestMultiSchemaChangeDropIndexedColumnsCancelled`.
/// Cancelling the `drop column b, drop column a, drop column d` job when
/// sub-job[1] (`a`) is in `StateDeleteReorganization` FAILS
/// (`MustCancelFailed` -- past the last undo point); the statement succeeds
/// and only column `c`'s value `3` remains.
// go-parity-gap: state-dependent cancellation of drop-column sub-jobs needs
// the online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_drop_indexed_columns_cancel_is_past_undo_point() {
}

/// Go `multi_schema_change_test.go:148::TestMultiSchemaChangeDropColumnsParallel`.
/// Go submits `drop column if exists b, drop column if exists c` twice
/// (`putTheSameDDLJobTwice`); the serialized second run is ported running in
/// `tests_ddl_multi_schema_change_sql`. (TestMultiSchemaChangeAddColumnsParallel
/// at line 62 is item 479 -- batch b107's scope, gap-ported there.)

/// Go `multi_schema_change_test.go:200-215::TestMultiSchemaChangeRenameColumns`
/// cancel half: cancelling `add column c int default 3, rename column b to d`
/// when sub-job[0] is in `StateWriteReorganization` answers
/// `errno.ErrCancelledDDLJob`; the table keeps `b` (value `2`) and `select d`
/// answers `errno.ErrBadField` -- the cancelled job rolled back fully.
// go-parity-gap: cancellation via afterWaitSchemaSynced hook needs the
// online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_rename_columns_cancel_rolls_back_fully() {
}

/// Go `multi_schema_change_test.go:227-241::TestMultiSchemaChangeRenameColumns`
/// DML half: while sub-job[0] is in `StateWriteReorganization`, another
/// session still reads `b` as `2`; after the ALTER commits `select d` reads
/// `2` and `select b` answers `errno.ErrBadField`.
// go-parity-gap: the beforeRunOneJobStep interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_rename_columns_dml_reads_during_write_reorg() {
}

/// Go `multi_schema_change_test.go:281-293::TestMultiSchemaChangeAlterColumns`
/// cancel half: cancelling `add column c int default 3, alter column b set
/// default 3` in `StateWriteReorganization` answers
/// `errno.ErrCancelledDDLJob`; a row inserted afterwards still takes the OLD
/// default (`1 2`), proving the default change rolled back.
// go-parity-gap: cancellation via afterWaitSchemaSynced hook needs the
// online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_alter_columns_cancel_keeps_old_default() {
}

/// Go `multi_schema_change_test.go:318-330::TestMultiSchemaChangeAlterColumns`
/// DML half: while sub-job[0] is in `StateWriteOnly` another session inserts
/// a row; the statement succeeds and the table reads `1 2 3`.
// go-parity-gap: the beforeRunOneJobStep interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_alter_columns_dml_inserts_during_write_only() {
}

/// Go `multi_schema_change_test.go:339-362::TestMultiSchemaChangeChangeColumns`
/// cancel half: cancelling `add column c int default 3, change column b d
/// bigint default 4` in `StateWriteReorganization` answers
/// `errno.ErrCancelledDDLJob`; `select b` still reads `2` and `select d`
/// answers `errno.ErrBadField`.
// go-parity-gap: cancellation via afterWaitSchemaSynced hook needs the
// online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_change_columns_cancel_rolls_back_fully() {
}

/// Go `multi_schema_change_test.go:364-392::TestMultiSchemaChangeRenameTable`
/// racing half: with a `beforeRunOneJobStep` hook, a concurrent
/// `rename column b to c, change column a e bigint` issued while
/// `rename to t1` runs FAILS (the table moved under it); the renamed table
/// then accepts the same ALTER. The tail (rename-then-alter over the renamed
/// table) is ported running in `tests_ddl_multi_schema_change_sql`.
// go-parity-gap: racing a DDL against a rename needs failpoint interleaving
// of the job queue.
#[test]
#[ignore]
fn multi_schema_change_rename_table_race_fails_then_reapplies() {
}

/// Go `multi_schema_change_test.go:406-429::TestMultiSchemaChangeAddIndexesCancelled`
/// cancel-success half: cancelling the four-index ALTER when index `t2` is in
/// `StateWriteReorganization` answers `errno.ErrCancelledDDLJob`; `show
/// index from t` then lists NOTHING and the data reads `1 2 3` with
/// `admin check table` passing -- the whole job rolled back.
// go-parity-gap: sub-job reorganization on cancel needs the online-DDL job
// queue.
#[test]
#[ignore]
fn multi_schema_change_add_indexes_cancel_rolls_back_all_indexes() {
}

/// Go `multi_schema_change_test.go:431-440::TestMultiSchemaChangeAddIndexesCancelled`
/// cancel-failed half: the cancel fires when index `t1` reaches
/// `StatePublic` and FAILS (`MustCancelFailed`); all four indexes exist and
/// serve `select * from t use index(t, t1, t2, t3)`.
// go-parity-gap: non-revertible sub-job marking needs the online-DDL job
// queue.
#[test]
#[ignore]
fn multi_schema_change_add_indexes_cancel_fails_once_public() {
}

/// Go `multi_schema_change_test.go:442-464::TestMultiSchemaChangeDropIndexesCancelled`
/// middle-state half: the cancel fires when sub-job[1] is in
/// `StateDeleteOnly` and FAILS; the drop commits and `USE INDEX` on `a`,
/// `b` and `idx` each answers `errno.ErrKeyDoesNotExist`. The committed
/// outcome is ported running in `tests_ddl_multi_schema_change_sql`; the
/// state-timed cancellation is the gap.
// go-parity-gap: state-timed cancellation needs the online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_drop_indexes_cancel_fails_in_delete_only() {
}

/// Go `multi_schema_change_test.go:466-480::TestMultiSchemaChangeDropIndexesCancelled`
/// cancel-success half: the cancel fires while sub-job[1] is still public and
/// SUCCEEDS (`MustCancelDone`); the statement answers
/// `errno.ErrCancelledDDLJob` and `USE INDEX` on all three names still works
/// -- the drop rolled back entirely.
// go-parity-gap: reverting a partly-finished drop job needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_drop_indexes_cancel_in_public_rolls_back() {
}

/// Go `multi_schema_change_test.go:510-518::TestMultiSchemaChangeRenameIndexes`
/// cancel half: cancelling `add column c int default 3, rename index t to
/// t1` in `StateWriteReorganization` answers `errno.ErrCancelledDDLJob`;
/// `USE INDEX (t)` still reads `1 2` and `USE INDEX (t1)` answers
/// `errno.ErrKeyDoesNotExist`.
// go-parity-gap: cancellation via afterWaitSchemaSynced hook needs the
// online-DDL job queue.
#[test]
#[ignore]
fn multi_schema_change_rename_indexes_cancel_restores_names() {
}

/// Go `multi_schema_change_test.go:552-577::TestMultiSchemaChangeModifyColumnsCancelled`.
/// Cancelling `modify column a tinyint, modify column b bigint, modify
/// column c char(20)` when sub-job[2] is in `StateWriteReorganization`
/// answers `errno.ErrCancelledDDLJob`; rows read `1 2 3` through the table
/// AND through indexes i1/i2/i3, `admin check table` passes, and
/// `information_schema.columns` still reports `c` as `int` -- a fully rolled
/// back three-column modify.
// go-parity-gap: sub-job reorganization on cancel needs the online-DDL job
// queue.
#[test]
#[ignore]
fn multi_schema_change_modify_columns_cancel_rolls_back_types() {
}

/// Go `multi_schema_change_test.go:603-638::TestMultiSchemaChangeAlterIndex`
/// failpoint half: while sub-job[1] (`modify column a tinyint`) is in
/// `StateWriteReorganization`, another session still reads through index
/// `i1` (`select * from t use index(i1)` succeeds) -- the pre-existing index
/// keeps serving during the column backfill.
// go-parity-gap: the afterWaitSchemaSynced interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_alter_index_reads_during_backfill() {
}

/// Go `multi_schema_change_test.go:640-667::TestMultiSchemaChangeMixCancelled`.
/// A ten-action mix (add column, add indexes, drop columns with and without
/// IF EXISTS, drop indexes) is cancelled when sub-job[8] is in
/// `StateWriteReorganization`; the statement answers
/// `errno.ErrCancelledDDLJob` and the table reads `1 2 3` through both
/// surviving indexes i1/i2 with `admin check table` passing.
// go-parity-gap: mixed-job reorganization on cancel needs the online-DDL job
// queue.
#[test]
#[ignore]
fn multi_schema_change_mix_cancel_rolls_back_ten_actions() {
}

/// Go `multi_schema_change_test.go:669-697::TestMultiSchemaChangeAdminShowDDLJobs`.
/// While sub-job[0] is in `StateDeleteOnly`, `admin show ddl jobs 1` lists
/// three rows: the history `create table`, and the running multi-schema job
/// whose subjob line reads `add index /* subjob */` in state `delete only`
/// with non-empty start/finish times and query, plus the create-table
/// history row.
// go-parity-gap: `admin show ddl jobs` exposes only completed statements in
// this tier; there are no running sub-job rows to observe.
#[test]
#[ignore]
fn multi_schema_change_admin_show_ddl_jobs_lists_running_subjobs() {
}

/// Go `multi_schema_change_test.go:713-724::TestMultiSchemaChangeWithExpressionIndex`
/// failpoint half: while sub-job[1] (the expression index) is in
/// `StateWriteOnly`, another session's `update t set a = 3 where a = 1` and
/// `insert into t values (10, 10)` BOTH succeed -- write-only expression
/// indexes do not constrain writes yet.
// go-parity-gap: the beforeRunOneJobStep interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_expression_index_dml_during_write_only() {
}

/// Go `multi_schema_change_test.go:751-763::TestMultiSchemaChangeSchemaVersion`.
/// Across four multi-schema statements (drop/add columns, add index+column,
/// alter index+drop column) the `beforeWaitSchemaSynced` hook observes NO
/// repeated schema version: each sub-job advances the version exactly once.
// go-parity-gap: schema-version bumping per sub-job is job-queue machinery.
#[test]
#[ignore]
fn multi_schema_change_schema_versions_never_repeat() {
}

/// Go `multi_schema_change_test.go:774-819::TestMultiSchemaChangeMixedWithUpdate`.
/// While sub-job[8] (`drop column c_drop_2`) is in `StateDeleteOnly`, a
/// fourteen-column UPDATE touching added, dropped, modified and
/// index-relevant columns succeeds; the fourteen-action DDL then completes.
// go-parity-gap: the beforeRunOneJobStep interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_mixed_ddl_interleaves_fourteen_column_update() {
}

/// Go `multi_schema_change_test.go:849-867::TestMultiSchemaChangeDMLUpdate`:
/// with an `afterWaitSchemaSynced` hook running insert/update/delete against
/// the table, `change column b e int unsigned, change column d f int
/// unsigned` completes and the dropped table leaves no trace.
// go-parity-gap: the afterWaitSchemaSynced interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_change_dml_update_during_sub_jobs() {
}

/// Go `multi_schema_change_test.go:869-888::TestMultiSchemaChangeBlockedByRowLevelChecksum`.
/// Multi-schema ADD COLUMN is refused with `errno.ErrUnsupportedDDLOperation`
/// whenever row-level checksum is ENABLED globally (even with the session
/// flag off) or in the session (even with the global off); with both off the
/// ALTER passes.
// go-parity-gap: the EnableRowLevelChecksum multi-schema gate
// (`checkMultiSchemaInfo` callers) is not plumbed into this tier's ALTER
// path.
#[test]
#[ignore]
fn multi_schema_change_blocked_by_row_level_checksum_gate() {
}

/// Go `multi_schema_change_test.go:890-979::TestMultiSchemaChangePreservesCloudStorageMode`.
/// With cloud storage, dist task and fast reorg enabled, the add-index
/// sub-job's proxy job inherits `ReorgMeta.UseCloudStorage` from the parent,
/// the batched hook flips parent false/proxy true, and the NEXT proxy built
/// from the parent's sub-jobs keeps that mode.
// go-parity-gap: ReorgMeta cloud-storage propagation is online-DDL reorg
// machinery this tier does not build.
#[test]
#[ignore]
fn multi_schema_change_preserves_cloud_storage_mode_through_proxies() {
}

/// Go `multi_schema_change_test.go:981-1004::TestMultiSchemaChangePollJobCount`:
/// the 3-action multi-schema triggers exactly 30 `onRunOneJobStep` firings
/// (one per sub-job state transition) and 10 `beforePollDDLJob` firings.
// go-parity-gap: there is no job poller to count in this tier.
#[test]
#[ignore]
fn multi_schema_change_poll_job_count_matches_state_transitions() {
}

/// Go `multi_schema_change_test.go:1006-1026::TestMultiSchemaChangeMDLView`.
/// With the `tidb_mdl_view` created, after a multi-schema ADD COLUMN
/// finishes and another transaction is OPEN with an insert, `select
/// count(*) from mysql.tidb_mdl_view` reads `0` -- no metadata lock lingers
/// once the DDL is done.
// go-parity-gap: the mdl view over cluster processlists is server-session
// machinery this tier does not build.
#[test]
#[ignore]
fn multi_schema_change_mdl_view_empty_after_ddl_completes() {
}

/// Go `multi_schema_change_test.go:1028-1057::TestMultiSchemaChangeWithoutMDL`:
/// with `tidb_enable_metadata_lock` toggled off globally, four multi-schema
/// statements (drop columns, drop indexes, modify columns, modify with
/// reorg) all succeed.
// go-parity-gap: the global metadata-lock switch is server-session
// machinery; this tier has no lock to disable.
#[test]
#[ignore]
fn multi_schema_change_without_metadata_lock_all_variants_apply() {
}
