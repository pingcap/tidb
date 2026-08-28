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

//! Ports of the `pkg/ddl/tests/adminpause` family (part12 items 678-692 of
//! `pkg/ddl`'s `func Test*`/`func Benchmark*` declarations sorted by file
//! and line; item 677, `main_test.go:27::TestMain`, is the package's
//! failpoint/lease bootstrap harness with no assertions and is recorded as
//! skipped in the batch receipt), read from `origin/master`.
//!
//! Every Go test here drives the ONLINE DDL job queue: a failpoint
//! (`beforeRunOneJobStep` for pause, `afterWaitSchemaSynced`/
//! `OnJobUpdatedExported` for resume, `beforeRefreshJob` for cancel) parks a
//! running job at a chosen schema state, an `admin pause/resume/cancel ddl
//! jobs <id>` statement lands from a second session, and the test requires
//! the exact `<id> successful` result row plus the paused statement's
//! `[ddl:8212]ErrCancelledDDLJob` (or clean completion for non-pausable
//! jobs). This tier has no job queue, no job ids, no schema states and no
//! `admin pause/resume/cancel` commands (`grep` over
//! `rust/crates/tidb-executor/src` finds none of them), so each test is
//! recorded as an explicit gap with the contract re-derived from the Go
//! source. Nothing is approximated.

/// Go `TestPauseCancelAndRerunSchemaStmt`
/// (`pkg/ddl/tests/adminpause/pause_cancel_test.go:161`): for every case in
/// `schemaDDLStmtCase`, `tableDDLStmt` and `placeRulDDLStmtCase`
/// (`pause_and_resume.go`, the package's shared matrices), a pausable DDL
/// is paused mid-run via `admin pause ddl jobs <id>` (result
/// `<id> successful`), cancelled via `admin cancel ddl jobs <id>` from the
/// `beforeRefreshJob` hook (result `<id> successful`), the statement
/// surfaces `[ddl:8212]ErrCancelledDDLJob`, and re-running the statement
/// afterwards succeeds (rollback statements cleaned the preconditions).
// go-parity-gap: no job queue, no schema-state walk, and no admin
// pause/cancel command in this tier.
#[test]
#[ignore = "go-parity-gap: admin pause/cancel of in-flight DDL jobs needs the job queue"]
fn pause_cancel_and_rerun_schema_statements() {
    // Contract (pause_cancel_test.go:161-186): each pausable case ends
    // ErrCancelledDDLJob with `<jobID> successful` on both admin commands;
    // non-pausable cases complete; every case re-runs cleanly after.
}

/// Go `TestPauseCancelAndRerunIndexStmt`
/// (`pkg/ddl/tests/adminpause/pause_cancel_test.go:190`): with the
/// `mockTiFlashStoreCount` and `MockCheckColumnarIndexProcess` failpoints
/// armed and `tbl_user_10`/`tbl_user_with_vec` built, every
/// `indexDDLStmtCase` is pause-cancelled and re-run exactly like the schema
/// family.
// go-parity-gap: no job queue, no admin commands, and the TiFlash-mock
// failpoints have no carrier here.
#[test]
#[ignore = "go-parity-gap: index DDL jobs, admin commands and TiFlash-mock failpoints are not transcreated"]
fn pause_cancel_and_rerun_index_statements() {
    // Contract (pause_cancel_test.go:190-206): every indexDDLStmtCase ends
    // paused+cancelled with 8212 and re-runs cleanly.
}

/// Go `TestPauseCancelAndRerunColumnStmt`
/// (`pkg/ddl/tests/adminpause/pause_cancel_test.go:208`): every
/// `columnDDLStmtCase` over `tbl_user_10` is pause-cancelled mid-job and
/// re-runs cleanly afterwards.
// go-parity-gap: no job queue and no admin pause/cancel commands.
#[test]
#[ignore = "go-parity-gap: column DDL jobs cannot be paused or cancelled in this tier"]
fn pause_cancel_and_rerun_column_statements() {
    // Contract (pause_cancel_test.go:208-220): every columnDDLStmtCase ends
    // ErrCancelledDDLJob with both admin commands reporting
    // `<jobID> successful`, then re-runs cleanly.
}

/// Go `TestPauseCancelAndRerunPartitionTableStmt`
/// (`pkg/ddl/tests/adminpause/pause_cancel_test.go:222`): over the
/// partitioned `tbl_user_partition`, every `tablePartitionDDLStmtCase` is
/// pause-cancelled mid-job and re-runs cleanly.
// go-parity-gap: no job queue and no admin pause/cancel commands.
#[test]
#[ignore = "go-parity-gap: partition DDL jobs cannot be paused or cancelled in this tier"]
fn pause_cancel_and_rerun_partition_table_statements() {
    // Contract (pause_cancel_test.go:222-239): every tablePartitionDDLStmtCase
    // ends ErrCancelledDDLJob with `<jobID> successful` results, then
    // re-runs cleanly.
}

/// Go `TestPauseOnWriteConflict`
/// (`pkg/ddl/tests/adminpause/pause_negative_test.go:35`): with
/// `mockFailedCommandOnConcurencyDDL` armed inside the
/// `beforeRunOneJobStep` hook at StateWriteReorganization, `admin pause ddl
/// jobs <id>` fails with the literal "mock failed admin command on ddl
/// jobs" and the ADD INDEX STILL SUCCEEDS (a failed pause command must not
/// disturb the job); in the second round the pause succeeds, `admin cancel`
/// after a 5s sleep also succeeds (`<id> successful` on both), and the
/// ALTER reports `[ddl:8212]ErrCancelledDDLJob`.
// go-parity-gap: no job queue, no admin commands and no failpoint seams in
// this tier.
#[test]
#[ignore = "go-parity-gap: pause-on-write-conflict retry semantics need the job queue"]
fn pause_on_write_conflict_never_disturbs_the_running_job() {
    // Contract (pause_negative_test.go:35-95): a failed pause leaves the ADD
    // INDEX green; a good pause + delayed cancel ends the ALTER with 8212
    // and `<id> successful` on both commands.
}

/// Go `TestPauseFailedOnCommit`
/// (`pkg/ddl/tests/adminpause/pause_negative_test.go:97`): with
/// `mockCommitFailedOnDDLCommand` armed at StateWriteReorganization,
/// `ddl.PauseJobs` (the in-process API, not the SQL command) fails with the
/// literal "mock commit failed on admin command on ddl jobs" and returns
/// exactly one per-job error, while the ADD INDEX itself still completes.
// go-parity-gap: `ddl.PauseJobs` and the commit failpoint seam are not
// transcreated.
#[test]
#[ignore = "go-parity-gap: ddl.PauseJobs and its commit failpoint are not transcreated"]
fn pause_jobs_reports_a_commit_failure_as_one_job_error() {
    // Contract (pause_negative_test.go:97-132): pauseErr ==
    // "mock commit failed on admin command on ddl jobs", len(jobErrs) == 1,
    // and the ADD INDEX finishes.
}

/// Go `TestPauseAndResumeSchemaStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:212`): every
/// `schemaDDLStmtCase`, `tableDDLStmt` and `placeRulDDLStmtCase` is paused
/// (`beforeRunOneJobStep`), resumed (`afterWaitSchemaSynced`/
/// OnJobUpdatedExported — result `<id> successful`), completes cleanly, and
/// the rollback statements reset the schema for the next case.
// go-parity-gap: no job queue, schema states, or admin pause/resume.
#[test]
#[ignore = "go-parity-gap: admin pause/resume of in-flight DDL jobs needs the job queue"]
fn pause_and_resume_schema_statements() {
    // Contract (pause_resume_test.go:212-236): pausable cases see
    // `<id> successful` on pause AND resume and finish cleanly; non-pausable
    // cases never trip either hook.
}

/// Go `TestPauseAndResumeIndexStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:231`): every
/// `indexDDLStmtCase` with the TiFlash mocks armed is paused and resumed
/// with `<id> successful` on both commands.
// go-parity-gap: no job queue, admin commands, or TiFlash-mock failpoints.
#[test]
#[ignore = "go-parity-gap: index DDL jobs cannot be paused/resumed in this tier"]
fn pause_and_resume_index_statements() {
    // Contract (pause_resume_test.go:231-245): every indexDDLStmtCase
    // pauses, resumes, and completes.
}

/// Go `TestPauseAndResumeColumnStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:247`): every
/// `columnDDLStmtCase` pauses and resumes with clean completion.
// go-parity-gap: no job queue and no admin pause/resume.
#[test]
#[ignore = "go-parity-gap: column DDL jobs cannot be paused/resumed in this tier"]
fn pause_and_resume_column_statements() {
    // Contract (pause_resume_test.go:247-257): every columnDDLStmtCase
    // pauses, resumes, and completes.
}

/// Go `TestPauseAndResumePartitionTableStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:259`): every
/// `tablePartitionDDLStmtCase` over the partitioned table pauses and
/// resumes with clean completion.
// go-parity-gap: no job queue and no admin pause/resume.
#[test]
#[ignore = "go-parity-gap: partition DDL jobs cannot be paused/resumed in this tier"]
fn pause_and_resume_partition_table_statements() {
    // Contract (pause_resume_test.go:259-270): every tablePartitionDDLStmtCase
    // pauses, resumes, and completes.
}

/// Go `TestPauseResumeCancelAndRerunSchemaStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:272`): the full
/// pause→resume→cancel→rerun ladder for `schemaDDLStmtCase`, `tableDDLStmt`
/// and `placeRulDDLStmtCase` — pause and resume both report
/// `<id> successful`, the cancel (from `beforeRefreshJob`) reports
/// `<id> successful`, the statement ends ErrCancelledDDLJob, and the re-run
/// after cancel succeeds.
// go-parity-gap: no job queue and no admin pause/resume/cancel.
#[test]
#[ignore = "go-parity-gap: the pause-resume-cancel-rerun ladder needs the job queue"]
fn pause_resume_cancel_and_rerun_schema_statements() {
    // Contract (pause_resume_test.go:272-300): every pausable case walks
    // pause → resume → cancel with `<id> successful` results each time, ends
    // 8212, and re-runs cleanly.
}

/// Go `TestPauseResumeCancelAndRerunIndexStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:302`): the full ladder
/// for `indexDDLStmtCase` under the TiFlash mocks.
// go-parity-gap: no job queue, admin commands, or TiFlash-mock failpoints.
#[test]
#[ignore = "go-parity-gap: index jobs cannot walk the pause-resume-cancel ladder in this tier"]
fn pause_resume_cancel_and_rerun_index_statements() {
    // Contract (pause_resume_test.go:302-319): every indexDDLStmtCase walks
    // pause → resume → cancel → rerun.
}

/// Go `TestPauseResumeCancelAndRerunColumnStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:321`): the full ladder
/// for `columnDDLStmtCase`, then (per the Go comment's partition-tuple
/// caveat) `truncate tbl_user` plus a rebuilt `tbl_user_partition` and the
/// full ladder for `tablePartitionDDLStmtCase`.
// go-parity-gap: no job queue and no admin pause/resume/cancel.
#[test]
#[ignore = "go-parity-gap: column/partition jobs cannot walk the ladder in this tier"]
fn pause_resume_cancel_and_rerun_column_statements() {
    // Contract (pause_resume_test.go:321-348): every columnDDLStmtCase and
    // then every tablePartitionDDLStmtCase walks pause → resume → cancel →
    // rerun; the partition tuples are truncated first.
}

/// Go `TestPauseResumeCancelAndRerunPartitionTableStmt`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:350`): the full ladder
/// for `tablePartitionDDLStmtCase` over `tbl_user_partition`.
// go-parity-gap: no job queue and no admin pause/resume/cancel.
#[test]
#[ignore = "go-parity-gap: partition jobs cannot walk the ladder in this tier"]
fn pause_resume_cancel_and_rerun_partition_table_statements() {
    // Contract (pause_resume_test.go:350-365): every tablePartitionDDLStmtCase
    // walks pause → resume → cancel → rerun.
}

/// Go `TestPauseJobDependency`
/// (`pkg/ddl/tests/adminpause/pause_resume_test.go:367`): a paused
/// `modify column b varchar(16)` (parked by the
/// `afterModifyColumnStateDeleteOnly` hook) BLOCKS a following
/// `alter table t add column c int` on the same table — the add-column
/// session makes no progress for 3s — and `admin resume ddl jobs <id>`
/// unblocks both; both statements finish without error.
// go-parity-gap: no job queue (so no pause blocking), no
// afterModifyColumnStateDeleteOnly hook, and no admin resume command.
#[test]
#[ignore = "go-parity-gap: same-table job dependency behind a paused job needs the job queue"]
fn a_paused_job_blocks_same_table_ddl_until_resumed() {
    // Contract (pause_resume_test.go:367-421): add column waits while
    // modify column is paused; admin resume unblocks modify then add; both
    // report no error.
}
