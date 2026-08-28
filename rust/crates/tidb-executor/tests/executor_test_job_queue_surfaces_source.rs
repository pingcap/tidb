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

//! Port ledger for the job-queue surfaces of `pkg/ddl/executor_test.go` and
//! the single-failpoint DDL path of `pkg/ddl/fail_test.go` and
//! `pkg/ddl/external_workload_ttl_test.go` (`pkg/ddl.part6` batch b105,
//! executor_test.go items 314-320 of the pkg/ddl enumeration, except item
//! 316 TestIsJobRollbackable which is functionally ported in
//! ddl_job_rollbackable_source.rs).
//!
//! The six `executor_test.go` tests below enqueue or observe DDL JOBS --
//! `addDDLJobs` over the `mysql.tidb_ddl_job` queue, `GetAllDDLJobs`
//! (pkg/ddl/ddl.go:1710), `IterAllDDLJobs`, failpoint-hooked job delivery,
//! and the session's table-lock bookkeeping on submit/finish. The job queue,
//! its delivery loop, and the failpoint seams they interleave with are not
//! transcreated in this tier (the driver applies metadata directly), so each
//! test has no Rust carrier. Item 320 (`fail_test.go`) and the
//! external-workload TTL integration run the same machinery through
//! `beforeRunOneJobStep` hooks.

/// GO PORT of `pkg/ddl/executor_test.go:43 TestGetDDLJobs`.
///
/// Re-derived contract: inside one open transaction, ten `ActionCreateTable`
/// jobs enqueued through `addDDLJobs` become visible incrementally --
/// `GetAllDDLJobs` (pkg/ddl/ddl.go:1710) reports `i+1` jobs after the i-th
/// enqueue -- and `IterAllDDLJobs` over the same txn yields the same set
/// filtered to not-yet-started jobs (`job.Started()` skips); after all ten,
/// both views agree job-for-job on ID, SchemaID=1 and Type, in enqueue
/// order, and the transaction rolls back cleanly.
#[test]
#[ignore = "go-parity-gap: addDDLJobs/GetAllDDLJobs/IterAllDDLJobs over the mysql.tidb_ddl_job queue (pkg/ddl/ddl.go:1710+) need the DDL job queue, which is not transcreated"]
fn get_ddl_jobs_lists_enqueued_jobs_incrementally_and_in_order() {}

/// GO PORT of `pkg/ddl/executor_test.go:98 TestGetDDLJobsIsSort`.
///
/// Re-derived contract: jobs enqueued OUT of id order across the THREE lists
/// -- 5 drop-table jobs (ids 10-14) on the default list, 5 create-table jobs
/// (ids 0-4) on the default list, 5 add-index jobs (ids 5-9) on the
/// AddIndexJobListKey -- come back from `GetAllDDLJobs` as one slice sorted
/// by job ID ascending (0..14), because `GetAllDDLJobs` merges the two lists
/// and sorts the union.
#[test]
#[ignore = "go-parity-gap: GetAllDDLJobs' list merge (default + AddIndexJobListKey) needs the DDL job queue, which is not transcreated"]
fn get_ddl_jobs_merges_both_lists_sorted_by_job_id() {}

/// GO PORT of `pkg/ddl/executor_test.go:148
/// TestWrappedQueryInterruptedRetriesDDLJobCancellation`.
///
/// Re-derived contract: with the job scheduler blocked (`beforeLoadAndDeliverJobs`)
/// and `mockFailedCommandOnConcurencyDDL` failing the first cancel attempt,
/// an `alter table t add index idx(a)` killed through `waitJobSubmitted`
/// ends with the job in `JobStateCancelling`, the cancel retried past the
/// mock failure (semantic equality, not sentinel identity -- the killer's
/// signal carries a stack), the session's error is
/// `dbterror.ErrCancelledDDLJob`, and the table ends with NO index
/// (`show index from t` is empty).
#[test]
#[ignore = "go-parity-gap: the cancel/retry delivery loop, its failpoint seams, and ErrCancelledDDLJob are not transcreated"]
fn wrapped_query_interrupted_retries_ddl_job_cancellation() {}

/// GO PORT of `pkg/ddl/executor_test.go:228 TestCreateViewConcurrently`.
///
/// Re-derived contract: five concurrent
/// `create or replace view v as select * from t` statements each enqueue at
/// most one `ActionCreateView` job at a time -- the `onDDLCreateView` hook
/// counts running create-view jobs and any second concurrent job would trip
/// `counterErr` -- and all five succeed with the counter back to zero, i.e.
/// create-view jobs serialize rather than run concurrently.
#[test]
#[ignore = "go-parity-gap: concurrent job delivery through afterDeliveryJob/onDDLCreateView hooks needs the DDL job queue, which is not transcreated"]
fn create_view_jobs_never_run_concurrently() {}

/// GO PORT of `pkg/ddl/executor_test.go:268 TestCreateDropCreateTable`.
///
/// Re-derived contract: a `create table t` that starts while `drop table t`
/// waits for schema sync (`afterWaitSchemaSynced` arm +
/// `mockOwnerCheckAllVersionSlow` on the drop) must NOT be visible to the
/// drop, and the finished jobs' `BinlogInfo.FinishedTS` order --
/// read back from `mysql.tidb_ddl_history` -- proves it: the FIRST create's
/// finishTS < the drop's finishTS < the SECOND create's finishTS.
#[test]
#[ignore = "go-parity-gap: needs the job queue's schema-sync wait, the owner-check failpoint and mysql.tidb_ddl_history -- none transcreated"]
fn create_drop_create_orders_finish_timestamps() {}

/// GO PORT of `pkg/ddl/executor_test.go:328 TestHandleLockTable`.
///
/// Re-derived contract (pkg/ddl/executor.go:7484-7513): submitting a
/// `TRUNCATE TABLE` job (`TruncateTableArgs{NewTableID: 2}`) through
/// `HandleLockTablesOnSuccessSubmit` propagates every session table lock
/// onto the NEW table id -- a session holding `TableLockRead` on table 1
/// ends with TWO locks, both resolvable through `CheckTableLocked` as read
/// -- and `HandleLockTablesOnFinish` removes the OLD table's lock entry on
/// success while keeping it (and the new lock) when the DDL failed, so a
/// failed truncate leaves exactly the original lock on table 1.
#[test]
#[ignore = "go-parity-gap: HandleLockTablesOnSuccessSubmit/HandleLockTablesOnFinish (pkg/ddl/executor.go:7484-7513) run inside job submit/finish and are not transcreated"]
fn handle_lock_tables_propagates_locks_to_the_truncated_id() {}
