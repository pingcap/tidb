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

//! Ports of the `pkg/ddl/executor_test.go` family (part6 items 314–320),
//! `pkg/ddl/external_workload_ttl_test.go` (item 321) and
//! `pkg/ddl/fail_test.go` (item 322) of the package's `func Test*`/`func
//! Benchmark*` declarations sorted by file and line, read from
//! `origin/master`.
//!
//! Every one of these Go tests but one drives the online-DDL JOB QUEUE —
//! `mysql.tidb_ddl_job` rows, a scheduling worker, schema-state transitions,
//! `admin show ddl jobs` output or failpoint hooks into the queue. That
//! machinery is not transcreated in this workspace, so those ports are
//! `#[ignore]`d documentaries with the re-derived contract; the one pure
//! predicate (`Job.IsRollbackable`, pkg/meta/model/job.go:864) IS
//! transcreated in `tidb-model` and its Go matrix is asserted live.

use tidb_model::{ActionType, Job, SchemaState};

// --- TestIsJobRollbackable (pkg/ddl/executor_test.go:128) ---
//
// Go walks four (action, state) cases over a bare job and requires
// `job.IsRollbackable()` to agree with each: DROP INDEX is rollbackable at
// StateNone but not once the index half-exists (StateDeleteOnly);
// DROP SCHEMA and DROP COLUMN at StateDeleteOnly are already past their
// cancel point (they only revert at StatePublic, i.e. before the job starts
// writing).
#[test]
fn is_job_rollbackable_matches_the_go_matrix() {
    let cases = [
        (ActionType::ACTION_DROP_INDEX, SchemaState::NONE, true),
        (ActionType::ACTION_DROP_INDEX, SchemaState::DELETE_ONLY, false),
        (ActionType::ACTION_DROP_SCHEMA, SchemaState::DELETE_ONLY, false),
        (ActionType::ACTION_DROP_COLUMN, SchemaState::DELETE_ONLY, false),
    ];
    for (action_type, schema_state, expected) in cases {
        let mut job = Job::default();
        job.type_ = action_type;
        job.schema_state = schema_state;
        assert_eq!(
            job.is_rollbackable(),
            expected,
            "Go case {action_type:?}@{schema_state:?} (pkg/meta/model/job.go:864 IsRollbackable)"
        );
    }
}

// --- TestGetDDLJobs (pkg/ddl/executor_test.go:43) ---
//
// Go opens a transaction, `addDDLJobs`-inserts ten ActionCreateTable jobs
// one at a time, and after each insert requires `ddl.GetAllDDLJobs` to
// return exactly i+1 jobs and `ddl.IterAllDDLJobs` (skipping started jobs)
// to agree with it; finally the two views match job-for-job (ID order,
// SchemaID 1) and the transaction rolls back.
//
// go-parity-gap: the mysql.tidb_ddl_job queue — addDDLJobs, GetAllDDLJobs,
// IterAllDDLJobs — is not transcreated in this workspace.
#[test]
#[ignore = "go-parity-gap: the mysql.tidb_ddl_job queue (addDDLJobs/GetAllDDLJobs/IterAllDDLJobs) is not transcreated"]
fn get_ddl_jobs_lists_every_enqueued_job_in_order() {
    // Contract (pkg/ddl/executor_test.go:43-96): after n inserts the queue
    // reads back n jobs in id order with SchemaID 1 and ActionCreateTable.
}

// --- TestGetDDLJobsIsSort (pkg/ddl/executor_test.go:98) ---
//
// Go enqueues 10 DROP TABLE jobs (ids 10..14), 5 CREATE TABLE jobs
// (ids 0..4) and 5 ADD INDEX jobs (ids 5..9) across the default and
// add-index queues, then requires `GetAllDDLJobs` to merge them into one
// id-sorted list of 15.
//
// go-parity-gap: the job queues and their merge are not transcreated.
#[test]
#[ignore = "go-parity-gap: the mysql.tidb_ddl_job queue is not transcreated"]
fn get_ddl_jobs_merges_queues_in_id_order() {
    // Contract (pkg/ddl/executor_test.go:98-126): the merged view is sorted
    // by Job.ID across the default and add-index job lists.
}

// --- TestWrappedQueryInterruptedRetriesDDLJobCancellation
//     (pkg/ddl/executor_test.go:148) ---
//
// Go parks the DDL scheduler on a failpoint, arms a one-shot failure of the
// admin-cancel command, kills the `alter table t add index idx(a)` session
// with QueryInterrupted while `waitJobSubmitted` runs, and requires the job
// to reach JobStateCancelling despite the injected failure (the wrapped
// QueryInterrupted error RETRIES the cancellation), the statement to end in
// ErrCancelledDDLJob, and the table to keep zero indexes.
//
// go-parity-gap: scheduler failpoints, the job table and the cancel-retry
// loop are not transcreated.
#[test]
#[ignore = "go-parity-gap: DDL job scheduling, admin-cancel retry and failpoints are not transcreated"]
fn wrapped_query_interrupted_retries_ddl_job_cancellation() {
    // Contract (pkg/ddl/executor_test.go:148-226): a killed ADD INDEX ends
    // cancelled even when the first cancel attempt fails, leaving no index.
}

// --- TestCreateViewConcurrently (pkg/ddl/executor_test.go:228) ---
//
// Go fires five sessions that all `create or replace view v as select * from
// t;` and, via the onDDLCreateView/afterDeliveryJob failpoints, requires that
// at no moment two CREATE VIEW jobs are in flight (the counter never exceeds
// one) — the create-view job serialization guarantee.
//
// go-parity-gap: the job queue that serializes CREATE VIEW deliveries is not
// transcreated; `run_create_view_in` here has no queue to arbitrate.
#[test]
#[ignore = "go-parity-gap: CREATE VIEW jobs are not enqueued or serialized in this tier"]
fn create_view_jobs_never_run_concurrently() {
    // Contract (pkg/ddl/executor_test.go:228-266): five racing
    // create-or-replace-view statements all succeed, and the create-view job
    // counter never exceeds one.
}

// --- TestCreateDropCreateTable (pkg/ddl/executor_test.go:268) ---
//
// Go drops `t` while, on the afterWaitSchemaSynced failpoint, a second
// session's `create table t (b int)` starts; reading the three jobs' meta
// from mysql.tidb_ddl_history requires the FIRST create's FinishedTS < the
// drop's < the SECOND create's — a dropped table may be re-created the
// moment the drop is synced, and the finish timestamps order strictly.
//
// go-parity-gap: job history records (mysql.tidb_ddl_history, BinlogInfo.
// FinishedTS) do not exist in this tier.
#[test]
#[ignore = "go-parity-gap: no DDL job history or FinishedTS recording in this tier"]
fn create_drop_create_orders_finish_timestamps() {
    // Contract (pkg/ddl/executor_test.go:268-326): create0TS < dropTS <
    // create1TS across the three serialized jobs.
}

// --- TestHandleLockTable (pkg/ddl/executor_test.go:328) ---
//
// Go drives `HandleLockTablesOnSuccessSubmit` and `HandleLockTablesOnFinish`
// for a TRUNCATE job over a session holding a READ table lock on the table:
// on submit, the lock migrates to the NEW table id (both ids locked, both
// READ); on success-finish, the old id's lock is released and the new one
// remains; on failed-finish, the OLD id keeps its lock and the new one's is
// dropped.
//
// go-parity-gap: the session table-lock registry
// (HasLockedTables/CheckTableLocked/AddTableLock) and the HandleLockTables*
// hooks are not transcreated (LOCK TABLES classifies as Forward here).
#[test]
#[ignore = "go-parity-gap: session table locks and HandleLockTablesOnSubmit/Finish are not transcreated"]
fn handle_lock_table_migrates_locks_across_truncate() {
    // Contract (pkg/ddl/executor_test.go:328-391): submit duplicates the
    // lock onto the new id; finish-with-success keeps the new id's;
    // finish-with-error keeps the old id's.
}

// --- TestExternalWorkloadTTLDDLIntegration
//     (pkg/ddl/external_workload_ttl_test.go:173) ---
//
// Go injects a recording external-workload manager (role master) and walks
// four subtests: (1) a TTL CREATE TABLE whose registration fails rolls the
// DDL back — no table, nothing registered; (2) a TTL table created with a
// FOREIGN KEY still registers exactly its own id; (3) DROP TABLE deletes the
// TTL registration; (4) a failing delete aborts the drop.
//
// go-parity-gap: the external workload manager registration hooks
// (registerTTLTable/deleteTTLTableFromExternalWorkload) and the domain that
// hosts them are not transcreated.
#[test]
#[ignore = "go-parity-gap: external-workload TTL registration hooks are not transcreated"]
fn external_workload_ttl_ddl_integration() {
    // Contract (pkg/ddl/external_workload_ttl_test.go:173-260): TTL DDL
    // registers the table id with the external workload manager, and a
    // registration/delete failure aborts the DDL.
}

// --- TestFailBeforeDecodeArgs (pkg/ddl/fail_test.go:27) ---
//
// Go arms the errorBeforeDecodeArgs failpoint for one reorganization step of
// `testCreateColumn(… "c3" … default 3)` so the job's arg-decode fails once,
// requires the WRITE_ONLY state to be observed exactly once, and requires
// the retried job to finish successfully (testCheckJobDone) — a DDL job
// survives a mid-flight decode failure by retrying.
//
// go-parity-gap: the DDL job runner and its failpoints do not exist in this
// tier.
#[test]
#[ignore = "go-parity-gap: the DDL job runner and beforeRunOneJobStep failpoints are not transcreated"]
fn fail_before_decode_args_retries_and_finishes() {
    // Contract (pkg/ddl/fail_test.go:27-63): a decode failure inside the job
    // runner is retried; the column is still added with default 3 and the
    // job ends done.
}
