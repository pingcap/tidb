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

//! Documented go-parity-gap ports of the job-submission and job-worker
//! tests in this batch: `pkg/ddl/job_submitter_test.go`,
//! `pkg/ddl/job_worker_test.go`, `pkg/ddl/jobsubmit/submit_test.go`, and
//! `pkg/ddl/jobsubmit/table_mode_test.go` (all read from the origin/master
//! snapshot). The tier they exercise — `ddl.JobSubmitter`
//! (`GenGIDAndInsertJobsWithRetry`), the owner/worker loop
//! (`pkg/ddl/job_worker.go:364 JobNeedGC`, owner campaigning), and
//! `jobsubmit.SubmitBatch` / `jobsubmit.BuildAlterTableModeJob` — is the
//! online-DDL job queue this workspace does not build yet (the
//! `crate::ddl` module doc: DDL applies synchronously to metadata). Each
//! port carries the re-derived contract with Go symbol locations so the
//! behavior can be pinned the moment that tier exists.

/// Go `job_submitter_test.go:474::TestGenIDAndInsertJobsWithRetryQPS`. Go
/// itself opens with `t.Skip("it's for offline test only, skip it in CI")`
/// — it is an offline QPS benchmark (100 threads x 30000 iterations with a
/// payload-size flag), never a CI assertion. Nothing to pin.
// go-parity-gap: Go t.Skip's this offline-only QPS benchmark in CI; there
// is no behavior to pin even on the Go side.
#[test]
#[ignore]
fn gen_id_and_insert_jobs_with_retry_qps_is_an_offline_only_benchmark() {
}

/// Go `job_submitter_test.go:537::TestGenGIDAndInsertJobsWithRetryOnErr`.
/// Go plants `jobsubmit/mockGenGIDRetryableError` (`3*return(true)`) and an
/// `onGenGIDRetry` hook that bumps the global ID by 100 between retries,
/// then requires: the submit retries exactly 3 times (hook counter == 3);
/// the final global ID moved by 300 (hook) + 2 (the job's own
/// table+partition-free allocation); after success the job-done channel map
/// holds exactly the new job ID; and the job's TableID is `newGID - 1`
/// (`ddl.JobSubmitter.GenGIDAndInsertJobsWithRetry`,
/// pkg/ddl/job_submitter.go).
// go-parity-gap: the JobSubmitter retry/done-channel machinery is not
// transcreated; this tier has no job queue.
#[test]
#[ignore]
fn gen_gid_and_insert_jobs_with_retry_survives_three_retryable_errors() {
}

/// Go `job_submitter_test.go:585::TestSubmitJobAfterDDLIsClosed`. Go stops
/// the domain's DDL (`dom.DDL().Stop()`) with the `afterDDLCloseCancel`
/// failpoint capturing the error of a `create database test2` issued after
/// close, and requires the error to be exactly "context canceled" —
/// submission after the DDL loop is closed is refused, not silently lost.
// go-parity-gap: there is no DDL lifecycle to stop in this tier; DDL runs
// synchronously against the catalog.
#[test]
#[ignore]
fn submit_job_after_ddl_is_closed_answers_context_canceled() {
}

/// Go `job_worker_test.go:37::TestCheckOwner`. With a 5s schema lease,
/// after one lease period the single-node domain's owner manager reports
/// `IsOwner() == true` and `dom.GetSchemaLease() == testLease` — the
/// campaign-over-etcd election converging to self-ownership.
// go-parity-gap: owner campaigning over etcd is not part of this tier.
#[test]
#[ignore]
fn check_owner_campaign_elects_the_single_node() {
}

/// Go `job_worker_test.go:45::TestInvalidDDLJob`. Submitting a job whose
/// type is `model.ActionNone` through `ExecutorForTest.DoDDLJobWrapper`
/// answers `[ddl:8204]invalid ddl job type: none` (the worker's
/// `ActionNone` guard in pkg/ddl/ddl.go's job dispatch) without touching
/// metadata.
// go-parity-gap: DoDDLJobWrapper and the 8204 dispatch guard are not
// transcreated; this tier has no job type dispatch.
#[test]
#[ignore]
fn invalid_ddl_job_type_none_answers_8204() {
}

/// Go `job_worker_test.go:63::TestAddBatchJobError`. With
/// `jobsubmit/mockAddBatchDDLJobsErr` (`return(true)`), submitting a job
/// errors with exactly "mockAddBatchDDLJobsErr" and — the point of the
/// "should not hang forever" comment — the submitter RETURNS rather than
/// blocking on the job-done channel that will never fire.
// go-parity-gap: the add-batch failpoint and its done-channel cleanup are
// job-queue internals; this tier has no job queue.
#[test]
#[ignore]
fn add_batch_job_error_returns_instead_of_hanging() {
}

/// Go `job_worker_test.go:83::TestParallelDDL`. Go submits 10 DDLs across
/// 3 databases/3 tables concurrently, blocking delivery until all 11 are
/// queued (the `beforeLoadAndDeliverJobs` hook counts 5 reorg + 6 plain),
/// then requires the per-table `$.seq_num` order recorded in
/// `@@tidb_last_ddl_info`: table t1's jobs keep submission order
/// (seq[0]<seq[1]<seq[2]<seq[4]<seq[8]), t2's pair keeps order
/// (seq[3]<seq[5]), and t3's chain through the dropped database keeps
/// order (seq[6]<seq[7]<seq[9]) — same-table DDLs are serialized,
/// cross-table DDLs run in parallel.
// go-parity-gap: per-table job serialization and seq_num bookkeeping are
// scheduler properties; this tier applies DDL synchronously.
#[test]
#[ignore]
fn parallel_ddl_serializes_same_table_jobs_by_seq_num() {
}

/// Go `job_worker_test.go:213::TestJobNeedGC`, the pure truth table over
/// `pkg/ddl/job_worker.go:364 JobNeedGC`:
/// - cancelled ADD INDEX → false;
/// - done ADD COLUMN → false; done/rollback-done ADD INDEX and ADD
///   PRIMARY KEY → true;
/// - MULTI-SUBJOB form: a done multi-schema change whose sub-jobs contain
///   no index work → false; with a done ADD INDEX sub-job → true; with a
///   done DROP COLUMN sub-job → true; a rollback-done multi-schema change
///   with a rollback-done ADD INDEX sub-job → true.
/// The function is not transcreated in this workspace, so the truth table
/// has no Rust surface yet; the port documents the full table.
// go-parity-gap: pkg/ddl/job_worker.go JobNeedGC is not transcreated; the
// truth table above has no Rust surface yet.
#[test]
#[ignore]
fn job_need_gc_truth_table_over_index_bearing_jobs() {
}

/// Go `jobsubmit/submit_test.go:132::TestSubmitBatchEnqueuesTableModeJob`.
/// `jobsubmit.SubmitBatch` (pkg/ddl/jobsubmit/submit.go:53) over a built
/// ALTER-TABLE-MODE spec must: allocate an ID above the initial global ID,
/// set state `JobStateQueueing` and a non-zero StartTS, stamp
/// `ast.BDRRoleNone`; persist the job readable via `SysTblMgr.GetJobByID`
/// with type `ActionAlterTableMode`, schema/table IDs 100/200, query
/// "skip", lowercased involving-schema-info `[{testdb, t1}]`, and args
/// carrying `TableModeImport`; and leave one `mysql.tidb_ddl_job` row with
/// `schema_ids`="100 200". The subtests additionally pin schema/name
/// lowercasing, missing-`TraceInfo` initialization to `&tracing.TraceInfo{}`,
/// and preservation of an existing `TraceInfo` pointer.
// go-parity-gap: jobsubmit.SubmitBatch and the system-table job store are
// not transcreated.
#[test]
#[ignore]
fn submit_batch_enqueues_table_mode_job_with_normalized_names_and_trace() {
}

/// Go `jobsubmit/submit_test.go:224::TestSubmitBatchAllocatesIDsAndInsertsJob`.
/// A v2 `ActionCreateTable` spec whose TableInfo carries a 2-partition
/// `PartitionInfo` must get: a job ID and table ID above the initial
/// global ID, the table ID written back into `CreateTableArgs.TableInfo.ID`,
/// one fresh ID per partition definition, and a `mysql.tidb_ddl_job` row
/// whose `schema_ids`/`table_ids` columns read `0 <tableID>`.
// go-parity-gap: batch ID allocation into job args is jobsubmit machinery.
#[test]
#[ignore]
fn submit_batch_allocates_table_and_partition_ids() {
}

/// Go `jobsubmit/submit_test.go:263::TestSubmitBatchChecksAndPauseState`.
/// Four guards of SubmitBatch: an involving-schema-info with an empty table
/// name is refused ("must have non-empty name") leaving zero job rows; a
/// stored flashback-cluster job blocks any submit ("have flashback cluster
/// job"); with the cluster BDR role set to primary, a restricted DDL with
/// CDCWriteSource 0 is denied ("bdr role") leaving zero rows; and under the
/// fake server-state syncer's upgrading state the job is inserted as
/// `JobStatePausing` with `AdminCommandBySystem` — paused before running,
/// not dropped.
// go-parity-gap: SubmitBatch's BDR/flashback/upgrade guards are not
// transcreated.
#[test]
#[ignore]
fn submit_batch_checks_involving_names_flashback_bdr_and_upgrade_pause() {
}

/// Go `jobsubmit/submit_test.go:308::TestSubmitBatchRetryCleanup`. With
/// `BeforeInsertWithAssignedIDs` recording callbacks and
/// `mockGenGIDRetryableError` (`1*return(true)`), a submit that retries
/// must run the cleanup fn of the FIRST attempt only (assignedIDs.len==2,
/// cleanupIDs.len==1, cleanupIDs[0]==assignedIDs[0]), land the second
/// assignment as the job's ID, and leave exactly one stored job.
// go-parity-gap: the retry-cleanup hook contract belongs to the
// untranscreated SubmitBatch.
#[test]
#[ignore]
fn submit_batch_retry_cleans_up_only_the_abandoned_attempt() {
}

/// Go `jobsubmit/table_mode_test.go:37::TestBuildAlterTableModeJob`.
/// `jobsubmit.BuildAlterTableModeJob` (pkg/ddl/jobsubmit/table_mode.go:26)
/// over a session with CDCWriteSource 7 and ANSI_QUOTES: builds a
/// JobVersion2 job of type `ActionAlterTableMode` with schema/table IDs
/// 101/202, LOWERCASED schema/table names, BinlogInfo present, the CDC
/// source and SQL mode copied, query "skip", involving info
/// `[{testdb, t1}]`; args exactly `AlterTableModeArgs{TableMode:
/// TableModeImport, SchemaID: 101, TableID: 202}`; noop=false; and the
/// session's QueryString value cleared.
// go-parity-gap: BuildAlterTableModeJob is not transcreated.
#[test]
#[ignore]
fn build_alter_table_mode_job_copies_session_facts_into_the_job() {
}

/// Go `jobsubmit/table_mode_test.go:72::TestBuildAlterTableModeJobNoopAndInvalidMode`.
/// Building with CurrentMode == TargetMode (Import→Import) answers
/// noop=true with nil job/args; targeting `TableModeRestore` answers
/// `infoschema.ErrInvalidTableModeSet` (pkg/infoschema/error.go:114) with
/// noop=false and nil job/args.
// go-parity-gap: BuildAlterTableModeJob and infoschema.ErrInvalidTableModeSet
// are not transcreated.
#[test]
#[ignore]
fn build_alter_table_mode_job_noop_when_unchanged_and_invalid_for_restore() {
}
