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

//! Port ledger for `pkg/ddl/job_worker_test.go` (pkg/ddl.part7 items
//! 411-415 of the local enumeration). All five exercise the DDL worker/owner
//! runtime; the crate carries no worker runtime, so all are documentary gap
//! ports with contracts re-derived from the Go bodies.

/// GO PORT of `pkg/ddl/job_worker_test.go:37 TestCheckOwner`.
///
/// Re-derived contract: after one schema lease (`testLease`, 5s) on a
/// single-node mock cluster, the DDL owner manager reports `IsOwner()` true
/// and the domain's schema lease equals the configured lease — owner election
/// converges within one lease.
#[test]
#[ignore = "go-parity-gap: owner-manager election and schema-lease machinery are not transcreated"]
fn check_owner_elects_the_single_node_within_one_lease() {}

/// GO PORT of `pkg/ddl/job_worker_test.go:45 TestInvalidDDLJob`.
///
/// Re-derived contract: `DoDDLJobWrapper` on a job whose Type is
/// ActionNone fails synchronously with "[ddl:8204]invalid ddl job type:
/// none" — unknown action types are rejected at submission, not queued.
#[test]
#[ignore = "go-parity-gap: DoDDLJobWrapper and its ddl:8204 invalid-type guard have no Rust carrier"]
fn invalid_ddl_job_type_is_rejected_with_8204() {}

/// GO PORT of `pkg/ddl/job_worker_test.go:63 TestAddBatchJobError`.
///
/// Re-derived contract: with `mockAddBatchDDLJobsErr` armed, submitting a
/// valid job returns the injected "mockAddBatchDDLJobsErr" error promptly —
/// the job runner must not hang forever when batch-inserting fails.
#[test]
#[ignore = "go-parity-gap: needs the add-batch-DDL-jobs failure path of the submitter/worker pair"]
fn add_batch_job_error_surfaces_promptly_without_hanging() {}

/// GO PORT of `pkg/ddl/job_worker_test.go:83 TestParallelDDL`.
///
/// Re-derived contract: 11 jobs over three tables in two databases (adds,
/// drops, indexes, add-column and column modify) queued at once and then
/// delivered finish with each table's own statement sequence strictly
/// ordered by its `seq_num` window — per-table ordering is preserved under
/// parallel execution across tables.
#[test]
#[ignore = "go-parity-gap: needs the parallel job-delivery scheduler and seq_num record windows"]
fn parallel_ddl_preserves_per_table_statement_ordering() {}

/// GO PORT of `pkg/ddl/job_worker_test.go:213 TestJobNeedGC`.
///
/// Re-derived contract (pkg/ddl/job_worker.go:363-403): `JobNeedGC` is false
/// for a cancelled add-index job and for a DONE add-column; true for
/// DONE/ROLLBACK-DONE add-index and add-primary-key; a DONE add-index whose
/// warning is ErrCantDropFieldOrKey would be false (the not-exists warning
/// carries no ranges to delete, job_worker.go:367-371); and
/// ActionMultiSchemaChange recurses into its sub-jobs via ToProxyJob
/// (job_worker.go:388-397): [add-column, rebase-auto-id] DONE is false while
/// any sub-job that needs GC on its own (add-index DONE/ROLLBACK_DONE,
/// drop-column DONE) flips the whole job to true, including a
/// ROLLBACK_DONE mix of [add-index, add-column, cancelled rebase-auto-id].
#[test]
#[ignore = "go-parity-gap: JobNeedGC (pkg/ddl/job_worker.go:363-403) and SubJob.ToProxyJob have no Rust carrier"]
fn job_need_gc_keys_on_action_state_and_subjob_recursion() {}
