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

//! Port ledger for `pkg/ddl/job_submitter_test.go` (pkg/ddl.part7 items
//! 406-410 of the local enumeration). All five drive `ddl.JobSubmitter`
//! against an embedded-unistore store; the crate carries the merge-only
//! seed of job_submitter.go in tidb-exec (outside this gate) and no
//! submitter, so all are documentary gap ports.

/// GO PORT of `pkg/ddl/job_submitter_test.go:51 TestGenIDAndInsertJobsWithRetry`.
///
/// Re-derived contract: ten threads x 500 iterations of
/// `GenGIDAndInsertJobsWithRetry` on one create-table JobWrapper (retry cnt
/// pinned to 1) leave exactly 5000 jobs in the DDL job queue whose IDs are
/// pairwise unique and all greater than the starting global ID, while the
/// global ID advanced by MORE than 5000 (each retry burns IDs).
#[test]
#[ignore = "go-parity-gap: JobSubmitter.GenGIDAndInsertJobsWithRetry (pkg/ddl/job_submitter.go) needs the meta mutator, job table and owner-manager surfaces, none transcreated"]
fn gen_gid_and_insert_jobs_allocates_unique_ids_under_concurrency() {}

/// GO PORT of `pkg/ddl/job_submitter_test.go:114 TestCombinedIDAllocation`.
///
/// Re-derived contract: the required global-ID count per job wrapper is
/// 1 (job) + action-specific extras only when IDs are NOT pre-allocated —
/// create-tables sums 1 per table plus its partition count (batch of
/// {1,2,0} partitions costs 1+3+1+2); create-table costs 1+1+partitions;
/// create-sequence/create-view/create-db/create-resource-group cost 2;
/// alter-table-partitioning costs 1+partitions (NewTableID) plus
/// definitions; truncate/add/reorganize/remove partitioning cost 1 plus
/// their partition lists; truncate-table costs 1+NewTableID+partitions.
/// Processing the 26 cases one-by-one and together advances the global ID
/// by exactly the summed counts, and the IDAllocated=false pass stamps
/// every job ID, table ID, schema ID, RG ID, partition definition ID and
/// NewTableID with fresh unique values above the initial ID (13 such
/// cases, `allocatedIDCount` distinct IDs overall).
#[test]
#[ignore = "go-parity-gap: JobWrapper ID-count/allocation (pkg/ddl/job_submitter.go getRequiredGIDCount/assignGIDsForJobs) has no Rust carrier in this gate's closure"]
fn combined_id_allocation_counts_job_table_and_partition_ids() {}

/// GO PORT of `pkg/ddl/job_submitter_test.go:474
/// TestGenIDAndInsertJobsWithRetryQPS`.
///
/// Re-derived contract: an offline-only throughput harness (100 threads x
/// 30000 iterations of job submission with a 1KB payload) that Go itself
/// skips in CI (`t.Skip("it's for offline test only...")`); it only prints
/// per-thread QPS, asserting nothing beyond submission success.
#[test]
#[ignore = "go-parity-gap: Go skips this offline QPS harness in CI; the submitter it measures is not transcreated"]
fn gen_gid_and_insert_jobs_qps_is_an_offline_only_harness() {}

/// GO PORT of `pkg/ddl/job_submitter_test.go:537
/// TestGenGIDAndInsertJobsWithRetryOnErr`.
///
/// Re-derived contract: with `mockGenGIDRetryableError` failing the global
/// ID generation 3 times, each retry's `onGenGIDRetry` hook observes an
/// EMPTY `DDLJobDoneChMap` (the failure path cleaned the registered
/// channels) and burns 100 extra global IDs; after success the map holds
/// exactly one entry keyed by the final global ID, the job's TableID is
/// `newGID-1`, and the total advance is 3*100+2 IDs (300 burned + job ID +
/// table ID).
#[test]
#[ignore = "go-parity-gap: needs the retryable-gen-global-ID failpoint and DDLJobDoneChMap lifecycle, none transcreated"]
fn gen_gid_retry_clears_done_channels_and_burns_ids_per_retry() {}

/// GO PORT of `pkg/ddl/job_submitter_test.go:585
/// TestSubmitJobAfterDDLIsClosed`.
///
/// Re-derived contract: submitting a DDL after `dom.DDL().Stop()` (with
/// `afterDDLCloseCancel` capturing the synchronous error) fails with exactly
/// "context canceled" rather than hanging or silently succeeding.
#[test]
#[ignore = "go-parity-gap: needs the DDL close/cancel lifecycle of the domain, which the crate does not model"]
fn submit_job_after_ddl_is_closed_fails_with_context_canceled() {}
