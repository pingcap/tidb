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

//! Port ledger for `pkg/ddl/job_scheduler_testkit_test.go` (pkg/ddl.part7
//! items 403-405 of the local enumeration). All three are mockstore
//! concurrency tests over the owner's job scheduler; no scheduler exists on
//! the Rust side, so all are documentary gap ports.

/// GO PORT of `pkg/ddl/job_scheduler_testkit_test.go:38 TestDDLScheduling`.
///
/// Re-derived contract: ten DDL statements (three add-index on one table,
/// two creates, two range-partition exchanges, add-partition, and add-index
/// on two further tables) are submitted concurrently while the recorder
/// snapshots `mysql.tidb_ddl_job` after each enqueue; the assertion is the
/// Concurrent-DDL RFC rule — jobs that cannot run concurrently (same table,
/// or partition exchange partners) must have strictly non-crossing record
/// windows in the snapshots, while independent tables may interleave.
#[test]
#[ignore = "go-parity-gap: needs the owner job scheduler's concurrent-delivery rules and mysql.tidb_ddl_job snapshots, none transcreated"]
fn ddl_scheduling_never_interleaves_conflicting_jobs_records() {}

/// GO PORT of `pkg/ddl/job_scheduler_testkit_test.go:173
/// TestUpgradingRelatedJobState`.
///
/// Re-derived contract: during cluster upgrade (`mockUpgradingState`), the
/// first add-index (still StateWriteOnly when the upgrade flag flips) runs
/// to DONE; a pausable job in reorg ends CANCELLING after `admin cancel`
/// with "[ddl:8214]Cancelled DDL job", an unpausable one ends ROLLINGBACK,
/// and one already rolling back ends ROLLBACK_DONE — pausing during upgrade
/// must not lose or corrupt either path.
#[test]
#[ignore = "go-parity-gap: needs the upgrade-state job pausing logic (processJobDuringUpgrade) and job-state observability"]
fn upgrading_related_job_state_preserves_pause_and_cancel_paths() {}

/// GO PORT of `pkg/ddl/job_scheduler_testkit_test.go:222
/// TestGeneralDDLWithQuery`.
///
/// Re-derived contract: two general DDLs submitted while the first is gated
/// before delivery (`beforeLoadAndDeliverJobs`) and both gated on
/// `waitJobSubmitted` — an `alter table add column` and a
/// `create view ... as select * from t` — must both enqueue (the
/// `mysql.tidb_ddl_job` count is 2): the view's query must not block the
/// queue via MDL registration.
#[test]
#[ignore = "go-parity-gap: needs the scheduler's MDL-free general-DDL enqueue path and waitJobSubmitted hook"]
fn general_ddl_with_query_is_not_blocked_by_mdl_registration() {}
