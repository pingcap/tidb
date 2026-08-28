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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/cancel_test.go` (items 38-40) and
//! `pkg/ddl/cluster_test.go` (items 41-44) of the pkg/ddl.part1 slice, read
//! from `origin/master`.
//!
//! Both files pin behaviors of the DDL JOB PIPELINE: cancel/rolling-back
//! bookkeeping (`ADMIN CANCEL DDL JOBS` across every job type, driven by the
//! `beforeDeliveryJob`/`beforeRunOneJobStep`/`afterWaitSchemaSynced`
//! failpoint hooks) and `FLASHBACK CLUSTER TO TIMESTAMP`
//! (`model.ActionFlashbackCluster` jobs interacting with PD schedule config,
//! GC safe points and global variables). This tier has no DDL job queue, no
//! failpoint seams and no PD/infosync surface, so every test here is a
//! documentary `#[ignore]`; none of the Go expectations is approximated.

/// GO PORT of `pkg/ddl/cancel_test.go:249 TestCancelVariousJobs`.
///
/// Go iterates one cancel attempt per DDL job type (add/drop index and
/// column, create/drop/truncate/rename table and database, flashback,
/// shuffle-type jobs, ...) against pre-built schema and 2049 rows, asserting
/// for each whether the cancel succeeds per the job's current state machine
/// position and that the job history records the outcome.
#[test]
#[ignore = "go-parity-gap: needs the DDL job queue with delivery/rolling-back failpoints (beforeDeliveryJob, mockBackfillSlow) and ADMIN CANCEL DDL JOBS; the job pipeline is not transcreated in this tier"]
fn cancel_various_jobs() {}

/// GO PORT of `pkg/ddl/cancel_test.go:397 TestCancelForAddUniqueIndex`.
///
/// Go pins that cancelling an ADD UNIQUE INDEX job while it is in
/// StateWriteOnly / StateDeleteOnly / StateDeleteReorganization rolls the job
/// back with ErrDupEntry (duplicate rows pre-exist) and leaves the table with
/// zero indices.
#[test]
#[ignore = "go-parity-gap: needs the add-index backfill state machine and its rollback path; not transcreated in this tier"]
fn cancel_for_add_unique_index() {}

/// GO PORT of `pkg/ddl/cancel_test.go:432 TestCancelJobBeforeRun`.
///
/// Go pins that a `TRUNCATE TABLE` cancelled inside
/// `beforeTransitOneJobStep` (before any state transition) fails with
/// ErrCancelledDDLJob and leaves the table's rows untouched.
#[test]
#[ignore = "go-parity-gap: needs the job submission -> beforeTransitOneJobStep window and ErrCancelledDDLJob routing; the job pipeline is not transcreated"]
fn cancel_job_before_run() {}

/// GO PORT of `pkg/ddl/cluster_test.go:37 TestFlashbackCloseAndResetPDSchedule`.
///
/// Go pins that a FLASHBACK CLUSTER job closes PD schedulers
/// (`merge-schedule-limit` -> 0) on entering StateWriteReorganization and
/// restores the saved value when the job is cancelled mid-flight.
#[test]
#[ignore = "go-parity-gap: FLASHBACK CLUSTER (model.ActionFlashbackCluster jobs), infosync PD schedule config and the injectSafeTS failpoint are not transcreated in this tier"]
fn flashback_close_and_reset_pd_schedule() {}

/// GO PORT of `pkg/ddl/cluster_test.go:85 TestAddDDLDuringFlashback`.
///
/// Go pins that submitting any DDL while a flashback-cluster job is
/// in-flight fails with "Can't add ddl job, have flashback cluster job".
#[test]
#[ignore = "go-parity-gap: needs the flashback-cluster job gating DDL submission; the job pipeline is not transcreated"]
fn add_during_flashback() {}

/// GO PORT of `pkg/ddl/cluster_test.go:119 TestGlobalVariablesOnFlashback`.
///
/// Go pins the global-variable side effects of a flashback cluster: while
/// the job runs, `tidb_gc_enable`/`tidb_enable_auto_analyze`/`tidb_ttl_job_enable`
/// read OFF and `tidb_super_read_only` ON; after it finishes the first three
/// are restored to their pre-flashback values while `tidb_ttl_job_enable`
/// stays OFF.
#[test]
#[ignore = "go-parity-gap: needs the flashback-cluster job's global-variable save/restore around the job state machine; not transcreated in this tier"]
fn global_variables_on_flashback() {}

/// GO PORT of `pkg/ddl/cluster_test.go:197 TestCancelFlashbackCluster`.
///
/// Go pins cancel windows: cancelling at StateDeleteOnly succeeds and
/// restores `tidb_ttl_job_enable` = on; cancelling at
/// StateWriteReorganization fails and the flashback completes (variable
/// left OFF).
#[test]
#[ignore = "go-parity-gap: needs the flashback-cluster job state machine and its cancel hook (afterWaitSchemaSynced); not transcreated in this tier"]
fn cancel_flashback_cluster() {}
