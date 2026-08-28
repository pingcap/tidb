// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/job_test.go`: the IMPORT job
//! lifecycle against `mysql.tidb_import_jobs`
//! (`job.go:86`). The Rust tier has neither the job table nor the
//! `JobInfo`/`Summary` surface, so each test records the lifecycle contract a
//! port must satisfy. Go source: `pkg/executor/importer/job.go`.

/// Go `pkg/executor/importer/job_test.go:43::TestJobHappyPath`:
/// `CreateJob` (`job.go:216`) returns a fresh pending job whose
/// `GetJob` (`job.go:164`) view has zero start/end times;
/// `GetActiveJobCnt` (`job.go:193`) counts it; `StartJob` (:261) moves it to
/// `importing` with a start time; `FailJob` (:353) to `failed` with step
/// `validating` (`JobStepValidating`, `job.go:80`) and the error message plus
/// row-count summary; `FinishJob` (:333) to `finished`. Actions taken BEFORE
/// `StartJob` are no-ops on the stored status.
#[test]
#[ignore = "go-parity-gap: the tidb_import_jobs lifecycle (job.go CreateJob/StartJob/FinishJob/FailJob) is unported"]
fn import_job_lifecycle_records_status_step_and_summary() {}

/// Go `pkg/executor/importer/job_test.go:201::TestGetAndCancelJob`:
/// `CancelJob` (`job.go:489`) marks the job `cancelled` with message
/// "cancelled by user" and no start/end time (unless it was running, in which
/// case the start time survives), is idempotent when called twice, and drops
/// the job from `GetActiveJobCnt`.
#[test]
#[ignore = "go-parity-gap: CancelJob/GetJob (job.go:489/:164) are unported; no tidb_import_jobs table"]
fn import_job_cancel_is_idempotent_and_clears_active_count() {}

/// Go `pkg/executor/importer/job_test.go:315::TestFailJobBeforeStart`:
/// failing a job that has not started still transitions it to `failed` with
/// the error message recorded, but leaves start/end times unset
/// (`FailJob`, `job.go:353`).
#[test]
#[ignore = "go-parity-gap: FailJob (job.go:353) is unported"]
fn import_job_fail_before_start_records_error_without_times() {}

/// Go `pkg/executor/importer/job_test.go:351::TestJobInfo_CanCancel`:
/// `JobInfo.CanCancel` (`job.go:136`) is true only while the job is pending or
/// running (a finished/failed/cancelled/ expired job cannot be cancelled).
#[test]
#[ignore = "go-parity-gap: JobInfo.CanCancel (job.go:136) is unported"]
fn import_job_can_cancel_only_in_pending_or_running_status() {}

/// Go `pkg/executor/importer/job_test.go:368::TestGetJobInfoNullField`:
/// `GetJob` (`job.go:164`) maps NULL columns of `tidb_import_jobs` (group key,
/// error message) to zero values without error.
#[test]
#[ignore = "go-parity-gap: GetJob NULL-column handling (job.go:164) is unported"]
fn import_job_get_maps_null_columns_to_zero_values() {}
