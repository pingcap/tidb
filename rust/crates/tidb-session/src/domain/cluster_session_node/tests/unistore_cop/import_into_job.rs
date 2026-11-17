// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/executor/importer/job_test.go`: IMPORT INTO jobs in
//! `mysql.tidb_import_jobs`, over the embedded unistore node's DXF sessions.

use super::*;
use serde_json::json;
use tidb_executor::importer::job::{self, ImportParameters, JobInfo, Summary};

/// One session's SQL executor and affected-row count, as Go's tests read
/// `tk.Session()`.
fn with_conn(f: impl FnOnce(&dyn tidb_dxf_storage::SessionContext, &tidb_gocontext::Context)) {
    let (stack, _users) = cop_backed_stack();
    let tm = tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool());
    let ctx = tidb_gocontext::Context::background();
    let mut f = Some(f);
    tm.with_new_session(&mut |se| {
        (f.take().unwrap())(se, &ctx);
        Ok(())
    })
    .unwrap();
    drop(stack);
}

fn mock_summary(rows: i64) -> Summary {
    Summary { imported_rows: rows, ..Summary::default() }
}

fn params() -> ImportParameters {
    ImportParameters {
        columns_and_vars: "(a, b, c)".to_owned(),
        set_clause: "d = 1".to_owned(),
        format: "csv".to_owned(),
        options: [("skip_rows".to_owned(), json!(1.0)), ("detached".to_owned(), json!(null))].into_iter().collect(),
        ..ImportParameters::default()
    }
}

fn job_info(created_by: &str) -> JobInfo {
    JobInfo {
        table_schema: "test".to_owned(),
        table_name: "t".to_owned(),
        table_id: 1,
        created_by: created_by.to_owned(),
        parameters: params(),
        source_file_size: 123,
        status: "pending".to_owned(),
        ..JobInfo::default()
    }
}

/// Go `jobInfoEqual`: equal but for the times.
fn job_info_equal(expected: &JobInfo, got: &JobInfo) {
    let mut cloned = expected.clone();
    cloned.create_time = got.create_time.clone();
    cloned.start_time = got.start_time.clone();
    cloned.update_time = got.update_time.clone();
    cloned.end_time = got.end_time.clone();
    assert_eq!(&cloned, got);
}

fn create(se: &dyn tidb_dxf_storage::SessionContext, ctx: &tidb_gocontext::Context, info: &JobInfo) -> i64 {
    let conn = se.sql_executor();
    job::create_job(ctx, conn.as_ref(), &info.table_schema, &info.table_name, info.table_id, &info.created_by, "", &info.parameters, info.source_file_size)
        .unwrap()
}

fn get(se: &dyn tidb_dxf_storage::SessionContext, ctx: &tidb_gocontext::Context, id: i64, user: &str, sup: bool) -> JobInfo {
    job::get_job(ctx, se.sql_executor().as_ref(), id, user, sup).unwrap()
}

fn active(se: &dyn tidb_dxf_storage::SessionContext, ctx: &tidb_gocontext::Context) -> i64 {
    job::get_active_job_cnt(ctx, se.sql_executor().as_ref(), "test", "t").unwrap()
}

/// Go `TestJobHappyPath`.
#[test]
fn test_job_happy_path() {
    with_conn(|se, ctx| {
        let conn = se.sql_executor();
        let conn = conn.as_ref();
        type Action = fn(&dyn tidb_sqlexec::SqlExecutor, &tidb_gocontext::Context, i64);
        let cases: [(Action, bool, &str, &str, i64, &str); 2] = [
            (|c, ctx, id| job::finish_job(ctx, c, id, Some(&mock_summary(111))).unwrap(), true, "finished", "", 111, ""),
            (
                |c, ctx, id| job::fail_job(ctx, c, id, "some error", Some(&mock_summary(111))).unwrap(),
                false,
                "failed",
                job::JOB_STEP_VALIDATING,
                111,
                "some error",
            ),
        ];
        for (action, pre_start_no_op, status, step, rows, err_msg) in cases {
            let mut info = job_info("root@%");
            let id = create(se, ctx, &info);
            info.id = id;
            let got = get(se, ctx, id, "root@%", false);
            assert!(got.create_time.is_some() && got.start_time.is_none() && got.end_time.is_none());
            job_info_equal(&info, &got);
            assert_eq!(active(se, ctx), 1);
            if pre_start_no_op {
                action(conn, ctx, id);
                job_info_equal(&info, &get(se, ctx, id, "root@%", false));
            }
            job::start_job(ctx, conn, id, job::JOB_STEP_IMPORTING).unwrap();
            let got = get(se, ctx, id, "root@%", false);
            assert!(got.create_time.is_some() && got.start_time.is_some() && got.end_time.is_none());
            info.status = "running".to_owned();
            info.step = job::JOB_STEP_IMPORTING.to_owned();
            job_info_equal(&info, &got);
            assert_eq!(active(se, ctx), 1);
            job::job_2_step(ctx, conn, id, job::JOB_STEP_VALIDATING).unwrap();
            assert_eq!(active(se, ctx), 1);
            action(conn, ctx, id);
            let got = get(se, ctx, id, "root@%", false);
            assert!(got.create_time.is_some() && got.start_time.is_some() && got.end_time.is_some());
            info.status = status.to_owned();
            info.step = step.to_owned();
            info.summary = Some(mock_summary(rows));
            info.error_message = err_msg.to_owned();
            job_info_equal(&info, &got);
            assert_eq!(active(se, ctx), 0);
            let end_time = got.end_time.clone();
            action(conn, ctx, id);
            assert_eq!(get(se, ctx, id, "root@%", false).end_time, end_time);
        }
        // Go's "prepare phase transition".
        let created = ImportParameters {
            file_location: "s3://bucket/path.csv".to_owned(),
            format: "auto".to_owned(),
            options: [("detached".to_owned(), json!(null)), ("thread".to_owned(), json!(16.0))].into_iter().collect(),
            ..ImportParameters::default()
        };
        let id = job::create_job(ctx, conn, "test", "t", 1, "root@%", "", &created, 123).unwrap();
        job::start_job(ctx, conn, id, job::JOB_STEP_PREPARING).unwrap();
        let info = get(se, ctx, id, "root@%", true);
        assert_eq!((info.status.as_str(), info.step.as_str()), ("running", job::JOB_STEP_PREPARING));
        let start_time = info.start_time.clone();
        assert!(start_time.is_some());
        job::update_job_prepared_info(ctx, conn, id, 456, "csv").unwrap();
        let info = get(se, ctx, id, "root@%", true);
        assert_eq!(info.source_file_size, 456);
        assert_eq!(info.parameters.format, "csv");
        assert_eq!(info.parameters.file_location, created.file_location);
        assert_eq!(info.parameters.options, created.options);
        job::job_2_step(ctx, conn, id, job::JOB_STEP_GLOBAL_SORTING).unwrap();
        let info = get(se, ctx, id, "root@%", true);
        assert_eq!((info.status.as_str(), info.step.as_str()), ("running", job::JOB_STEP_GLOBAL_SORTING));
        assert_eq!(info.start_time, start_time);
    });
}
/// Go `TestGetAndCancelJob`.
#[test]
fn test_get_and_cancel_job() {
    with_conn(|se, ctx| {
        let conn = se.sql_executor();
        let conn = conn.as_ref();
        let mut info = job_info("user-for-test@%");
        let id1 = create(se, ctx, &info);
        info.id = id1;
        job_info_equal(&info, &get(se, ctx, id1, &info.created_by, false));
        assert_eq!(active(se, ctx), 1);
        job::cancel_job(ctx, conn, id1).unwrap();
        assert_eq!(se.affected_rows(), 1);
        let got = get(se, ctx, id1, &info.created_by, false);
        // A cancelled job has no start or end time.
        assert!(got.create_time.is_some() && got.start_time.is_none() && got.end_time.is_none());
        info.status = "cancelled".to_owned();
        info.error_message = "cancelled by user".to_owned();
        job_info_equal(&info, &got);
        assert_eq!(active(se, ctx), 0);
        job::cancel_job(ctx, conn, id1).unwrap();
        assert_eq!(se.affected_rows(), 0);

        info.status = "pending".to_owned();
        info.error_message.clear();
        info.created_by = "user-for-test-2@%".to_owned();
        let id2 = create(se, ctx, &info);
        info.id = id2;
        job_info_equal(&info, &get(se, ctx, id2, &info.created_by, false));
        job::start_job(ctx, conn, id2, job::JOB_STEP_IMPORTING).unwrap();
        info.status = "running".to_owned();
        info.step = job::JOB_STEP_IMPORTING.to_owned();
        job_info_equal(&info, &get(se, ctx, id2, &info.created_by, false));
        job::cancel_job(ctx, conn, id2).unwrap();
        assert_eq!(se.affected_rows(), 1);
        let got = get(se, ctx, id2, &info.created_by, false);
        assert!(got.start_time.is_some() && got.end_time.is_none());
        info.status = "cancelled".to_owned();
        info.error_message = "cancelled by user".to_owned();
        job_info_equal(&info, &got);
        assert_eq!(active(se, ctx), 0);

        let not_found = job::get_job(ctx, conn, 999_999_999, &info.created_by, false).unwrap_err();
        assert!(not_found.equal(&tidb_exeerrors::ERR_LOAD_DATA_JOB_NOT_FOUND));
        let denied = job::get_job(ctx, conn, id2, "aaa", false).unwrap_err();
        assert!(denied.equal(&tidb_plannererrors::ERR_SPECIFIC_ACCESS_DENIED));
        job::get_job(ctx, conn, id2, "aaa", true).unwrap();
        let jobs = job::get_all_viewable_jobs(ctx, conn, "user-for-test-2@%", false).unwrap();
        assert_eq!(jobs.iter().map(|j| j.id).collect::<Vec<_>>(), [id2]);
        let jobs = job::get_all_viewable_jobs(ctx, conn, "user-for-test-2@%", true).unwrap();
        assert_eq!(jobs.iter().map(|j| j.id).collect::<Vec<_>>(), [id1, id2]);
    });
}

/// Go `TestCancelPendingJob`.
#[test]
fn test_cancel_pending_job() {
    with_conn(|se, ctx| {
        let conn = se.sql_executor();
        let conn = conn.as_ref();
        let parameters = ImportParameters { format: "csv".to_owned(), ..ImportParameters::default() };
        let pending = job::create_job(ctx, conn, "test", "t", 1, "root@%", "", &parameters, 123).unwrap();
        assert_eq!(active(se, ctx), 1);
        job::cancel_pending_job(ctx, conn, pending).unwrap();
        assert_eq!(se.affected_rows(), 1);
        let job = get(se, ctx, pending, "root@%", true);
        assert!(job.is_cancelled());
        assert_eq!(job.error_message, "cancelled by user");
        assert!(job.start_time.is_none() && job.end_time.is_none());
        assert_eq!(active(se, ctx), 0);
        let running = job::create_job(ctx, conn, "test", "t", 1, "root@%", "", &parameters, 123).unwrap();
        job::start_job(ctx, conn, running, job::JOB_STEP_IMPORTING).unwrap();
        job::cancel_pending_job(ctx, conn, running).unwrap();
        assert_eq!(se.affected_rows(), 0);
        let job = get(se, ctx, running, "root@%", true);
        assert_eq!((job.status.as_str(), job.step.as_str()), ("running", job::JOB_STEP_IMPORTING));
        assert!(!job.is_cancelled() && job.error_message.is_empty());
        assert_eq!(active(se, ctx), 1);
    });
}
/// Go `TestFailJobBeforeStart`.
#[test]
fn test_fail_job_before_start() {
    with_conn(|se, ctx| {
        let conn = se.sql_executor();
        let conn = conn.as_ref();
        let parameters = ImportParameters { format: "csv".to_owned(), ..ImportParameters::default() };
        let id = job::create_job(ctx, conn, "test", "t", 1, "root@%", "", &parameters, 123).unwrap();
        job::fail_job(ctx, conn, id, "failed before start", Some(&mock_summary(0))).unwrap();
        let got = get(se, ctx, id, "root@%", false);
        assert_eq!((got.status.as_str(), got.error_message.as_str()), ("failed", "failed before start"));
        assert!(got.start_time.is_none() && got.end_time.is_some());
        assert_eq!(got.summary, Some(mock_summary(0)));
        assert_eq!(active(se, ctx), 0);
    });
}

/// Go `TestJobInfo_CanCancel`.
#[test]
fn test_job_info_can_cancel() {
    let mut info = JobInfo::default();
    for (status, can_cancel, is_cancelled, is_success) in [
        ("pending", true, false, false),
        ("running", true, false, false),
        ("finished", false, false, true),
        ("failed", false, false, false),
        ("cancelled", false, true, false),
        ("canceled", false, false, false),
    ] {
        info.status = status.to_owned();
        assert_eq!((info.can_cancel(), info.is_cancelled(), info.is_success()), (can_cancel, is_cancelled, is_success), "{status}");
    }
}

/// Go `TestGetJobInfoNullField`: NULL times, summary and error message.
#[test]
fn test_get_job_info_null_field() {
    with_conn(|se, ctx| {
        let conn = se.sql_executor();
        let conn = conn.as_ref();
        let mut info = job_info("user-for-test@%");
        let id1 = create(se, ctx, &info);
        job::start_job(ctx, conn, id1, job::JOB_STEP_IMPORTING).unwrap();
        job::fail_job(ctx, conn, id1, "failed", Some(&mock_summary(0))).unwrap();
        let id2 = create(se, ctx, &info);
        let got = job::get_all_viewable_jobs(ctx, conn, "", true).unwrap();
        assert_eq!(got.len(), 2);
        info.id = id1;
        info.status = "failed".to_owned();
        info.step = job::JOB_STEP_IMPORTING.to_owned();
        info.error_message = "failed".to_owned();
        info.summary = Some(mock_summary(0));
        job_info_equal(&info, &got[0]);
        assert!(got[0].start_time.is_some() && got[0].end_time.is_some());
        info.id = id2;
        info.status = "pending".to_owned();
        info.step.clear();
        info.summary = None;
        info.error_message.clear();
        job_info_equal(&info, &got[1]);
        assert!(got[1].start_time.is_none() && got[1].end_time.is_none());
    });
}
