// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/executor/show.go`'s `fetchShowImportJobs` /
//! `fetchShowImportGroups` and `ImportIntoActionExec` (CANCEL IMPORT JOB),
//! over job rows created directly, like Go
//! `TestCancelImportJobWithoutDXFTask`.
//!
//! Expected rows come from the Go capture `zzprobe/e05show`
//! (`GOTOOLCHAIN=go1.27.3 go test --tags=intest ./zzprobe/e05show/`), with
//! the time columns (10-12, 14) left out.

use super::table_mode::refusal;
use super::*;
use tidb_executor::importer::job::{self, ImportParameters, Summary};

/// A row without the time columns, joined like the Go capture.
fn text(session: &mut Session, sql: &str) -> Vec<String> {
    displayed(rows(session, sql))
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| !(10..=12).contains(i) && *i != 14)
                .map(|(_, v)| if v == "NULL" { "<nil>".to_owned() } else { v })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn show_and_cancel_import_jobs_like_go() {
    let (stack, users) = cop_backed_stack();
    let tm = Arc::new(tidb_dxf_storage::TaskManager::new(stack.factory.dxf_session_pool()));
    tidb_dxf_storage::set_task_manager(Some(Arc::clone(&tm)));
    tidb_dxf_storage::set_dxf_svc_task_mgr(Arc::clone(&tm));
    let ctx = tidb_gocontext::Context::background();
    tm.init_meta(&ctx, ":4000", "").unwrap();
    let params = ImportParameters {
        columns_and_vars: "(a, @b)".to_owned(),
        set_clause: "c=@b".to_owned(),
        file_location: "s3://bucket/a.csv".to_owned(),
        format: "csv".to_owned(),
        ..ImportParameters::default()
    };
    let mut ids = Vec::new();
    tm.with_new_session(&mut |se| {
        let conn = se.sql_executor();
        let c = conn.as_ref();
        let pending = job::create_job(&ctx, c, "test", "t`x", 11, "root@%", "", &params, 0).unwrap();
        let finished = job::create_job(&ctx, c, "te`st", "t", 12, "root@%", "g1", &params, 123_456_789).unwrap();
        job::start_job(&ctx, c, finished, job::JOB_STEP_IMPORTING).unwrap();
        let summary = Summary { imported_rows: 42, conflict_row_cnt: 3, too_many_conflicts: true, ..Summary::default() };
        job::finish_job(&ctx, c, finished, Some(&summary)).unwrap();
        let failed = job::create_job(&ctx, c, "test", "t", 13, "root@%", "g1", &params, -1).unwrap();
        job::fail_job(&ctx, c, failed, "boom", None).unwrap();
        let other = job::create_job(&ctx, c, "test", "t", 14, "someone@%", "", &params, 0).unwrap();
        ids = vec![pending, finished, failed, other];
        Ok(())
    })
    .unwrap();
    assert_eq!(ids, [1, 2, 3, 4]);

    let mut root = stack.factory.open_session(session_context(2401)).unwrap();
    let job1 = "1|<nil>|s3://bucket/a.csv|`test`.`t``x`|11||pending|N/A|<nil>||root@%|<nil>|<nil>|<nil>|<nil>|<nil>|<nil>";
    let job2 = "2|g1|s3://bucket/a.csv|`te``st`.`t`|12||finished|117.7MiB|42|3 conflicted rows. Too many conflicted rows, checksum skipped.|root@%|<nil>|<nil>|<nil>|<nil>|<nil>|<nil>";
    let job3 = "3|g1|s3://bucket/a.csv|`test`.`t`|13||failed|-1B|<nil>|boom|root@%|<nil>|<nil>|<nil>|<nil>|<nil>|<nil>";
    let job4 = "4|<nil>|s3://bucket/a.csv|`test`.`t`|14||pending|N/A|<nil>||someone@%|<nil>|<nil>|<nil>|<nil>|<nil>|<nil>";
    assert_eq!(text(&mut root, "show import job 1"), [job1]);
    assert_eq!(text(&mut root, "show import job 2"), [job2]);
    assert_eq!(text(&mut root, "show import job 3"), [job3]);
    assert_eq!(text(&mut root, "show import job 4"), [job4]);
    assert_eq!(text(&mut root, "show import jobs"), [job1, job2, job3, job4]);
    assert_eq!(text(&mut root, "show import jobs where Job_ID > 2"), [job3, job4]);
    assert_eq!(refusal(&mut root, "show import job 999").as_deref(), Some("8170|Job ID 999 doesn't exist"));
    assert_eq!(
        refusal(&mut root, "cancel import job 3").as_deref(),
        Some("8171|The current job status cannot perform the operation. CANCEL")
    );
    assert_eq!(refusal(&mut root, "cancel import job 999").as_deref(), Some("8170|Job ID 999 doesn't exist"));
    assert_eq!(refusal(&mut root, "cancel import job 1"), None);
    assert_eq!(
        text(&mut root, "show import job 1"),
        ["1|<nil>|s3://bucket/a.csv|`test`.`t``x`|11||cancelled|0B|<nil>|cancelled by user|root@%|<nil>|<nil>|<nil>|<nil>|<nil>|<nil>"]
    );
    let groups = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql)).into_iter().map(|row| row[..7].join(" ")).collect()
    };
    assert_eq!(groups(&mut root, "show import groups"), ["g1 2 0 0 1 1 0"]);
    assert_eq!(groups(&mut root, "show import group 'g1'"), ["g1 2 0 0 1 1 0"]);

    rows(&mut root, "create user 'u1'@'%' identified by 'abc'");
    let identity = users
        .authenticate_native(
            "u1",
            "127.0.0.1",
            &super::super::node_fixture::SALT,
            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
        )
        .expect("u1 logs in");
    let mut u1 = stack
        .factory
        .open_session(super::super::node_fixture::session_context_with_identity(2402, identity))
        .unwrap();
    assert!(text(&mut u1, "show import jobs").is_empty());
    let denied = "1227|Access denied; you need (at least one of) the SUPER privilege(s) for this operation";
    assert_eq!(refusal(&mut u1, "show import job 1").as_deref(), Some(denied));
    assert_eq!(refusal(&mut u1, "cancel import job 1").as_deref(), Some(denied));
    tidb_dxf_storage::set_task_manager(None);
}
