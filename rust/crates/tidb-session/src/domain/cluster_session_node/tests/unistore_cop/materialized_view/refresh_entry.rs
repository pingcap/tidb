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

//! Go master `pkg/executor/test/materializedview/materialized_view_refresh_entry_test.go`.

use super::create::new_mview_test_kit;
use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, users, tk)
}

fn user_session(stack: &UnistoreClusterStack, users: &ConfiguredUserStore, connection_id: u64, user: &str) -> Session {
    let mut session = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(connection_id, log_in(users, user)))
        .unwrap();
    must_exec_all(&mut session, &["set tidb_mview_enable = on"]);
    session
}

#[test]
fn test_materialized_view_refresh_sql_entry() {
    for (mode, step, plan) in [
        ("fast", "DATA_CHANGE_FAST_MERGE", "MViewDeltaMerge"),
        ("complete in place", "DATA_CHANGE_COMPLETE_INSERT", "Insert"),
        ("complete delta apply", "DATA_CHANGE_COMPLETE_DELTA_APPLY", "MViewCompleteDeltaApply"),
        ("complete out of place", "OUT_OF_PLACE_LOAD_SHADOW", "Insert"),
    ] {
        // t.Run(tc.mode)
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table src_entry (id int primary key, g int not null, v int not null)",
            "insert into src_entry values (1,1,10)",
            "create materialized view log on src_entry (id,g,v)",
            "create materialized view mv_entry (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_entry group by g",
            "insert into src_entry values (2,1,20)",
        ]);
        let watermark = answer(&mut tk, "select last_success_read_tso from mysql.tidb_mview_refresh_info");
        let id_sql = "select tidb_table_id from information_schema.tables where table_schema='test' and table_name='mv_entry'";
        let id = answer(&mut tk, id_sql);
        let sql = format!("refresh materialized view mv_entry {mode}");
        for _ in 0..2 {
            let rows = answer(&mut tk, &format!("{sql} dry run"));
            assert!(rows.contains(step) && rows.contains(plan) && rows.contains("FINALIZE_HIST"), "{mode}: {rows}");
            assert!(!rows.contains("status:success"), "{mode}: {rows}");
            assert_eq!(answer(&mut tk, "select * from mv_entry"), r#"RS:[["1","10","1"]]"#);
            assert_eq!(answer(&mut tk, "select last_success_read_tso from mysql.tidb_mview_refresh_info"), watermark);
            assert_eq!(single_value(&mut tk, "select count(*) from mysql.tidb_mview_refresh_hist"), "0");
            assert_eq!(
                single_value(&mut tk, "select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'"),
                "0"
            );
        }
        let rows = answer(&mut tk, &format!("{sql} with profile"));
        assert!(rows.contains(step) && rows.contains(plan), "{mode}: {rows}");
        assert!(rows.contains("status:success, time:") && rows.contains("FINALIZE_HIST"), "{mode}: {rows}");
        assert_eq!(answer(&mut tk, "select * from mv_entry"), r#"RS:[["1","30","2"]]"#);
        assert_eq!(
            answer(&mut tk, "select refresh_status,refresh_method from mysql.tidb_mview_refresh_hist"),
            format!(r#"RS:[["success","{mode} manual"]]"#)
        );
        let current_id = answer(&mut tk, id_sql);
        if mode == "complete out of place" {
            assert_ne!(id, current_id);
        } else {
            assert_eq!(id, current_id);
        }
        must_exec_all(&mut tk, &["insert into src_entry values (3,2,40)", &sql]);
        assert!(tk.affected_rows() > 0, "{mode}");
        assert_eq!(answer(&mut tk, "select * from mv_entry order by g"), r#"RS:[["1","30","2"],["2","40","1"]]"#);
        // "Planning again after a real refresh must not reuse the caller's MDL
        // table cache."
        assert!(answer(&mut tk, &format!("{sql} dry run")).contains(step), "{mode}");
        assert!(!tk.vars.restricted_sql && !tk.vars.in_mview_maintenance, "{mode}");
    }
}

#[test]
fn test_materialized_view_refresh_entry_validation() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table src_entry (g int not null, v int not null)",
        "create materialized view log on src_entry (g,v)",
        "create materialized view mv_entry (g,s,cnt) as select g,sum(v),count(*) from src_entry group by g",
    ]);
    exec_err_contains(&mut tk, "refresh materialized view mv_entry with async mode complete in place", "WITH ASYNC MODE is not supported yet");
    for suffix in ["", " dry run", " with profile"] {
        must_exec_all(&mut tk, &["begin"]);
        exec_err_contains(
            &mut tk,
            &format!("refresh materialized view mv_entry complete in place{suffix}"),
            "cannot run REFRESH MATERIALIZED VIEW in explicit transaction",
        );
        must_exec_all(&mut tk, &["rollback"]);
    }
    must_exec_all(&mut tk, &["prepare stmt from 'refresh materialized view mv_entry complete in place'", "begin"]);
    exec_err_contains(&mut tk, "execute stmt", "cannot run REFRESH MATERIALIZED VIEW in explicit transaction");
    must_exec_all(&mut tk, &[
        "rollback",
        "deallocate prepare stmt",
        "set autocommit=0",
        "refresh materialized view mv_entry complete in place",
        "insert into src_entry values (1,10)",
    ]);
    exec_err_contains(&mut tk, "refresh materialized view mv_entry complete in place", "cannot run REFRESH MATERIALIZED VIEW in explicit transaction");
    must_exec_all(&mut tk, &["rollback", "set autocommit=1"]);
}

#[test]
fn test_materialized_view_refresh_entry_privileges() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table src_entry (g int not null, v int not null)",
        "create materialized view log on src_entry (g,v)",
        "create materialized view mv_entry (g,s,cnt) as select g,sum(v),count(*) from src_entry group by g",
        // The node logs in by password; Go's testkit `Auth` takes none.
        "create user 'refresh_entry_u'@'%' identified by 'abc'",
    ]);
    let mut tk_user = user_session(&stack, &users, 3301, "refresh_entry_u");
    let sql = "refresh materialized view test.mv_entry complete delta apply";
    exec_err_contains(&mut tk_user, sql, "OPERATE VIEW command denied");
    must_exec_all(&mut tk, &["grant operate view on test.mv_entry to 'refresh_entry_u'@'%'"]);
    exec_err_contains(&mut tk_user, sql, "SELECT command denied");
    for suffix in [" dry run", " with profile"] {
        exec_err_contains(&mut tk_user, &format!("{sql}{suffix}"), "SHOW VIEW command denied");
    }
    must_exec_all(&mut tk, &["grant show view on test.mv_entry to 'refresh_entry_u'@'%'"]);
    for suffix in [" dry run", " with profile"] {
        exec_err_contains(&mut tk_user, &format!("{sql}{suffix}"), "SELECT command denied");
    }
    must_exec_all(&mut tk, &["grant select on test.src_entry to 'refresh_entry_u'@'%'"]);
    must_exec_all(&mut tk_user, &[sql]);
    for suffix in [" dry run", " with profile"] {
        assert!(answer(&mut tk_user, &format!("{sql}{suffix}")).contains("FINALIZE_HIST"), "{suffix}");
    }
}

#[test]
fn test_cancel_materialized_view_refresh_job_request() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table src_cancel (g int not null, v int not null)",
        "create materialized view log on src_cancel (g,v)",
        "create materialized view mv_cancel (g,s,cnt) as select g,sum(v),count(*) from src_cancel group by g",
    ]);
    let id = latest_table(&stack, "mv_cancel").id;
    must_exec_all(&mut tk, &[
        &format!(
            "insert into mysql.tidb_mview_refresh_hist (refresh_job_id,mview_id,refresh_method,refresh_status) values (1,{id},'fast manual','running'),(2,{id},'fast manual','success')"
        ),
        "cancel materialized view refresh job 1",
    ]);
    assert_eq!(single_value(&mut tk, "select cancel_request_time is not null from mysql.tidb_mview_refresh_hist where refresh_job_id=1"), "1");
    for job in [1, 2, 3] {
        exec_err_contains(&mut tk, &format!("cancel materialized view refresh job {job}"), &format!("cannot cancel materialized view refresh job {job}"));
    }
}

#[test]
fn test_cancel_materialized_view_refresh_sql() {
    for mode in ["fast", "complete in place", "complete delta apply", "complete out of place"] {
        // t.Run(mode)
        let (stack, users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table src_cancel (g int not null, v int not null)",
            "insert into src_cancel values (1,10)",
            "create materialized view log on src_cancel (g,v)",
            "create materialized view mv_cancel (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_cancel group by g",
            "insert into src_cancel values (1,20)",
        ]);
        let watermark = answer(&mut tk, "select last_success_read_tso from mysql.tidb_mview_refresh_info");
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(10)");
        let pause =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", "pause");
        let mut tk_refresh = new_mview_test_kit(&stack, 3302);
        let refresh = std::thread::spawn(move || answer(&mut tk_refresh, &format!("refresh materialized view test.mv_cancel {mode}")));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while single_value(&mut tk, "select count(*) from mysql.tidb_mview_refresh_hist where refresh_status='running'") != "1" {
            assert!(std::time::Instant::now() < deadline, "{mode}: the refresh never reached its running history row");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let job_id = single_value(&mut tk, "select refresh_job_id from mysql.tidb_mview_refresh_hist where refresh_status='running'");
        must_exec_all(&mut tk, &["create user 'refresh_cancel_u'@'%' identified by 'abc'"]);
        let mut tk_cancel = user_session(&stack, &users, 3303, "refresh_cancel_u");
        let cancel_sql = format!("cancel materialized view refresh job {job_id}");
        exec_err_contains(&mut tk_cancel, &cancel_sql, "OPERATE VIEW command denied");
        must_exec_all(&mut tk, &["grant operate view on test.mv_cancel to 'refresh_cancel_u'@'%'"]);
        let (requested, _watch) = wait_cancel_watcher_requested("refresh-");
        must_exec_all(&mut tk_cancel, &[&cancel_sql]);
        requested.recv_timeout(std::time::Duration::from_secs(10)).expect("refresh cancellation");
        drop(pause);
        let error = refresh.join().unwrap();
        assert!(error.contains("materialized view task canceled manually"), "{mode}: {error}");
        assert_eq!(answer(&mut tk, "select * from mv_cancel"), r#"RS:[["1","10","1"]]"#);
        assert_eq!(answer(&mut tk, "select last_success_read_tso from mysql.tidb_mview_refresh_info"), watermark);
        assert_eq!(
            answer(&mut tk, "select refresh_status,refresh_failed_reason from mysql.tidb_mview_refresh_hist"),
            r#"RS:[["failed","cancelled manually by 'refresh_cancel_u'@'%'"]]"#,
            "{mode}"
        );
        assert_eq!(
            single_value(&mut tk, "select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'"),
            "0"
        );
    }
}
