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

//! Go master `pkg/executor/test/materializedview/materialized_view_show_test.go`.

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

/// Go `testkit.Rows("a b c")`: one row, its cells split on spaces.
fn row(cells: &str) -> String {
    let cells: Vec<String> = cells.split(' ').map(str::to_owned).collect();
    format!("RS:{}", serde_json::to_string(&[cells]).unwrap())
}

fn rows_of(tk: &mut Session, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows = answer(tk, sql);
    serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {rows}"))).unwrap()
}

fn table_in(stack: &UnistoreClusterStack, schema: &str, name: &str) -> tidb_model::TableInfo {
    stack.factory.catalog.get_latest().table_info_by_name(schema, name).expect(name).1.read().clone()
}

/// Go master `TestShowMaterializedView`: its eight subtests.
#[test]
fn test_show_materialized_view() {
    show_materialized_views();
    show_materialized_view_logs();
    show_materialized_view_status_privilege();
    show_materialized_view_remain_logs();
    show_materialized_view_log_wait_purge();
    show_create_materialized_view();
    show_create_materialized_view_log();
    grant_materialized_view_object_privileges();
}

/// Go master `testShowMaterializedViews`.
fn show_materialized_views() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) as select a, sum(b), count(1) from t group by a",
        "create database test_show_mv_other",
        "create table test_show_mv_other.t (a int not null, b int not null)",
        "create materialized view log on test_show_mv_other.t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view test_show_mv_other.mv_other (a, s, cnt) as select a, sum(b), count(1) from test_show_mv_other.t group by a",
    ]);
    let mv_id = table_in(&stack, "test", "mv").id;
    let other_id = table_in(&stack, "test_show_mv_other", "mv_other").id;
    let expected = row(&format!("{mv_id} mv"));
    assert_eq!(answer(&mut tk, "show materialized views"), expected);
    assert_eq!(answer(&mut tk, &format!("show materialized views where mview_id = {mv_id}")), expected);
    assert_eq!(answer(&mut tk, "show materialized views where mview_name = 'mv'"), expected);
    assert_eq!(answer(&mut tk, "show materialized views where mview_name = 'mv_not_exist'"), "RS:[]");
    let other = row(&format!("{other_id} mv_other"));
    assert_eq!(answer(&mut tk, "show materialized views from test_show_mv_other"), other);
    assert_eq!(answer(&mut tk, "show materialized views in test_show_mv_other like 'mv_%'"), other);
}

/// Go master `testShowMaterializedViewLogs`.
fn show_materialized_view_logs() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create database test_show_mlog_other",
        "create table test_show_mlog_other.t_other (a int not null, b int not null)",
        "create materialized view log on test_show_mlog_other.t_other (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let base = table_in(&stack, "test", "t");
    let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&base)).unwrap().read().clone();
    let other_base = table_in(&stack, "test_show_mlog_other", "t_other");
    let other_mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&other_base)).unwrap().read().clone();
    let other_expected = row(&format!("{} {} {} {}", other_mlog.id, other_mlog.name.original(), other_base.id, other_base.name.original()));
    let expected = row(&format!("{} {} {} {}", mlog.id, mlog.name.original(), base.id, base.name.original()));
    assert_eq!(answer(&mut tk, "show materialized view logs"), expected);
    assert_eq!(answer(&mut tk, &format!("show materialized view logs where mlog_id = {}", mlog.id)), expected);
    assert_eq!(answer(&mut tk, &format!("show materialized view logs where base_table_id = {}", base.id)), expected);
    assert_eq!(answer(&mut tk, &format!("show materialized view logs where mlog_name = '{}'", mlog.name.original())), expected);
    assert_eq!(answer(&mut tk, &format!("show materialized view logs where base_table_name = '{}'", base.name.original())), expected);
    assert_eq!(answer(&mut tk, "show materialized view logs where base_table_name = 't_not_exist'"), "RS:[]");
    assert_eq!(answer(&mut tk, "show materialized view logs from test_show_mlog_other"), other_expected);
    assert_eq!(answer(&mut tk, "show materialized view logs in test_show_mlog_other like '$mlog$%'"), other_expected);
    assert_eq!(answer(&mut tk, "show materialized view logs in test_show_mlog_other like 'test_show_mlog_other.$mlog$%'"), "RS:[]");

    // The node logs in by password; Go's testkit `Auth` takes none.
    must_exec_all(&mut tk, &["create user 'show_mlog_u'@'%' identified by 'abc'", "grant select on test.t to 'show_mlog_u'@'%'"]);
    let mut tk_user = user_session(&stack, &users, 3401, "show_mlog_u");
    assert_eq!(answer(&mut tk_user, "show materialized view logs from test"), "RS:[]");
    must_exec_all(&mut tk, &["grant alter on test.`$mlog$t` to 'show_mlog_u'@'%'"]);
    assert_eq!(answer(&mut tk_user, "show materialized view logs from test"), expected);
    must_exec_all(&mut tk, &["drop user 'show_mlog_u'@'%'"]);
}

/// Go master `testShowMaterializedViewStatusPrivilege`.
fn show_materialized_view_status_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_show_mv_status (a int not null, b int not null)",
        "create materialized view log on t_show_mv_status (a, b) purge next date_add(now(), interval 1 hour)",
        "insert into t_show_mv_status values (1, 10)",
        "create materialized view mv_show_status (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_show_mv_status group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_show_status").id;
    let base = latest_table(&stack, "t_show_mv_status");
    let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&base)).unwrap().read().clone();
    let mv_expected = |pending: i64| row(&format!("{mv_id} mv_show_status {} {} {pending}", mlog.id, mlog.name.original()));
    let mlog_expected = |pending: i64| row(&format!("{} {} {} t_show_mv_status {pending}", mlog.id, mlog.name.original(), base.id));
    let remain = "show materialized view test.mv_show_status remain_logs";
    let wait = "show materialized view log on test.t_show_mv_status wait_purge";
    assert_eq!(answer(&mut tk, remain), mv_expected(0));
    assert_eq!(answer(&mut tk, wait), mlog_expected(1));
    must_exec_all(&mut tk, &["insert into t_show_mv_status values (2, 20)"]);
    assert_eq!(answer(&mut tk, remain), mv_expected(1));
    assert_eq!(answer(&mut tk, wait), mlog_expected(1));
    must_exec_all(&mut tk, &["refresh materialized view mv_show_status complete delta apply"]);
    assert_eq!(answer(&mut tk, remain), mv_expected(0));
    assert_eq!(answer(&mut tk, wait), mlog_expected(2));

    must_exec_all(&mut tk, &["create user 'show_mv_status_u'@'%' identified by 'abc'"]);
    let mut tk_user = user_session(&stack, &users, 3402, "show_mv_status_u");
    exec_err_contains(&mut tk_user, remain, "SHOW VIEW command denied");
    exec_err_contains(&mut tk_user, wait, "SHOW VIEW command denied");
    must_exec_all(&mut tk, &["grant show view on test.mv_show_status to 'show_mv_status_u'@'%'"]);
    assert_eq!(answer(&mut tk_user, remain), mv_expected(0));
    exec_err_contains(&mut tk_user, wait, "SHOW VIEW command denied");
    must_exec_all(&mut tk, &["grant show view on test.`$mlog$t_show_mv_status` to 'show_mv_status_u'@'%'"]);
    assert_eq!(answer(&mut tk_user, wait), mlog_expected(2));
    must_exec_all(&mut tk, &["drop user 'show_mv_status_u'@'%'"]);
}

/// Go master `testShowMaterializedViewRemainLogs`.
fn show_materialized_view_remain_logs() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_remain_logs (a int not null, b int not null)",
        "create materialized view log on t_remain_logs (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_remain_logs (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_remain_logs group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_remain_logs").id;
    let base = latest_table(&stack, "t_remain_logs");
    let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&base)).unwrap().read().clone();
    let expected = |remain: i64| row(&format!("{mv_id} mv_remain_logs {} {} {remain}", mlog.id, mlog.name.original()));
    let sql = "show materialized view mv_remain_logs remain_logs";
    assert_eq!(answer(&mut tk, sql), expected(0));
    must_exec_all(&mut tk, &["insert into t_remain_logs values (1, 10), (1, 20), (2, 30)"]);
    assert_eq!(answer(&mut tk, sql), expected(3));
    must_exec_all(&mut tk, &["refresh materialized view mv_remain_logs fast"]);
    assert_eq!(answer(&mut tk, sql), expected(0));
    exec_err_contains(&mut tk, "show materialized view t_remain_logs remain_logs", "is not MATERIALIZED VIEW");
    exec_err_contains(&mut tk, "show materialized view mv_remain_logs unknown_option", "syntax error: expected REMAIN_LOGS");
}

/// Go master `testShowMaterializedViewLogWaitPurge`.
fn show_materialized_view_log_wait_purge() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_wait_purge (a int not null, b int not null)",
        "create materialized view log on t_wait_purge (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_wait_purge (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_wait_purge group by a",
    ]);
    let base = latest_table(&stack, "t_wait_purge");
    let mlog = stack.factory.catalog.get_latest().table_info_by_id(mlog_id(&base)).unwrap().read().clone();
    let expected = |wait: i64| row(&format!("{} {} {} t_wait_purge {wait}", mlog.id, mlog.name.original(), base.id));
    let sql = "show materialized view log on t_wait_purge wait_purge";
    assert_eq!(answer(&mut tk, sql), expected(0));
    must_exec_all(&mut tk, &["insert into t_wait_purge values (1, 10), (1, 20), (2, 30)"]);
    assert_eq!(answer(&mut tk, sql), expected(0));
    must_exec_all(&mut tk, &["refresh materialized view mv_wait_purge fast"]);
    assert_eq!(answer(&mut tk, sql), expected(3));
    must_exec_all(&mut tk, &["purge materialized view log on t_wait_purge"]);
    assert_eq!(answer(&mut tk, sql), expected(0));
    must_exec_all(&mut tk, &["create temporary table t_wait_purge (a int)"]);
    assert_eq!(answer(&mut tk, sql), expected(0));
    must_exec_all(&mut tk, &["create table t_wait_purge_no_log (a int)"]);
    exec_err_contains(
        &mut tk,
        "show materialized view log on t_wait_purge_no_log wait_purge",
        "materialized view log does not exist for base table test.t_wait_purge_no_log",
    );
    exec_err_contains(&mut tk, "show materialized view log on t_wait_purge unknown_option", "syntax error: expected WAIT_PURGE");
}

/// Go master `testShowCreateMaterializedView`.
fn show_create_materialized_view() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_show_mv (a int, b int not null)",
        "create materialized view log on t_show_mv (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_show_mv (a, s, cnt) comment = 'c1' shard_row_id_bits = 2 pre_split_regions = 2 refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_show_mv group by a",
    ]);
    let rows = rows_of(&mut tk, "show create materialized view mv_show_mv");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("mv_show_mv"));
    let show = rows[0][1].clone().unwrap();
    for part in [
        "CREATE MATERIALIZED VIEW `mv_show_mv` (`a`, `s`, `cnt`)",
        "COMMENT = 'c1'",
        "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2",
        "REFRESH FAST NEXT DATE_ADD(NOW(), INTERVAL 1 HOUR)",
        "AS SELECT `a`,SUM(`b`),COUNT(1) FROM `test`.`t_show_mv` GROUP BY `a`",
    ] {
        assert!(show.contains(part), "{part}: {show}");
    }
    assert!(!show.contains("ATTRIBUTES='"), "{show}");
    assert!(tidb_parser::parse(&show).is_ok(), "{show}");
    assert_eq!(rows[0][2].as_deref(), Some("utf8mb4"));
    assert_eq!(rows[0][3].as_deref(), Some("utf8mb4_bin"));

    must_exec_all(&mut tk, &[
        "create materialized view mv_show_mv_attr (a, s, cnt) refresh fast attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_show_mv group by a",
    ]);
    let rows = rows_of(&mut tk, "show create materialized view mv_show_mv_attr");
    assert_eq!(rows.len(), 1);
    let show = rows[0][1].clone().unwrap();
    assert!(show.contains("ATTRIBUTES='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'"), "{show}");

    must_exec_all(&mut tk, &[
        "create materialized view mv_show_mv_attr_partial (a, s, cnt) refresh fast attributes='mview_alert_warning=0,mview_alert_overdue=10' as select a, sum(b), count(1) from t_show_mv group by a",
    ]);
    let rows = rows_of(&mut tk, "show create materialized view mv_show_mv_attr_partial");
    assert_eq!(rows.len(), 1);
    let show = rows[0][1].clone().unwrap();
    assert!(show.contains("ATTRIBUTES='mview_alert_overdue=10'") && !show.contains("mview_alert_warning=0"), "{show}");
}

/// Go master `testShowCreateMaterializedViewLog`.
fn show_create_materialized_view_log() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_show_mlog (a int, b int)",
        "create materialized view log on t_show_mlog (a, b) shard_row_id_bits = 2 pre_split_regions = 2 purge start with cast('2026-01-02 03:04:05' as datetime) next date_add(now(), interval 1 hour) alert rows 2048",
    ]);
    let sql = "show create materialized view log on t_show_mlog";
    let rows = rows_of(&mut tk, sql);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("t_show_mlog"));
    let show = rows[0][1].clone().unwrap();
    for part in [
        "CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)",
        "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2",
        "PURGE START WITH CAST('2026-01-02 03:04:05' AS DATETIME) NEXT DATE_ADD(NOW(), INTERVAL 1 HOUR)",
        "ALERT ROWS 2048",
    ] {
        assert!(show.contains(part), "{part}: {show}");
    }
    assert!(tidb_parser::parse(&show).is_ok(), "{show}");

    must_exec_all(&mut tk, &["create temporary table t_show_mlog (a int)"]);
    let rows = rows_of(&mut tk, sql);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("t_show_mlog"));
    assert!(rows[0][1].as_deref().unwrap().contains("CREATE MATERIALIZED VIEW LOG ON `t_show_mlog` (`a`, `b`)"));

    must_exec_all(&mut tk, &["create user 'u_show_create_mlog'@'%' identified by 'abc'"]);
    let mut tk_show = user_session(&stack, &users, 3403, "u_show_create_mlog");
    let user_sql = "show create materialized view log on test.t_show_mlog";
    exec_err_contains(&mut tk_show, user_sql, "SHOW VIEW command denied");
    must_exec_all(&mut tk, &["grant show view on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'"]);
    exec_err_contains(&mut tk_show, user_sql, "SELECT command denied");
    must_exec_all(&mut tk, &["grant select on test.`$mlog$t_show_mlog` to 'u_show_create_mlog'@'%'"]);
    let user_rows = rows_of(&mut tk_show, user_sql);
    assert_eq!(user_rows.len(), 1);
    assert_eq!(user_rows[0][0].as_deref(), Some("t_show_mlog"));
    assert_eq!(user_rows[0][1].as_deref(), Some(show.as_str()));

    must_exec_all(&mut tk, &["create table t_no_mlog (a int)"]);
    exec_err_contains(&mut tk, "show create materialized view log on t_no_mlog", "'test.t_no_mlog' is not BASE TABLE WITH MATERIALIZED VIEW LOG");
    must_exec_all(&mut tk, &["drop user 'u_show_create_mlog'@'%'"]);
}

/// Go master `testGrantMaterializedViewObjectPrivileges`.
fn grant_materialized_view_object_privileges() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_grant_mv_priv (a int)",
        "create materialized view log on t_grant_mv_priv (a)",
        "create materialized view mv_grant_priv (a, cnt) as select a, count(1) from t_grant_mv_priv group by a",
        "create user 'u_grant_mv_priv'@'%'",
        "grant all privileges on test.mv_grant_priv to 'u_grant_mv_priv'@'%'",
    ]);
    let privs = |tk: &mut Session, table: &str| {
        let rows = rows_of(
            tk,
            &format!("select Table_priv, Column_priv from mysql.tables_priv where User = 'u_grant_mv_priv' and Table_name = '{table}'"),
        );
        assert_eq!(rows.len(), 1, "{table}");
        (rows[0][0].clone().unwrap_or_default(), rows[0][1].clone().unwrap_or_default())
    };
    let (table_privs, column_privs) = privs(&mut tk, "mv_grant_priv");
    for want in ["Select", "Show View", "Alter", "Drop", "Operate View"] {
        assert!(table_privs.contains(want), "{want}: {table_privs}");
    }
    for refused in ["Insert", "Update", "Delete"] {
        assert!(!table_privs.contains(refused), "{refused}: {table_privs}");
    }
    assert_eq!(column_privs, "");
    exec_err_contains(&mut tk, "grant insert on test.mv_grant_priv to 'u_grant_mv_priv'@'%'", "cannot grant Insert privilege on materialized view");
    exec_err_contains(&mut tk, "grant operate view (a) on test.mv_grant_priv to 'u_grant_mv_priv'@'%'", "COLUMN GRANT");

    must_exec_all(&mut tk, &["grant all privileges on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'"]);
    let (table_privs, column_privs) = privs(&mut tk, "$mlog$t_grant_mv_priv");
    for want in ["Select", "Show View", "Alter", "Drop", "Operate View"] {
        assert!(table_privs.contains(want), "{want}: {table_privs}");
    }
    for refused in ["Insert", "Update", "Delete"] {
        assert!(!table_privs.contains(refused), "{refused}: {table_privs}");
    }
    assert_eq!(column_privs, "");
    exec_err_contains(
        &mut tk,
        "grant update on test.`$mlog$t_grant_mv_priv` to 'u_grant_mv_priv'@'%'",
        "cannot grant Update privilege on materialized view log",
    );
    must_exec_all(&mut tk, &["drop user 'u_grant_mv_priv'@'%'"]);
}
