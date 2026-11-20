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

//! Go master `pkg/ddl/tests/materializedviewlog/materialized_view_alter_test.go`.

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

/// Go `mustExecMViewMaintenance`.
fn must_exec_mview_maintenance(tk: &mut Session, sql: &str) {
    assert_eq!(exec_refresh_internal(tk, sql), "OK", "{sql}");
}

fn rows_of(tk: &mut Session, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows = answer(tk, sql);
    serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {rows}"))).unwrap()
}

#[test]
fn test_create_vector_index_on_materialized_view_log_table_rejected() {
    // Go `CreateMockStoreAndDomainWithSchemaLease(..., mockstore.WithMockTiFlash(2))`.
    let stack = super::super::tiflash_partition::tiflash_stack(2);
    let mut tk = go_master_mview_tables(&stack);
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_mlog_vec (id int, v vector(3))",
        "create materialized view log on t_mlog_vec (v) purge next date_add(now(), interval 1 hour)",
        "alter table `$mlog$t_mlog_vec` set tiflash replica 1",
    ]);
    exec_err_contains(
        &mut tk,
        "create vector index idx_mlog_vec on `$mlog$t_mlog_vec`((vec_cosine_distance(v))) USING HNSW",
        "CREATE INDEX on materialized view log table",
    );
}

#[test]
fn test_alter_materialized_view_log_purge_updates_next_unix_seconds_with_mlog_alter_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mlog_purge_priv (a int, b int)",
        "create materialized view log on t_mlog_purge_priv (a, b) purge next date_add(now(), interval 2 hour)",
        // The node logs in by password; Go's testkit `Auth` takes none.
        "create user 'mv_alter_purge_u'@'%' identified by 'abc'",
        "create user 'mv_alter_purge_select_u'@'%' identified by 'abc'",
        "grant alter on test.`$mlog$t_mlog_purge_priv` to 'mv_alter_purge_u'@'%'",
        "grant select on test.t_mlog_purge_priv to 'mv_alter_purge_select_u'@'%'",
    ]);
    must_exec_all(&mut user_session(&stack, &users, 3001, "mv_alter_purge_u"), &[
        "alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 25 minute)",
    ]);
    exec_err_contains(
        &mut user_session(&stack, &users, 3002, "mv_alter_purge_select_u"),
        "alter materialized view log on test.t_mlog_purge_priv purge next date_add(now(), interval 30 minute)",
        "ALTER command denied",
    );
    let mlog = latest_table(&stack, "$mlog$t_mlog_purge_priv");
    assert_eq!(mlog.materialized_view_log.as_ref().unwrap().read().purge_next, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)");
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = {}",
                mlog.id
            )
        ),
        r#"RS:[["1","1","1"]]"#
    );
    must_exec_all(&mut tk, &["drop user 'mv_alter_purge_u'@'%'", "drop user 'mv_alter_purge_select_u'@'%'"]);
}

#[test]
fn test_alter_materialized_view_log_add_column_basic() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mlog_add_column (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null, untouched int)",
        "create materialized view log on t_mlog_add_column (id)",
        "insert into t_mlog_add_column values (1, 7, 'old', '2026-01-02', 'memo', 100)",
        "alter materialized view log on t_mlog_add_column add column (n, s, d, note)",
    ]);
    let mlog = latest_table(&stack, "$mlog$t_mlog_add_column");
    let tracked: Vec<String> =
        mlog.materialized_view_log.as_ref().unwrap().read().columns.iter().map(|column| column.original().to_owned()).collect();
    assert_eq!(tracked, ["id", "n", "s", "d", "note"]);
    let columns: Vec<tidb_model::ColumnInfo> = mlog.columns.iter_deref().map(|column| column.read().clone()).collect();
    let names: Vec<&str> = columns.iter().map(|column| column.name.original()).collect();
    assert_eq!(names, ["id", "n", "s", "d", "note", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"]);
    let column = |name: &str| columns.iter().find(|column| column.name.lowercase() == name).unwrap().clone();
    for name in ["n", "s", "d", "note"] {
        assert!(tidb_mysql::has_not_null_flag(column(name).field_type.flags() as usize), "{name}");
    }
    let origin_default = |name: &str| format!("{}", column(name).get_origin_default_value());
    assert_eq!(origin_default("n"), "0");
    assert_eq!(origin_default("s"), " ");
    assert_eq!(origin_default("d"), "0000-00-00");
    assert_eq!(origin_default("note"), " ");
    assert_eq!(column("note").field_type.code(), tidb_datatype::FieldTypeCode::Blob);
    assert_eq!(
        answer(&mut tk, "select n, hex(s), cast(d as char), hex(note), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_column`"),
        r#"RS:[["0","20","0000-00-00","20","I","1"]]"#
    );
    let show = rows_of(&mut tk, "show create materialized view log on t_mlog_add_column");
    let show = show[0][1].clone().unwrap();
    assert!(show.contains("CREATE MATERIALIZED VIEW LOG ON `t_mlog_add_column` (`id`, `n`, `s`, `d`, `note`)"), "{show}");

    must_exec_mview_maintenance(&mut tk, "delete from `$mlog$t_mlog_add_column`");
    must_exec_all(&mut tk, &["update t_mlog_add_column set untouched = 101 where id = 1"]);
    assert_eq!(answer(&mut tk, "select * from `$mlog$t_mlog_add_column`"), "RS:[]");
    must_exec_all(&mut tk, &["update t_mlog_add_column set s = 'new' where id = 1"]);
    let mut changed = rows_of(&mut tk, "select id, n, s, cast(d as char), note, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_column`");
    changed.sort();
    let want: Vec<Vec<Option<String>>> = [
        ["1", "7", "new", "2026-01-02", "memo", "U", "1"],
        ["1", "7", "old", "2026-01-02", "memo", "U", "-1"],
    ]
    .iter()
    .map(|row| row.iter().map(|cell| Some((*cell).to_owned())).collect())
    .collect();
    assert_eq!(changed, want);

    must_exec_all(&mut tk, &[
        "create table t_mlog_add_column_atomic (id int, b int, c int)",
        "create materialized view log on t_mlog_add_column_atomic (id)",
    ]);
    let cancel_tk = Arc::new(std::sync::Mutex::new(stack.factory.open_session(session_context(3003)).unwrap()));
    let triggered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_done = Arc::new(std::sync::Mutex::new(None::<String>));
    {
        let _after_sync = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", {
            let (cancel_tk, triggered, cancel_done) = (Arc::clone(&cancel_tk), Arc::clone(&triggered), Arc::clone(&cancel_done));
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if triggered.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let matches = job.type_ == tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE
                    && job.schema_name.to_string() == "test"
                    && job.table_name.to_string() == "$mlog$t_mlog_add_column_atomic"
                    && job.multi_schema_info.as_ref().is_some_and(|info| {
                        let info = info.read();
                        info.sub_jobs.len() == 2
                            && info.sub_jobs.iter_deref().nth(1).is_some_and(|sub| sub.read().schema_state == tidb_model::SchemaState::WRITE_REORGANIZATION)
                    });
                if !matches {
                    triggered.store(false, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                let result = answer(&mut cancel_tk.lock().unwrap(), &format!("admin cancel ddl jobs {}", job.id));
                *cancel_done.lock().unwrap() = Some(result);
            }
        });
        exec_err_contains(&mut tk, "alter materialized view log on t_mlog_add_column_atomic add column (b, c)", "Cancelled DDL job");
    }
    let cancelled = cancel_done.lock().unwrap().clone().expect("expected mlog multi-column add cancellation");
    assert!(cancelled.contains("successful"), "{cancelled}");
    let atomic = latest_table(&stack, "$mlog$t_mlog_add_column_atomic");
    let tracked: Vec<String> =
        atomic.materialized_view_log.as_ref().unwrap().read().columns.iter().map(|column| column.original().to_owned()).collect();
    assert_eq!(tracked, ["id"]);
    assert!(!atomic.columns.iter_deref().any(|column| matches!(column.read().name.lowercase(), "b" | "c")));
    let show = rows_of(&mut tk, "show create materialized view log on t_mlog_add_column_atomic");
    let show = show[0][1].clone().unwrap();
    assert!(show.contains("CREATE MATERIALIZED VIEW LOG ON `t_mlog_add_column_atomic` (`id`)"), "{show}");
    assert!(!show.contains("`b`") && !show.contains("`c`"), "{show}");
}

#[test]
fn test_alter_materialized_view_log_add_column_default_semantics() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mlog_add_defaults (id int, nullable_varchar varchar(10), nullable_text text, nn_enum enum('a','b') not null, nn_set set('x','y') not null, nullable_enum enum('a','b'), nullable_set set('x','y'), nn_varchar varchar(10) not null, nn_text text not null)",
        "create materialized view log on t_mlog_add_defaults (id)",
    ]);
    must_exec_mview_maintenance(&mut tk, "insert into `$mlog$t_mlog_add_defaults` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)");
    must_exec_all(&mut tk, &[
        "alter materialized view log on t_mlog_add_defaults add column (nullable_varchar, nullable_text, nn_enum, nn_set, nullable_enum, nullable_set, nn_varchar, nn_text)",
    ]);
    assert_eq!(
        answer(
            &mut tk,
            "select nullable_varchar is null, nullable_text is null, cast(nn_enum as char), cast(nn_set as char), nullable_enum is null, nullable_set is null, hex(nn_varchar), hex(nn_text), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_add_defaults`"
        ),
        r#"RS:[["1","1","a","","1","1","20","20","I","1"]]"#
    );
}

#[test]
fn test_alter_materialized_view_log_purge_best_effort_info_update_warning() {
    let (stack, _users, mut tk) = mview_test_kit();
    let mut tk_lock = stack.factory.open_session(session_context(3004)).unwrap();
    must_exec_all(&mut tk_lock, &["set tidb_mview_enable = on", "use test"]);
    must_exec_all(&mut tk, &[
        "create table t_mlog_info_lock (a int, b int)",
        "create materialized view log on t_mlog_info_lock (a, b) purge next date_add(now(), interval 2 hour)",
    ]);
    let id = latest_table(&stack, "$mlog$t_mlog_info_lock").id;
    const EXPECTED_NEXT_UNIX_SECONDS: i64 = 1_925_089_445;
    must_exec_all(&mut tk, &[&format!(
        "update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = {EXPECTED_NEXT_UNIX_SECONDS} where MLOG_ID = {id}"
    )]);
    must_exec_all(&mut tk_lock, &[
        "begin pessimistic",
        &format!("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = NEXT_PURGE_UNIX_SECONDS where MLOG_ID = {id}"),
    ]);
    must_exec_all(&mut tk, &["alter materialized view log on t_mlog_info_lock purge next date_add(now(), interval 25 minute)"]);
    let warnings = answer(&mut tk, "show warnings");
    assert!(
        warnings.contains("alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_PURGE_UNIX_SECONDS within 10s due to row lock contention"),
        "{warnings}"
    );
    let mlog = latest_table(&stack, "$mlog$t_mlog_info_lock");
    assert_eq!(mlog.materialized_view_log.as_ref().unwrap().read().purge_next, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)");
    assert_eq!(
        single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS = {EXPECTED_NEXT_UNIX_SECONDS} from mysql.tidb_mlog_purge_info where MLOG_ID = {id}")),
        "1"
    );
    must_exec_all(&mut tk_lock, &["rollback"]);
}

#[test]
fn test_alter_materialized_view_log_add_column_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_add_mlog_priv (a int, b int)",
        "create materialized view log on t_add_mlog_priv (a)",
        "create user 'u_add_mlog_no_select'@'%' identified by 'abc'",
        "create user 'u_add_mlog_ok'@'%' identified by 'abc'",
        "grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_no_select'@'%'",
    ]);
    exec_err_contains(
        &mut user_session(&stack, &users, 3005, "u_add_mlog_no_select"),
        "alter materialized view log on test.t_add_mlog_priv add column (b)",
        "SELECT command denied",
    );
    must_exec_all(&mut tk, &[
        "grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_ok'@'%'",
        "grant select on test.t_add_mlog_priv to 'u_add_mlog_ok'@'%'",
    ]);
    must_exec_all(&mut user_session(&stack, &users, 3006, "u_add_mlog_ok"), &["alter materialized view log on test.t_add_mlog_priv add column (b)"]);
    must_exec_all(&mut tk, &["drop user 'u_add_mlog_no_select'@'%'", "drop user 'u_add_mlog_ok'@'%'"]);
}

#[test]
fn test_alter_materialized_view_log_add_column_supports_new_materialized_view() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_add_mlog_mv (a int not null, b int not null, c int not null)",
        "insert into t_add_mlog_mv values (1, 10, 100), (1, 20, 200), (2, 30, 300)",
        "create materialized view log on t_add_mlog_mv (a, b)",
    ]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_add_mlog_col_before (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a",
        "materialized view log does not contain column c",
    );
    must_exec_all(&mut tk, &[
        "alter materialized view log on t_add_mlog_mv add column (c)",
        "create materialized view mv_add_mlog_col_after (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_add_mlog_col_after order by a"), r#"RS:[["1","300","2"],["2","300","1"]]"#);
    must_exec_all(&mut tk, &[
        "update t_add_mlog_mv set c = 150 where a = 1 and b = 10",
        "insert into t_add_mlog_mv values (2, 40, 400)",
        "refresh materialized view mv_add_mlog_col_after fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_add_mlog_col_after order by a"), r#"RS:[["1","350","2"],["2","700","2"]]"#);
}
