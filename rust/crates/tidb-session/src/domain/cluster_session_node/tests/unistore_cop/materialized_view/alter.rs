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

//! Go master `pkg/ddl/tests/materializedview/materialized_view_alter_test.go`.

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

fn decode_job(hex: &str) -> tidb_model::Job {
    let bytes: Vec<u8> = (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect();
    let mut job = tidb_model::Job::default();
    job.decode(&bytes).unwrap();
    job
}

/// Go `ddl.GetLastNHistoryDDLJobs(m, n)`.
fn last_history_jobs(session: &mut Session, n: usize) -> Vec<tidb_model::Job> {
    let rows = answer(session, &format!("select hex(job_meta) from mysql.tidb_ddl_history order by job_id desc limit {n}"));
    let rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap()).unwrap();
    rows.iter().map(|row| decode_job(&row[0])).collect()
}

/// Go `ddl.GetHistoryJobByID`.
fn history_job(session: &mut Session, job_id: &str) -> tidb_model::Job {
    decode_job(&single_value(session, &format!("select hex(job_meta) from mysql.tidb_ddl_history where job_id = {job_id}")))
}

fn involving_tables(job: &tidb_model::Job) -> std::collections::BTreeSet<String> {
    job.get_involving_schema_info()
        .snapshot()
        .iter()
        .map(|info| format!("{}\x00{}", info.database, info.table))
        .collect()
}

fn show_create(session: &mut Session, table: &str) -> String {
    let rows = answer(session, &format!("show create table `{table}`"));
    let rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{table}: {rows}"))).unwrap();
    rows[0][1].clone()
}

fn insert_alert(session: &mut Session, mview_id: i64, name: &str, refresh_failed: &str) {
    must_exec_all(session, &[&format!(
        "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values ({mview_id}, 'test', '{name}', 'warning', {refresh_failed}, UTC_TIMESTAMP(), UTC_TIMESTAMP())"
    )]);
}

fn refresh_next(stack: &UnistoreClusterStack, view: &str) -> String {
    latest_table(stack, view).materialized_view.as_ref().expect("a materialized view").read().refresh_next.clone()
}

#[test]
fn test_alter_materialized_view_refresh_disable_schedule_ignores_alert_delete_failure() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_alert_fail (a int not null, b int not null)",
        "create materialized view log on t_mv_alert_fail (a, b)",
        "create materialized view mv_alert_fail (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_alert_fail group by a",
    ]);
    let id = latest_table(&stack, "mv_alert_fail").id;
    insert_alert(&mut tk, id, "mv_alert_fail", "NULL");
    let _missing = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewRefreshAlertTableNotExists",
        "return(true)",
    );
    must_exec_all(&mut tk, &["alter materialized view mv_alert_fail refresh"]);
    assert_eq!(
        single_value(&mut tk, &format!("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")),
        "1"
    );
}

#[test]
fn test_alter_materialized_view_refresh_best_effort_info_update_warning() {
    let (stack, _users, mut tk) = mview_test_kit();
    let mut tk_lock = new_mview_test_kit(&stack, 2801);
    must_exec_all(&mut tk, &[
        "create table t_mv_info_lock (a int not null, b int not null)",
        "create materialized view log on t_mv_info_lock (a, b)",
        "create materialized view mv_info_lock (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_info_lock group by a",
    ]);
    let id = latest_table(&stack, "mv_info_lock").id;
    const EXPECTED_NEXT_UNIX_SECONDS: i64 = 1_925_089_445;
    must_exec_all(&mut tk, &[&format!(
        "update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = {EXPECTED_NEXT_UNIX_SECONDS} where MVIEW_ID = {id}"
    )]);
    must_exec_all(&mut tk_lock, &[
        "begin pessimistic",
        &format!("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = NEXT_REFRESH_UNIX_SECONDS where MVIEW_ID = {id}"),
    ]);
    must_exec_all(&mut tk, &["alter materialized view mv_info_lock refresh next date_add(now(), interval 25 minute)"]);
    let warnings = answer(&mut tk, "show warnings");
    assert!(
        warnings.contains("alter materialized view refresh: metadata updated but failed to update mysql.tidb_mview_refresh_info.NEXT_REFRESH_UNIX_SECONDS within 10s due to row lock contention"),
        "{warnings}"
    );
    assert_eq!(refresh_next(&stack, "mv_info_lock"), "DATE_ADD(NOW(), INTERVAL 25 MINUTE)");
    assert_eq!(
        single_value(
            &mut tk,
            &format!("select NEXT_REFRESH_UNIX_SECONDS = {EXPECTED_NEXT_UNIX_SECONDS} from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")
        ),
        "1"
    );
    must_exec_all(&mut tk_lock, &["rollback"]);
}

#[test]
fn test_alter_materialized_view_refresh_updates_next_unix_seconds_with_alter_privilege_only() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "create materialized view log on t (a, b)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a",
        // The node logs in by password; Go's testkit `Auth` takes none.
        "create user 'mv_alter_refresh_u'@'%' identified by 'abc'",
        "grant alter on test.mv to 'mv_alter_refresh_u'@'%'",
    ]);
    let mut tk_user = user_session(&stack, &users, 2802, "mv_alter_refresh_u");
    must_exec_all(&mut tk_user, &["alter materialized view test.mv refresh next date_add(now(), interval 25 minute)"]);
    assert_eq!(refresh_next(&stack, "mv"), "DATE_ADD(NOW(), INTERVAL 25 MINUTE)");
    let id = latest_table(&stack, "mv").id;
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}"
            )
        ),
        r#"RS:[["1","1","1"]]"#
    );
    must_exec_all(&mut tk, &["drop user 'mv_alter_refresh_u'@'%'"]);
}

#[test]
fn test_materialized_view_base_modify_column_multi_schema_involving_schema_info() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_multi_schema (a int not null, b int not null)",
        "create materialized view log on t_multi_schema (a, b)",
        "create materialized view mv_multi_schema (a, b, cnt) as select a, b, count(1) from t_multi_schema group by a, b",
        "alter table t_multi_schema modify column a bigint not null, modify column b bigint not null",
    ]);
    let history = last_history_jobs(&mut tk, 16)
        .into_iter()
        .find(|job| job.type_ == tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE && job.table_name.to_string() == "t_multi_schema")
        .expect("the multi-schema job");
    let want: std::collections::BTreeSet<String> =
        ["test\x00t_multi_schema", "test\x00$mlog$t_multi_schema", "test\x00mv_multi_schema"].iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(involving_tables(&history), want);
    for table in ["t_multi_schema", "$mlog$t_multi_schema", "mv_multi_schema"] {
        let create = show_create(&mut tk, table);
        assert!(create.contains("`a` bigint") && create.contains("`b` bigint"), "{table}: {create}");
    }
}

fn assert_min_max_refusal(error: &str, view: &str) {
    assert!(error.contains(&format!("required by materialized view {view}")), "{error}");
    assert!(error.contains("MIN/MAX fast refresh"), "{error}");
}

#[test]
fn test_materialized_view_ddl_protects_min_max_supporting_base_table_indexes() {
    let (_stack, _users, mut tk) = mview_test_kit();
    let create_min_max_mv = |tk: &mut Session, base: &str, mv: &str, index_ddl: &str| {
        must_exec_all(tk, &[
            &format!("create table {base} (a int not null, b int not null, c int not null{index_ddl})"),
            &format!("create materialized view log on {base} (a, b, c)"),
            &format!("create materialized view {mv} (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from {base} group by a, b"),
        ]);
    };

    create_min_max_mv(&mut tk, "t_drop_idx", "mv_drop_idx", ", index idx_ab(a, b)");
    assert_min_max_refusal(&answer(&mut tk, "drop index idx_ab on t_drop_idx"), "mv_drop_idx");
    assert_min_max_refusal(&answer(&mut tk, "alter table t_drop_idx drop index idx_ab"), "mv_drop_idx");

    create_min_max_mv(&mut tk, "t_invisible_idx", "mv_invisible_idx", ", index idx_ab(a, b)");
    assert_min_max_refusal(&answer(&mut tk, "alter table t_invisible_idx alter index idx_ab invisible"), "mv_invisible_idx");

    must_exec_all(&mut tk, &[
        "create table t_create_invisible_idx (a int not null, b int not null, c int not null, index idx_ab(a, b))",
        "create materialized view log on t_create_invisible_idx (a, b, c)",
        "alter table t_create_invisible_idx alter index idx_ab invisible",
    ]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_create_invisible_idx (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from t_create_invisible_idx group by a, b",
        "requires base table index whose leading columns cover all GROUP BY columns",
    );

    must_exec_all(&mut tk, &[
        "create table t_drop_pk (a int not null primary key nonclustered, c int not null)",
        "create materialized view log on t_drop_pk (a, c)",
        "create materialized view mv_drop_pk (a, minc, cnt) refresh fast next now() as select a, min(c), count(1) from t_drop_pk group by a",
    ]);
    assert_min_max_refusal(&answer(&mut tk, "alter table t_drop_pk drop primary key"), "mv_drop_pk");

    create_min_max_mv(&mut tk, "t_alt_idx", "mv_alt_idx", ", index idx_ab(a, b), index idx_ba(b, a)");
    must_exec_all(&mut tk, &[
        "drop index idx_ab on t_alt_idx",
        "insert into t_alt_idx values (1, 2, 10), (1, 2, 5)",
        "refresh materialized view mv_alt_idx fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, b, minc, cnt from mv_alt_idx"), r#"RS:[["1","2","5","2"]]"#);

    let create_min_max_mv_with_where = |tk: &mut Session, base: &str, mv: &str| {
        must_exec_all(tk, &[
            &format!("create table {base} (a int not null, b int not null, c int not null, index idx_ab(a, b), index idx_ba(b, a))"),
            &format!("create materialized view log on {base} (a, b, c)"),
            &format!("create materialized view {mv} (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from {base} where c > 0 group by a, b"),
        ]);
    };
    create_min_max_mv_with_where(&mut tk, "t_where_drop_idx", "mv_where_drop_idx");
    must_exec_all(&mut tk, &["drop index idx_ab on t_where_drop_idx"]);
    assert_min_max_refusal(&answer(&mut tk, "drop index idx_ba on t_where_drop_idx"), "mv_where_drop_idx");

    create_min_max_mv_with_where(&mut tk, "t_where_invisible_idx", "mv_where_invisible_idx");
    must_exec_all(&mut tk, &["alter table t_where_invisible_idx alter index idx_ab invisible"]);
    assert_min_max_refusal(&answer(&mut tk, "alter table t_where_invisible_idx alter index idx_ba invisible"), "mv_where_invisible_idx");

    create_min_max_mv(&mut tk, "t_multi_schema_replace_idx", "mv_multi_schema_replace_idx", ", index idx_ab(a, b)");
    must_exec_all(&mut tk, &[
        "alter table t_multi_schema_replace_idx drop index idx_ab, add index idx_ba(b, a)",
        "insert into t_multi_schema_replace_idx values (1, 2, 10), (1, 2, 5)",
        "refresh materialized view mv_multi_schema_replace_idx fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, b, minc, cnt from mv_multi_schema_replace_idx"), r#"RS:[["1","2","5","2"]]"#);

    create_min_max_mv(&mut tk, "t_multi_schema_bad_idx", "mv_multi_schema_bad_idx", ", index idx_ab(a, b)");
    assert_min_max_refusal(
        &answer(&mut tk, "alter table t_multi_schema_bad_idx drop index idx_ab, add index idx_c(c)"),
        "mv_multi_schema_bad_idx",
    );
}

#[test]
fn test_materialized_view_ddl_protects_min_max_supporting_base_table_indexes_against_concurrent_drop() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_concurrent_drop_idx (a int not null, b int not null, c int not null, index idx_ab(a, b), index idx_ba(b, a))",
        "create materialized view log on t_concurrent_drop_idx (a, b, c)",
        "create materialized view mv_concurrent_drop_idx (a, b, minc, cnt) refresh fast next now() as select a, b, min(c), count(1) from t_concurrent_drop_idx group by a, b",
    ]);
    let paused_job_id = std::sync::atomic::AtomicI64::new(0);
    let (blocked, resume) = (Latch::default(), Latch::default());
    let _pause = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (blocked, resume) = (blocked.clone(), resume.clone());
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if job.type_ != tidb_model::ActionType::ACTION_DROP_INDEX
                || job.table_name.to_string() != "t_concurrent_drop_idx"
                || job.schema_state != tidb_model::SchemaState::PUBLIC
            {
                return;
            }
            if paused_job_id
                .compare_exchange(0, job.id, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
                .is_err()
            {
                return;
            }
            blocked.open();
            resume.wait();
        }
    });
    struct Resume(Latch);
    impl Drop for Resume {
        fn drop(&mut self) {
            self.0.open();
        }
    }
    let resume = Resume(resume);
    let mut tk1 = new_mview_test_kit(&stack, 2803);
    let first = std::thread::spawn(move || answer(&mut tk1, "drop index idx_ab on t_concurrent_drop_idx"));
    blocked.wait_for("the first drop index job to pause");
    let mut tk2 = new_mview_test_kit(&stack, 2804);
    let second = std::thread::spawn(move || answer(&mut tk2, "drop index idx_ba on t_concurrent_drop_idx"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while single_value(&mut tk, "select count(*) from mysql.tidb_ddl_job") != "2" {
        assert!(std::time::Instant::now() < deadline, "the second drop index job was never queued");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    drop(resume);
    assert_eq!(first.join().unwrap(), "OK");
    assert_min_max_refusal(&second.join().unwrap(), "mv_concurrent_drop_idx");
    must_exec_all(&mut tk, &[
        "insert into t_concurrent_drop_idx values (1, 2, 10), (1, 2, 5)",
        "refresh materialized view mv_concurrent_drop_idx fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, b, minc, cnt from mv_concurrent_drop_idx"), r#"RS:[["1","2","5","2"]]"#);
}

#[test]
fn test_alter_materialized_view_refresh_disable_schedule_updates_alert() {
    for (refresh_failed, expect_alert_row) in [(false, false), (true, true)] {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_alert (a int not null, b int not null)",
            "create materialized view log on t_mv_alert (a, b)",
            "create materialized view mv_alert (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t_mv_alert group by a",
        ]);
        let id = latest_table(&stack, "mv_alert").id;
        insert_alert(&mut tk, id, "mv_alert", if refresh_failed { "'YES'" } else { "NULL" });
        must_exec_all(&mut tk, &["alter materialized view mv_alert refresh"]);
        assert_eq!(
            single_value(&mut tk, &format!("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")),
            "1"
        );
        if !expect_alert_row {
            assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {id}")), "0");
            continue;
        }
        assert_eq!(
            answer(&mut tk, &format!("select ALERT_LEVEL is null, REFRESH_FAILED from mysql.tidb_mview_refresh_alert where MVIEW_ID = {id}")),
            r#"RS:[["1","YES"]]"#
        );
    }
}

#[test]
fn test_alter_materialized_view_refresh_schedule_utc() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set time_zone = '+00:00'",
        "create table t (a int not null, b int not null)",
        "create materialized view log on t (a, b)",
        "create materialized view mv (a, s, cnt) refresh fast next cast('2030-01-01 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a",
    ]);
    let info = |stack: &UnistoreClusterStack| latest_table(stack, "mv").materialized_view.as_ref().expect("a materialized view").read().clone();
    let id = latest_table(&stack, "mv").id;
    let initial = info(&stack);
    must_exec_all(&mut tk, &["set time_zone = '+08:00'", "alter materialized view mv refresh"]);
    let current = info(&stack);
    assert_eq!(current.refresh_schedule_sql_mode, initial.refresh_schedule_sql_mode);
    assert!(current.refresh_next.is_empty());

    must_exec_all(&mut tk, &[
        "set sql_mode = 'PIPES_AS_CONCAT'",
        "alter materialized view mv refresh next cast(date_add('2030-01-01', interval (1 || 2) day) as datetime)",
    ]);
    let current = info(&stack);
    assert_eq!(current.definition_sql_mode, initial.definition_sql_mode);
    assert_eq!(current.refresh_schedule_sql_mode, tk.vars().sql_mode().0);
    assert_eq!(
        single_value(
            &mut tk,
            &format!("select NEXT_REFRESH_UNIX_SECONDS = TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', '2030-01-13 00:00:00') from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")
        ),
        "1"
    );
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "alter materialized view mv refresh next cast('2021-03-14 02:30:00' as datetime)",
    ]);
    assert_eq!(
        single_value(&mut tk, &format!("select NEXT_REFRESH_UNIX_SECONDS = 1615689000 from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")),
        "1"
    );
}

#[test]
fn test_alter_table_with_materialized_view_dependencies() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, -1)",
        "create materialized view log on t (a, b)",
        "create materialized view mv (a, cnt) as select a, count(1) from t group by a",
        "create materialized view mv_where (a, cnt) as select a, count(1) from t where b > 0 group by a",
        "create materialized view mv_out_alias (k, cnt) as select a as k, count(1) from t group by a",
        "create materialized view mv_sum (a, s, cnt) as select a, sum(b), count(1) from t group by a",
        "alter table t modify column a bigint not null",
    ]);
    let jobs: Vec<Vec<Option<String>>> = serde_json::from_str(
        answer(&mut tk, "admin show ddl jobs where JOB_TYPE='modify column'").strip_prefix("RS:").unwrap(),
    )
    .unwrap();
    let history = history_job(&mut tk, jobs[0][0].as_deref().unwrap());
    let want: std::collections::BTreeSet<String> =
        ["test\x00t", "test\x00$mlog$t", "test\x00mv", "test\x00mv_where", "test\x00mv_out_alias", "test\x00mv_sum"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
    assert_eq!(involving_tables(&history), want);
    for (table, column) in [("t", "a"), ("$mlog$t", "a"), ("mv", "a"), ("mv_where", "a"), ("mv_out_alias", "k"), ("mv_sum", "a")] {
        let create = show_create(&mut tk, table);
        assert!(create.contains(&format!("`{column}` bigint")), "{table}: {create}");
    }
    assert_eq!(answer(&mut tk, "select a, cnt from mv order by a"), r#"RS:[["1","1"],["2","1"]]"#);
    assert_eq!(answer(&mut tk, "select a, cnt from mv_where order by a"), r#"RS:[["1","1"]]"#);
    assert_eq!(answer(&mut tk, "select k, cnt from mv_out_alias order by k"), r#"RS:[["1","1"],["2","1"]]"#);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_sum order by a"), r#"RS:[["1","10","1"],["2","-1","1"]]"#);

    exec_err_contains(&mut tk, "alter table t modify column b bigint not null", "does not support modifying columns used in WHERE clause");
    exec_err_contains(&mut tk, "alter table t change column a a2 bigint", "does not support renaming");
    exec_err_contains(&mut tk, "alter table t modify column b smallint", "only supports no-reorg compatible type changes");
    must_exec_all(&mut tk, &["drop materialized view mv_where"]);
    exec_err_contains(
        &mut tk,
        "alter table t modify column b bigint not null",
        "does not support modifying columns used in non-direct SELECT expressions",
    );

    must_exec_all(&mut tk, &[
        "create table t_change_ok (a int not null, b int not null)",
        "insert into t_change_ok values (1, 10), (2, 20)",
        "create materialized view log on t_change_ok (a, b)",
        "create materialized view mv_change_ok (a, cnt) as select a, count(1) from t_change_ok group by a",
        "alter table t_change_ok change column a a bigint not null",
    ]);
    for table in ["t_change_ok", "$mlog$t_change_ok", "mv_change_ok"] {
        let create = show_create(&mut tk, table);
        assert!(create.contains("`a` bigint"), "{table}: {create}");
    }
    assert_eq!(answer(&mut tk, "select a, cnt from mv_change_ok order by a"), r#"RS:[["1","1"],["2","1"]]"#);
}

/// Go `TableInfo.FindIndexByName`.
fn find_index(table: &tidb_model::TableInfo, name: &str) -> Option<tidb_model::IndexInfo> {
    table.indices.iter_deref().map(|index| index.read().clone()).find(|index| index.name.lowercase() == name)
}

fn table_json(stack: &UnistoreClusterStack, name: &str) -> String {
    serde_json::to_string(&latest_table(stack, name)).unwrap()
}

#[test]
fn test_materialized_view_ddl_protects_group_key_index() {
    // (name, not null, composite, clustered, index name, is primary)
    for (name, not_null, composite, clustered, index_name, is_primary) in [
        ("nullable", "", false, "off", "g1", false),
        ("nullable_composite", "", true, "on", "g1", false),
        ("primary_nonclustered", "not null", false, "off", "PRIMARY", true),
        ("primary_int_handle", "not null", false, "on", "PRIMARY", true),
        ("primary_common_handle", "not null", true, "on", "PRIMARY", true),
    ] {
        // t.Run("frontend/"+tc.name)
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            &format!("set tidb_enable_clustered_index={clustered}"),
            &format!("create table t (a int {not_null}, b int not null)"),
            "insert into t values (1, 10), (1, 20), (2, 30)",
        ]);
        if not_null.is_empty() {
            must_exec_all(&mut tk, &["insert into t values (null, 40)"]);
        }
        must_exec_all(&mut tk, &["create materialized view log on t (a, b)"]);
        let (group_by, columns, order_by) = if composite { ("a, b", "g1, g2, cnt", "g1, g2") } else { ("a", "g1, cnt", "g1") };
        must_exec_all(&mut tk, &[
            &format!("create materialized view mv ({columns}) as select {group_by}, count(*) from t group by {group_by}"),
            "create index idx_user on mv (g1)",
        ]);
        let mv = latest_table(&stack, "mv");
        if mv.pk_is_handle {
            assert!(is_primary, "{name}");
        } else {
            let index = find_index(&mv, &index_name.to_lowercase()).unwrap_or_else(|| panic!("{name}: the group key index"));
            assert!(index.unique, "{name}");
            assert_eq!(index.primary, is_primary, "{name}");
            assert!(!index.invisible, "{name}");
        }
        let original = table_json(&stack, "mv");
        let rows = answer(&mut tk, &format!("select * from mv order by {order_by}"));
        let case_changed_name =
            if index_name.to_uppercase() == index_name { index_name.to_lowercase() } else { index_name.to_uppercase() };
        for sql in [
            "drop index `%s` on mv".to_owned(),
            "drop index if exists `%s` on mv".to_owned(),
            "alter table mv drop index `%s`".to_owned(),
            "alter table mv drop index if exists `%s`".to_owned(),
            "alter table mv rename index `%s` to renamed_group_key".to_owned(),
            "alter table mv rename key `%s` to renamed_group_key".to_owned(),
            format!("alter table mv rename index `%s` to `{case_changed_name}`"),
            "alter table mv add index idx_added (cnt), drop index `%s`".to_owned(),
            "alter table mv rename index idx_user to idx_renamed, drop index `%s`".to_owned(),
            "alter table mv rename index `%s` to renamed_group_key, add index idx_added (cnt)".to_owned(),
            "alter table mv alter index `%s` invisible".to_owned(),
            "alter table mv add index idx_added (cnt), alter index `%s` invisible".to_owned(),
            "alter table mv rename index idx_user to idx_renamed, alter index `%s` invisible".to_owned(),
            "alter table mv alter index `%s` invisible, add index idx_added (cnt)".to_owned(),
        ] {
            let sql = sql.replace("%s", index_name);
            exec_err_contains(&mut tk, &sql, &format!("on automatically created index {index_name} of materialized view mv"));
            assert_eq!(table_json(&stack, "mv"), original, "{name}: {sql}");
        }
        if is_primary {
            for sql in ["alter table mv drop primary key", "alter table mv add index idx_added (cnt), drop primary key"] {
                exec_err_contains(&mut tk, sql, "on automatically created index PRIMARY of materialized view mv");
            }
        }
        if !mv.pk_is_handle {
            must_exec_all(&mut tk, &[&format!("alter table mv alter index `{index_name}` visible")]);
        }
        must_exec_all(&mut tk, &["alter table mv alter index idx_user invisible"]);
        assert!(find_index(&latest_table(&stack, "mv"), "idx_user").unwrap().invisible);
        must_exec_all(&mut tk, &["alter table mv alter index idx_user visible"]);
        assert!(!find_index(&latest_table(&stack, "mv"), "idx_user").unwrap().invisible);
        must_exec_all(&mut tk, &[
            "alter table mv rename index idx_user to idx_renamed",
            "alter table mv drop index idx_renamed",
            "create index idx_user on mv (cnt)",
            "drop index idx_user on mv",
            "drop index if exists missing_index on mv",
            "create unique index idx_base on t (b)",
            "alter table t rename index idx_base to idx_base_renamed",
            "drop index idx_base_renamed on t",
        ]);
        assert_eq!(answer(&mut tk, &format!("select * from mv order by {order_by}")), rows, "{name}");
        must_exec_all(&mut tk, &["insert into t values (1, 10)", "refresh materialized view mv fast"]);
        let want = answer(&mut tk, &format!("select {group_by}, count(*) from t group by {group_by} order by {group_by}"));
        assert_eq!(answer(&mut tk, &format!("select * from mv order by {order_by}")), want, "{name}");
    }

    for primary in [false, true] {
        for operation in ["drop", "rename", "invisible"] {
            // t.Run(fmt.Sprintf("owner/primary=%t/operation=%s", primary, operation))
            let (stack, _users, mut tk) = mview_test_kit();
            let (not_null, index_name, mut action) = if primary {
                ("not null", "PRIMARY", tidb_model::ActionType::ACTION_DROP_PRIMARY_KEY)
            } else {
                ("", "g1", tidb_model::ActionType::ACTION_DROP_INDEX)
            };
            must_exec_all(&mut tk, &[
                "set tidb_enable_clustered_index=off",
                &format!("create table t (a int {not_null})"),
                "create materialized view log on t (a)",
                "create materialized view mv (g1, cnt) as select a, count(*) from t group by a",
            ]);
            let mv = latest_table(&stack, "mv");
            let original = table_json(&stack, "mv");
            let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
            let mut index_args = vec![tidb_model::IndexArg {
                index_name: tidb_ast::CiString::new(index_name),
                if_exist: true,
                ..tidb_model::IndexArg::default()
            }];
            if operation == "rename" {
                action = tidb_model::ActionType::ACTION_RENAME_INDEX;
                index_args.push(tidb_model::IndexArg {
                    index_name: tidb_ast::CiString::new("renamed_group_key"),
                    ..tidb_model::IndexArg::default()
                });
            }
            let mut args = tidb_model::JobArgsValue::ModifyIndex(Some(tidb_model::GoShared::new(tidb_model::ModifyIndexArgs {
                index_args: index_args.into(),
                op_type: tidb_model::IndexOp::DROP_INDEX,
                ..tidb_model::ModifyIndexArgs::default()
            })));
            if operation == "invisible" {
                action = tidb_model::ActionType::ACTION_ALTER_INDEX_VISIBILITY;
                args = tidb_model::JobArgsValue::AlterIndexVisibility(Some(tidb_model::GoShared::new(
                    tidb_model::AlterIndexVisibilityArgs {
                        index_name: tidb_model::GoField::new(tidb_ast::CiString::new(index_name)),
                        invisible: tidb_model::GoField::new(true),
                    },
                )));
            }
            let mut job = tidb_model::Job::default();
            job.version = tidb_model::get_job_ver_in_use();
            job.schema_id = schema_id;
            job.schema_name = "test".into();
            job.table_id = mv.id;
            job.table_name = mv.name.lowercase().to_owned().into();
            job.type_ = action;
            job.schema_state = tidb_model::SchemaState::PUBLIC;
            job.binlog_info = Some(tidb_model::GoShared::new(tidb_model::HistoryInfo::default()));
            // "Submit directly so the owner must enforce the constraint
            // without the frontend checks."
            let error = deliver_captured_job(&mut tk, tidb_ddl::JobWrapper { job, args, id_allocated: false })
                .expect_err("the owner refuses");
            assert!(
                error.contains(&format!("on automatically created index {index_name} of materialized view mv")),
                "primary={primary} {operation}: {error}"
            );
            let history = latest_history_job(&mut tk);
            assert_eq!(history.type_, action);
            assert_eq!(history.state, tidb_model::JobState::CANCELLED, "primary={primary} {operation}");
            assert_eq!(table_json(&stack, "mv"), original, "primary={primary} {operation}");
        }
    }
}
