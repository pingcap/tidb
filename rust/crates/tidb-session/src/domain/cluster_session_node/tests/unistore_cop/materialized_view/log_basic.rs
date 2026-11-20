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

//! Go master `pkg/ddl/tests/materializedviewlog/materialized_view_basic_test.go`.

use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Session) {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, tk)
}

fn log_info(stack: &UnistoreClusterStack, base: &str) -> tidb_model::table::MaterializedViewLogInfo {
    latest_table(stack, &format!("$mlog${base}")).materialized_view_log.as_ref().expect("a log").read().clone()
}

fn tracked_columns(info: &tidb_model::table::MaterializedViewLogInfo) -> Vec<String> {
    info.columns.iter().map(|column| column.original().to_owned()).collect()
}

fn column_names(table: &tidb_model::TableInfo) -> Vec<String> {
    table.columns.iter_deref().map(|column| column.read().name.original().to_owned()).collect()
}

fn show_create(tk: &mut Session, table: &str) -> String {
    let rows = answer(tk, &format!("show create table `{table}`"));
    let rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{rows}"))).unwrap();
    rows[0][1].clone()
}

#[test]
fn test_create_materialized_view_log_basic() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int, b int)",
        "create materialized view log on t (a) purge start with cast('2026-01-02 03:04:05' as datetime) next cast('2026-01-02 03:14:05' as datetime) alert rows 1234",
    ]);
    let expected_sql_mode = tk.vars().sql_mode().0;
    assert_eq!(single_value(&mut tk, "select count(*) from information_schema.tables where table_schema='test' and table_name='$mlog$t'"), "1");
    let (base, mlog) = (latest_table(&stack, "t"), latest_table(&stack, "$mlog$t"));
    assert_eq!(mlog_id(&base), mlog.id);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {}", mlog.id)), "1");
    let info = mlog.materialized_view_log.as_ref().unwrap().read().clone();
    assert_eq!(info.base_table_id, base.id);
    assert_eq!(tracked_columns(&info), ["a"]);
    assert_eq!(info.purge_method, "DEFERRED");
    assert_eq!(info.purge_start_with, "CAST('2026-01-02 03:04:05' AS DATETIME)");
    assert_eq!(info.purge_next, "CAST('2026-01-02 03:14:05' AS DATETIME)");
    assert_eq!(info.log_accumulation_alert_rows, Some(1234));
    assert_eq!(info.purge_schedule_sql_mode, expected_sql_mode);
    let dml_type = mlog.columns.iter_deref().find(|column| column.read().name.lowercase() == "_mlog$_dml_type");
    let old_new = mlog.columns.iter_deref().find(|column| column.read().name.lowercase() == "_mlog$_old_new");
    assert!(dml_type.is_some());
    assert_eq!(old_new.expect("_MLOG$_OLD_NEW").read().field_type.code(), tidb_datatype::FieldTypeCode::Tiny);
    assert_eq!(answer(&mut tk, "create materialized view log on t (a)"), "ERR 1050 Table 'test.$mlog$t' already exists");
}

#[test]
fn test_create_materialized_view_log_preserves_text_column_types() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_text_types (id bigint not null primary key, c_tiny tinytext, c_text text, c_medium mediumtext, c_long longtext)",
        "create materialized view log on t_text_types (id, c_tiny, c_text, c_medium, c_long)",
    ]);
    let create = show_create(&mut tk, "$mlog$t_text_types");
    for line in [
        "  `c_tiny` tinytext DEFAULT NULL",
        "  `c_text` text DEFAULT NULL",
        "  `c_medium` mediumtext DEFAULT NULL",
        "  `c_long` longtext DEFAULT NULL",
    ] {
        assert!(create.contains(line), "{line}: {create}");
    }
}

#[test]
fn test_create_materialized_view_log_column_key_flag() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table base_test(id int, v1 int, v2 int, v3 int, v4 int, index k(v1,v2,v3,v4))",
        "create materialized view log on base_test(v1, v2) purge next date_add(now(), interval 1 hour)",
    ]);
    for column in ["v1", "v2"] {
        assert_eq!(
            single_value(
                &mut tk,
                &format!("select column_key from information_schema.columns where table_schema='test' and table_name='$mlog$base_test' and column_name='{column}'")
            ),
            ""
        );
    }
}

#[test]
fn test_create_materialized_view_log_rejects_duplicate_columns() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_dup (id bigint not null primary key, g1 int not null)"]);
    exec_err_contains(&mut tk, "create materialized view log on t_dup (id, id)", "Duplicate column name");
}

#[test]
fn test_create_materialized_view_log_name_length_by_rune() {
    let (_stack, mut tk) = mview_test_kit();
    let prefix = tidb_model::table::MATERIALIZED_VIEW_LOG_TABLE_NAME_PREFIX;
    let max_len = tidb_mysql::consts::MaxTableNameLength;
    let max_base_name_len = max_len - prefix.chars().count();
    let mlog_name = |base: &str| tidb_model::table::materialized_view_log_table_name(&tidb_ast::CiString::new(base)).original().to_owned();
    let ascii_base_name = "t".repeat(max_len);
    let ascii_mlog_name = format!("{prefix}{}", "t".repeat(max_base_name_len));
    assert_eq!(mlog_name(&ascii_base_name), ascii_mlog_name);
    must_exec_all(&mut tk, &[
        &format!("create table `{ascii_base_name}` (id int primary key)"),
        &format!("create materialized view log on `{ascii_base_name}` (id)"),
    ]);
    let lookup = format!("select table_name from information_schema.tables where table_schema='test' and table_name='{ascii_mlog_name}'");
    assert_eq!(answer(&mut tk, &lookup), format!(r#"RS:[["{ascii_mlog_name}"]]"#));
    must_exec_all(&mut tk, &[&format!("insert into `{ascii_base_name}` values (1)")]);
    assert_eq!(
        answer(&mut tk, &format!("select id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `{ascii_mlog_name}`")),
        r#"RS:[["1","I","1"]]"#
    );
    must_exec_all(&mut tk, &[&format!("drop materialized view log on `{ascii_base_name}`")]);
    assert_eq!(answer(&mut tk, &lookup), "RS:[]");

    let max_name = "表".repeat(max_base_name_len);
    let max_mlog_name = mlog_name(&max_name);
    assert_eq!(max_mlog_name.chars().count(), max_len);
    must_exec_all(&mut tk, &[
        &format!("create table `{max_name}` (a int)"),
        &format!("create materialized view log on `{max_name}` (a)"),
    ]);
    let count = format!("select count(*) from information_schema.tables where table_schema='test' and table_name='{max_mlog_name}'");
    assert_eq!(single_value(&mut tk, &count), "1");

    let too_long_name = "表".repeat(max_base_name_len + 1);
    assert_eq!(mlog_name(&too_long_name), max_mlog_name);
    must_exec_all(&mut tk, &[&format!("create table `{too_long_name}` (a int)")]);
    exec_err_contains(&mut tk, &format!("create materialized view log on `{too_long_name}` (a)"), "already exists");
    must_exec_all(&mut tk, &[
        &format!("drop materialized view log on `{max_name}`"),
        &format!("create materialized view log on `{too_long_name}` (a)"),
    ]);
    assert_eq!(single_value(&mut tk, &count), "1");
}

#[test]
fn test_create_materialized_view_log_purge_expr_type_validation() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t (a int, b int)"]);
    // Go `dbterror.ErrGeneralUnsupportedDDL` (8200).
    let error = answer(&mut tk, "create materialized view log on t (a) purge immediate");
    assert!(error.starts_with("ERR 8200 ") && error.contains("PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG"), "{error}");
    exec_err_contains(
        &mut tk,
        "create materialized view log on t (a) purge start with 1 next date_add(now(), interval 1 hour)",
        "PURGE START WITH expression must return DATE/DATETIME/TIMESTAMP",
    );
    exec_err_contains(&mut tk, "create materialized view log on t (a) purge next 600", "PURGE NEXT expression must return DATE/DATETIME/TIMESTAMP");
    must_exec_all(&mut tk, &[
        "create table t_date_schedule (a int)",
        "create materialized view log on t_date_schedule (a) purge next cast('2030-01-02' as date)",
        "create table t_current_date_schedule (a int)",
        "create materialized view log on t_current_date_schedule (a) purge start with current_date next date_add(now(), interval 1 hour)",
        "create materialized view log on t (a) purge start with now() next date_add(now(), interval 1 hour)",
    ]);
}

#[test]
fn test_create_materialized_view_log_meta_column_name_conflict() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_conflict (`_MLOG$_DML_TYPE` int, a int)"]);
    let error = answer(&mut tk, "create materialized view log on t_conflict (`_MLOG$_DML_TYPE`, a)");
    assert!(error.starts_with("ERR 1060 "), "{error}");
}

#[test]
fn test_create_materialized_view_log_reject_unsupported_columns() {
    let (_stack, mut tk) = mview_test_kit();
    for table in ["t_tinyblob", "t_blob", "t_mediumblob", "t_longblob"] {
        must_exec_all(&mut tk, &[&format!("create table {table} (id bigint not null primary key, b {} null)", &table[2..])]);
        exec_err_contains(&mut tk, &format!("create materialized view log on {table} (id, b)"), "CREATE MATERIALIZED VIEW LOG does not support BLOB column b");
    }
    must_exec_all(&mut tk, &[
        "create table t_text_ok (id bigint not null primary key, c1 tinytext null, c2 text null, c3 mediumtext null, c4 longtext null)",
        "create materialized view log on t_text_ok (id, c1, c2, c3, c4)",
        "create table t_json (id bigint not null primary key, j json null)",
    ]);
    exec_err_contains(&mut tk, "create materialized view log on t_json (id, j)", "CREATE MATERIALIZED VIEW LOG does not support JSON column j");
    must_exec_all(&mut tk, &[
        "create table t_gen (id bigint not null primary key, g1 int not null, g_virtual int as (g1 + 1) virtual, g_stored int as (g1 + 2) stored)",
        "create materialized view log on t_gen (id, g_virtual, g_stored)",
        "create table t_untracked_unsupported (id bigint not null primary key, b blob null, j json null, g int as (id + 1) stored)",
        "create materialized view log on t_untracked_unsupported (id)",
    ]);
}

#[test]
fn test_drop_materialized_view_log_before_base_table() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_drop_seq (a int)",
        "create materialized view log on t_drop_seq (a)",
        "drop materialized view log on t_drop_seq",
        "drop table if exists t_drop_seq",
    ]);
}

#[test]
fn test_alter_materialized_view_log_purge_expr_type_validation() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t (a int, b int)", "create materialized view log on t (a) purge next date_add(now(), interval 1 hour)"]);
    exec_err_contains(
        &mut tk,
        "alter materialized view log on t purge start with 1 next date_add(now(), interval 1 hour)",
        "PURGE START WITH expression must return DATE/DATETIME/TIMESTAMP",
    );
    exec_err_contains(&mut tk, "alter materialized view log on t purge next 300", "PURGE NEXT expression must return DATE/DATETIME/TIMESTAMP");
    exec_err_contains(&mut tk, "alter materialized view log on t purge immediate", "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG");
    must_exec_all(&mut tk, &[
        "alter materialized view log on t purge next cast('2030-01-02' as date)",
        "alter materialized view log on t purge start with current_date next date_add(now(), interval 1 hour)",
        "alter materialized view log on t purge start with now() next date_add(now(), interval 1 hour)",
    ]);
}

fn purge_check(tk: &mut Session, mlog_id: i64, columns: &str) -> String {
    answer(tk, &format!("select {columns} from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}"))
}

#[test]
fn test_create_materialized_view_log_purge_info_next_unix_seconds_derivation() {
    let (stack, mut tk) = mview_test_kit();
    let mlog_id = |stack: &UnistoreClusterStack, base: &str| latest_table(stack, &format!("$mlog${base}")).id;
    let between = |low: &str, high: &str| {
        format!(
            "NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {low}), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {high})"
        )
    };
    must_exec_all(&mut tk, &[
        "create table t_purge_start_only (a int)",
        "create materialized view log on t_purge_start_only (a) purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)",
    ]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_start_only"), &between("30 minute", "2 hour")), r#"RS:[["1","1","1"]]"#);
    must_exec_all(&mut tk, &[
        "create table t_purge_next_only (a int)",
        "create materialized view log on t_purge_next_only (a) purge next date_add(now(), interval 20 minute)",
    ]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_next_only"), &between("10 minute", "1 hour")), r#"RS:[["1","1","1"]]"#);
    must_exec_all(&mut tk, &["create table t_purge_no_schedule (a int)", "create materialized view log on t_purge_no_schedule (a)"]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_no_schedule"), "NEXT_PURGE_UNIX_SECONDS is null"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &[
        "create table t_purge_near_now (a int)",
        "create materialized view log on t_purge_near_now (a) purge start with now() next date_add(now(), interval 40 minute)",
    ]);
    assert_eq!(purge_check(&mut tk, mlog_id(&stack, "t_purge_near_now"), &between("20 minute", "2 hour")), r#"RS:[["1","1","1"]]"#);
    must_exec_all(&mut tk, &[
        "set time_zone = '+08:00'",
        "create table t_purge_date (a int)",
        "create materialized view log on t_purge_date (a) purge next cast('2030-01-02' as date)",
    ]);
    assert_eq!(
        purge_check(&mut tk, mlog_id(&stack, "t_purge_date"), "NEXT_PURGE_UNIX_SECONDS = TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', '2030-01-02 00:00:00')"),
        r#"RS:[["1"]]"#
    );
}

#[test]
fn test_create_materialized_view_log_reject_non_base_object() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int)",
        "create view v as select a from t",
        "create sequence s",
        "create global temporary table gt (a int) on commit delete rows",
        "create materialized view log on t (a)",
        "create table t_mv_base (a int not null, b int not null)",
        "create materialized view log on t_mv_base (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, cnt) refresh fast as select a, count(1) from t_mv_base group by a",
    ]);
    for sql in [
        "create materialized view log on v (a)",
        "create materialized view log on s (a)",
        "create materialized view log on gt (a)",
        "create materialized view log on mysql.user (User)",
    ] {
        exec_err_contains(&mut tk, sql, "is not BASE TABLE");
    }
    // Go's testkit session has no user, so its privilege check passes and
    // the executor refuses the object; a logged-in user (as here) is refused
    // CREATE VIEW on information_schema first (Go `RequestVerification`'s
    // `IsMemDB` rule).
    exec_err_contains(
        &mut tk,
        "create materialized view log on information_schema.tables (TABLE_SCHEMA)",
        "CREATE MATERIALIZED VIEW LOG command denied",
    );
    must_exec_all(&mut tk, &[
        "create materialized view log on mv (a, cnt)",
        "create table t_mlog_base (a int)",
        "create materialized view log on t_mlog_base (a)",
    ]);
    exec_err_contains(&mut tk, "create materialized view log on `$mlog$t_mlog_base` (a)", "is not BASE TABLE");
}

#[test]
fn test_create_materialized_view_log_updates_placement_bundle() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create placement policy mlog_p followers=1",
        "alter database test placement policy mlog_p",
        "create table t_placement (a int)",
        "create materialized view log on t_placement (a)",
    ]);
    let placement = answer(&mut tk, "show placement for table `$mlog$t_placement`");
    assert!(placement.contains("TABLE test.$mlog$t_placement"), "{placement}");
    assert!(placement.contains("FOLLOWERS=1"), "{placement}");
}

#[test]
fn test_create_materialized_view_log_allows_generated_columns() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "CREATE TABLE t_gen (id BIGINT NOT NULL PRIMARY KEY, base BIGINT NOT NULL, gv BIGINT AS (base + 1) VIRTUAL, gs BIGINT AS (base + 2) STORED)",
        "CREATE MATERIALIZED VIEW LOG ON t_gen (id, gv, gs)",
    ]);
    assert_eq!(
        answer(&mut tk, "select column_name from information_schema.columns where table_schema='test' and table_name='$mlog$t_gen' order by ordinal_position"),
        r#"RS:[["id"],["gv"],["gs"],["_MLOG$_DML_TYPE"],["_MLOG$_OLD_NEW"]]"#
    );
    assert_eq!(tracked_columns(&log_info(&stack, "t_gen")), ["id", "gv", "gs"]);
}

#[test]
fn test_create_materialized_view_log_accumulation_alert() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_alert_default (a int)",
        "create table t_alert_zero (a int)",
        "create table t_alert_custom (a int)",
        "create table t_alert_negative (a int)",
    ]);
    exec_err_contains(&mut tk, "create materialized view log on t_alert_negative (a) alert rows -1", "invalid ALERT ROWS value: -1 (must be non-negative)");
    must_exec_all(&mut tk, &[
        "create materialized view log on t_alert_default (a)",
        "create materialized view log on t_alert_zero (a) alert rows 0",
        "create materialized view log on t_alert_custom (a) alert rows 2048",
    ]);
    let default = log_info(&stack, "t_alert_default");
    assert_eq!(default.log_accumulation_alert_rows, None);
    assert_eq!(default.effective_log_accumulation_alert_rows(), (0, false));
    let zero = log_info(&stack, "t_alert_zero");
    assert_eq!(zero.log_accumulation_alert_rows, Some(0));
    assert_eq!(zero.effective_log_accumulation_alert_rows(), (0, false));
    let custom = log_info(&stack, "t_alert_custom");
    assert_eq!(custom.log_accumulation_alert_rows, Some(2048));
    assert_eq!(custom.effective_log_accumulation_alert_rows(), (2048, true));
}

#[test]
fn test_drop_materialized_view_log_removes_purge_state() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_drop_mlog_purge_state (a int)", "create materialized view log on t_drop_mlog_purge_state (a)"]);
    let id = latest_table(&stack, "$mlog$t_drop_mlog_purge_state").id;
    let count = format!("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = {id}");
    assert_eq!(single_value(&mut tk, &count), "1");
    must_exec_all(&mut tk, &["drop materialized view log on t_drop_mlog_purge_state"]);
    assert_eq!(single_value(&mut tk, &count), "0");
}

#[test]
fn test_alter_materialized_view_log_ddl() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b varchar(10) not null, c int)",
        "create materialized view log on t (a) purge next date_add(now(), interval 2 hour)",
        "alter materialized view log on t add column (b, c)",
    ]);
    let mlog = latest_table(&stack, "$mlog$t");
    assert_eq!(tracked_columns(&log_info(&stack, "t")), ["a", "b", "c"]);
    assert_eq!(column_names(&mlog), ["a", "b", "c", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"]);
    exec_err_contains(&mut tk, "alter materialized view log on t add column (b)", "Duplicate column name");
    exec_err_contains(&mut tk, "alter materialized view log on t add column (missing_col)", "Unknown column");
    must_exec_all(&mut tk, &["alter materialized view log on t purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)"]);
    let info = log_info(&stack, "t");
    assert_eq!(info.purge_method, "DEFERRED");
    assert_eq!(info.purge_start_with, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)");
    assert_eq!(info.purge_next, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)");
    assert_eq!(purge_check(&mut tk, mlog.id, "NEXT_PURGE_UNIX_SECONDS is not null"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &["alter materialized view log on t purge"]);
    let info = log_info(&stack, "t");
    assert!(info.purge_start_with.is_empty() && info.purge_next.is_empty());
    assert_eq!(purge_check(&mut tk, mlog.id, "NEXT_PURGE_UNIX_SECONDS is null"), r#"RS:[["1"]]"#);
}

#[test]
fn test_alter_materialized_view_log_purge_schedule_utc() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set time_zone = '+00:00'",
        "create table t (a int)",
        "create materialized view log on t (a) purge next cast('2030-01-01 10:00:00' as datetime)",
    ]);
    let id = latest_table(&stack, "$mlog$t").id;
    let initial = log_info(&stack, "t").purge_schedule_sql_mode;
    must_exec_all(&mut tk, &["set time_zone = '+08:00'", "alter materialized view log on t purge"]);
    let info = log_info(&stack, "t");
    assert_eq!(info.purge_schedule_sql_mode, initial);
    assert!(info.purge_next.is_empty());
    must_exec_all(&mut tk, &[
        "set sql_mode = 'PIPES_AS_CONCAT'",
        "alter materialized view log on t purge next cast(date_add('2030-01-01', interval (1 || 2) day) as datetime)",
    ]);
    assert_eq!(log_info(&stack, "t").purge_schedule_sql_mode, tk.vars().sql_mode().0);
    assert_eq!(
        purge_check(&mut tk, id, "NEXT_PURGE_UNIX_SECONDS = TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', '2030-01-13 00:00:00')"),
        r#"RS:[["1"]]"#
    );
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "alter materialized view log on t purge next cast('2021-03-14 02:30:00' as datetime)",
    ]);
    assert_eq!(purge_check(&mut tk, id, "NEXT_PURGE_UNIX_SECONDS = 1615689000"), r#"RS:[["1"]]"#);
}

#[test]
fn test_alter_materialized_view_log_add_column_rejects_invalid_columns() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_add_mlog_invalid (a int, b int, c int)", "create materialized view log on t_add_mlog_invalid (a)"]);
    for (sql, code) in [
        ("alter materialized view log on t_add_mlog_invalid add column (a)", "ERR 1060 "),
        ("alter materialized view log on t_add_mlog_invalid add column (b, b)", "ERR 1060 "),
        ("alter materialized view log on t_add_mlog_invalid add column (missing_col)", "ERR 1054 "),
        ("alter materialized view log on t_add_mlog_invalid add column (`_MLOG$_DML_TYPE`)", "ERR 1060 "),
    ] {
        let answer = answer(&mut tk, sql);
        assert!(answer.starts_with(code), "{sql}: {answer}");
    }
    assert_eq!(
        answer(&mut tk, "alter materialized view log on t_add_mlog_invalid add column (b), add column (c)"),
        "ERR 8200 Unsupported ALTER MATERIALIZED VIEW LOG with multiple ADD COLUMN actions"
    );
    must_exec_all(&mut tk, &[
        "create table t_add_mlog_unsupported (id int, b blob, j json, g1 int, g2 int as (g1 + 1) stored)",
        "create materialized view log on t_add_mlog_unsupported (id)",
    ]);
    exec_err_contains(&mut tk, "alter materialized view log on t_add_mlog_unsupported add column (b)", "ALTER MATERIALIZED VIEW LOG does not support BLOB column b");
    exec_err_contains(&mut tk, "alter materialized view log on t_add_mlog_unsupported add column (j)", "ALTER MATERIALIZED VIEW LOG does not support JSON column j");
    must_exec_all(&mut tk, &[
        "insert into t_add_mlog_unsupported (id, g1) values (1, 10)",
        "alter materialized view log on t_add_mlog_unsupported add column (g2)",
        "update t_add_mlog_unsupported set g1 = 20 where id = 1",
    ]);
    let rows = answer(&mut tk, "select id, g2, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_add_mlog_unsupported` where `_MLOG$_DML_TYPE` = 'U'");
    let mut rows: Vec<Vec<String>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap()).unwrap();
    rows.sort();
    assert_eq!(rows, vec![vec!["1", "11", "U", "-1"], vec!["1", "21", "U", "1"]]);
}

#[test]
fn test_alter_materialized_view_log_add_column_copies_base_column_metadata() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_add_mlog_col (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null)",
        "create materialized view log on t_add_mlog_col (id)",
        "alter materialized view log on t_add_mlog_col add column (n, s, d, note)",
    ]);
    assert_eq!(tracked_columns(&log_info(&stack, "t_add_mlog_col")), ["id", "n", "s", "d", "note"]);
    let mlog = latest_table(&stack, "$mlog$t_add_mlog_col");
    let column = |name: &str| mlog.columns.iter_deref().find(|column| column.read().name.lowercase() == name).unwrap().read().clone();
    for name in ["n", "s", "d", "note"] {
        assert!(tidb_mysql::has_not_null_flag(column(name).field_type.flags() as usize), "{name}");
    }
    assert_eq!(column("note").field_type.code(), tidb_datatype::FieldTypeCode::Blob);
}
