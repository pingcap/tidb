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

//! Go master `pkg/ddl/tests/materializedview/materialized_view_basic_test.go`.

use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Session) {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, tk)
}

fn id_of(stack: &UnistoreClusterStack, name: &str) -> i64 {
    latest_table(stack, name).id
}

fn mview_info(stack: &UnistoreClusterStack, name: &str) -> tidb_model::table::MaterializedViewInfo {
    latest_table(stack, name).materialized_view.as_ref().expect("a materialized view").read().clone()
}

#[test]
fn test_mview_refresh_source_info_lifecycle() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_sources (a int not null, b int not null)",
        "insert into t_sources values (1, 10), (2, 20)",
        "create materialized view log on t_sources (a, b)",
        "create materialized view mv_sources (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_sources group by a",
        "create materialized view mv_sources_peer (a, cnt) refresh fast as select a, count(*) from t_sources group by a",
    ]);
    let (mv_id, peer_id, mlog_id) = (id_of(&stack, "mv_sources"), id_of(&stack, "mv_sources_peer"), id_of(&stack, "$mlog$t_sources"));
    assert_eq!(
        answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID"),
        format!(r#"RS:[["{mv_id}","{mlog_id}"],["{peer_id}","{mlog_id}"]]"#)
    );
    must_exec_all(&mut tk, &["drop materialized view mv_sources"]);
    assert_eq!(
        answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog)"),
        format!(r#"RS:[["{peer_id}","{mlog_id}"]]"#)
    );
    must_exec_all(&mut tk, &["drop materialized view mv_sources_peer"]);
    assert_eq!(single_value(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), "0");
}

#[test]
fn test_create_materialized_view_validation_coverage() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["set time_zone = '+08:00'", "create table t_mv_validation (a int not null, b int not null)"]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_no_log (a, cnt) as select a, count(1) from t_mv_validation group by a",
        "materialized view log does not exist",
    );
    must_exec_all(&mut tk, &["insert into t_mv_validation values (1, 10), (1, 5), (2, 7)", "create materialized view log on t_mv_validation (a, b)"]);
    exec_err_contains(&mut tk, "create materialized view mv_no_count (a, s) as select a, sum(b) from t_mv_validation group by a", "must contain count(*)/count(1)");
    exec_err_contains(
        &mut tk,
        "create materialized view mv_avg (a, avgv, cnt) as select a, avg(b), count(1) from t_mv_validation group by a",
        "unsupported aggregate function",
    );
    exec_err_contains(
        &mut tk,
        "create materialized view mv_rand (a, cnt) as select a, count(1) from t_mv_validation where rand() > 0 group by a",
        "WHERE clause must be deterministic",
    );

    // "COUNT(column) is supported when the required COUNT(*)/COUNT(1) is also
    // present."
    must_exec_all(&mut tk, &["create materialized view mv_count_column (a, cnt_b, cnt) as select a, count(b), count(1) from t_mv_validation group by a"]);
    assert_eq!(answer(&mut tk, "select a, cnt_b, cnt from mv_count_column order by a"), r#"RS:[["1","2","2"],["2","1","1"]]"#);

    let info = mview_info(&stack, "mv_count_column");
    assert_eq!(info.base_table_ids.iter().copied().collect::<Vec<_>>(), vec![id_of(&stack, "t_mv_validation")]);
    assert_eq!(info.refresh_method, "FAST");
    assert!(info.refresh_start_with.is_empty());
    assert!(info.refresh_next.is_empty());
    assert_eq!((info.alert_warning_sec, info.alert_overdue_sec, info.alert_refresh_failed), (0, 0, false));
    assert_eq!(info.definition_sql_mode, tk.vars().sql_mode().0);
    assert_eq!(info.refresh_schedule_sql_mode, tk.vars().sql_mode().0);
    assert!(info.definition_time_zone.name.is_empty());
    assert_eq!(info.definition_time_zone.offset, 8 * 60 * 60);

    // "Aggregate function names are case-insensitive."
    must_exec_all(&mut tk, &["create materialized view mv_upper_aggregate (a, s, cnt) as select a, SUM(b), COUNT(1) from t_mv_validation group by a"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_upper_aggregate order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#);
    must_exec_all(&mut tk, &[
        "create materialized view mv_alias (a, cnt) as select x.a, count(1) from t_mv_validation x where x.b > 0 group by x.a",
    ]);
    assert_eq!(answer(&mut tk, "select a, cnt from mv_alias order by a"), r#"RS:[["1","2"],["2","1"]]"#);
    must_exec_all(&mut tk, &["create materialized view mv_out_alias (k, cnt) as select a as k, count(1) from t_mv_validation group by a"]);
    assert_eq!(answer(&mut tk, "select k, cnt from mv_out_alias order by k"), r#"RS:[["1","2"],["2","1"]]"#);

    must_exec_all(&mut tk, &[
        "create table t_mv_sum_time (a int not null, d date not null default '2020-01-01', dt datetime not null default '2020-01-01 00:00:00', ts timestamp not null default '2020-01-01 00:00:00', dur time not null default '00:00:00')",
        "create materialized view log on t_mv_sum_time (a, d, dt, ts, dur)",
    ]);
    for col in ["d", "dt", "ts", "dur"] {
        exec_err_contains(
            &mut tk,
            &format!("create materialized view mv_sum_{col} (a, s, cnt) as select a, sum({col}), count(1) from t_mv_sum_time group by a"),
            "does not support SUM on DATE/DATETIME/TIMESTAMP/TIME column",
        );
    }

    // "MIN/MAX still require a supporting base-table index, regardless of
    // function case."
    must_exec_all(&mut tk, &[
        "create table t_mv_minmax_validation (a int not null, b int not null, c int not null, index idx_cab(c, a, b))",
        "create materialized view log on t_mv_minmax_validation (a, b, c)",
    ]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_upper_min (a, b, minc, cnt) as select a, b, MIN(c), COUNT(1) from t_mv_minmax_validation group by a, b",
        "requires base table index whose leading columns cover all GROUP BY columns",
    );

    // "Every referenced column must be present in the materialized view log."
    must_exec_all(&mut tk, &["create table t_mv_missing_column (a int not null, b int not null)", "create materialized view log on t_mv_missing_column (a)"]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_missing_count_column (a, cnt_b, cnt) as select a, count(b), count(1) from t_mv_missing_column group by a",
        "does not contain column b",
    );
}

#[test]
fn test_create_materialized_view_nullable_aggregate_validation() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_mv_nullable_sum (a int not null, b int)", "create materialized view log on t_mv_nullable_sum (a, b)"]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_nullable_sum_bad (a, s, cnt) as select a, sum(b), count(1) from t_mv_nullable_sum group by a",
        "requires matching COUNT",
    );
    must_exec_all(&mut tk, &[
        "create materialized view mv_nullable_sum (a, s, cnt_b, cnt) as select a, sum(b), count(b), count(1) from t_mv_nullable_sum group by a",
        "create materialized view mv_nullable_sum_duplicate_count (a, s, cnt_b1, cnt_b2, cnt) as select a, sum(b), count(b) as cnt_b1, count(b) as cnt_b2, count(1) from t_mv_nullable_sum group by a",
        "create table t_mv_nullable_minmax (a int not null, b int, index idx_ab(a, b))",
        "create materialized view log on t_mv_nullable_minmax (a, b)",
        "create materialized view mv_nullable_minmax (a, minb, maxb, cnt) as select a, min(b), max(b), count(1) from t_mv_nullable_minmax group by a",
    ]);
}

#[test]
fn test_materialized_view_base_set_ti_flash_replica() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_tiflash (a int not null, b int not null)",
        "create materialized view log on t_mv_tiflash (a, b)",
        "create materialized view mv_tiflash (a, cnt) as select a, count(1) from t_mv_tiflash group by a",
    ]);
    // "TiFlash availability is environment-dependent in mock storage. The MV
    // dependency guard must not reject this operation."
    let result = answer(&mut tk, "alter table t_mv_tiflash set tiflash replica 1");
    assert!(!result.contains("ALTER TABLE on base table with materialized view dependencies"), "{result}");
}

#[test]
fn test_materialized_view_comment_only_base_column_modify() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_comment (a int not null, b int not null)",
        "create materialized view log on t_mv_comment (a, b)",
        "create materialized view mv_comment (a, cnt) as select a, count(1) from t_mv_comment group by a",
        "alter table t_mv_comment modify column b int not null comment 'comment-only change'",
    ]);
    assert_eq!(
        single_value(
            &mut tk,
            "select column_comment from information_schema.columns where table_schema = 'test' and table_name = 't_mv_comment' and column_name = 'b'"
        ),
        "comment-only change"
    );
    must_exec_all(&mut tk, &["alter table t_mv_comment modify column b int not null comment 'comment-only change'"]);
}

#[test]
fn test_create_materialized_view_column_flags() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table base_mv_flags(id bigint not null auto_increment primary key, g1 int not null, v1 bigint not null, key idx_g1_id(g1, id))",
        "create materialized view log on base_mv_flags(id, g1, v1) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_flags (g1, cnt, s_v1, min_id, max_id) as select g1, count(1), sum(v1), min(id), max(id) from base_mv_flags group by g1",
    ]);
    for col in ["min_id", "max_id"] {
        for what in ["column_key", "extra"] {
            assert_eq!(
                single_value(
                    &mut tk,
                    &format!("select {what} from information_schema.columns where table_schema='test' and table_name='mv_flags' and column_name='{col}'")
                ),
                "",
                "{col} {what}"
            );
        }
    }
}

#[test]
fn test_materialized_view_comment_length() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_comment_len (id bigint not null primary key, g1 int not null, v1 bigint not null, key idx_g1(g1))",
        "create materialized view log on t_mv_comment_len (id, g1, v1)",
    ]);
    // Go `ddl.MaxCommentLength * 2`.
    const MAX_TABLE_COMMENT_LENGTH: usize = 1024 * 2;
    let comment_max_len = "y".repeat(MAX_TABLE_COMMENT_LENGTH);
    let comment_too_long = "y".repeat(MAX_TABLE_COMMENT_LENGTH + 1);
    let create_mv_sql = |name: &str, comment: &str| {
        format!("create materialized view {name} (g1, cnt) comment = '{comment}' refresh fast as select g1, count(1) from t_mv_comment_len group by g1")
    };
    let too_long = |name: &str| format!("Comment for table '{name}' is too long (max = {MAX_TABLE_COMMENT_LENGTH})");
    let comment_length = |tk: &mut Session, name: &str| {
        single_value(tk, &format!("select length(table_comment) from information_schema.tables where table_schema = 'test' and table_name = '{name}'"))
    };

    must_exec_all(&mut tk, &["set @@sql_mode='STRICT_TRANS_TABLES'", &create_mv_sql("mv_comment_max", &comment_max_len)]);
    exec_err_contains(&mut tk, &create_mv_sql("mv_comment_too_long", &comment_too_long), &too_long("mv_comment_too_long"));
    must_exec_all(&mut tk, &[
        &create_mv_sql("mv_alter_comment", ""),
        &format!("alter materialized view mv_alter_comment comment = '{comment_max_len}'"),
    ]);
    assert_eq!(comment_length(&mut tk, "mv_alter_comment"), MAX_TABLE_COMMENT_LENGTH.to_string());
    exec_err_contains(&mut tk, &format!("alter materialized view mv_alter_comment comment = '{comment_too_long}'"), &too_long("mv_alter_comment"));

    must_exec_all(&mut tk, &["set @@sql_mode=''", &create_mv_sql("mv_comment_truncated", &comment_too_long)]);
    assert_eq!(
        answer(&mut tk, "show warnings"),
        format!(r#"RS:[["Warning","1628","{}"]]"#, too_long("mv_comment_truncated"))
    );
    assert_eq!(comment_length(&mut tk, "mv_comment_truncated"), MAX_TABLE_COMMENT_LENGTH.to_string());
    must_exec_all(&mut tk, &[
        &create_mv_sql("mv_alter_comment_truncated", ""),
        &format!("alter materialized view mv_alter_comment_truncated comment = '{comment_too_long}'"),
    ]);
    assert_eq!(
        answer(&mut tk, "show warnings"),
        format!(r#"RS:[["Warning","1628","{}"]]"#, too_long("mv_alter_comment_truncated"))
    );
    assert_eq!(comment_length(&mut tk, "mv_alter_comment_truncated"), MAX_TABLE_COMMENT_LENGTH.to_string());
}

#[test]
fn test_create_materialized_view_refresh_expr_type_validation() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    exec_err_contains(
        &mut tk,
        "create materialized view mv_bad_next (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a",
        "REFRESH NEXT expression must return DATE/DATETIME/TIMESTAMP",
    );
    exec_err_contains(
        &mut tk,
        "create materialized view mv_bad_start (a, s, cnt) refresh fast start with 1 next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        "REFRESH START WITH expression must return DATE/DATETIME/TIMESTAMP",
    );
    must_exec_all(&mut tk, &[
        "create materialized view mv_date_schedule (a, s, cnt) refresh fast next cast('2030-01-02' as date) as select a, sum(b), count(1) from t group by a",
        "create materialized view mv_current_date_schedule (a, s, cnt) refresh fast start with current_date next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        "create materialized view mv_ok (a, s, cnt) refresh fast start with now() next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
}

#[test]
fn test_drop_materialized_view_when_disabled() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_drop_when_disabled (a int not null)",
        "create materialized view log on t_drop_when_disabled (a)",
        "create materialized view mv_drop_when_disabled (a, cnt) as select a, count(1) from t_drop_when_disabled group by a",
        "set tidb_mview_enable = off",
        "drop materialized view mv_drop_when_disabled",
        "drop materialized view log on t_drop_when_disabled",
    ]);
}

#[test]
fn test_drop_materialized_view_if_exists() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["drop materialized view if exists missing_mv", "drop materialized view if exists missing_schema.missing_mv"]);
    exec_err_contains(&mut tk, "drop materialized view log if exists on missing_base", "Table 'test.missing_base' doesn't exist");
    exec_err_contains(
        &mut tk,
        "drop materialized view log if exists on missing_schema.missing_base",
        "Table 'missing_schema.missing_base' doesn't exist",
    );
    must_exec_all(&mut tk, &[
        "create table t_drop_if_exists (a int not null)",
        "create table t_no_mlog_drop_if_exists (a int not null)",
        "create materialized view log on t_drop_if_exists (a)",
        "create materialized view mv_drop_if_exists (a, cnt) as select a, count(1) from t_drop_if_exists group by a",
    ]);
    exec_err_contains(&mut tk, "drop materialized view if exists t_drop_if_exists", "is not MATERIALIZED VIEW");
    must_exec_all(&mut tk, &["drop materialized view log if exists on mv_drop_if_exists"]);
    assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1051","Unknown table 'test.mv_drop_if_exists'"]]"#);
    must_exec_all(&mut tk, &[
        "drop materialized view if exists mv_drop_if_exists",
        "drop materialized view if exists mv_drop_if_exists",
        "create view v_drop_if_exists as select * from t_drop_if_exists",
    ]);
    exec_err_contains(&mut tk, "drop materialized view log if exists on v_drop_if_exists", "is not BASE TABLE");
    must_exec_all(&mut tk, &["drop materialized view log if exists on t_no_mlog_drop_if_exists"]);
    assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1051","Unknown table 'test.t_no_mlog_drop_if_exists'"]]"#);
    must_exec_all(&mut tk, &[
        "drop materialized view log if exists on t_drop_if_exists",
        "drop materialized view log if exists on t_drop_if_exists",
    ]);
}

#[test]
fn test_alter_materialized_view_refresh_expr_type_validation() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "create materialized view log on t (a, b)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    exec_err_contains(&mut tk, "alter materialized view mv refresh next 300", "REFRESH NEXT expression must return DATE/DATETIME/TIMESTAMP");
    exec_err_contains(
        &mut tk,
        "alter materialized view mv refresh start with 1 next date_add(now(), interval 1 hour)",
        "REFRESH START WITH expression must return DATE/DATETIME/TIMESTAMP",
    );
    must_exec_all(&mut tk, &[
        "alter materialized view mv refresh next cast('2030-01-02' as date)",
        "alter materialized view mv refresh start with current_date next date_add(now(), interval 1 hour)",
        "alter materialized view mv refresh start with now() next date_add(now(), interval 1 hour)",
    ]);
}

#[test]
fn test_create_materialized_view_rejects_unsupported_select_clauses() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t (a int not null, b int not null)", "create materialized view log on t (a, b)"]);
    for (sql, err_part) in [
        ("create materialized view mv_distinct (a, cnt) as select distinct a, count(1) from t group by a", "does not support SELECT DISTINCT"),
        ("create materialized view mv_having (a, cnt) as select a, count(1) from t group by a having count(1) > 0", "does not support HAVING clause"),
        ("create materialized view mv_order (a, cnt) as select a, count(1) from t group by a order by a", "does not support ORDER BY clause"),
        ("create materialized view mv_limit (a, cnt) as select a, count(1) from t group by a limit 1", "does not support LIMIT clause"),
        ("create materialized view mv_rollup (a, cnt) as select a, count(1) from t group by a with rollup", "does not support GROUP BY WITH ROLLUP"),
        (
            "create materialized view mv_cte (a, s, cnt) as with cte as (select a from t) select a, sum(b), count(1) from t group by a",
            "common table expressions",
        ),
        ("create materialized view mv_lock (a, s, cnt) as select a, sum(b), count(1) from t group by a for update", "locking clauses"),
        (
            "create materialized view mv_into (a, s, cnt) as select a, sum(b), count(1) from t group by a into outfile '/tmp/mv.out'",
            "SELECT INTO",
        ),
        ("create materialized view mv_as_of (a, s, cnt) as select a, sum(b), count(1) from t as of timestamp now() group by a", "AS OF"),
        (
            "create materialized view mv_sample (a, s, cnt) as select a, sum(b), count(1) from t tablesample system (50) group by a",
            "TABLESAMPLE",
        ),
    ] {
        exec_err_contains(&mut tk, sql, err_part);
    }
}

#[test]
fn test_create_table_like_should_not_carry_materialized_view_metadata() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_src (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        "create table t_like like t",
        "create table mv_like like mv_src",
        "create table mlog_like like `$mlog$t`",
    ]);
    assert!(latest_table(&stack, "mv_src").materialized_view.is_some());
    assert!(latest_table(&stack, "$mlog$t").materialized_view_log.is_some());
    for name in ["mv_like", "mlog_like"] {
        let table = latest_table(&stack, name);
        assert!(table.materialized_view.is_none(), "{name}");
        assert!(table.materialized_view_log.is_none(), "{name}");
        assert!(table.materialized_view_base.is_none(), "{name}");
    }
    let base = latest_table(&stack, "t");
    assert_eq!(mlog_id(&base), id_of(&stack, "$mlog$t"));
    assert_eq!(mview_ids(&base), vec![id_of(&stack, "mv_src")]);
}

fn next_refresh_check(tk: &mut Session, id: i64, columns: &str) -> String {
    answer(tk, &format!("select {columns} from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}"))
}

#[test]
fn test_create_materialized_view_refresh_info_next_unix_seconds_derivation() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a",
    ]);
    let between = |low: &str, high: &str| {
        format!(
            "NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {low}), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {high})"
        )
    };
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_start_only"), &between("30 minute", "2 hour")), r#"RS:[["1","1","1"]]"#);
    must_exec_all(&mut tk, &[
        "create materialized view mv_next_only (a, s, cnt) refresh fast next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_next_only"), &between("10 minute", "1 hour")), r#"RS:[["1","1","1"]]"#);
    must_exec_all(&mut tk, &["create materialized view mv_no_schedule (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a"]);
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_no_schedule"), "NEXT_REFRESH_UNIX_SECONDS is null"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &[
        "create materialized view mv_near_now (a, s, cnt) refresh fast start with now() next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_near_now"), &between("20 minute", "2 hour")), r#"RS:[["1","1","1"]]"#);
}

#[test]
fn test_create_materialized_view_refresh_info_next_unix_seconds_uses_utc() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set time_zone = '+08:00'",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_schedule_next (a, s, cnt) refresh fast next cast('2030-01-02 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(
        next_refresh_check(
            &mut tk,
            id_of(&stack, "mv_schedule_next"),
            "NEXT_REFRESH_UNIX_SECONDS = 1893578400, NEXT_REFRESH_UNIX_SECONDS = 1893549600"
        ),
        r#"RS:[["1","0"]]"#
    );
    must_exec_all(&mut tk, &[
        "create materialized view mv_schedule_start (a, s, cnt) refresh fast start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(
        next_refresh_check(
            &mut tk,
            id_of(&stack, "mv_schedule_start"),
            "NEXT_REFRESH_UNIX_SECONDS = 1893578400, NEXT_REFRESH_UNIX_SECONDS = 1893636000"
        ),
        r#"RS:[["1","0"]]"#
    );
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "create materialized view mv_dst_gap (a, s, cnt) refresh fast next cast('2021-03-14 02:30:00' as datetime) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_dst_gap"), "NEXT_REFRESH_UNIX_SECONDS = 1615689000"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &[
        "create materialized view mv_utc_now (a, s, cnt) refresh fast next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(
        next_refresh_check(
            &mut tk,
            id_of(&stack, "mv_utc_now"),
            "NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 50 minute)"
        ),
        r#"RS:[["1","1"]]"#
    );
    let _now = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/expression/injectNow", "return(1636275480)");
    must_exec_all(&mut tk, &[
        "set time_zone = 'America/Los_Angeles'",
        "create materialized view mv_dst_fallback (a, s, cnt) refresh fast next date_add(now(), interval 5 minute) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(next_refresh_check(&mut tk, id_of(&stack, "mv_dst_fallback"), "NEXT_REFRESH_UNIX_SECONDS = 1636275780"), r#"RS:[["1"]]"#);
}

/// Go `getLastSuccessReadTSO`.
fn get_last_success_read_tso(tk: &mut Session, mv_id: i64) -> (u64, bool) {
    let rows = answer(tk, &format!("select LAST_SUCCESS_READ_TSO is null, LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {mv_id}"));
    let rows: Vec<Vec<Option<String>>> = serde_json::from_str(rows.strip_prefix("RS:").unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    if rows[0][0].as_deref() == Some("1") {
        return (0, true);
    }
    (rows[0][1].as_deref().unwrap().parse().unwrap(), false)
}

/// The test's shadow of `view`: Go `ddl.BuildTableInfoWithLike` with the
/// shadow marker, created by `CreateMaterializedViewShadowTable`; its ID.
fn create_shadow_like(stack: &UnistoreClusterStack, tk: &mut Session, view: &str, shadow_name: &str) -> i64 {
    let mview = latest_table(stack, view);
    let mut shadow = tidb_ddl_build::table_info_build::build_table_info_with_like(
        "test",
        shadow_name,
        &mview,
        tidb_ast::CreateTableTemporary::None,
        false,
    )
    .unwrap();
    shadow.materialized_view_shadow = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewShadowInfo {
        source_mview_id: mview.id,
        ..Default::default()
    }));
    create_shadow_table(stack, tk, shadow);
    id_of(stack, shadow_name)
}

/// Go `dom.DDLExecutor().CreateMaterializedViewShadowTable`.
fn create_shadow_table(stack: &UnistoreClusterStack, tk: &mut Session, shadow: tidb_model::TableInfo) {
    let ddl = Arc::clone(&tk.domain_services().unwrap().ddl);
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    ddl.create_materialized_view_shadow_table(schema_id, "test", shadow, &tk.ddl_statement_context()).expect("the shadow is created");
}

/// Go `dom.DDLExecutor().RefreshMaterializedViewCompleteOutOfPlaceCutover`
/// with `setRefreshCutoverQueryString`.
#[allow(clippy::too_many_arguments)]
fn cutover(
    stack: &UnistoreClusterStack,
    tk: &mut Session,
    view: &str,
    old_mview_id: i64,
    shadow_table_id: i64,
    build_read_tso: u64,
    build_sources: Vec<tidb_model::MViewRefreshSource>,
    expected_old_mview_revision: Option<u64>,
    (last_success_read_tso, last_success_read_tso_null): (u64, bool),
) -> Result<(), String> {
    let ddl = Arc::clone(&tk.domain_services().unwrap().ddl);
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    let args = tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs {
        old_mlog_id: 0,
        new_mlog_id: 0,
        old_mview_id,
        shadow_table_id,
        build_read_tso,
        build_sources: Some(build_sources),
        expected_old_mview_revision,
        expected_last_success_read_tso: last_success_read_tso,
        expected_last_success_read_tso_null: last_success_read_tso_null,
        next_refresh_unix_seconds: None,
        should_update_next_refresh_unix_seconds: false,
    };
    let context = tk.ddl_statement_context().with_ddl_query(format!("REFRESH MATERIALIZED VIEW {view} COMPLETE OUT OF PLACE"));
    ddl.refresh_materialized_view_complete_out_of_place_cutover(schema_id, "test", view, args, &context)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// The cutover fixture: `t_name` with a log, `mv_name` on it and a shadow
/// `shadow_name`; the old view's ID, the shadow's, the expected read TSO, the
/// build sources and the view's revision.
fn cutover_fixture(
    stack: &UnistoreClusterStack,
    tk: &mut Session,
    base: &str,
    view: &str,
    shadow_name: &str,
) -> (i64, i64, (u64, bool), Vec<tidb_model::MViewRefreshSource>, u64) {
    must_exec_all(tk, &[
        &format!("create table {base} (a int not null, b int not null)"),
        &format!("create materialized view log on {base} (a, b)"),
        &format!("create materialized view {view} (a, s, cnt) as select a, sum(b), count(1) from {base} group by a"),
    ]);
    let old = latest_table(stack, view);
    let shadow_id = create_shadow_like(stack, tk, view, shadow_name);
    let expected = get_last_success_read_tso(tk, old.id);
    let sources = crate::mview_refresh::resolve_refresh_sources(&stack.factory.catalog.get_latest(), old.id).unwrap();
    (old.id, shadow_id, expected, sources, old.revision)
}

fn dependent_mviews_of(stack: &UnistoreClusterStack, mlog: &str) -> Vec<i64> {
    dependent_mview_ids(&latest_table(stack, mlog))
}

#[test]
fn test_materialized_view_out_of_place_cutover_updates_mlog_dependencies() {
    let (stack, mut tk) = mview_test_kit();
    let (old_id, shadow_id, expected, sources, revision) = cutover_fixture(
        &stack,
        &mut tk,
        "t_cutover_dependency",
        "mv_cutover_dependency",
        "__mv_shadow_cutover_dependency",
    );
    cutover(&stack, &mut tk, "mv_cutover_dependency", old_id, shadow_id, 123456789, sources, Some(revision), expected).unwrap();
    assert_eq!(id_of(&stack, "mv_cutover_dependency"), shadow_id);
    let dependents = dependent_mviews_of(&stack, "$mlog$t_cutover_dependency");
    assert!(dependents.contains(&shadow_id) && !dependents.contains(&old_id), "{dependents:?}");
    must_exec_all(&mut tk, &["drop materialized view mv_cutover_dependency", "drop materialized view log on t_cutover_dependency"]);
}

#[test]
fn test_materialized_view_out_of_place_cutover_failure_rolls_back_mlog_dependencies() {
    let (stack, mut tk) = mview_test_kit();
    let (old_id, shadow_id, expected, sources, revision) =
        cutover_fixture(&stack, &mut tk, "t_cutover_rollback", "mv_cutover_rollback", "__mv_shadow_cutover_rollback");
    let error = {
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError",
            "return",
        );
        cutover(&stack, &mut tk, "mv_cutover_rollback", old_id, shadow_id, 123456789, sources, Some(revision), expected).unwrap_err()
    };
    assert!(error.contains("error after migrating refresh info"), "{error}");
    assert_eq!(id_of(&stack, "mv_cutover_rollback"), old_id);
    assert_eq!(dependent_mviews_of(&stack, "$mlog$t_cutover_rollback"), vec![old_id]);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {old_id}")), "1");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {shadow_id}")), "0");
}

#[test]
fn test_materialized_view_out_of_place_cutover_publish_event_error_keeps_refresh_metadata_atomic() {
    let (stack, mut tk) = mview_test_kit();
    let (old_id, shadow_id, expected, sources, revision) = cutover_fixture(
        &stack,
        &mut tk,
        "t_cutover_publish_error",
        "mv_cutover_publish_error",
        "__mv_shadow_cutover_publish_error",
    );
    must_exec_all(&mut tk, &[&format!(
        "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, UPDATE_TIME) values ({old_id}, 'test', 'mv_cutover_publish_error', 'warning', UTC_TIMESTAMP())"
    )]);
    let error = {
        let _fail = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "return");
        cutover(&stack, &mut tk, "mv_cutover_publish_error", old_id, shadow_id, 123456789, sources.clone(), Some(revision), expected)
            .unwrap_err()
    };
    assert!(error.contains("mock publish event error"), "{error}");
    assert_eq!(id_of(&stack, "mv_cutover_publish_error"), old_id);
    assert_eq!(dependent_mviews_of(&stack, "$mlog$t_cutover_publish_error"), vec![old_id]);
    let count = |tk: &mut Session, table: &str, id: i64| single_value(tk, &format!("select count(*) from mysql.{table} where mview_id = {id}"));
    assert_eq!(count(&mut tk, "tidb_mview_refresh_info", old_id), "1");
    assert_eq!(count(&mut tk, "tidb_mview_refresh_info", shadow_id), "0");
    assert_eq!(count(&mut tk, "tidb_mview_refresh_alert", old_id), "1");

    cutover(&stack, &mut tk, "mv_cutover_publish_error", old_id, shadow_id, 123456790, sources, Some(revision), expected).unwrap();
    assert_eq!(id_of(&stack, "mv_cutover_publish_error"), shadow_id);
    assert_eq!(count(&mut tk, "tidb_mview_refresh_info", old_id), "0");
    assert_eq!(count(&mut tk, "tidb_mview_refresh_info", shadow_id), "1");
    assert_eq!(count(&mut tk, "tidb_mview_refresh_alert", old_id), "0");
}

#[test]
fn test_materialized_view_out_of_place_cutover_failure_keeps_old_affinity_group() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_cutover_affinity (a int not null, b int not null)",
        "create materialized view log on t_cutover_affinity (a, b)",
        "create materialized view mv_cutover_affinity (a, s, cnt) as select a, sum(b), count(1) from t_cutover_affinity group by a",
    ]);
    // The fixture writes the view's affinity straight into its metadata.
    let mut old_info = latest_table(&stack, "mv_cutover_affinity");
    old_info.affinity = Some(tidb_model::GoShared::new(tidb_model::TableAffinityInfo {
        // Go `ast.TableAffinityLevelTable`.
        level: "table".to_owned(),
    }));
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    // Go `kv.RunInNewTxn(... meta.NewMutator(txn).UpdateTable(dbInfo.ID, oldMViewInfo))`.
    stack
        .factory
        .meta_store
        .get()
        .unwrap()
        .run_in_new_txn(&tidb_gocontext::Context::background(), tidb_txnkv::INTERNAL_TXN_DDL, true, &mut |txn| {
            tidb_meta::transaction::Mutator::new(txn).update_table(schema_id, &mut old_info.clone_like_go())?;
            Ok(())
        })
        .unwrap();
    let definitions = tidb_ddl::ddl_affinity::build_affinity_group_definitions(&old_info, None).unwrap();
    tidb_domain_affinity::create_groups_if_not_exists(&definitions).unwrap();
    // Go's `dom.Reload()` finds the schema version unchanged and keeps its
    // cache; the cutover job reads the stored metadata.
    let old_id = id_of(&stack, "mv_cutover_affinity");
    let old_group = tidb_ddl::ddl_affinity::get_table_affinity_group_id(old_id);
    assert!(tidb_domain_affinity::get_groups(&[old_group.clone()]).unwrap().contains_key(&old_group));

    let shadow_id = create_shadow_like(&stack, &mut tk, "mv_cutover_affinity", "__mv_shadow_cutover_affinity");
    let expected = get_last_success_read_tso(&mut tk, old_id);
    let sources = crate::mview_refresh::resolve_refresh_sources(&stack.factory.catalog.get_latest(), old_id).unwrap();
    // "The fixture updates metadata directly, so this test intentionally does
    // not pass an expected revision to the cutover job."
    let error = {
        let _fail = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError", "return");
        cutover(&stack, &mut tk, "mv_cutover_affinity", old_id, shadow_id, 123456789, sources.clone(), None, expected).unwrap_err()
    };
    assert!(error.contains("error before commit"), "{error}");
    assert_eq!(id_of(&stack, "mv_cutover_affinity"), old_id);
    assert!(tidb_domain_affinity::get_groups(&[old_group.clone()]).unwrap().contains_key(&old_group));
    cutover(&stack, &mut tk, "mv_cutover_affinity", old_id, shadow_id, 123456790, sources, None, expected).unwrap();
    assert_eq!(id_of(&stack, "mv_cutover_affinity"), shadow_id);
    assert!(!tidb_domain_affinity::get_groups(&[old_group.clone()]).unwrap().contains_key(&old_group));
}

#[test]
fn test_materialized_view_shadow_manual_drop() {
    for drop_source in [false, true] {
        // t.Run(fmt.Sprintf("source_dropped_%t", dropSource))
        let (stack, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t (a int not null)",
            "insert into t values (1)",
            "create materialized view log on t (a)",
            "create materialized view mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(*) from t group by a",
        ]);
        let mv = latest_table(&stack, "mv");
        let now = chrono::Utc::now();
        let shadow_name =
            |created: chrono::DateTime<chrono::Utc>| format!("__mv_shadow_{}_{}", mv.id, created.timestamp_nanos_opt().unwrap());
        let ordinary_name = shadow_name(now);
        let expired_name = shadow_name(now - chrono::Duration::days(8));
        let recent_name = shadow_name(now - chrono::Duration::days(6));
        let future_name = shadow_name(now + chrono::Duration::days(1));
        let invalid_name = format!("__mv_shadow_{}_invalid", mv.id);
        must_exec_all(&mut tk, &[&format!("create table `{ordinary_name}` (a int)")]);
        for name in [&expired_name, &recent_name, &future_name, &invalid_name] {
            let mut shadow = mv.clone_like_go();
            shadow.id = 0;
            shadow.name = tidb_ast::CiString::new(name.clone());
            shadow.materialized_view = None;
            shadow.materialized_view_base = None;
            shadow.materialized_view_shadow = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewShadowInfo {
                source_mview_id: mv.id,
                ..Default::default()
            }));
            // "Creation time comes from the name; all these tables have fresh
            // UpdateTS."
            create_shadow_table(&stack, &mut tk, shadow);
        }
        if drop_source {
            must_exec_all(&mut tk, &["drop materialized view mv"]);
        }
        for name in [&recent_name, &future_name, &invalid_name] {
            exec_err_contains(&mut tk, &format!("drop table `{name}`"), "DROP TABLE on materialized view shadow table");
        }

        // "The worker must enforce retention even when executor prechecks are
        // bypassed."
        let recent = latest_table(&stack, &recent_name);
        let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
        let mut job = tidb_model::Job::default();
        job.version = tidb_model::get_job_ver_in_use();
        job.schema_id = schema_id;
        job.schema_name = "test".into();
        job.table_id = recent.id;
        job.table_name = recent_name.clone().into();
        job.type_ = tidb_model::ActionType::ACTION_DROP_TABLE;
        job.binlog_info = Some(tidb_model::GoShared::new(tidb_model::HistoryInfo::default()));
        job.involving_schema_info = tidb_goruntime::go_runtime::GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
            database: "test".into(),
            table: recent_name.clone().into(),
            ..Default::default()
        }]);
        let args = tidb_model::JobArgsValue::DropTable(Some(tidb_model::GoShared::new(tidb_model::DropTableArgs {
            identifiers: vec![tidb_model::Ident {
                schema: tidb_ast::CiString::new("test"),
                name: tidb_ast::CiString::new(recent_name.clone()),
            }],
            ..Default::default()
        })));
        let error = deliver_captured_job(&mut tk, tidb_ddl::JobWrapper { job, args, id_allocated: false }).unwrap_err();
        assert!(error.contains("DROP TABLE on materialized view shadow table"), "{error}");

        // "An expired name with fresh UpdateTS is droppable; ordinary tables
        // have no retention."
        must_exec_all(&mut tk, &[
            &format!("drop table `{expired_name}`, `{ordinary_name}`"),
            &format!("drop table if exists `{expired_name}`"),
        ]);
        let mut shown: Vec<Vec<String>> =
            serde_json::from_str(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'").strip_prefix("RS:").unwrap()).unwrap();
        shown.sort();
        let mut want = vec![vec![recent_name.clone()], vec![future_name.clone()], vec![invalid_name.clone()]];
        want.sort();
        assert_eq!(shown, want, "drop_source={drop_source}");
        if !drop_source {
            assert_eq!(answer(&mut tk, "select * from mv"), r#"RS:[["1","1"]]"#);
            exec_err_contains(&mut tk, "drop table mv", "DROP TABLE on materialized view table");
        }
    }
}

#[test]
fn test_materialized_view_shadow_can_only_be_dropped_by_internal_cleanup() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_shadow_cleanup (a int not null, b int not null)",
        "create materialized view log on t_shadow_cleanup (a, b)",
        "create materialized view mv_shadow_cleanup (a, s, cnt) as select a, sum(b), count(1) from t_shadow_cleanup group by a",
    ]);
    let shadow_id = create_shadow_like(&stack, &mut tk, "mv_shadow_cleanup", "__mv_shadow_cleanup");
    // Go sets the session's user; this node's sessions always carry one.
    exec_err_contains(&mut tk, "insert into `__mv_shadow_cleanup` values (1, 1, 1)", "is not updatable");
    exec_err_contains(&mut tk, "select * from `__mv_shadow_cleanup`", "SELECT command denied");
    exec_err_contains(&mut tk, "update t_shadow_cleanup n join __mv_shadow_cleanup s on n.a = s.a set n.b = s.s", "SELECT command denied");
    exec_err_contains(&mut tk, "delete n from t_shadow_cleanup n join __mv_shadow_cleanup s on n.a = s.a", "SELECT command denied");
    exec_err_contains(&mut tk, "drop table `__mv_shadow_cleanup`", "DROP TABLE on materialized view shadow table");
    assert_eq!(id_of(&stack, "__mv_shadow_cleanup"), shadow_id);

    let ddl = Arc::clone(&tk.domain_services().unwrap().ddl);
    ddl.drop_materialized_view_shadow_table("test", "__mv_shadow_cleanup", &tk.ddl_statement_context()).unwrap();
    assert!(stack.factory.catalog.get_latest().table_info_by_name("test", "__mv_shadow_cleanup").is_none());
    // Go `findLatestDDLHistoryJobID`.
    let rows: Vec<Vec<String>> = serde_json::from_str(
        answer(&mut tk, "select hex(job_meta) from mysql.tidb_ddl_history order by job_id desc limit 20").strip_prefix("RS:").unwrap(),
    )
    .unwrap();
    let cleanup_job_id = rows
        .iter()
        .map(|row| {
            let bytes: Vec<u8> = (0..row[0].len()).step_by(2).map(|i| u8::from_str_radix(&row[0][i..i + 2], 16).unwrap()).collect();
            let mut job = tidb_model::Job::default();
            job.decode(&bytes).unwrap();
            job
        })
        .find(|job| job.type_ == tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW)
        .map(|job| job.id)
        .expect("the cleanup job");
    assert_eq!(
        single_value(
            &mut tk,
            &format!(
                "select ((select count(*) from mysql.gc_delete_range where job_id={cleanup_job_id}) + (select count(*) from mysql.gc_delete_range_done where job_id={cleanup_job_id})) > 0"
            )
        ),
        "1"
    );
}

#[test]
fn test_create_materialized_view_reject_non_base_object() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t (a int)", "create materialized view log on t (a)", "create view v as select a from t"]);
    exec_err_contains(&mut tk, "create materialized view mv_v (a, c) as select a, count(1) from v group by a", "is not BASE TABLE");
}

#[test]
fn test_drop_table_materialized_view_constraints() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_drop_constraints (a int not null, b int not null)",
        "create materialized view log on t_drop_constraints (a, b)",
        "create materialized view mv_drop_constraints (a, s, cnt) as select a, sum(b), count(1) from t_drop_constraints group by a",
    ]);
    for (sql, err) in [
        ("drop table mv_drop_constraints", "DROP TABLE on materialized view table"),
        ("drop table `$mlog$t_drop_constraints`", "DROP TABLE on materialized view log table"),
        ("drop table t_drop_constraints", "DROP TABLE on base table with materialized view dependencies"),
        ("truncate table mv_drop_constraints", "TRUNCATE TABLE on materialized view table"),
        ("truncate table `$mlog$t_drop_constraints`", "TRUNCATE TABLE on materialized view log table"),
        ("truncate table t_drop_constraints", "TRUNCATE TABLE on base table with materialized view dependencies"),
    ] {
        exec_err_contains(&mut tk, sql, err);
    }
    must_exec_all(&mut tk, &["drop materialized view mv_drop_constraints"]);
    exec_err_contains(&mut tk, "drop table t_drop_constraints", "DROP TABLE on base table with materialized view log");
    exec_err_contains(&mut tk, "truncate table t_drop_constraints", "TRUNCATE TABLE on base table with materialized view log");
    must_exec_all(&mut tk, &["drop materialized view log on t_drop_constraints", "truncate table t_drop_constraints", "drop table t_drop_constraints"]);
}

#[test]
fn test_materialized_view_partition_ddl_constraints() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_partition_mlog (id bigint not null, v int not null)", "create materialized view log on t_partition_mlog (id, v)"]);
    exec_err_contains(
        &mut tk,
        "alter table t_partition_mlog\npartition by range (id) (\n  partition p0 values less than (100),\n  partition p1 values less than maxvalue\n)",
        "ALTER TABLE ... PARTITION BY with materialized view log",
    );
    must_exec_all(&mut tk, &[
        "create table t_partitioned (\n  id bigint not null primary key,\n  v int not null\n)\npartition by range (id) (\n  partition p0 values less than (100),\n  partition p1 values less than maxvalue\n)",
        "create table t_exchange_base (id bigint not null, v int not null)",
        "create materialized view log on t_exchange_base (id, v)",
        "create materialized view mv_exchange (v, cnt) as select v, count(*) from t_exchange_base group by v",
        "set @@tidb_enable_exchange_partition = 1",
    ]);
    exec_err_contains(
        &mut tk,
        "alter table t_partitioned exchange partition p0 with table t_exchange_base",
        "EXCHANGE PARTITION on non-partitioned table with materialized view dependencies",
    );
    exec_err_contains(
        &mut tk,
        "alter table t_partitioned exchange partition p0 with table `$mlog$t_exchange_base`",
        "EXCHANGE PARTITION on non-partitioned table with materialized view log",
    );
    exec_err_contains(
        &mut tk,
        "alter table t_partitioned exchange partition p0 with table mv_exchange",
        "EXCHANGE PARTITION on non-partitioned table materialized view table",
    );
    must_exec_all(&mut tk, &["set @@tidb_enable_exchange_partition = 0"]);
}

#[test]
fn test_alter_materialized_view_metadata_ddl() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a",
        "alter materialized view mv comment = 'updated comment'",
    ]);
    assert_eq!(latest_table(&stack, "mv").comment, "updated comment");
    let id = id_of(&stack, "mv");
    must_exec_all(&mut tk, &["alter materialized view mv refresh start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)"]);
    let info = mview_info(&stack, "mv");
    assert_eq!(info.refresh_method, "FAST");
    assert_eq!(info.refresh_start_with, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)");
    assert_eq!(info.refresh_next, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)");
    assert_eq!(next_refresh_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is not null"), r#"RS:[["1"]]"#);
    must_exec_all(&mut tk, &["alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'"]);
    let info = mview_info(&stack, "mv");
    assert_eq!((info.alert_warning_sec, info.alert_overdue_sec, info.alert_refresh_failed), (5, 10, true));
    exec_err_contains(&mut tk, "alter materialized view mv attributes='mview_alert_warning=20,mview_alert_overdue=10'", "must be less than or equal");
    must_exec_all(&mut tk, &["alter materialized view mv refresh"]);
    let info = mview_info(&stack, "mv");
    assert!(info.refresh_start_with.is_empty() && info.refresh_next.is_empty());
    assert_eq!(next_refresh_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is null"), r#"RS:[["1"]]"#);
}

#[test]
fn test_alter_materialized_view_attributes_updates_alert_thresholds() {
    let (stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_mv_attr (a int not null, b int not null)", "create materialized view log on t_mv_attr (a, b)"]);
    for (attributes, err_part) in [
        ("unknown=1", "unsupported ATTRIBUTES key"),
        ("mview_alert_warning=-1", "must be non-negative integer seconds"),
        ("mview_alert_refresh_failed=maybe", "must be yes or no"),
        ("mview_alert_warning=600,mview_alert_overdue=300", "must be less than or equal"),
    ] {
        exec_err_contains(
            &mut tk,
            &format!("create materialized view mv_attr_invalid (a, cnt) attributes='{attributes}' as select a, count(1) from t_mv_attr group by a"),
            err_part,
        );
    }
    must_exec_all(&mut tk, &[
        "create materialized view mv_attr_zero (a, cnt) attributes='mview_alert_warning=0,mview_alert_overdue=0' as select a, count(1) from t_mv_attr group by a",
    ]);
    let zero = mview_info(&stack, "mv_attr_zero");
    assert_eq!((zero.alert_warning_sec, zero.alert_overdue_sec, zero.alert_refresh_failed), (0, 0, false));
    must_exec_all(&mut tk, &[
        "create materialized view mv_attr (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) attributes='mview_alert_warning=300,mview_alert_overdue=600,mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_attr group by a",
    ]);
    let check = |stack: &UnistoreClusterStack, want: (i64, i64, bool), next: &str| {
        let info = mview_info(stack, "mv_attr");
        assert_eq!((info.alert_warning_sec, info.alert_overdue_sec, info.alert_refresh_failed), want);
        assert_eq!(info.refresh_next, next);
    };
    check(&stack, (300, 600, false), "DATE_ADD(NOW(), INTERVAL 2 HOUR)");
    must_exec_all(&mut tk, &["alter materialized view mv_attr attributes='mview_alert_warning=5,mview_alert_overdue=5,mview_alert_refresh_failed=yes'"]);
    check(&stack, (5, 5, true), "DATE_ADD(NOW(), INTERVAL 2 HOUR)");
    must_exec_all(&mut tk, &[
        "alter materialized view mv_attr refresh next date_add(now(), interval 25 minute)",
        "alter materialized view mv_attr attributes='mview_alert_warning=10,mview_alert_overdue=20,mview_alert_refresh_failed=no'",
    ]);
    check(&stack, (10, 20, false), "DATE_ADD(NOW(), INTERVAL 25 MINUTE)");
    exec_err_contains(&mut tk, "alter materialized view mv_attr attributes='unknown=1'", "unsupported ATTRIBUTES key");
    exec_err_contains(&mut tk, "alter materialized view mv_attr attributes='mview_alert_refresh_failed=maybe'", "must be yes or no");
    exec_err_contains(&mut tk, "alter materialized view mv_attr attributes='mview_alert_warning=20,mview_alert_overdue=10'", "must be less than or equal");
}

#[test]
fn test_materialized_view_related_tables_ddl_rejected() {
    let (_stack, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_ddl_mv (a int not null, b int)", "create materialized view log on t_ddl_mv (a, b)"]);
    for (sql, err) in [
        ("drop table t_ddl_mv", "DROP TABLE on base table with materialized view log"),
        ("rename table t_ddl_mv to t_ddl_mv2", "RENAME TABLE on base table with materialized view log"),
        ("drop table `$mlog$t_ddl_mv`", "DROP TABLE on materialized view log table"),
        ("rename table `$mlog$t_ddl_mv` to `$mlog$t_ddl_mv2`", "RENAME TABLE on materialized view log table"),
    ] {
        exec_err_contains(&mut tk, sql, err);
    }
    must_exec_all(&mut tk, &[
        "create materialized view mv_ddl_mv (a, cnt) as select a, count(1) from t_ddl_mv group by a",
        "alter table t_ddl_mv add column c int",
    ]);
    for (sql, err) in [
        ("alter table t_ddl_mv modify column a bigint", "does not support changing charset/collation/nullability of group keys"),
        ("drop table t_ddl_mv", "DROP TABLE on base table with materialized view dependencies"),
        ("rename table t_ddl_mv to t_ddl_mv2", "RENAME TABLE on base table with materialized view dependencies"),
        ("alter table mv_ddl_mv add column x int", "ALTER TABLE on materialized view table"),
        ("rename table mv_ddl_mv to mv_ddl_mv2", "RENAME TABLE on materialized view table"),
        ("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)", "ALTER TABLE on materialized view log table"),
        ("alter table `$mlog$t_ddl_mv` add column c int", "ALTER TABLE on materialized view log table"),
        ("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)", "CREATE INDEX on materialized view log table"),
    ] {
        exec_err_contains(&mut tk, sql, err);
    }
}

/// Go master `TestMViewRefreshSourceInfoLogSets`
/// (`pkg/ddl/tests/materializedview/mview_refresh_source_test.go`):
/// `mviewutil`'s refresh-source SQL over several logs and the missing-log
/// placeholder, on the bootstrap layout and on one with other column
/// offsets and a non-clustered handle.
#[test]
fn test_mview_refresh_source_info_log_sets() {
    use tidb_mviewutil::refresh_source::{delete_refresh_source_info, insert_refresh_source_ids, read_refresh_source_ids};
    let all_rows = "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog) order by MVIEW_ID, MLOG_ID";
    for layout in ["bootstrap", "nonclustered_with_extra_column"] {
        let (_stack, mut tk) = mview_test_kit();
        if layout != "bootstrap" {
            // "SQL access must not depend on column offsets or physical
            // handle encoding." (Go's next-gen reserved-object failpoint has
            // nothing to skip on this store.)
            must_exec_all(&mut tk, &[
                "drop table mysql.tidb_mview_fast_refresh_source_info",
                "create table mysql.tidb_mview_fast_refresh_source_info (MLOG_ID bigint not null, EXTRA int default 7, MVIEW_ID bigint not null, primary key (MVIEW_ID, MLOG_ID) nonclustered, key idx_mlog (MLOG_ID, MVIEW_ID))",
            ]);
        }
        // "Exercise multiple logs and the missing-log placeholder
        // independently of the SQL validator's current single-source
        // restriction."
        must_exec_all(&mut tk, &["begin optimistic"]);
        {
            let mut exec = crate::mview_refresh::session_sql_exec(&mut tk);
            insert_refresh_source_ids(&mut exec, 1001, &[0, 10, 11]).unwrap();
            insert_refresh_source_ids(&mut exec, 1002, &[10]).unwrap();
            assert_eq!(read_refresh_source_ids(&mut exec, 1001).unwrap(), vec![0, 10, 11]);
        }
        must_exec_all(&mut tk, &["commit"]);
        assert_eq!(
            answer(&mut tk, all_rows),
            r#"RS:[["1001","0"],["1001","10"],["1001","11"],["1002","10"]]"#,
            "{layout}"
        );

        must_exec_all(&mut tk, &["begin optimistic"]);
        let inserted = insert_refresh_source_ids(&mut crate::mview_refresh::session_sql_exec(&mut tk), 1002, &[9, 10, 12]);
        // "Optimistic SQL can defer duplicate-key checks until commit."
        let code = match inserted {
            Ok(()) => answer(&mut tk, "commit"),
            Err(error) => format!("ERR {}", error.to_mysql_error().code),
        };
        // Go `kv.ErrKeyExists`.
        assert!(code.starts_with("ERR 1062"), "{layout}: {code}");
        must_exec_all(&mut tk, &["rollback"]);
        assert_eq!(answer(&mut tk, "select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = 1002"), r#"RS:[["10"]]"#);

        must_exec_all(&mut tk, &["begin optimistic"]);
        delete_refresh_source_info(&mut crate::mview_refresh::session_sql_exec(&mut tk), &[1001]).unwrap();
        must_exec_all(&mut tk, &["commit"]);
        assert_eq!(
            answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog)"),
            r#"RS:[["1002","10"]]"#,
            "{layout}"
        );
    }
}
