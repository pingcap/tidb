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

//! Go master `pkg/executor/test/materializedview/materialized_view_refresh_test.go`
//! with its `testutil_test.go` helpers.

use super::*;

pub(super) fn mview_test_kit() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, users, tk)
}

/// Go `testkit.Rows(...)`: rows of space-separated cells.
pub(super) fn rows(rows: &[&str]) -> String {
    let rows: Vec<Vec<String>> = rows.iter().map(|row| row.split(' ').map(str::to_owned).collect()).collect();
    format!("RS:{}", serde_json::to_string(&rows).unwrap())
}

/// Go `tk.MustQuery(sql).Check(testkit.Rows(...))`: each row's cells
/// joined by spaces (`<nil>` for NULL), which is what Go compares.
pub(super) fn go_check(tk: &mut Session, sql: &str) -> Vec<String> {
    rows_of(tk, sql)
        .into_iter()
        .map(|row| row.into_iter().map(|cell| cell.unwrap_or_else(|| "<nil>".to_owned())).collect::<Vec<_>>().join(" "))
        .collect()
}

pub(super) fn rows_of(tk: &mut Session, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows = answer(tk, sql);
    serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {rows}"))).unwrap()
}

/// Go `requireRowsContainPrefix`: some row's first cell starts with `prefix`.
pub(super) fn require_rows_contain_prefix(rows: &[Vec<Option<String>>], prefix: &str) {
    assert!(
        rows.iter().any(|row| row.first().and_then(Option::as_deref).is_some_and(|cell| cell.starts_with(prefix))),
        "prefix not found: prefix={prefix} rows={rows:?}"
    );
}

/// Go `joinRowsAsText`: each row's first cell, one per line.
pub(super) fn join_rows_as_text(rows: &[Vec<Option<String>>]) -> String {
    rows.iter().filter_map(|row| row.first().cloned().flatten()).collect::<Vec<_>>().join("\n")
}

/// Go `readAffectedRowsMetricValue`.
fn read_affected_rows_metric_value(label: &str) -> f64 {
    tidb_metrics::executor::AFFECTED_ROWS.with_label_values(&[label]).get()
}

/// Go `requireRefreshMVStatementResult`.
pub(super) fn require_refresh_mv_statement_result(tk: &mut Session, insert_rows: u64, update_rows: u64, delete_rows: u64) {
    assert_eq!(tk.affected_rows(), insert_rows + update_rows + delete_rows);
    assert_eq!(tk.statement_message(), format!("Rows inserted: {insert_rows}  Updated: {update_rows}  Deleted: {delete_rows}"));
}

/// Go `setupMaterializedViewRefreshStatementResultTest`.
fn setup_materialized_view_refresh_statement_result_test() -> (UnistoreClusterStack, Session) {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_refresh_result (a int primary key, b int not null)",
        "insert into t_mv_refresh_result values (1, 10), (2, 20)",
        "create materialized view log on t_mv_refresh_result (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_refresh_result (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_result group by a",
    ]);
    (stack, tk)
}

/// Go `makeMaterializedViewRefreshResultStale`.
fn make_materialized_view_refresh_result_stale(tk: &mut Session) {
    must_exec_all(tk, &[
        "update t_mv_refresh_result set b = 11 where a = 1",
        "delete from t_mv_refresh_result where a = 2",
        "insert into t_mv_refresh_result values (3, 30)",
    ]);
}

/// The refresh-info read TSO `tidb_snapshot = commit_tso` sees for `view`.
fn refresh_info_snapshot_at(tk: &mut Session, schema: &str, view: &str, commit_tso: u64) -> String {
    // "MockTiKV tests need a GC safe point before enabling stale reads."
    must_set_mock_gc_safe_point(tk, chrono::Utc::now() - chrono::Duration::hours(1));
    must_exec_all(tk, &[&format!("set @@tidb_snapshot = '{commit_tso}'")]);
    let seen = answer(
        tk,
        &format!(
            "select r.LAST_SUCCESS_READ_TSO from information_schema.tables t join mysql.tidb_mview_refresh_info r on t.tidb_table_id = r.MVIEW_ID where t.table_schema = '{schema}' and t.table_name = '{view}'"
        ),
    );
    must_exec_all(tk, &["set @@tidb_snapshot = ''"]);
    seen
}

/// Go `requireRefreshInfoSnapshotMatchesLatestSuccessHist`.
pub(super) fn require_refresh_info_snapshot_matches_latest_success_hist(tk: &mut Session, schema: &str, view: &str) {
    let hist = rows_of(
        tk,
        &format!(
            "select REFRESH_READ_TSO, REFRESH_COMMIT_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = '{schema}' and MVIEW_NAME = '{view}' and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 1"
        ),
    );
    assert_eq!(hist.len(), 1);
    let read_tso: u64 = hist[0][0].as_deref().unwrap().parse().unwrap();
    let commit_tso: u64 = hist[0][1].as_deref().unwrap().parse().unwrap();
    assert_ne!(read_tso, 0);
    assert_ne!(commit_tso, 0);
    assert_eq!(refresh_info_snapshot_at(tk, schema, view, commit_tso), rows(&[&read_tso.to_string()]));
}

/// Go `requireRefreshInfoSnapshotMatchesHistAtCommitTSO`.
fn require_refresh_info_snapshot_matches_hist_at_commit_tso(tk: &mut Session, schema: &str, view: &str, commit_tso: u64) {
    let hist = rows_of(
        tk,
        &format!(
            "select REFRESH_READ_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = '{schema}' and MVIEW_NAME = '{view}' and REFRESH_STATUS = 'success' and REFRESH_COMMIT_TSO = {commit_tso}"
        ),
    );
    assert_eq!(hist.len(), 1);
    let read_tso: u64 = hist[0][0].as_deref().unwrap().parse().unwrap();
    assert_ne!(read_tso, 0);
    assert_eq!(refresh_info_snapshot_at(tk, schema, view, commit_tso), rows(&[&read_tso.to_string()]));
}

#[test]
fn test_materialized_view_refresh_complete_basic() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    let id = latest_table(&stack, "mv").id;
    let tso_sql = format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}");
    let old_ts: u64 = single_value(&mut tk, &tso_sql).parse().unwrap();
    assert_ne!(old_ts, 0);
    // "Make MV stale by changing base table."
    must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    must_exec_all(&mut tk, &["refresh materialized view mv complete delta apply"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
    let new_ts: u64 = single_value(&mut tk, &tso_sql).parse().unwrap();
    assert_ne!(old_ts, new_ts);
    assert_eq!(single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")), "1");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), "1");
    assert_eq!(answer(&mut tk, &format!("select MVIEW_SCHEMA, MVIEW_NAME from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), rows(&["test mv"]));
    assert_eq!(
        single_value(
            &mut tk,
            &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = 'TEST' and MVIEW_NAME = 'MV' and MVIEW_ID = {id}")
        ),
        "1"
    );
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select REFRESH_STATUS, REFRESH_METHOD = 'complete delta apply manual', REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_DURATION_SEC = cast(timestampdiff(microsecond, REFRESH_START_TIME, REFRESH_END_TIME) as decimal(18,6)) / 1000000, REFRESH_DURATION_SEC >= 0, REFRESH_READ_TSO > 0, REFRESH_COMMIT_TSO > REFRESH_READ_TSO, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}"
            )
        ),
        rows(&["success 1 1 1 1 1 1 1 1"])
    );
}

#[test]
fn test_materialized_view_refresh_statement_result() {
    for (mode, insert_rows, update_rows, delete_rows) in [
        ("fast", 1, 1, 1),
        ("fast as of timestamp", 1, 1, 1),
        ("complete delta apply", 1, 1, 1),
        ("complete in place", 2, 0, 2),
        ("complete out of place", 2, 0, 0),
    ] {
        // t.Run(tc.mode)
        let (_stack, mut tk) = setup_materialized_view_refresh_statement_result_test();
        // Go asserts `sessionctx.LastExecuteDDL` around the refresh: the
        // server skips a DDL's query metrics, and REFRESH is not one (this
        // node keys that on `Stmt::Ddl`).
        if mode == "fast as of timestamp" {
            must_exec_all(&mut tk, &["set time_zone = '+00:00'"]);
        }
        make_materialized_view_refresh_result_stale(&mut tk);
        let mut refresh_sql = format!("refresh materialized view mv_refresh_result {mode}");
        if mode == "fast as of timestamp" {
            let target = next_refresh_target();
            must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
            refresh_sql += &format!(" '{}'", target.format("%Y-%m-%d %H:%M:%S%.3f"));
        }
        let before_refresh_mv = (mode == "fast").then(|| read_affected_rows_metric_value("RefreshMV"));
        must_exec_all(&mut tk, &[&refresh_sql]);
        require_refresh_mv_statement_result(&mut tk, insert_rows, update_rows, delete_rows);
        if let Some(before) = before_refresh_mv {
            assert_eq!(read_affected_rows_metric_value("RefreshMV") - before, 3.0);
        }
        assert_eq!(answer(&mut tk, "select * from mv_refresh_result order by a"), rows(&["1 11 1", "3 30 1"]), "{mode}");
        // "Out-of-place refresh has no commit TSO. Its historical snapshot
        // behavior is covered by the cutover snapshot test below."
        if mode != "complete out of place" {
            require_refresh_info_snapshot_matches_latest_success_hist(&mut tk, "test", "mv_refresh_result");
        }
    }
}

#[test]
fn test_materialized_view_refresh_fast_as_of_timestamp_no_op_statement_result() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set time_zone = '+00:00'",
        "create table t_mv_refresh_result_noop (a int primary key, b int not null)",
        "insert into t_mv_refresh_result_noop values (1, 10), (2, 20)",
        "create materialized view log on t_mv_refresh_result_noop (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_refresh_result_noop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_refresh_result_noop group by a",
    ]);
    let id: i64 = single_value(
        &mut tk,
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_result_noop'",
    )
    .parse()
    .unwrap();
    let target = next_refresh_target();
    let target_tso = go_time_to_ts(target);
    must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
    let refresh = format!(
        "refresh materialized view mv_refresh_result_noop fast as of timestamp '{}'",
        target.format("%Y-%m-%d %H:%M:%S%.3f")
    );
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {target_tso} where MVIEW_ID = {id}"),
        &refresh,
    ]);
    require_refresh_mv_statement_result(&mut tk, 0, 0, 0);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), "0");
    // "An empty interval does not read input logs or advance the watermark."
    must_exec_all(&mut tk, &[&format!("insert into mysql.tidb_mview_fast_refresh_source_info values ({id}, 0)"), &refresh]);
    require_refresh_mv_statement_result(&mut tk, 0, 0, 0);
    assert_eq!(
        single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")),
        target_tso.to_string()
    );
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_preserves_previous_commit_tso_snapshot() {
    let (_stack, mut tk) = setup_materialized_view_refresh_statement_result_test();
    let id_sql = "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv_refresh_result'";
    let before = single_value(&mut tk, id_sql);
    make_materialized_view_refresh_result_stale(&mut tk);
    must_exec_all(&mut tk, &["refresh materialized view mv_refresh_result fast"]);
    let previous_commit_tso: u64 = single_value(
        &mut tk,
        "select REFRESH_COMMIT_TSO from mysql.tidb_mview_refresh_hist where MVIEW_SCHEMA = 'test' and MVIEW_NAME = 'mv_refresh_result' and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 1",
    )
    .parse()
    .unwrap();
    assert_ne!(previous_commit_tso, 0);
    must_exec_all(&mut tk, &[
        "update t_mv_refresh_result set b = 12 where a = 1",
        "insert into t_mv_refresh_result values (4, 40)",
        "refresh materialized view mv_refresh_result complete out of place",
    ]);
    assert_ne!(single_value(&mut tk, id_sql), before);
    require_refresh_info_snapshot_matches_hist_at_commit_tso(&mut tk, "test", "mv_refresh_result", previous_commit_tso);
}

#[test]
fn test_profile_materialized_view_refresh_step_runtime() {
    // t.Run("step_runtime")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_explain_analyze (a int not null, b int not null)",
            "insert into t_mv_explain_analyze values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t_mv_explain_analyze (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_mv_explain_analyze (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_explain_analyze group by a",
            "insert into t_mv_explain_analyze values (2, 3), (3, 4)",
        ]);
        let fast = rows_of(&mut tk, "refresh materialized view mv_mv_explain_analyze fast with profile");
        for prefix in ["[S01 TXN_BEGIN]", "[S04 DATA_CHANGE_FAST_MERGE]", "  MViewDeltaMerge", "[S07 FINALIZE_HIST]"] {
            require_rows_contain_prefix(&fast, prefix);
        }
        must_exec_all(&mut tk, &["insert into t_mv_explain_analyze values (4, 8)"]);
        let in_place = rows_of(&mut tk, "refresh materialized view mv_mv_explain_analyze complete in place with profile");
        for prefix in ["[S04 DATA_CHANGE_COMPLETE_DELETE]", "[S05 DATA_CHANGE_COMPLETE_INSERT]", "[S08 FINALIZE_HIST]"] {
            require_rows_contain_prefix(&in_place, prefix);
        }
        must_exec_all(&mut tk, &["update t_mv_explain_analyze set b = 11 where a = 1 and b = 10"]);
        let dry_run = rows_of(&mut tk, "refresh materialized view mv_mv_explain_analyze complete delta apply dry run");
        require_rows_contain_prefix(&dry_run, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]");
        require_rows_contain_prefix(&dry_run, "  MViewCompleteDeltaApply");
        let profile = rows_of(&mut tk, "refresh materialized view mv_mv_explain_analyze complete delta apply with profile");
        require_rows_contain_prefix(&profile, "[S04 DATA_CHANGE_COMPLETE_DELTA_APPLY]");
        require_rows_contain_prefix(&profile, "  MViewCompleteDeltaApply");
        let text = join_rows_as_text(&profile);
        assert!(text.contains("mview_complete_delta_apply:{writer:{") && text.contains("rows:{insert:"), "{text}");
        require_rows_contain_prefix(&profile, "[S07 FINALIZE_HIST]");
    }

    // t.Run("redaction")
    let (_stack, _users, mut tk) = mview_test_kit();
    let original_mode = single_value(&mut tk, "select @@global.tidb_redact_log");
    must_exec_all(&mut tk, &[
        "create table t_profile_redact (a int not null, b int not null)",
        "insert into t_profile_redact values (1, 123456790), (2, 1)",
        "create materialized view log on t_profile_redact (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_profile_redact (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_profile_redact where b > 123456789 group by a",
    ]);
    for (mode, predicate_suffix) in [("OFF", ", 123456789)"), ("ON", ", ?)"), ("MARKER", ", ‹123456789›)")] {
        must_exec_all(&mut tk, &[&format!("set global tidb_redact_log = {mode}")]);
        for refresh_mode in ["complete in place", "complete out of place"] {
            let plan = join_rows_as_text(&rows_of(&mut tk, &format!("refresh materialized view mv_profile_redact {refresh_mode} with profile")));
            assert!(plan.contains(predicate_suffix), "{mode} {refresh_mode}: {plan}");
            if mode != "OFF" {
                assert!(!plan.contains(", 123456789)"), "{mode} {refresh_mode}: {plan}");
            }
            assert_eq!(answer(&mut tk, "select a, s, cnt from mv_profile_redact"), rows(&["1 123456790 1"]));
        }
    }
    must_exec_all(&mut tk, &[&format!("set global tidb_redact_log = '{original_mode}'")]);
}

/// Go `requireRowsContainSubstring`.
pub(super) fn require_rows_contain_substring(rows: &[Vec<Option<String>>], substr: &str) {
    assert!(
        rows.iter().flatten().flatten().any(|cell| cell.contains(substr)),
        "substring not found: substring={substr} rows={rows:?}"
    );
}

/// Go `requireRowsContainAnyPrefix`.
pub(super) fn require_rows_contain_any_prefix(rows: &[Vec<Option<String>>], prefixes: &[&str]) {
    assert!(
        prefixes.iter().any(|prefix| rows
            .iter()
            .any(|row| row.first().and_then(Option::as_deref).is_some_and(|cell| cell.starts_with(prefix)))),
        "prefixes not found: prefixes={prefixes:?} rows={rows:?}"
    );
}

fn table_id(tk: &mut Session, name: &str) -> i64 {
    single_value(tk, &format!("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{name}'"))
        .parse()
        .unwrap()
}

fn last_success_read_tso_of(tk: &mut Session, id: i64) -> u64 {
    single_value(tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")).parse().unwrap()
}

fn as_of(time: chrono::DateTime<chrono::Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

#[test]
fn test_materialized_view_refresh_fast_dry_run() {
    // t.Run("current_db")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_dry_run (a int not null, b int not null)",
            "insert into t_mv_fast_dry_run values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t_mv_fast_dry_run (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_dry_run (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_mv_fast_dry_run group by a",
        ]);
        let dry_run = rows_of(&mut tk, "refresh materialized view mv_fast_dry_run fast dry run");
        require_rows_contain_prefix(&dry_run, "[S04 DATA_CHANGE_FAST_MERGE]");
        require_rows_contain_prefix(&dry_run, "  MViewDeltaMerge");
    }
    // t.Run("hazardous_purge")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_dry_run_purge_guard (a int not null, b int not null)",
            "insert into t_mv_fast_dry_run_purge_guard values (1, 10), (2, 20)",
            "create materialized view log on t_mv_fast_dry_run_purge_guard (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_dry_run_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_dry_run_purge_guard group by a",
            "refresh materialized view mv_fast_dry_run_purge_guard fast",
        ]);
        let mv_id = table_id(&mut tk, "mv_fast_dry_run_purge_guard");
        let mlog_id = table_id(&mut tk, "$mlog$t_mv_fast_dry_run_purge_guard");
        let last_success = last_success_read_tso_of(&mut tk, mv_id);
        must_exec_all(&mut tk, &[&format!(
            "update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = {} where MLOG_ID = {mlog_id}",
            last_success + 1
        )]);
        for sql in ["refresh materialized view mv_fast_dry_run_purge_guard fast dry run", "refresh materialized view mv_fast_dry_run_purge_guard fast"] {
            exec_err_contains(&mut tk, sql, "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO");
        }
    }
    // t.Run("noop_skips_purge")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "set time_zone = '+00:00'",
            "create table t_mv_fast_dry_run_noop_purge_guard (a int not null, b int not null)",
            "insert into t_mv_fast_dry_run_noop_purge_guard values (1, 10), (2, 20)",
            "create materialized view log on t_mv_fast_dry_run_noop_purge_guard (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_dry_run_noop_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_dry_run_noop_purge_guard group by a",
        ]);
        let mv_id = table_id(&mut tk, "mv_fast_dry_run_noop_purge_guard");
        let mlog_id = table_id(&mut tk, "$mlog$t_mv_fast_dry_run_noop_purge_guard");
        let target = chrono::DateTime::from_timestamp_millis((chrono::Utc::now() - chrono::Duration::seconds(1)).timestamp_millis()).unwrap();
        let target_tso = go_time_to_ts(target);
        must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
        must_exec_all(&mut tk, &[
            &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {target_tso} where MVIEW_ID = {mv_id}"),
            &format!("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = {} where MLOG_ID = {mlog_id}", target_tso + 1),
        ]);
        let dry_run = rows_of(
            &mut tk,
            &format!("refresh materialized view mv_fast_dry_run_noop_purge_guard fast as of timestamp '{}' dry run", as_of(target)),
        );
        require_rows_contain_prefix(&dry_run, "[S04 DATA_CHANGE_FAST_MERGE]");
        assert!(!dry_run.iter().flatten().flatten().any(|cell| cell.contains("MViewDeltaMerge")), "{dry_run:?}");
        must_exec_all(&mut tk, &[&format!(
            "refresh materialized view mv_fast_dry_run_noop_purge_guard fast as of timestamp '{}'",
            as_of(target)
        )]);
        assert_eq!(answer(&mut tk, "select * from mv_fast_dry_run_noop_purge_guard order by a"), rows(&["1 10 1", "2 20 1"]));
        assert_eq!(last_success_read_tso_of(&mut tk, mv_id), target_tso);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {mv_id}")), "0");
    }
    // t.Run("as_of_window")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "set time_zone = '+00:00'",
            "create table t_mv_refresh_asof_dry_run (a int not null, b int not null)",
            "create materialized view log on t_mv_refresh_asof_dry_run (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_asof_dry_run (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run group by a",
        ]);
        let mv_id = table_id(&mut tk, "mv_refresh_asof_dry_run");
        let last_success = last_success_read_tso_of(&mut tk, mv_id);
        assert_ne!(last_success, 0);
        let target = next_refresh_target();
        let target_tso = go_time_to_ts(target);
        must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
        let dry_run = rows_of(&mut tk, &format!("refresh materialized view mv_refresh_asof_dry_run fast as of timestamp '{}' dry run", as_of(target)));
        require_rows_contain_substring(&dry_run, "_tidb_commit_ts");
        require_rows_contain_substring(&dry_run, &last_success.to_string());
        require_rows_contain_substring(&dry_run, &target_tso.to_string());
    }
    // t.Run("without_as_of")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "set time_zone = '+00:00'",
            "create table t_mv_refresh_dry_run_no_asof (a int not null, b int not null)",
            "insert into t_mv_refresh_dry_run_no_asof values (1, 10), (2, 20)",
            "create materialized view log on t_mv_refresh_dry_run_no_asof (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_dry_run_no_asof (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_dry_run_no_asof group by a",
        ]);
        let mv_id = table_id(&mut tk, "mv_refresh_dry_run_no_asof");
        let last_success = last_success_read_tso_of(&mut tk, mv_id);
        assert_ne!(last_success, 0);
        let dry_run = rows_of(&mut tk, "refresh materialized view mv_refresh_dry_run_no_asof fast dry run");
        require_rows_contain_substring(&dry_run, "_tidb_commit_ts");
        require_rows_contain_substring(&dry_run, &last_success.to_string());
    }
    // t.Run("older_than_last_success")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "set time_zone = '+00:00'",
            "create table t_mv_refresh_asof_dry_run_old (a int not null, b int not null)",
            "create materialized view log on t_mv_refresh_asof_dry_run_old (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_asof_dry_run_old (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_dry_run_old group by a",
        ]);
        let mv_id = table_id(&mut tk, "mv_refresh_asof_dry_run_old");
        let last_success_time =
            chrono::DateTime::from_timestamp_millis((chrono::Utc::now() - chrono::Duration::hours(1)).timestamp_millis()).unwrap();
        let last_success = go_time_to_ts(last_success_time);
        let target = last_success_time - chrono::Duration::seconds(1);
        let target_tso = go_time_to_ts(target);
        must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
        must_exec_all(&mut tk, &[&format!(
            "update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {last_success} where MVIEW_ID = {mv_id}"
        )]);
        exec_err_contains(
            &mut tk,
            &format!("refresh materialized view mv_refresh_asof_dry_run_old fast as of timestamp '{}' dry run", as_of(target)),
            &format!("refresh materialized view fast as of timestamp: target tso {target_tso} is older than LAST_SUCCESS_READ_TSO {last_success}"),
        );
    }
}

#[test]
fn test_materialized_view_refresh_out_of_place_observe_load_shadow_plan_uses_build_sql() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_oop_observe (a int not null, b int not null)",
        "insert into t_mv_oop_observe values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_mv_oop_observe (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_mv_oop_observe (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_oop_observe group by a",
        "insert into t_mv_oop_observe values (2, 3), (3, 4)",
    ]);
    let dry_run = rows_of(&mut tk, "refresh materialized view mv_mv_oop_observe complete out of place dry run");
    require_rows_contain_prefix(&dry_run, "[S03 OUT_OF_PLACE_LOAD_SHADOW]");
    require_rows_contain_any_prefix(&dry_run, &["  Insert", "  ImportInto"]);
    must_exec_all(&mut tk, &["insert into t_mv_oop_observe values (4, 8)"]);
    let profile = rows_of(&mut tk, "refresh materialized view mv_mv_oop_observe complete out of place with profile");
    require_rows_contain_prefix(&profile, "[S03 OUT_OF_PLACE_LOAD_SHADOW]");
    require_rows_contain_any_prefix(&profile, &["  Insert", "  ImportInto"]);
    assert!(!join_rows_as_text(&profile).contains("@@tidb_last_query_info"));
}

/// Go `mustExecRefreshInternal`.
pub(super) fn must_exec_refresh_internal(tk: &mut Session, sql: &str) {
    assert_eq!(exec_refresh_internal(tk, sql), "OK", "{sql}");
}

fn last_refresh_method(tk: &mut Session, id: i64) -> String {
    single_value(tk, &format!("select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"))
}

/// The shared metadata of `view` in the latest schema (Go's
/// `mvTable.Meta().MaterializedView`, mutated in place).
fn shared_mview_info(stack: &UnistoreClusterStack, view: &str) -> tidb_model::GoShared<tidb_model::table::MaterializedViewInfo> {
    let (_, table) = stack.factory.catalog.get_latest().table_info_by_name("test", view).expect(view);
    let info = table.read().materialized_view.clone().expect("a materialized view");
    info
}

#[test]
fn test_materialized_view_refresh_scheduling() {
    let next_in = |low: &str, high: &str| {
        format!(
            "NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {low}), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval {high})"
        )
    };
    let info_check = |tk: &mut Session, id: i64, columns: &str| {
        answer(tk, &format!("select {columns} from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}"))
    };
    let setup = |table: &str, view: &str| {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            &format!("create table {table} (a int not null, b int not null)"),
            &format!("insert into {table} values (1, 10), (2, 20)"),
            &format!("create materialized view log on {table} (a, b) purge next date_add(now(), interval 1 hour)"),
            &format!("create materialized view {view} (a, s, cnt) refresh fast start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from {table} group by a"),
        ]);
        let id = latest_table(&stack, view).id;
        (stack, tk, id)
    };

    // t.Run("next_refresh")
    {
        let (_stack, mut tk, id) = setup("t_internal_next", "mv_internal_next");
        must_exec_all(&mut tk, &[&format!("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = null where MVIEW_ID = {id}")]);
        // "User SQL refresh should not update NEXT_REFRESH_UNIX_SECONDS."
        must_exec_all(&mut tk, &["refresh materialized view mv_internal_next complete delta apply"]);
        assert_eq!(info_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is null"), rows(&["1"]));
        assert_eq!(last_refresh_method(&mut tk, id), "complete delta apply manual");
        // "Internal SQL refresh should update NEXT_REFRESH_UNIX_SECONDS by
        // evaluating RefreshNext."
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_internal_next complete delta apply");
        assert_eq!(info_check(&mut tk, id, &next_in("20 minute", "2 hour")), rows(&["1 1 1"]));
        assert_eq!(last_refresh_method(&mut tk, id), "complete delta apply auto");
    }
    // t.Run("schedule_duration")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_refresh_schedule_duration (a int not null, b int not null)",
            "insert into t_refresh_schedule_duration values (1, 10), (2, 20)",
            "create materialized view log on t_refresh_schedule_duration (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_schedule_duration (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_schedule_duration group by a",
        ]);
        let id = latest_table(&stack, "mv_refresh_schedule_duration").id;
        assert_eq!(info_check(&mut tk, id, "LAST_SUCCESS_REFRESH_END_UNIX_SECONDS > 0"), rows(&["1"]));
        must_exec_all(&mut tk, &["insert into t_refresh_schedule_duration values (3, 30)", "refresh materialized view mv_refresh_schedule_duration fast"]);
        let hist = |tk: &mut Session, columns: &str| {
            answer(tk, &format!("select {columns} from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"))
        };
        assert_eq!(hist(&mut tk, "REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC is null"), r#"RS:[["fast manual","1"]]"#);
        must_exec_all(&mut tk, &[
            &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = UNIX_TIMESTAMP() - 120 where MVIEW_ID = {id}"),
            "insert into t_refresh_schedule_duration values (4, 40)",
        ]);
        let before = tidb_metrics::materialized_view::MV_SERVICE_REFRESH_SCHEDULE_DURATION_HISTOGRAM.get_sample_count();
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_refresh_schedule_duration fast");
        assert_eq!(
            hist(&mut tk, "REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC >= 100, REFRESH_SCHEDULE_DURATION_SEC <= 150"),
            r#"RS:[["fast auto","1","1"]]"#
        );
        assert!(tidb_metrics::materialized_view::MV_SERVICE_REFRESH_SCHEDULE_DURATION_HISTOGRAM.get_sample_count() > before);
        must_exec_all(&mut tk, &[
            &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = null where MVIEW_ID = {id}"),
            "insert into t_refresh_schedule_duration values (5, 50)",
        ]);
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_refresh_schedule_duration fast");
        assert_eq!(
            hist(&mut tk, "REFRESH_METHOD, REFRESH_SCHEDULE_DURATION_SEC is not null, REFRESH_SCHEDULE_DURATION_SEC >= 0"),
            r#"RS:[["fast auto","1","1"]]"#
        );
    }
    // t.Run("start_without_next")
    {
        let (stack, mut tk, id) = setup("t_internal_start_only", "mv_internal_start_only");
        // "Simulate scheduler metadata state: START WITH is set but NEXT is
        // empty."
        let info = shared_mview_info(&stack, "mv_internal_start_only");
        info.write().refresh_start_with = "DATE_ADD(NOW(), INTERVAL 2 HOUR)".to_owned();
        info.write().refresh_next = String::new();
        must_exec_all(&mut tk, &[&format!(
            "update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MVIEW_ID = {id}"
        )]);
        // "User SQL refresh should keep NEXT_REFRESH_UNIX_SECONDS unchanged."
        must_exec_all(&mut tk, &["refresh materialized view mv_internal_start_only complete delta apply"]);
        assert_eq!(info_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is not null"), rows(&["1"]));
        assert_eq!(last_refresh_method(&mut tk, id), "complete delta apply manual");
        // "Internal SQL refresh should explicitly set NEXT_REFRESH_UNIX_SECONDS
        // = NULL when START WITH exists and NEXT is empty."
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_internal_start_only complete delta apply");
        assert_eq!(info_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is null"), rows(&["1"]));
        assert_eq!(last_refresh_method(&mut tk, id), "complete delta apply auto");
    }
    // t.Run("no_schedule")
    {
        let (stack, mut tk, id) = setup("t_internal_no_schedule", "mv_internal_no_schedule");
        // "Simulate scheduler metadata state: schedule is fully removed."
        let info = shared_mview_info(&stack, "mv_internal_no_schedule");
        info.write().refresh_start_with = String::new();
        info.write().refresh_next = String::new();
        must_exec_all(&mut tk, &[&format!(
            "update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MVIEW_ID = {id}"
        )]);
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_internal_no_schedule complete delta apply");
        assert_eq!(info_check(&mut tk, id, "NEXT_REFRESH_UNIX_SECONDS is null"), rows(&["1"]));
        assert_eq!(last_refresh_method(&mut tk, id), "complete delta apply auto");
    }
    // t.Run("out_of_place")
    {
        let (stack, mut tk, old_id) = setup("t_internal_oop_next", "mv_internal_oop_next");
        must_exec_all(&mut tk, &[
            &format!("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS = null where MVIEW_ID = {old_id}"),
            "insert into t_internal_oop_next values (3, 30)",
        ]);
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv_internal_oop_next complete out of place");
        let new_id = latest_table(&stack, "mv_internal_oop_next").id;
        assert_ne!(old_id, new_id);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {old_id}")), "0");
        assert_eq!(info_check(&mut tk, new_id, &next_in("20 minute", "2 hour")), rows(&["1 1 1"]));
        assert_eq!(last_refresh_method(&mut tk, old_id), "complete out of place auto");
    }
}

#[test]
fn test_materialized_view_refresh_fast_method_tracks_manual_and_automatic() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        "insert into t values (2, 3), (3, 4)",
        "refresh materialized view mv fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
    let last = "select REFRESH_METHOD, REFRESH_ROWS > 0 from mysql.tidb_mview_refresh_hist order by REFRESH_JOB_ID desc limit 1";
    assert_eq!(answer(&mut tk, last), r#"RS:[["fast manual","1"]]"#);
    must_exec_all(&mut tk, &["insert into t values (4, 8)"]);
    must_exec_refresh_internal(&mut tk, "refresh materialized view mv fast");
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1", "4 8 1"]));
    assert_eq!(answer(&mut tk, last), r#"RS:[["fast auto","1"]]"#);
}

#[test]
fn test_materialized_view_refresh_fast_updates_stats_modify_count() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_fast_stats (a int not null, b int not null)",
        "insert into t_mv_fast_stats values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_mv_fast_stats (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_fast_stats (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_fast_stats group by a",
    ]);
    let id = latest_table(&stack, "mv_fast_stats").id;
    // Go `dom.StatsHandle().DumpStatsDeltaToKV(true)`.
    let dump = |stack: &UnistoreClusterStack| stack.factory.dump_stats_delta_to_kv(true, &[]).expect("the delta flush succeeds");
    dump(&stack);
    must_exec_all(&mut tk, &["analyze table mv_fast_stats"]);
    let meta = format!("select modify_count, count from mysql.stats_meta where table_id = {id}");
    assert_eq!(answer(&mut tk, &meta), rows(&["0 2"]));
    must_exec_all(&mut tk, &[
        "delete from t_mv_fast_stats where a = 1 and b = 10",
        "delete from t_mv_fast_stats where a = 2 and b = 7",
        "insert into t_mv_fast_stats values (3, 4)",
        "refresh materialized view mv_fast_stats fast",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_fast_stats order by a"), rows(&["1 5 1", "3 4 1"]));
    dump(&stack);
    assert_eq!(answer(&mut tk, &meta), rows(&["3 2"]));
}

/// Go `time.Now().UTC().Add(offset).Truncate(time.Millisecond)`.
fn now_millis(offset: chrono::Duration) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis((chrono::Utc::now() + offset).timestamp_millis()).unwrap()
}

fn as_of_fixture(table: &str, view: &str, rows_sql: Option<&str>) -> (Session, i64) {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["set time_zone = '+00:00'", &format!("create table {table} (a int not null, b int not null)")]);
    if let Some(rows_sql) = rows_sql {
        must_exec_all(&mut tk, &[&format!("insert into {table} values {rows_sql}")]);
    }
    must_exec_all(&mut tk, &[
        &format!("create materialized view log on {table} (a, b) purge next date_add(now(), interval 1 hour)"),
        &format!("create materialized view {view} (a, s, cnt) refresh fast as select a, sum(b), count(1) from {table} group by a"),
    ]);
    let id = table_id(&mut tk, view);
    (tk, id)
}

#[test]
fn test_materialized_view_refresh_fast_as_of_timestamp() {
    // t.Run("reject_non_fast")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_refresh_asof_nonfast (a int not null, b int not null)",
            "create materialized view log on t_mv_refresh_asof_nonfast (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_asof_nonfast (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_nonfast group by a",
        ]);
        exec_err_contains(
            &mut tk,
            "refresh materialized view mv_refresh_asof_nonfast complete delta apply as of timestamp '2021-04-15 00:00:00'",
            "You have an error in your SQL syntax",
        );
    }
    // t.Run("outer_semantics")
    {
        let (mut tk, id) = as_of_fixture("t_mv_refresh_asof_fast", "mv_refresh_asof_fast", Some("(1, 10), (2, 20)"));
        let from = now_millis(-chrono::Duration::hours(1));
        let from_tso = go_time_to_ts(from);
        must_set_mock_gc_safe_point(&mut tk, from - chrono::Duration::hours(1));
        must_exec_all(&mut tk, &[
            &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {from_tso} where MVIEW_ID = {id}"),
            // "Make the MV stale so the no-op path proves we did not
            // accidentally run a normal FAST refresh."
            "insert into t_mv_refresh_asof_fast values (3, 30)",
        ]);
        let view = "select a, s, cnt from mv_refresh_asof_fast order by a";
        assert_eq!(answer(&mut tk, view), rows(&["1 10 1", "2 20 1"]));
        let hist_count = format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}");
        let hist_before = single_value(&mut tk, &hist_count);
        let older = from - chrono::Duration::milliseconds(1);
        let newer = from + chrono::Duration::milliseconds(1);
        let newer_tso = go_time_to_ts(newer);
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_asof_fast fast as of timestamp '{}'", as_of(from))]);
        assert_eq!(answer(&mut tk, view), rows(&["1 10 1", "2 20 1"]));
        assert_eq!(last_success_read_tso_of(&mut tk, id), from_tso);
        assert_eq!(single_value(&mut tk, &hist_count), hist_before);
        let error = answer(&mut tk, &format!("refresh materialized view mv_refresh_asof_fast fast as of timestamp '{}'", as_of(older)));
        assert!(error.contains("target tso") && error.contains("older than LAST_SUCCESS_READ_TSO"), "{error}");
        let older_tso = go_time_to_ts(older);
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = {older_tso}, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                )
            ),
            r#"RS:[["failed","bounded fast manual","1","1"]]"#
        );
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_asof_fast fast as of timestamp '{}'", as_of(newer))]);
        assert_eq!(answer(&mut tk, view), rows(&["1 10 1", "2 20 1"]));
        assert_eq!(last_success_read_tso_of(&mut tk, id), newer_tso);
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = {newer_tso} from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                )
            ),
            r#"RS:[["success","bounded fast manual","1"]]"#
        );
    }
    // t.Run("watermark_windows")
    {
        let (mut tk, id) = as_of_fixture("t_mv_refresh_asof_window", "mv_refresh_asof_window", None);
        let initial_tso = last_success_read_tso_of(&mut tk, id);
        assert_ne!(initial_tso, 0);
        must_exec_all(&mut tk, &["insert into t_mv_refresh_asof_window values (1, 10)"]);
        let first = next_refresh_target();
        must_exec_all(&mut tk, &["insert into t_mv_refresh_asof_window values (2, 20)"]);
        let second = next_refresh_target();
        must_set_mock_gc_safe_point(&mut tk, first - chrono::Duration::hours(1));
        let (first_tso, second_tso) = (go_time_to_ts(first), go_time_to_ts(second));
        let view = "select a, s, cnt from mv_refresh_asof_window order by a";
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_asof_window fast as of timestamp '{}'", as_of(first))]);
        assert_eq!(answer(&mut tk, view), rows(&["1 10 1"]));
        assert_eq!(last_success_read_tso_of(&mut tk, id), first_tso);
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_refresh_asof_window fast as of timestamp '{}'", as_of(second))]);
        assert_eq!(answer(&mut tk, view), rows(&["1 10 1", "2 20 1"]));
        assert_eq!(last_success_read_tso_of(&mut tk, id), second_tso);
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select REFRESH_READ_TSO from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'success' order by REFRESH_JOB_ID desc limit 2"
                )
            ),
            rows(&[&second_tso.to_string(), &first_tso.to_string()])
        );
        assert!(initial_tso < first_tso && first_tso < second_tso);
    }
    // t.Run("automatic_method")
    {
        let (mut tk, id) = as_of_fixture("t_mv_refresh_asof_internal", "mv_refresh_asof_internal", Some("(1, 10), (2, 20)"));
        let from = now_millis(-chrono::Duration::hours(1));
        let from_tso = go_time_to_ts(from);
        must_set_mock_gc_safe_point(&mut tk, from - chrono::Duration::hours(1));
        must_exec_all(&mut tk, &[&format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {from_tso} where MVIEW_ID = {id}")]);
        let target = from + chrono::Duration::milliseconds(1);
        let target_tso = go_time_to_ts(target);
        must_exec_refresh_internal(&mut tk, &format!("refresh materialized view mv_refresh_asof_internal fast as of timestamp '{}'", as_of(target)));
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = {target_tso} from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                )
            ),
            r#"RS:[["success","bounded fast auto","1"]]"#
        );
    }
    // t.Run("early_failure_history")
    {
        let (mut tk, id) = as_of_fixture("t_mv_refresh_asof_early_fail", "mv_refresh_asof_early_fail", Some("(1, 10), (2, 20)"));
        let target = now_millis(-chrono::Duration::minutes(1));
        let target_tso = go_time_to_ts(target);
        must_set_mock_gc_safe_point(&mut tk, target - chrono::Duration::hours(1));
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist",
            r#"return("mock as-of early refresh failure")"#,
        );
        exec_err_contains(
            &mut tk,
            &format!("refresh materialized view mv_refresh_asof_early_fail fast as of timestamp '{}'", as_of(target)),
            "mock as-of early refresh failure",
        );
        assert_eq!(
            answer(
                &mut tk,
                &format!(
                    "select REFRESH_STATUS, REFRESH_METHOD = 'bounded fast manual', REFRESH_START_TIME is not null, REFRESH_END_TIME is not null, REFRESH_DURATION_SEC = cast(timestampdiff(microsecond, REFRESH_START_TIME, REFRESH_END_TIME) as decimal(18,6)) / 1000000, REFRESH_READ_TSO = {target_tso}, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                )
            ),
            rows(&["failed 1 1 1 1 1 1"])
        );
    }
}

#[test]
fn test_materialized_view_fast_refresh_rejects_hazardous_purge_hist() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_refresh_purge_guard (a int not null, b int not null)",
        "create materialized view log on t_refresh_purge_guard (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_refresh_purge_guard (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_purge_guard group by a",
        "insert into t_refresh_purge_guard values (1, 10), (2, 20)",
        "refresh materialized view mv_refresh_purge_guard fast",
        "insert into t_refresh_purge_guard values (3, 30)",
    ]);
    let mv_id = table_id(&mut tk, "mv_refresh_purge_guard");
    let mlog_id = table_id(&mut tk, "$mlog$t_refresh_purge_guard");
    let last_success = last_success_read_tso_of(&mut tk, mv_id);
    let running_purge = |job_id: u64| {
        format!(
            "insert into mysql.tidb_mlog_purge_hist (PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD, PURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME) values ({job_id}, {mlog_id}, 'test', 't_refresh_purge_guard', 'manual', now(6), 0, 'running', {}, now(6))",
            last_success + 1
        )
    };
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = {last_success} where MLOG_ID = {mlog_id}"),
        &running_purge(last_success + 1000),
    ]);
    exec_err_contains(&mut tk, "refresh materialized view mv_refresh_purge_guard fast", "materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO");
    assert_eq!(last_success_read_tso_of(&mut tk, mv_id), last_success);

    let (hit, pause) = (Latch::default(), Latch::default());
    let _after_begin = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterBegin", {
        let (hit, pause) = (hit.clone(), pause.clone());
        move |_| {
            hit.open();
            pause.wait();
        }
    });
    struct Release(Latch);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.open();
        }
    }
    let pause = Release(pause);
    must_exec_all(&mut tk, &[
        &format!("delete from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}"),
        &format!("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = {last_success} where MLOG_ID = {mlog_id}"),
    ]);
    let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3501);
    let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv_refresh_purge_guard fast"));
    hit.wait_for("the refresh at its failpoint");
    let mut tk_concurrent = super::create::new_mview_test_kit(&stack, 3502);
    must_exec_all(&mut tk_concurrent, &[&running_purge(last_success + 2000)]);
    drop(pause);
    let error = refresh.join().unwrap();
    assert!(error.contains("materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO"), "{error}");
}

#[test]
fn test_materialized_view_refresh_failed_alert_by_attribute() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "set time_zone = '+00:00'",
        "create table t_mv_refresh_failed_alert (a int not null, b int not null)",
        "insert into t_mv_refresh_failed_alert values (1, 10), (2, 20)",
        "create materialized view log on t_mv_refresh_failed_alert (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_refresh_failed_yes (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_mv_refresh_failed_alert group by a",
        "create materialized view mv_refresh_failed_no (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_refresh_failed_alert group by a",
    ]);
    let yes_id = table_id(&mut tk, "mv_refresh_failed_yes");
    let no_id = table_id(&mut tk, "mv_refresh_failed_no");
    {
        let _fail = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist",
            r#"return("mock refresh failed alert")"#,
        );
        exec_err_contains(&mut tk, "refresh materialized view mv_refresh_failed_yes fast", "mock refresh failed alert");
        exec_err_contains(&mut tk, "refresh materialized view mv_refresh_failed_no fast", "mock refresh failed alert");
        assert_eq!(
            answer(&mut tk, &format!("select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = {yes_id}")),
            r#"RS:[["YES","1"]]"#
        );
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {no_id}")), "0");
    }
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mview_refresh_alert set ALERT_LEVEL = 'warning' where MVIEW_ID = {yes_id}"),
        "refresh materialized view mv_refresh_failed_yes fast",
    ]);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {yes_id}")), "0");
}

#[test]
fn test_materialized_view_refresh_fallback_reports_alert_before_insert_hist_failure() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mv_refresh_failed_alert_fallback (a int not null, b int not null)",
        "insert into t_mv_refresh_failed_alert_fallback values (1, 10), (2, 20)",
        "create materialized view log on t_mv_refresh_failed_alert_fallback (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_refresh_failed_alert_fallback (a, s, cnt) refresh fast attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_mv_refresh_failed_alert_fallback group by a",
    ]);
    let id = table_id(&mut tk, "mv_refresh_failed_alert_fallback");
    let _early = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist",
        r#"return("mock fallback refresh failed alert")"#,
    );
    let _insert = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockInsertRefreshHistFailedError", "return(true)");
    let error = exec_to_err(&mut tk, "refresh materialized view mv_refresh_failed_alert_fallback fast");
    assert!(error.contains("failed to insert failed refresh history after error"), "{error}");
    assert!(error.contains("mock fallback refresh failed alert"), "{error}");
    assert_eq!(
        answer(&mut tk, &format!("select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = {id}")),
        r#"RS:[["YES","1"]]"#
    );
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), "0");
}

#[test]
fn test_materialized_view_refresh_fast_as_of_timestamp_gc_safe_point() {
    for min_max in [false, true] {
        for dry_run in [false, true] {
            // t.Run(fmt.Sprintf("minmax=%t/dry_run=%t", minMax, dryRun))
            let (_stack, _users, mut tk) = mview_test_kit();
            must_exec_all(&mut tk, &["set time_zone = '+00:00'"]);
            if min_max {
                must_exec_all(&mut tk, &["create table t_mv_refresh_asof_gc (a int not null, b int not null, key idx_a(a))"]);
            } else {
                must_exec_all(&mut tk, &["create table t_mv_refresh_asof_gc (a int not null, b int not null)"]);
            }
            must_exec_all(&mut tk, &["create materialized view log on t_mv_refresh_asof_gc (a, b) purge next date_add(now(), interval 1 hour)"]);
            if min_max {
                must_exec_all(&mut tk, &[
                    "create materialized view mv_refresh_asof_gc (a, cnt, mx, mn) refresh fast as select a, count(1), max(b), min(b) from t_mv_refresh_asof_gc group by a",
                ]);
            } else {
                must_exec_all(&mut tk, &[
                    "create materialized view mv_refresh_asof_gc (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_asof_gc group by a",
                ]);
            }
            let id = table_id(&mut tk, "mv_refresh_asof_gc");
            let from = now_millis(-chrono::Duration::hours(1));
            let from_tso = go_time_to_ts(from);
            must_exec_all(&mut tk, &[
                &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {from_tso} where MVIEW_ID = {id}"),
                "insert into t_mv_refresh_asof_gc values (1, 10)",
            ]);
            let target = next_refresh_target();
            let target_tso = go_time_to_ts(target);
            must_set_mock_gc_safe_point(&mut tk, target + chrono::Duration::seconds(1));
            let mut refresh = format!("refresh materialized view mv_refresh_asof_gc fast as of timestamp '{}'", as_of(target));
            let label = format!("minmax={min_max}/dry_run={dry_run}");
            // Go `variable.ErrSnapshotTooOld` (8055).
            let snapshot_too_old = |answer: &str| assert!(answer.starts_with("ERR 8055 "), "{label}: {answer}");
            if dry_run {
                refresh += " dry run";
                if min_max {
                    snapshot_too_old(&answer(&mut tk, &refresh));
                } else {
                    let dry = rows_of(&mut tk, &refresh);
                    require_rows_contain_substring(&dry, "_tidb_commit_ts");
                    require_rows_contain_substring(&dry, &from_tso.to_string());
                    require_rows_contain_substring(&dry, &target_tso.to_string());
                }
                continue;
            }
            if min_max {
                snapshot_too_old(&answer(&mut tk, &refresh));
                assert_eq!(answer(&mut tk, "select * from mv_refresh_asof_gc"), "RS:[]");
                assert_eq!(last_success_read_tso_of(&mut tk, id), from_tso);
                assert_eq!(
                    answer(
                        &mut tk,
                        &format!(
                            "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = {target_tso}, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                        )
                    ),
                    r#"RS:[["failed","bounded fast manual","1","1"]]"#
                );
            } else {
                must_exec_all(&mut tk, &[&refresh]);
                assert_eq!(answer(&mut tk, "select a, s, cnt from mv_refresh_asof_gc"), rows(&["1 10 1"]));
                assert_eq!(last_success_read_tso_of(&mut tk, id), target_tso);
                assert_eq!(
                    answer(
                        &mut tk,
                        &format!(
                            "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO = {target_tso} from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
                        )
                    ),
                    r#"RS:[["success","bounded fast manual","1"]]"#
                );
            }
        }
    }
}

#[test]
fn test_materialized_view_refresh_fast_min_max() {
    // t.Run("bit", testMaterializedViewRefreshFastMinMaxBit)
    for width in [8_u32, 16, 64] {
        for nullable in [false, true] {
            let (_stack, _users, mut tk) = mview_test_kit();
            let not_null = if nullable { "" } else { "not null" };
            must_exec_all(&mut tk, &[
                &format!("create table t_mv_bit (id int primary key, g int not null, b bit({width}) {not_null}, key(g, b))"),
                "create materialized view log on t_mv_bit (g, b)",
            ]);
            let high: u64 = 1 << (width - 1);
            let (low, max_value) = (high - 1, high | (high - 1));
            must_exec_all(&mut tk, &[
                &format!("insert into t_mv_bit values (1, 1, {low}), (2, 1, {high}), (3, 2, {low})"),
                "create materialized view mv_bit (g, cnt, bcnt, mn, mx) refresh fast as select g, count(1), count(b), min(b), max(b) from t_mv_bit group by g",
            ]);
            let label = format!("bit{width}/nullable={nullable}");
            let check = |tk: &mut Session| {
                assert_eq!(
                    answer(tk, "select g, cnt, bcnt, cast(mn as unsigned), cast(mx as unsigned) from mv_bit order by g"),
                    answer(tk, "select g, count(1), count(b), cast(min(b) as unsigned), cast(max(b) as unsigned) from t_mv_bit group by g order by g"),
                    "{label}"
                );
            };
            check(&mut tk);
            assert_eq!(answer(&mut tk, "select cast(mn as unsigned), cast(mx as unsigned) from mv_bit where g = 1"), rows(&[&format!("{low} {high}")]));
            // "Update an existing group and insert new groups, including an
            // all-NULL group."
            must_exec_all(&mut tk, &[&format!("insert into t_mv_bit values (4, 1, {max_value}), (5, 3, {high})")]);
            if nullable {
                must_exec_all(&mut tk, &["insert into t_mv_bit values (6, 4, null), (7, 1, null)"]);
            } else {
                must_exec_all(&mut tk, &[&format!("insert into t_mv_bit values (6, 4, 0), (7, 1, {low})")]);
            }
            must_exec_all(&mut tk, &["refresh materialized view mv_bit fast"]);
            check(&mut tk);
            // "Removing extrema forces recomputation alongside a group with a
            // fast-path update."
            must_exec_all(&mut tk, &[
                &format!("update t_mv_bit set b = {low} where id = 4"),
                &format!("update t_mv_bit set b = {max_value} where id = 3"),
                &format!("insert into t_mv_bit values (8, 3, {max_value})"),
                "refresh materialized view mv_bit fast",
            ]);
            check(&mut tk);
            must_exec_all(&mut tk, &["delete from t_mv_bit where id in (1, 2, 3, 5)", "refresh materialized view mv_bit fast"]);
            check(&mut tk);
            if nullable {
                must_exec_all(&mut tk, &["update t_mv_bit set b = null where g = 1"]);
            } else {
                must_exec_all(&mut tk, &["update t_mv_bit set b = 0 where g = 1"]);
            }
            must_exec_all(&mut tk, &[&format!("update t_mv_bit set b = {high} where id = 6"), "refresh materialized view mv_bit fast"]);
            check(&mut tk);
            must_exec_all(&mut tk, &["delete from t_mv_bit", "refresh materialized view mv_bit fast"]);
            check(&mut tk);
            must_exec_all(&mut tk, &[
                &format!("insert into t_mv_bit values (9, 1, {low}), (10, 1, {max_value}), (11, 2, {high}), (12, 2, 0)"),
                "refresh materialized view mv_bit fast",
            ]);
            check(&mut tk);
            must_exec_all(&mut tk, &["refresh materialized view mv_bit fast"]);
            check(&mut tk);
        }
    }
    // t.Run("basic")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_minmax (a int not null, b int not null, key idx_a(a))",
            "create materialized view log on t_mv_fast_minmax (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_minmax (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax group by a",
            "insert into t_mv_fast_minmax values (1, 10), (1, 20), (1, 30), (2, 5), (2, 8)",
            "refresh materialized view mv_fast_minmax complete delta apply",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv_fast_minmax order by a"), rows(&["1 3 30 10", "2 2 8 5"]));
        must_exec_all(&mut tk, &[
            "delete from t_mv_fast_minmax where a = 1 and b = 30",
            "delete from t_mv_fast_minmax where a = 2 and b = 5",
            "insert into t_mv_fast_minmax values (2, 12)",
            "refresh materialized view mv_fast_minmax fast",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv_fast_minmax order by a"), rows(&["1 2 20 10", "2 2 12 8"]));
    }
    // t.Run("supporting_index")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "CREATE TABLE t_mv_fast_minmax_hint (id BIGINT NOT NULL, c01 VARCHAR(32) NOT NULL DEFAULT '', c02 VARCHAR(64) NOT NULL DEFAULT '', c03 INT NOT NULL DEFAULT 0, c04 INT NOT NULL DEFAULT 0, c05 INT NOT NULL DEFAULT 0, c06 VARCHAR(16) NOT NULL DEFAULT '', c07 DATE NOT NULL, c08 INT NOT NULL DEFAULT 0, c09 DATETIME NOT NULL, c10 DATETIME DEFAULT '2100-01-01 00:00:00', m01 DECIMAL(19,6) NOT NULL DEFAULT 0, m02 DECIMAL(19,6) NOT NULL DEFAULT 0, m03 DECIMAL(19,6) NOT NULL DEFAULT 0, PRIMARY KEY (id), KEY idx_c02_c09 (c02, c09), KEY idx_mv_minmax (c07, c02, c06, c05, c04, c03, c01, c08, m02, m03))",
            "CREATE MATERIALIZED VIEW LOG ON t_mv_fast_minmax_hint(c04, c03, c01, c02, m01, m02, m03, c08, c05, c06, c07)",
            "CREATE MATERIALIZED VIEW mv_fast_minmax_hint (c01, c02, c03, c04, c05, c06, c07, cnt, sum_m01, sum_m02, sum_m03, max_m02, max_m03, min_m03) REFRESH FAST AS SELECT t.c01, t.c02, t.c03, t.c04, t.c05, t.c06, t.c07, COUNT(1), SUM(t.m01), SUM(t.m02), SUM(t.m03), MAX(t.m02), MAX(t.m03), MIN(t.m03) FROM t_mv_fast_minmax_hint AS t WHERE t.c08 = 2 AND t.c01 != 0 AND t.c01 != '' AND t.c02 != '' AND t.c06 != '0' AND t.c06 != '' AND t.c05 != 0 AND t.c03 != 0 AND t.c04 != 0 AND t.c01 = 'sample' GROUP BY t.c01, t.c02, t.c03, t.c04, t.c05, t.c06, t.c07",
            "INSERT INTO t_mv_fast_minmax_hint VALUES (1, 'sample', 'k-1', 20, 10, 30, 'X', '2026-06-26', 2, '2026-06-26 10:00:00', '2026-06-26 10:05:00', 100.000000, 80.000000, -5.000000)",
            "REFRESH MATERIALIZED VIEW mv_fast_minmax_hint FAST",
        ]);
        require_refresh_mv_statement_result(&mut tk, 0, 0, 0);
        assert_eq!(answer(&mut tk, "SELECT * FROM mv_fast_minmax_hint"), "RS:[]");
        require_refresh_info_snapshot_matches_latest_success_hist(&mut tk, "test", "mv_fast_minmax_hint");
        // "Repeated empty refreshes still advance the refresh watermark and
        // commit history."
        must_exec_all(&mut tk, &["REFRESH MATERIALIZED VIEW mv_fast_minmax_hint FAST"]);
        require_refresh_mv_statement_result(&mut tk, 0, 0, 0);
        require_refresh_info_snapshot_matches_latest_success_hist(&mut tk, "test", "mv_fast_minmax_hint");
    }
    // t.Run("missing_index")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_minmax_noidx (id int not null primary key, a int not null, b int not null, key idx_a(a))",
            "create materialized view log on t_mv_fast_minmax_noidx (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_minmax_noidx (a, cnt, mx, mn) refresh fast next date_add(now(), interval 1 hour) as select a, count(1), max(b), min(b) from t_mv_fast_minmax_noidx group by a",
            "insert into t_mv_fast_minmax_noidx values (1, 1, 10), (2, 1, 20)",
            "refresh materialized view mv_fast_minmax_noidx complete delta apply",
        ]);
        exec_err_contains(
            &mut tk,
            "alter table t_mv_fast_minmax_noidx alter index idx_a invisible",
            "Unsupported ALTER INDEX INVISIBLE on base table index idx_a required by materialized view mv_fast_minmax_noidx for MIN/MAX fast refresh",
        );
    }
    // t.Run("nullable_duplicate_count")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_nullable_dup_count (a int not null, b int, key idx_ab(a, b))",
            "create materialized view log on t_mv_fast_nullable_dup_count (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_nullable_dup_count (a, cnt, cnt_b1, cnt_b2, s, mx, mn) refresh fast as select a, count(1), count(b) as cnt_b1, count(b) as cnt_b2, sum(b), max(b), min(b) from t_mv_fast_nullable_dup_count group by a",
            "insert into t_mv_fast_nullable_dup_count values (1, 10), (1, null), (1, 5), (2, null)",
            "refresh materialized view mv_fast_nullable_dup_count complete delta apply",
        ]);
        let view = "select a, cnt, cnt_b1, cnt_b2, s, mx, mn from mv_fast_nullable_dup_count order by a";
        assert_eq!(answer(&mut tk, view), r#"RS:[["1","3","2","2","15","10","5"],["2","1","0","0",null,null,null]]"#);
        must_exec_all(&mut tk, &[
            "delete from t_mv_fast_nullable_dup_count where a = 1 and b = 10",
            "insert into t_mv_fast_nullable_dup_count values (1, 20), (1, null), (2, 7), (3, null), (3, 4)",
            "refresh materialized view mv_fast_nullable_dup_count fast",
        ]);
        assert_eq!(answer(&mut tk, view), rows(&["1 4 2 2 25 20 5", "2 2 1 1 7 7 7", "3 2 1 1 4 4 4"]));
        must_exec_all(&mut tk, &[
            "delete from t_mv_fast_nullable_dup_count where a = 1 and b in (5, 20)",
            "delete from t_mv_fast_nullable_dup_count where a = 2 and b = 7",
            "refresh materialized view mv_fast_nullable_dup_count fast",
        ]);
        assert_eq!(
            answer(&mut tk, view),
            r#"RS:[["1","2","0","0",null,null,null],["2","1","0","0",null,null,null],["3","2","1","1","4","4","4"]]"#
        );
    }
    // t.Run("where_separate_indexes")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_mv_fast_minmax_where (id bigint not null primary key, g1 int not null, v1 bigint not null, v2 decimal(10,2) not null, c1 int not null, f1 int not null, key idx_g1(g1), key idx_f1(f1))",
            "create materialized view log on t_mv_fast_minmax_where (id, g1, v1, v2, c1, f1) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_fast_minmax_where (g1, cnt, s_v1, min_v2, max_v2, cnt_c1) refresh fast next date_add(now(), interval 1 hour) as select g1, count(*), sum(v1), min(v2), max(v2), count(c1) from t_mv_fast_minmax_where where f1 = 1 group by g1",
            "insert into t_mv_fast_minmax_where values (19001, 1, 1, 1.00, 1, 1), (19002, 2, 2, 2.00, 1, 1)",
            "refresh materialized view mv_fast_minmax_where fast",
        ]);
        let view = "select * from mv_fast_minmax_where order by g1";
        assert_eq!(answer(&mut tk, view), rows(&["1 1 1 1.00 1.00 1", "2 1 2 2.00 2.00 1"]));
        must_exec_all(&mut tk, &[
            "update t_mv_fast_minmax_where set g1 = 2, v1 = 10, f1 = 2, v2 = 9.00, c1 = 1 where id = 19001",
            "refresh materialized view mv_fast_minmax_where fast",
        ]);
        assert_eq!(answer(&mut tk, view), rows(&["2 1 2 2.00 2.00 1"]));
        must_exec_all(&mut tk, &["update t_mv_fast_minmax_where set f1 = 1 where id = 19001", "refresh materialized view mv_fast_minmax_where fast"]);
        assert_eq!(answer(&mut tk, view), rows(&["2 2 12 2.00 9.00 2"]));
    }
}

/// The refresh-history fixture: `t (a, b)` with its log and `mv`, made
/// stale; the view's ID.
fn refresh_history_fixture() -> (UnistoreClusterStack, Session, i64) {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let id = latest_table(&stack, "mv").id;
    must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)"]);
    (stack, tk, id)
}

#[test]
fn test_materialized_view_refresh_history() {
    let last_hist = |tk: &mut Session, id: i64, columns: &str| {
        answer(tk, &format!("select {columns} from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"))
    };
    // t.Run("finalize_retry")
    {
        let (_stack, mut tk, id) = refresh_history_fixture();
        let _finalize = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError", "1*return(true)");
        must_exec_all(&mut tk, &["refresh materialized view mv complete delta apply"]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
        assert_eq!(
            last_hist(&mut tk, id, "REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null"),
            r#"RS:[["success","complete delta apply manual","1","1","1"]]"#
        );
        assert_eq!(
            single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'")),
            "0"
        );
    }
    // t.Run("finalize_and_cleanup_failure")
    {
        let (_stack, mut tk, id) = refresh_history_fixture();
        let _finalize = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError", "return(true)");
        let _release =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockReleaseMVRefreshAdvisoryLockFullyCount", "return(0)");
        exec_err_contains(&mut tk, "refresh materialized view mv complete delta apply", "advisory lock cleanup invariant violated");
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
        assert_eq!(
            last_hist(&mut tk, id, "REFRESH_STATUS, REFRESH_END_TIME is null, REFRESH_FAILED_REASON is null"),
            r#"RS:[["running","1","1"]]"#
        );
        assert_eq!(
            single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'failed'")),
            "0"
        );
    }
    // t.Run("running_lifecycle")
    {
        let (stack, mut tk, id) = refresh_history_fixture();
        let (hit, pause) = (Latch::default(), Latch::default());
        let _after_running = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning",
            {
                let (hit, pause) = (hit.clone(), pause.clone());
                move |_| {
                    hit.open();
                    pause.wait();
                }
            },
        );
        struct Release(Latch);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.open();
            }
        }
        let release = Release(pause);
        let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3601);
        let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv complete delta apply"));
        hit.wait_for("the running history row");
        assert_eq!(
            last_hist(
                &mut tk,
                id,
                "REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is null, LAST_HEARTBEAT_TIME is not null"
            ),
            r#"RS:[["running","complete delta apply manual","1","1","1","1"]]"#
        );
        drop(release);
        assert_eq!(refresh.join().unwrap(), "OK");
        assert_eq!(
            last_hist(&mut tk, id, "REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null"),
            r#"RS:[["success","complete delta apply manual","1","1","1"]]"#
        );
    }
    // t.Run("heartbeat")
    {
        let (stack, mut tk, id) = refresh_history_fixture();
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        let _heartbeat = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskHistHeartbeatInterval", "return(50)");
        let pause =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", "pause");
        let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3602);
        let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv complete delta apply"));
        let heartbeat_sql = format!(
            "select cast(LAST_HEARTBEAT_TIME as char) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1"
        );
        let heartbeat = |tk: &mut Session| rows_of(tk, &heartbeat_sql).first().and_then(|row| row[0].clone()).filter(|value| !value.is_empty());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let first = loop {
            if let Some(first) = heartbeat(&mut tk) {
                break first;
            }
            assert!(std::time::Instant::now() < deadline, "no running heartbeat");
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while heartbeat(&mut tk).is_none_or(|current| current == first) {
            assert!(std::time::Instant::now() < deadline, "the heartbeat never moved");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        drop(pause);
        assert_eq!(refresh.join().unwrap(), "OK");
        assert_eq!(last_hist(&mut tk, id, "REFRESH_STATUS, LAST_HEARTBEAT_TIME is not null"), r#"RS:[["success","1"]]"#);
    }
}

/// Go's `refreshMaterializedViewAfterBegin` pause: (hit, release guard).
fn pause_after_begin() -> (Latch, Latch, tidb_failpoint::Guard) {
    let (hit, pause) = (Latch::default(), Latch::default());
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterBegin", {
        let (hit, pause) = (hit.clone(), pause.clone());
        move |_| {
            hit.open();
            pause.wait();
        }
    });
    (hit, pause, guard)
}

#[test]
fn test_materialized_view_refresh_complete_read_tso_snapshot_matches_mv() {
    let (stack, _users, mut tk) = mview_test_kit();
    // "For mocktikv, safe point is not initialized, we manually insert it
    // for snapshot to use."
    must_exec_all(&mut tk, &[
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20160102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)') ON DUPLICATE KEY UPDATE variable_value = '20160102-15:04:05 -0700', comment = 'All versions after safe point can be accessed. (DO NOT EDIT)'",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2"]));
    let id = latest_table(&stack, "mv").id;
    // "Make MV stale by changing base table."
    must_exec_all(&mut tk, &["insert into t values (1, 1)"]);
    let (hit, pause, _after_begin) = pause_after_begin();
    let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3701);
    let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv complete delta apply"));
    hit.wait_for("the refresh after BEGIN");
    // "Commit new base table data after refresh txn start_ts but before
    // refresh reads with for_update_ts."
    must_exec_all(&mut super::create::new_mview_test_kit(&stack, 3702), &["insert into t values (2, 7)"]);
    pause.open();
    assert_eq!(refresh.join().unwrap(), "OK");
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 16 3", "2 7 1"]));
    let read_tso = last_success_read_tso_of(&mut tk, id);
    assert_ne!(read_tso, 0);
    must_exec_all(&mut tk, &[&format!("set @@tidb_snapshot = '{read_tso}'")]);
    assert_eq!(answer(&mut tk, "select a, sum(b), count(1) from t group by a order by a"), rows(&["1 16 3", "2 7 1"]));
    must_exec_all(&mut tk, &["set @@tidb_snapshot = ''"]);
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = {read_tso}, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
            )
        ),
        r#"RS:[["success","complete delta apply manual","1","1","1"]]"#
    );
}

#[test]
fn test_materialized_view_refresh_complete_refresh_info_cas_update_after_concurrent_pre_update() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        // "Make MV stale by changing base table."
        "insert into t values (2, 3)",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    let id = latest_table(&stack, "mv").id;
    let old_ts = last_success_read_tso_of(&mut tk, id);
    assert_ne!(old_ts, 0);
    let (hit, pause, _after_begin) = pause_after_begin();
    let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3703);
    let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv complete delta apply"));
    hit.wait_for("the refresh after BEGIN");
    // "Update refresh info row after refresh txn start_ts but before it does
    // the locking read."
    let injected = old_ts + 1000;
    must_exec_all(&mut super::create::new_mview_test_kit(&stack, 3704), &[&format!(
        "update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {injected} where MVIEW_ID = {id}"
    )]);
    pause.open();
    assert_eq!(refresh.join().unwrap(), "OK");
    // "Refresh should succeed and must override LAST_SUCCESS_READ_TSO by
    // this refresh's read tso."
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2"]));
    let new_ts = last_success_read_tso_of(&mut tk, id);
    assert!(new_ts != 0 && new_ts != injected, "{new_ts}");
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO = {new_ts}, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"
            )
        ),
        r#"RS:[["success","complete delta apply manual","1","1","1"]]"#
    );
}

#[test]
fn test_materialized_view_refresh_complete_concurrent_nowait() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let mv = latest_table(&stack, "mv");
    let schema_id = stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id;
    let mut tk_locker = super::create::new_mview_test_kit(&stack, 3705);
    let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3706);
    let lock_name = format!("mv_refresh_{schema_id}_{}", mv.id);
    assert_eq!(single_value(&mut tk_locker, &format!("select get_lock('{lock_name}', 1)")), "1");
    for method in ["fast", "complete delta apply", "complete in place", "complete out of place"] {
        // Go `exeerrors.ErrMVAdvisoryLockConflict` (8182).
        let refused = answer(&mut tk_refresh, &format!("refresh materialized view mv {method}"));
        assert!(refused.starts_with("ERR 8182 "), "{method}: {refused}");
    }
    assert_eq!(single_value(&mut tk_locker, &format!("select release_lock('{lock_name}')")), "1");
    must_exec_all(&mut tk_locker, &["begin pessimistic"]);
    answer(&mut tk_locker, &format!("select MVIEW_ID from mysql.tidb_mview_refresh_info where MVIEW_ID = {} for update", mv.id));
    exec_err_contains(&mut tk_refresh, "refresh materialized view mv complete delta apply", "NOWAIT");
    must_exec_all(&mut tk_locker, &["rollback"]);
    // "After releasing the lock, refresh should succeed."
    must_exec_all(&mut tk_refresh, &["refresh materialized view mv complete delta apply"]);
}

#[test]
fn test_materialized_view_refresh_complete_missing_refresh_info_row() {
    let (_stack, mut tk, id) = refresh_history_fixture();
    must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")]);
    exec_err_contains(&mut tk, "refresh materialized view mv complete delta apply", "tidb_mview_refresh_info");
}

#[test]
fn test_materialized_view_refresh_with_async_mode_complete() {
    let (_stack, mut tk, _id) = refresh_history_fixture();
    exec_err_contains(&mut tk, "refresh materialized view mv with async mode complete delta apply", "WITH ASYNC MODE is not supported yet");
}

/// Go `mustExecCompleteDeltaApplyImplementStmt`: the COMPLETE DELTA APPLY
/// implement statement run as materialized view maintenance's restricted SQL.
fn must_exec_complete_delta_apply_implement_stmt(tk: &mut Session, schema: &str, view: &str) {
    let stmt = tidb_ast::RefreshMaterializedViewImplementStmt {
        refresh_stmt: tidb_ast::RefreshMaterializedViewStmt {
            view_name: vec![schema.to_owned(), view.to_owned()],
            with_async_mode: false,
            mode: tidb_ast::RefreshMaterializedViewMode::CompleteDeltaApply,
            observe_type: tidb_ast::RefreshMaterializedViewObserveType::None,
            as_of: None,
        },
        last_successful_refresh_read_tso: 0,
        target_refresh_read_tso: 0,
        mlog_retained_lower_tso: 0,
        registered_mlog_ids: Vec::new(),
    };
    let implement = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::RefreshMaterializedViewImplement(Box::new(stmt))));
    let sql = implement.restore();
    let original = (tk.vars.in_mview_maintenance, tk.vars.restricted_sql);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = (true, true);
    let result = tk.node_conn().run_ordinary_write(&sql, &implement);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = original;
    result.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

/// The CompleteDeltaApply fixture: `t (a, b)` with its log and `mv`.
fn complete_delta_apply_fixture() -> (UnistoreClusterStack, Session, i64) {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    let id = latest_table(&stack, "mv").id;
    (stack, tk, id)
}

#[test]
fn test_materialized_view_refresh_complete_delta_apply() {
    // t.Run("basic")
    {
        let (_stack, mut tk, id) = complete_delta_apply_fixture();
        let old_ts = last_success_read_tso_of(&mut tk, id);
        assert_ne!(old_ts, 0);
        must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)", "refresh materialized view mv complete delta apply"]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
        assert_ne!(last_success_read_tso_of(&mut tk, id), old_ts);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), "1");
        assert_eq!(
            answer(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")),
            r#"RS:[["success","complete delta apply manual","1","1","1","1"]]"#
        );
        must_exec_all(&mut tk, &["insert into t values (4, 8)"]);
        must_exec_refresh_internal(&mut tk, "refresh materialized view mv complete delta apply");
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1", "4 8 1"]));
        assert_eq!(
            single_value(&mut tk, &format!("select REFRESH_METHOD from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
            "complete delta apply auto"
        );
    }
    // t.Run("in_place")
    {
        let (_stack, mut tk, id) = complete_delta_apply_fixture();
        must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)", "refresh materialized view mv complete in place"]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
        assert_eq!(
            answer(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_ROWS is null, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")),
            r#"RS:[["success","complete in place manual","1","1","1","1"]]"#
        );
    }
    // t.Run("implement_stmt")
    let cases: [(&str, &[&str], &[&str]); 5] = [
        ("insert-only", &["insert into t values (2, 3), (3, 4)"], &["1 15 2", "2 10 2", "3 4 1"]),
        ("delete-only", &["delete from t where a = 1 and b = 5"], &["1 10 1", "2 7 1"]),
        ("update-only", &["update t set b = 8 where a = 2 and b = 7"], &["1 15 2", "2 8 1"]),
        (
            "mixed",
            &["update t set b = 11 where a = 1 and b = 10", "delete from t where a = 2 and b = 7", "insert into t values (3, 4)"],
            &["1 16 2", "3 4 1"],
        ),
        ("noop", &[], &["1 15 2", "2 7 1"]),
    ];
    for (name, prepare, expected) in cases {
        let (_stack, mut tk, _id) = complete_delta_apply_fixture();
        must_exec_all(&mut tk, prepare);
        must_exec_complete_delta_apply_implement_stmt(&mut tk, "test", "mv");
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(expected), "{name}");
    }
    // t.Run("for_update_ts")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10), (1, 5)",
            "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        ]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2"]));
        let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3711);
        must_exec_all(&mut tk_refresh, &["begin pessimistic"]);
        must_exec_all(&mut super::create::new_mview_test_kit(&stack, 3712), &["insert into t values (2, 7)"]);
        must_exec_complete_delta_apply_implement_stmt(&mut tk_refresh, "test", "mv");
        assert_eq!(answer(&mut tk_refresh, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
        must_exec_all(&mut tk_refresh, &["commit"]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    }
    // t.Run("rollback")
    {
        let (_stack, mut tk, id) = complete_delta_apply_fixture();
        must_exec_all(&mut tk, &["create materialized view log on mv (a, s, cnt)"]);
        let old_ts = last_success_read_tso_of(&mut tk, id);
        assert_ne!(old_ts, 0);
        must_exec_all(&mut tk, &["insert into t values (2, 8)"]);
        let _fp = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
            r#"return("mock refresh data change failure")"#,
        );
        exec_err_contains(&mut tk, "refresh materialized view mv complete delta apply", "mock refresh data change failure");
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
        assert_eq!(answer(&mut tk, "select * from `$mlog$mv`"), "RS:[]");
        assert_eq!(single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO = {old_ts} from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")), "1");
        assert_eq!(
            answer(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
            r#"RS:[["failed","complete delta apply manual","1","1","1"]]"#
        );
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'")), "0");
        let reason = single_value(&mut tk, &format!("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"));
        assert!(reason.contains("mock refresh data change failure"), "{reason}");
    }
}

#[test]
fn test_materialized_view_refresh_early_failure_writes_hist() {
    let (_stack, mut tk, id) = complete_delta_apply_fixture();
    let before: i64 = single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")).parse().unwrap();
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist",
        r#"return("mock early refresh failure")"#,
    );
    exec_err_contains(&mut tk, "refresh materialized view mv complete delta apply", "mock early refresh failure");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id}")), (before + 1).to_string());
    assert_eq!(
        answer(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
        r#"RS:[["failed","complete delta apply manual","1","1","1"]]"#
    );
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'")), "0");
    let reason = single_value(&mut tk, &format!("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"));
    assert!(reason.contains("mock early refresh failure"), "{reason}");
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_cutover_basic() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    let old_id = latest_table(&stack, "mv").id;
    must_exec_all(&mut tk, &[&format!(
        "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values ({old_id}, 'test', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())"
    )]);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {old_id}")), "1");
    let next_refresh = answer(&mut tk, &format!("select NEXT_REFRESH_UNIX_SECONDS from mysql.tidb_mview_refresh_info where MVIEW_ID = {old_id}"));

    must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)", "refresh materialized view mv complete out of place"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
    let new_id = latest_table(&stack, "mv").id;
    assert_ne!(old_id, new_id);
    let base = latest_table(&stack, "t");
    assert!(base.materialized_view_base.is_some());
    let ids = mview_ids(&base);
    assert!(ids.contains(&new_id) && !ids.contains(&old_id), "{ids:?}");
    assert_eq!(answer(&mut tk, "select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info"), rows(&[&format!("{new_id} {}", mlog_id(&base))]));
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {old_id}")), "0");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {old_id}")), "0");
    assert_eq!(single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = {new_id}")), "1");
    assert_eq!(answer(&mut tk, &format!("select NEXT_REFRESH_UNIX_SECONDS from mysql.tidb_mview_refresh_info where MVIEW_ID = {new_id}")), next_refresh);
    assert_eq!(
        go_check(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO > 0, REFRESH_FAILED_REASON is null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {old_id} order by REFRESH_JOB_ID desc limit 1")),
        vec!["success complete out of place manual 1 1"]
    );
    assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
    let job_id = first_cell(&mut tk, "admin show ddl jobs where JOB_TYPE='refresh materialized view complete out-of-place cutover'");
    assert_eq!(
        single_value(&mut tk, &format!("select ((select count(*) from mysql.gc_delete_range where job_id={job_id}) + (select count(*) from mysql.gc_delete_range_done where job_id={job_id})) > 0")),
        "1"
    );
}

/// The first cell of a non-empty result (Go `rows[0][0]`).
fn first_cell(tk: &mut Session, sql: &str) -> String {
    let answer = answer(tk, sql);
    let parsed: serde_json::Value = serde_json::from_str(answer.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {answer}"))).unwrap();
    match &parsed[0][0] {
        serde_json::Value::String(text) => text.clone(),
        other => panic!("{sql}: {other} in {answer}"),
    }
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_updates_mlog_dependencies() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_dep (a int not null, b int not null)",
        "create materialized view log on t_dep (a, b)",
        "create materialized view mv_dep (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_dep group by a",
    ]);
    let old_id = latest_table(&stack, "mv_dep").id;
    must_exec_all(&mut tk, &["refresh materialized view mv_dep complete out of place"]);
    let new_id = latest_table(&stack, "mv_dep").id;
    assert_ne!(old_id, new_id);
    let mlog = latest_table(&stack, "$mlog$t_dep");
    assert!(mlog.materialized_view_log.is_some());
    let dependents = dependent_mview_ids(&mlog);
    assert!(dependents.contains(&new_id) && !dependents.contains(&old_id), "{dependents:?}");
    must_exec_all(&mut tk, &["drop materialized view mv_dep", "drop materialized view log on t_dep"]);
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_cutover_failure() {
    let shadow_tables = r"show tables like '\_\_mv\_shadow\_%'";
    // t.Run("refresh_info_rollback")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a",
            "set global tidb_ddl_error_count_limit = 0",
        ]);
        let old_id = latest_table(&stack, "mv").id;
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError", "1*return()");
        must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)"]);
        exec_to_err(&mut tk, "refresh materialized view mv complete out of place");
        assert_eq!(latest_table(&stack, "mv").id, old_id);
        let mlog = latest_table(&stack, "$mlog$t");
        assert!(mlog.materialized_view_log.is_some());
        assert_eq!(dependent_mview_ids(&mlog), vec![old_id]);
        assert_eq!(answer(&mut tk, "select MVIEW_ID from mysql.tidb_mview_refresh_info"), rows(&[&old_id.to_string()]));
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    }
    // t.Run("publish_event")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_cutover_publish_error (a int not null, b int not null)",
            "create materialized view log on t_cutover_publish_error (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_cutover_publish_error (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_cutover_publish_error group by a",
        ]);
        let old_id = latest_table(&stack, "mv_cutover_publish_error").id;
        must_exec_all(&mut tk, &[
            &format!("insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, UPDATE_TIME) values ({old_id}, 'test', 'mv_cutover_publish_error', 'warning', UTC_TIMESTAMP())"),
            "set global tidb_ddl_error_count_limit = 0",
        ]);
        {
            let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "1*return()");
            must_exec_all(&mut tk, &["insert into t_cutover_publish_error values (1, 10)"]);
            let error = exec_to_err(&mut tk, "refresh materialized view mv_cutover_publish_error complete out of place");
            assert!(error.contains("mock publish event error"), "{error}");
        }
        assert_eq!(latest_table(&stack, "mv_cutover_publish_error").id, old_id);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {old_id}")), "1");
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {old_id}")), "1");
        must_exec_all(&mut tk, &["refresh materialized view mv_cutover_publish_error complete out of place"]);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where MVIEW_ID = {old_id}")), "0");
    }
    // t.Run("watermark_cas")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        ]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
        let id = latest_table(&stack, "mv").id;
        let tk_concurrent = std::sync::Arc::new(std::sync::Mutex::new(super::create::new_mview_test_kit(&stack, 3721)));
        let _fp = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", {
            let tk_concurrent = tk_concurrent.clone();
            move |_| {
                must_exec_all(&mut tk_concurrent.lock().unwrap(), &[&format!(
                    "update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = LAST_SUCCESS_READ_TSO + 1 where MVIEW_ID = {id}"
                )]);
            }
        });
        must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)"]);
        let error = exec_to_err(&mut tk, "refresh materialized view mv complete out of place");
        assert!(error.contains("stale LAST_SUCCESS_READ_TSO"), "{error}");
        assert_eq!(latest_table(&stack, "mv").id, id);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")), "1");
        assert_eq!(
            go_check(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
            vec!["failed complete out of place manual 1 1"]
        );
        assert_eq!(answer(&mut tk, shadow_tables), "RS:[]");
        // "Cutover CAS mismatch should keep old MV serving table unchanged."
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    }
    // t.Run("revision_cas")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a",
        ]);
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
        let mv = latest_table(&stack, "mv");
        must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)"]);
        let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow", "pause");
        let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3722);
        let refresh = std::thread::spawn(move || exec_to_err(&mut tk_refresh, "refresh materialized view mv complete out of place"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while answer(&mut tk, shadow_tables) == "RS:[]" {
            assert!(std::time::Instant::now() < deadline, "the shadow table never appeared");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        must_exec_all(&mut super::create::new_mview_test_kit(&stack, 3723), &["create index idx_mv_s_refresh_ddl on mv (s)"]);
        drop(pause);
        let error = refresh.join().unwrap();
        assert!(error.contains("stale old materialized view revision detected before cutover"), "{error}");
        assert!(!error.contains("%!(EXTRA"), "{error}");
        let current = latest_table(&stack, "mv");
        assert_eq!(current.id, mv.id);
        assert!(error.contains(&format!("expected {} but got {}", mv.revision, current.revision)), "{error}");
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {}", mv.id)), "1");
        assert_eq!(
            go_check(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {} order by REFRESH_JOB_ID desc limit 1", mv.id)),
            vec!["failed complete out of place manual 1 1"]
        );
        assert_eq!(answer(&mut tk, shadow_tables), "RS:[]");
        assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
        assert_ne!(answer(&mut tk, "show index from mv where Key_name = 'idx_mv_s_refresh_ddl'"), "RS:[]");
    }
}

/// Waits for the one `__mv_shadow_%` table a paused out-of-place refresh
/// created (Go `require.Eventually` over `show tables like ...`).
fn eventually_shadow_table(tk: &mut Session) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let shown = answer(tk, r"show tables like '\_\_mv\_shadow\_%'");
        if shown != "RS:[]" {
            return first_cell(tk, r"show tables like '\_\_mv\_shadow\_%'");
        }
        assert!(std::time::Instant::now() < deadline, "the shadow table never appeared");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_shadow_table_protected() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_shadow_guard (a int not null, b int not null)",
        "insert into t_shadow_guard values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_shadow_guard (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_shadow_guard (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t_shadow_guard group by a",
        "insert into t_shadow_guard values (2, 3), (3, 4)",
        // Go sets the session user `test`@`%` directly without `Auth`, so
        // its privilege manager passes every check (`RequestVerification`'s
        // `p.user == "" && p.host == ""`) and only the shadow-table guards
        // see the user. A Rust session logs in: the user exists and holds
        // every privilege.
        "create user 'test'@'%' identified by 'abc'",
        "grant all privileges on *.* to 'test'@'%'",
    ]);
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow", "pause");
    let mut tk_refresh = super::create::new_mview_test_kit(&stack, 3731);
    let refresh = std::thread::spawn(move || answer(&mut tk_refresh, "refresh materialized view mv_shadow_guard complete out of place"));
    let shadow = eventually_shadow_table(&mut tk);
    assert_eq!(
        exec_to_err(&mut tk, &format!("create materialized view log on `{shadow}` (a)")),
        format!("[ddl:1347]'test.{shadow}' is not BASE TABLE")
    );
    let mut tk_user = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(3732, log_in(&users, "test")))
        .unwrap();
    must_exec_all(&mut tk_user, &["set tidb_mview_enable = on", "use test"]);
    for (sql, expected) in [
        (format!("insert into `{shadow}` values (9, 9, 9)"), "not updatable"),
        (format!("alter table `{shadow}` add column x int"), "ALTER TABLE on materialized view shadow table"),
        (format!("drop table `{shadow}`"), "DROP TABLE on materialized view shadow table"),
        (format!("update t_shadow_guard n join `{shadow}` s on n.a = s.a set n.b = s.s"), "SELECT command denied"),
        (format!("delete n from t_shadow_guard n join `{shadow}` s on n.a = s.a"), "SELECT command denied"),
    ] {
        let error = exec_to_err(&mut tk_user, &sql);
        assert!(error.contains(expected), "{sql}: {error}");
    }
    drop(pause);
    assert_eq!(refresh.join().unwrap(), "OK");
    assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_shadow_guard order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_cutover_with_unsigned_build_read_tso() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let old_id = latest_table(&stack, "mv").id;
    // "Exercise unsigned SQL persistence without using the synthetic TSO as
    // a storage snapshot."
    let build_read_tso = (1u64 << 63) + 12345;
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlacePersistReadTSO",
        &format!(r#"return("{build_read_tso}")"#),
    );
    must_exec_all(&mut tk, &["insert into t values (2, 3), (3, 4)", "refresh materialized view mv complete out of place"]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
    let new_id = latest_table(&stack, "mv").id;
    assert_ne!(old_id, new_id);
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = {old_id}")), "0");
    assert_eq!(last_success_read_tso_of(&mut tk, new_id), build_read_tso);
}

#[test]
fn test_materialized_view_refresh_complete_out_of_place_build_failure_cleans_shadow() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let id = single_value(&mut tk, "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mv'");
    must_exec_all(&mut tk, &["insert into t values (2, 8)"]);
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow",
        r#"return("mock out-of-place shadow load failure")"#,
    );
    let error = exec_to_err(&mut tk, "refresh materialized view mv complete out of place");
    assert!(error.contains("mock out-of-place shadow load failure"), "{error}");
    assert_eq!(
        go_check(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
        vec!["failed complete out of place manual 1 1"]
    );
    assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
    // Go scans the last 20 history jobs for ActionDropMaterializedViewShadow.
    let cleanup_job_id = first_cell(&mut tk, "admin show ddl jobs 20 where JOB_TYPE = 'drop materialized view shadow table'");
    assert_eq!(
        single_value(&mut tk, &format!("select ((select count(*) from mysql.gc_delete_range where job_id={cleanup_job_id}) + (select count(*) from mysql.gc_delete_range_done where job_id={cleanup_job_id})) > 0")),
        "1"
    );
    // "Out-of-place build failure should not modify old MV serving table."
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
}

#[test]
fn test_materialized_view_refresh_complete_failure_keeps_refresh_info_read_tso() {
    let (_stack, mut tk, id) = complete_delta_apply_fixture();
    let old_ts = last_success_read_tso_of(&mut tk, id);
    assert_ne!(old_ts, 0);
    must_exec_all(&mut tk, &["insert into t values (2, 8)"]);
    let _refresh = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
        r#"return("mock refresh data change failure")"#,
    );
    let _finalize = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError", "1*return(true)");
    let error = exec_to_err(&mut tk, "refresh materialized view mv complete delta apply");
    assert!(error.contains("mock refresh data change failure"), "{error}");
    // "MV data should remain unchanged due to transactional refresh."
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 7 1"]));
    assert_eq!(single_value(&mut tk, &format!("select LAST_SUCCESS_READ_TSO = {old_ts} from mysql.tidb_mview_refresh_info where MVIEW_ID = {id}")), "1");
    assert_eq!(
        go_check(&mut tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null, REFRESH_READ_TSO is null, REFRESH_FAILED_REASON is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1")),
        vec!["failed complete delta apply manual 1 1 1"]
    );
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'")), "0");
    let reason = single_value(&mut tk, &format!("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"));
    assert!(reason.contains("mock refresh data change failure"), "{reason}");
}

#[test]
fn test_materialized_view_refresh_failure_reports_alert_before_finalize_hist_failure() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) attributes='mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t group by a",
    ]);
    let id = latest_table(&stack, "mv").id;
    must_exec_all(&mut tk, &["insert into t values (2, 8)"]);
    let _refresh = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
        r#"return("mock refresh data change failure")"#,
    );
    let _finalize = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockFinalizeRefreshHistError", "return(true)");
    let error = exec_to_err(&mut tk, "refresh materialized view mv complete delta apply");
    assert!(error.contains("failed to finalize refresh history after error"), "{error}");
    assert!(error.contains("mock refresh data change failure"), "{error}");
    assert_eq!(
        go_check(&mut tk, &format!("select REFRESH_FAILED, ALERT_LEVEL is null from mysql.tidb_mview_refresh_alert where MVIEW_ID = {id}")),
        vec!["YES 1"]
    );
}

#[test]
fn test_materialized_view_refresh_complete_with_constraint_check_in_place_pessimistic_off() {
    let (_stack, mut tk, _id) = complete_delta_apply_fixture();
    // "Turn off in-place constraint check for pessimistic txn. Refresh
    // should still succeed."
    must_exec_all(&mut tk, &[
        "set session tidb_constraint_check_in_place_pessimistic = off",
        "insert into t values (2, 3), (3, 4)",
        "refresh materialized view mv complete in place",
    ]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv order by a"), rows(&["1 15 2", "2 10 2", "3 4 1"]));
    assert_eq!(single_value(&mut tk, "select @@tidb_constraint_check_in_place_pessimistic"), "0");
}

#[test]
fn test_materialized_view_refresh_requires_operate_view_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
        // Go creates the user with an empty password; a Rust session logs in
        // with the fixture's.
        "create user 'mv_refresh_u'@'%' identified by 'abc'",
    ]);
    let mut tk_user = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(3741, log_in(&users, "mv_refresh_u")))
        .unwrap();
    must_exec_all(&mut tk_user, &["set tidb_mview_enable = on"]);
    let error = exec_to_err(&mut tk_user, "refresh materialized view test.mv complete delta apply");
    assert!(error.contains("OPERATE VIEW command denied"), "{error}");
    must_exec_all(&mut tk, &["grant operate view on test.mv to 'mv_refresh_u'@'%'"]);
    let error = exec_to_err(&mut tk_user, "refresh materialized view test.mv complete delta apply");
    assert!(error.contains("SELECT command denied"), "{error}");
    must_exec_all(&mut tk, &["grant select on test.t to 'mv_refresh_u'@'%'"]);
    must_exec_all(&mut tk_user, &["refresh materialized view test.mv complete delta apply"]);
    let error = exec_to_err(&mut tk_user, "refresh materialized view test.mv complete delta apply dry run");
    assert!(error.contains("SHOW VIEW command denied"), "{error}");
    must_exec_all(&mut tk, &["grant show view on test.mv to 'mv_refresh_u'@'%'"]);
    let dry_run = answer(&mut tk_user, "refresh materialized view test.mv complete delta apply dry run");
    assert!(dry_run.contains("INSERT_HIST_RUNNING") && dry_run.contains("FINALIZE_HIST"), "{dry_run}");
    must_exec_all(&mut tk, &["drop user 'mv_refresh_u'@'%'"]);
}

/// Go's `require.Eventually` over the view's one running history row.
fn eventually_refresh_running(tk: &mut Session, id: i64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let sql = format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'");
    while single_value(tk, &sql) != "1" {
        assert!(std::time::Instant::now() < deadline, "the refresh never wrote its running history row");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// The cancellation fixture: `<table>` with its log and `<view>`, the
/// monitor polling every 50 ms, and the refresh `<mode>` started in the
/// background and paused after its running history row.
fn paused_refresh(
    stack: &UnistoreClusterStack,
    tk: &mut Session,
    table: &str,
    view: &str,
    mode: &str,
    conn_id: u64,
) -> (i64, tidb_failpoint::Guard, std::thread::JoinHandle<String>) {
    must_exec_all(tk, &[
        &format!("create table {table} (a int not null, b int not null)"),
        &format!("insert into {table} values (1, 10), (1, 5), (2, 7)"),
        &format!("create materialized view log on {table} (a, b) purge next date_add(now(), interval 1 hour)"),
        &format!("create materialized view {view} (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from {table} group by a"),
    ]);
    let id = latest_table(stack, view).id;
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", "pause");
    let mut tk_refresh = super::create::new_mview_test_kit(stack, conn_id);
    let sql = format!("refresh materialized view {view} {mode}");
    let refresh = std::thread::spawn(move || exec_to_err(&mut tk_refresh, &sql));
    eventually_refresh_running(tk, id);
    (id, pause, refresh)
}

fn request_cancel_by_hist(tk: &mut Session, id: i64, requester: &str) {
    must_exec_all(tk, &[&format!(
        "UPDATE mysql.tidb_mview_refresh_hist SET CANCEL_REQUEST_TIME = NOW(6), CANCEL_REQUESTED_BY = '{}' WHERE MVIEW_ID = {id} AND REFRESH_STATUS = 'running' AND CANCEL_REQUEST_TIME IS NULL",
        requester.replace('\'', "''")
    )]);
}

#[test]
fn test_materialized_view_refresh_cancellation() {
    let ten_seconds = std::time::Duration::from_secs(10);
    let last_hist = |tk: &mut Session, id: i64| {
        go_check(tk, &format!("select REFRESH_STATUS, REFRESH_METHOD, REFRESH_END_TIME is not null from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"))
    };
    let last_reason = |tk: &mut Session, id: i64| {
        single_value(tk, &format!("select REFRESH_FAILED_REASON from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} order by REFRESH_JOB_ID desc limit 1"))
    };
    let running = |tk: &mut Session, id: i64| {
        single_value(tk, &format!("select count(*) from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running'"))
    };
    // t.Run("watcher_hist_request")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        let (id, pause, refresh) =
            paused_refresh(&stack, &mut tk, "t_refresh_cancel_watch", "mv_refresh_cancel_watch", "complete delta apply", 3751);
        let requester = "'refresh_watcher_req'@'stage-c'";
        let (requested, _watch) = wait_cancel_watcher_requested("refresh-");
        request_cancel_by_hist(&mut tk, id, requester);
        requested.recv_timeout(ten_seconds).expect("timeout waiting for refresh cancel watcher to observe request");
        drop(pause);
        let error = refresh.join().unwrap();
        assert!(error.contains("materialized view task canceled manually"), "{error}");
        assert_eq!(last_hist(&mut tk, id), vec!["failed complete delta apply manual 1"]);
        assert_eq!(running(&mut tk, id), "0");
        assert_eq!(last_reason(&mut tk, id), format!("cancelled manually by {requester}"));
    }
    // t.Run("cancel_job")
    {
        let (stack, users, mut tk) = mview_test_kit();
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        let (id, pause, refresh) =
            paused_refresh(&stack, &mut tk, "t_refresh_cancel_job", "mv_refresh_cancel_job", "complete delta apply", 3752);
        let job_id = single_value(
            &mut tk,
            &format!("select REFRESH_JOB_ID from mysql.tidb_mview_refresh_hist where MVIEW_ID = {id} and REFRESH_STATUS = 'running' order by REFRESH_JOB_ID desc limit 1"),
        );
        // Go creates the user with an empty password; a Rust session logs
        // in with the fixture's.
        must_exec_all(&mut tk, &["create user 'mv_refresh_cancel_u'@'%' identified by 'abc'"]);
        let mut tk_cancel = stack
            .factory
            .open_session(super::super::super::node_fixture::session_context_with_identity(3753, log_in(&users, "mv_refresh_cancel_u")))
            .unwrap();
        must_exec_all(&mut tk_cancel, &["set tidb_mview_enable = on"]);
        let error = exec_to_err(&mut tk_cancel, &format!("cancel materialized view refresh job {job_id}"));
        assert!(error.contains("OPERATE VIEW command denied"), "{error}");
        must_exec_all(&mut tk, &["grant operate view on test.mv_refresh_cancel_job to 'mv_refresh_cancel_u'@'%'"]);
        let (requested, _watch) = wait_cancel_watcher_requested("refresh-");
        answer(&mut tk_cancel, &format!("cancel materialized view refresh job {job_id}"));
        requested.recv_timeout(ten_seconds).expect("timeout waiting for refresh cancel watcher to observe request");
        drop(pause);
        let error = refresh.join().unwrap();
        assert!(error.contains("materialized view task canceled manually"), "{error}");
        assert_eq!(last_hist(&mut tk, id), vec!["failed complete delta apply manual 1"]);
        assert_eq!(last_reason(&mut tk, id), "cancelled manually by 'mv_refresh_cancel_u'@'%'");
        must_exec_all(&mut tk, &["drop user 'mv_refresh_cancel_u'@'%'"]);
    }
    // t.Run("job_not_running")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        let error = exec_to_err(&mut tk, "cancel materialized view refresh job 1");
        assert!(error.contains("cannot cancel materialized view refresh job 1"), "{error}");
        let error = exec_to_err(&mut tk, "cancel materialized view log purge job 1");
        assert!(error.contains("cannot cancel materialized view log purge job 1"), "{error}");
    }
    // t.Run("watcher_stops_after_finish")
    {
        let (_stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table t_refresh_cancel_watch_stop (a int not null, b int not null)",
            "insert into t_refresh_cancel_watch_stop values (1, 10), (1, 5), (2, 7)",
            "create materialized view log on t_refresh_cancel_watch_stop (a, b) purge next date_add(now(), interval 1 hour)",
            "create materialized view mv_refresh_cancel_watch_stop (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_refresh_cancel_watch_stop group by a",
        ]);
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        let poll_count = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));
        let _polled = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvTaskMonitorPolled", {
            let poll_count = poll_count.clone();
            move |name: &mut dyn std::any::Any| {
                if name.downcast_ref::<String>().is_some_and(|name| name.starts_with("refresh-")) {
                    poll_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });
        must_exec_all(&mut tk, &[
            "insert into t_refresh_cancel_watch_stop values (2, 3), (3, 4)",
            "refresh materialized view mv_refresh_cancel_watch_stop complete delta apply",
        ]);
        let count_after_return = poll_count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(count_after_return > 0);
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::SeqCst), count_after_return);
    }
    // t.Run("watcher_stops_before_shadow")
    {
        let (stack, _users, mut tk) = mview_test_kit();
        let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
        let create_shadow_hit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _create_shadow = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterCreateShadow", {
            let hit = create_shadow_hit.clone();
            move |_| hit.store(true, std::sync::atomic::Ordering::SeqCst)
        });
        let (id, pause, refresh) =
            paused_refresh(&stack, &mut tk, "t_oop_cancel_watch", "mv_oop_cancel_watch", "complete out of place", 3754);
        let requester = "'oop_watcher_req'@'stage-c'";
        let (requested, _watch) = wait_cancel_watcher_requested("refresh-");
        request_cancel_by_hist(&mut tk, id, requester);
        requested.recv_timeout(ten_seconds).expect("timeout waiting for refresh cancel watcher to observe request");
        drop(pause);
        let error = refresh.join().unwrap();
        assert!(error.contains("materialized view task canceled manually"), "{error}");
        assert!(!create_shadow_hit.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(last_hist(&mut tk, id), vec!["failed complete out of place manual 1"]);
        assert_eq!(running(&mut tk, id), "0");
        assert_eq!(last_reason(&mut tk, id), format!("cancelled manually by {requester}"));
        assert_eq!(answer(&mut tk, r"show tables like '\_\_mv\_shadow\_%'"), "RS:[]");
    }
}
