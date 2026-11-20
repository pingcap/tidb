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
