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

//! Go master `pkg/planner/core/casetest/mview/mv_refresh_test.go`'s cases
//! that read the statement's for-update TS through the transaction manager:
//! this port's session publishes it to the executor tier, so they run over a
//! session. The mock-schema cases live in `tidb-executor`'s
//! `casetest_mview_tests.rs`.

use super::refresh_operator::execute_refresh_operator;
use super::*;

/// Go `oracle.ExtractPhysical`.
const fn extract_physical(ts: u64) -> u64 {
    ts >> 18
}

/// Go `oracle.ComposeTS(physical, 0)`.
const fn compose_ts(physical: u64) -> u64 {
    physical << 18
}

/// Go's `create table` / log / view setup, then `begin pessimistic`; answers
/// the transaction's start TS.
fn begin_pessimistic_over_fast_view(tk: &mut Session, table: &str, view: &str) -> u64 {
    must_exec_all(tk, &[
        "set @@tidb_mview_enable = 1",
        "use test",
        &format!("create table {table} (a int not null, b int not null)"),
        &format!("create materialized view log on {table} (a, b) purge next date_add(now(), interval 1 hour)"),
        &format!("create materialized view {view} (a, cnt) refresh fast as select a, count(1) from {table} group by a"),
        "begin pessimistic",
    ]);
    let start_ts: u64 = single_value(tk, "select @@tidb_current_ts").parse().unwrap();
    assert_ne!(start_ts, 0);
    start_ts
}

/// Go's `RefreshMaterializedViewImplementStmt` without a target TSO.
fn unbounded_fast_implement(
    view: &str,
    last_successful_refresh_read_tso: u64,
    mlog_retained_lower_tso: u64,
    registered_mlog_ids: Vec<i64>,
) -> tidb_ast::RefreshMaterializedViewImplementStmt {
    tidb_ast::RefreshMaterializedViewImplementStmt {
        refresh_stmt: tidb_ast::RefreshMaterializedViewStmt {
            view_name: vec!["test".to_owned(), view.to_owned()],
            with_async_mode: false,
            mode: tidb_ast::RefreshMaterializedViewMode::Fast,
            observe_type: tidb_ast::RefreshMaterializedViewObserveType::None,
            as_of: None,
        },
        last_successful_refresh_read_tso,
        target_refresh_read_tso: 0,
        mlog_retained_lower_tso,
        registered_mlog_ids,
    }
}

fn registered_mlog_ids(tk: &mut Session) -> Vec<i64> {
    vec![single_value(tk, "select mlog_id from mysql.tidb_mview_fast_refresh_source_info").parse().unwrap()]
}

/// Go `requireMLogCommitTSSelectionEstRows` over the statement's plan rows.
fn require_m_log_commit_ts_selection_est_rows(rows: &[Vec<String>], commit_ts_info: &str, est_rows: &str) {
    let row = rows.iter().find(|row| row.iter().skip(2).any(|cell| cell.contains(commit_ts_info))).unwrap_or_else(|| {
        panic!("mlog commit-ts selection not found: commitTSInfo={commit_ts_info} rows={rows:?}")
    });
    assert_eq!(est_rows, row[1], "{rows:?}");
}

#[test]
fn test_explain_refresh_mv_fast_plan_uses_for_update_ts_for_unbounded_refresh() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    let start_ts =
        begin_pessimistic_over_fast_view(&mut tk, "t_mlog_commit_ts_for_update_est", "mv_mlog_commit_ts_for_update_est");
    let start_physical = extract_physical(start_ts);
    let retained_lower_tso = compose_ts(start_physical - 10000);
    let last_success_tso = compose_ts(start_physical - 5000);
    let registered = registered_mlog_ids(&mut tk);

    std::thread::sleep(std::time::Duration::from_millis(20));
    execute_refresh_operator(
        &mut tk,
        unbounded_fast_implement("mv_mlog_commit_ts_for_update_est", last_success_tso, retained_lower_tso, registered),
    );
    // Go `txnMgr.GetStmtForUpdateTS()`: the statement's for-update TS.
    let retained_upper_tso = u64::try_from(tk.current_for_update_ts().value()).unwrap();
    let retained_upper_physical = extract_physical(retained_upper_tso);
    assert!(retained_upper_physical > start_physical, "{retained_upper_physical} <= {start_physical}");
    let expected_rows = format!(
        "{:.2}",
        10000.0 * (retained_upper_physical - (start_physical - 5000)) as f64
            / (retained_upper_physical - (start_physical - 10000)) as f64
    );
    let rows = tk.last_statement_analyzed_plan_rows().expect("the statement's plan");
    require_m_log_commit_ts_selection_est_rows(
        &rows,
        "test.$mlog$t_mlog_commit_ts_for_update_est._tidb_commit_ts",
        &expected_rows,
    );
    must_exec_all(&mut tk, &["rollback"]);
}

#[test]
fn test_explain_refresh_mv_fast_plan_obtains_for_update_ts_when_not_provided() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    let start_ts =
        begin_pessimistic_over_fast_view(&mut tk, "t_mlog_commit_ts_stmt_for_update", "mv_mlog_commit_ts_stmt_for_update");
    let start_physical = extract_physical(start_ts);
    let retained_lower_tso = compose_ts(start_physical - 10000);
    let last_success_tso = compose_ts(start_physical - 5000);
    let registered = registered_mlog_ids(&mut tk);

    let (plan, _) = execute_refresh_operator(
        &mut tk,
        unbounded_fast_implement("mv_mlog_commit_ts_stmt_for_update", last_success_tso, retained_lower_tso, registered),
    );
    assert_ne!(0, tk.current_for_update_ts().value());
    assert!(plan.starts_with("MViewDeltaMerge"), "{plan}");
    must_exec_all(&mut tk, &["rollback"]);
}
