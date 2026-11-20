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

//! Go master `pkg/executor/test/materializedview/materialized_view_refresh_operator_test.go`.

use super::*;

/// Go `executeRefreshOperator`: the internal statement as materialized view
/// maintenance's restricted SQL, then the root operator's analyzed plan row
/// (its id and execution info).
fn execute_refresh_operator(tk: &mut Session, stmt: tidb_ast::RefreshMaterializedViewImplementStmt) -> (String, String) {
    let implement = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::RefreshMaterializedViewImplement(Box::new(stmt))));
    let sql = implement.restore();
    let original = (tk.vars.in_mview_maintenance, tk.vars.restricted_sql);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = (true, true);
    let result = tk.node_conn().run_ordinary_write(&sql, &implement);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = original;
    result.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let rows = tk.last_statement_analyzed_plan_rows().expect("the statement's analyzed plan");
    let root = rows.first().expect("a root operator").clone();
    (root[0].clone(), root.join(" "))
}

fn implement(
    mode: tidb_ast::RefreshMaterializedViewMode,
    last_successful_refresh_read_tso: u64,
    target_refresh_read_tso: u64,
    registered_mlog_ids: Vec<i64>,
) -> tidb_ast::RefreshMaterializedViewImplementStmt {
    tidb_ast::RefreshMaterializedViewImplementStmt {
        refresh_stmt: tidb_ast::RefreshMaterializedViewStmt {
            view_name: vec!["mv_refresh_ops".to_owned()],
            with_async_mode: false,
            mode,
            observe_type: tidb_ast::RefreshMaterializedViewObserveType::None,
            as_of: None,
        },
        last_successful_refresh_read_tso,
        target_refresh_read_tso,
        mlog_retained_lower_tso: 0,
        registered_mlog_ids,
    }
}

#[test]
fn test_materialized_view_refresh_operators() {
    for fixed_read_tso in [false, true] {
        // t.Run("fixed_read_tso="+...)
        let (_stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t_refresh_ops (id int primary key, g int not null, v int, key(g))",
            "create materialized view log on t_refresh_ops (id, g, v)",
            "insert into t_refresh_ops values (1, 1, 10), (2, 1, 20), (3, 2, 30)",
            "create materialized view mv_refresh_ops (g, cnt, vcnt, s, mn, mx) refresh fast next date_add(now(), interval 1 hour) as select g, count(*), count(v), sum(v), min(v), max(v) from t_refresh_ops group by g",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv_refresh_ops order by g"), r#"RS:[["1","2","2","30","10","20"],["2","1","1","30","30","30"]]"#);
        let last_read_tso: u64 = single_value(&mut tk, "select last_success_read_tso from mysql.tidb_mview_refresh_info").parse().unwrap();
        let registered_mlog_id: i64 = single_value(&mut tk, "select mlog_id from mysql.tidb_mview_fast_refresh_source_info").parse().unwrap();
        must_exec_all(&mut tk, &[
            "delete from t_refresh_ops where id = 2",
            "update t_refresh_ops set v = 5 where id = 3",
            "insert into t_refresh_ops values (4, 1, null), (5, 3, 40)",
        ]);
        let mut target_read_tso = 0;
        if fixed_read_tso {
            must_exec_all(&mut tk, &["insert into mysql.tidb values ('tikv_gc_safe_point', '20060102-15:04:05 +0000', '')", "begin"]);
            target_read_tso = single_value(&mut tk, "select @@tidb_current_ts").parse().unwrap();
            assert_ne!(target_read_tso, 0);
            // "Recomputing MIN/MAX must use the target snapshot, even when a
            // newer value has committed before the internal maintenance
            // statement starts."
            must_exec_all(&mut tk, &["commit", "update t_refresh_ops set v = 1 where id = 3"]);
        }
        must_exec_all(&mut tk, &["begin pessimistic"]);
        let (plan, stats) = execute_refresh_operator(
            &mut tk,
            implement(tidb_ast::RefreshMaterializedViewMode::Fast, last_read_tso, target_read_tso, vec![registered_mlog_id]),
        );
        assert!(plan.starts_with("MViewDeltaMerge"), "{plan}");
        assert!(stats.contains("mview_delta_merge_agg"), "{stats}");
        assert!(stats.contains("rows:{insert:1, update:2, delete:0, noop:0}"), "{stats}");
        must_exec_all(&mut tk, &["commit"]);
        assert_eq!(
            answer(&mut tk, "select * from mv_refresh_ops order by g"),
            r#"RS:[["1","2","1","10","10","10"],["2","1","1","5","5","5"],["3","1","1","40","40","40"]]"#,
            "fixed_read_tso={fixed_read_tso}"
        );

        must_exec_all(&mut tk, &[
            "delete from t_refresh_ops where id in (1, 3)",
            "insert into t_refresh_ops values (6, 3, 60), (7, 4, 7)",
            "begin pessimistic",
        ]);
        let (plan, stats) = execute_refresh_operator(&mut tk, implement(tidb_ast::RefreshMaterializedViewMode::CompleteDeltaApply, 0, 0, Vec::new()));
        assert!(plan.starts_with("MViewCompleteDeltaApply"), "{plan}");
        assert!(stats.contains("mview_complete_delta_apply"), "{stats}");
        assert!(stats.contains("rows:{insert:1, update:2, delete:1}"), "{stats}");
        must_exec_all(&mut tk, &["commit"]);
        assert_eq!(
            answer(&mut tk, "select * from mv_refresh_ops order by g"),
            r#"RS:[["1","1","0",null,null,null],["3","2","2","100","40","60"],["4","1","1","7","7","7"]]"#,
            "fixed_read_tso={fixed_read_tso}"
        );
    }
}
