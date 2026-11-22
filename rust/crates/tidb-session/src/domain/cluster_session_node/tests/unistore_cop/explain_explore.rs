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

//! Go `pkg/bindinfo/binding_auto_test.go`: `EXPLAIN EXPLORE` and the planner
//! hooks of its plan generation.

use super::sql_kit::{error, exec_all, q, session};
use super::*;

fn query_of(sql: &str) -> tidb_ast::QueryStmt {
    match tidb_parser::parse(sql).expect("the SQL parses") {
        tidb_ast::Stmt::Query(query) => query.as_ref().clone(),
        other => panic!("{sql}: not a query: {other:?}"),
    }
}

/// Go `bindingAutoSuiteData.LoadTestCases`: the inputs and recorded outputs
/// of one case of `binding_auto_suite_{in,out}.json`.
fn suite_case(name: &str) -> (Vec<String>, Vec<serde_json::Value>) {
    let input: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../testdata/binding_auto_suite_in.json")).unwrap();
    let output: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../testdata/binding_auto_suite_out.json")).unwrap();
    let cases = input
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| case.as_str().unwrap().to_owned())
        .collect();
    let outputs = output
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["Name"] == name)
        .unwrap()["Cases"]
        .as_array()
        .unwrap()
        .clone();
    (cases, outputs)
}

#[test]
fn test_gen_plan_with_sctx() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t1 (a int, b int, c int, key(a), key(b))",
            "create table t2 (a int, b int, c int, key(a), key(b))",
            "set tidb_cost_model_version = 2",
        ],
    );
    let mut check = |tk: &mut Session, sql: &str, expected_hint: &str, expected_plan: &str| {
        let (plan_digest, plan_hint, plan_text) = tk
            .plan_explore_query(tidb_executor::gen_brief_plan, &query_of(sql))
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(!plan_digest.is_empty(), "sql: {sql}");
        assert!(plan_hint.contains(expected_hint), "sql: {sql}: {plan_hint}");
        let operators: Vec<&str> = plan_text.iter().map(|row| row[0].as_str()).collect();
        assert!(operators.join(",").contains(expected_plan), "sql: {sql}: {operators:?}");
    };
    check(&mut tk, "select count(1) from t1 where a=1", "stream_agg", "StreamAgg");

    exec_all(&mut tk, &["set tidb_opt_stream_agg_cost_factor = 10000"]);
    check(&mut tk, "select count(1) from t1 where a=1", "hash_agg", "HashAgg");
    exec_all(&mut tk, &["set tidb_opt_stream_agg_cost_factor = 1"]);

    check(&mut tk, "select * from t1, t2 where t1.a=t2.a and t2.b=1", "inl_hash_join", "IndexHashJoin");

    exec_all(
        &mut tk,
        &["set tidb_opt_index_join_cost_factor = 100000", "set tidb_opt_hash_join_cost_factor = 100000"],
    );
    check(&mut tk, "select * from t1, t2 where t1.a=t2.a and t2.b=1", "merge_join", "MergeJoin");
}

#[test]
fn test_explain_explore_basic() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    let check = |tk: &mut Session, sql: &str, expected_row_count: usize| {
        let rows = q(tk, sql);
        assert_eq!(rows.len(), expected_row_count, "sql: {sql}");
        for row in &rows {
            assert!(!row[3].is_empty(), "sql: {sql}: empty plan digest");
        }
    };

    exec_all(&mut tk, &["create table t (a int, b int, c varchar(10), key(a))"]);
    check(&mut tk, "explain explore select a from t where b=1", 1);
    exec_all(&mut tk, &["create global binding using select a from t where b=1"]);
    check(&mut tk, "explain explore select a from t where b=1", 2);
    check(&mut tk, "explain explore SELECT a FROM t WHERE b=1", 2);
    check(&mut tk, "explain explore SELECT a FROM t WHERE b= 1", 2);
    check(&mut tk, "explain explore      SELECT  a FROM test.t WHERE b= 1", 2);
    assert!(
        q(&mut tk, r#"explain explore "23109784b802bcef5398dd81d3b1c5b79200c257c101a5b9f90758206f3d09ed""#).len() >= 1
    );

    check(&mut tk, "explain explore select a from t where b in (1, 2, 3)", 1);
    exec_all(&mut tk, &["create global binding using select a from t where b in (1, 2, 3)"]);
    check(&mut tk, "explain explore select a from t where b in (1, 2, 3)", 2);
    check(&mut tk, "explain explore select a from t where b in (1, 2)", 2);
    check(&mut tk, "explain explore select a from t where b in (1)", 2);
    check(&mut tk, "explain explore SELECT a from t WHere b in (1)", 2);

    check(&mut tk, "explain explore select a from t where c = ''", 1);
    exec_all(&mut tk, &["create global binding using select a from t where c = ''"]);
    check(&mut tk, "explain explore select a from t where c = ''", 2);
    check(&mut tk, "explain explore select a from t where c = '123'", 2);
    check(&mut tk, r#"explain explore select a from t where c = '\"'"#, 2);
    check(&mut tk, "explain explore select a from t where c = '              '", 2);
    check(&mut tk, r#"explain explore select a from t where c = """#, 2);
    check(&mut tk, r#"explain explore select a from t where c = "\'""#, 2);

    error(&mut tk, "explain explore 'xxx'");
    error(&mut tk, "explain explore SELECT A FROM");
}

fn explore_plans_have_indexes(tk: &mut Session, sql: &str) {
    let rows = q(tk, sql);
    let has_index_a = rows.iter().any(|row| row[2].contains("index:a"));
    let has_index_b = rows.iter().any(|row| row[2].contains("index:b"));
    assert!(has_index_a, "expected index a plan in explain explore output: {rows:?}");
    assert!(has_index_b, "expected index b plan in explain explore output: {rows:?}");
}

#[test]
fn test_explain_explore_index_hints() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table t (a int, b int, c int, key(a), key(b))"]);
    explore_plans_have_indexes(&mut tk, "explain explore select * from t where a=1 and b=1");
}

#[test]
fn test_explain_explore_index_hint_with_alias() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table t (a int, b int, x varchar(10), key(a), key(b))"]);
    explore_plans_have_indexes(
        &mut tk,
        r#"explain explore select 1 from t t_alias where a=1 and b=1 and x like "%xx%""#,
    );
}

#[test]
fn test_explain_explore_no_decorrelate_hint() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table o (a int, b int, c int, d int, key(b))",
            "create table r (a int, b int, key(a), key(b))",
            "create table o1 (a int, key(a))",
        ],
    );
    let rows = q(
        &mut tk,
        "explain explore select o.* from o where exists (select 1 from r inner join o1 on o1.a=r.a where r.b=o.b)",
    );
    assert!(
        rows.iter().any(|row| row[1].contains("no_decorrelate")),
        "expected no_decorrelate plan in explain explore output: {rows:?}"
    );
}

#[test]
fn test_relevant_opt_vars_and_fixes() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t1 (a int, b int, c varchar(10), key(a), key(b))",
            "create table t2 (a int, b int, c varchar(10), key(a), key(b))",
        ],
    );
    let (input, output) = suite_case("TestRelevantOptVarsAndFixes");
    for (sql, expected) in input.iter().zip(&output) {
        let (vars, fixes) = tk
            .plan_explore_query(tidb_executor::record_relevant_opt_vars_and_fixes, &query_of(sql))
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let mut expected_vars = expected["Vars"].as_str().unwrap().to_owned();
        if sql == "select a from t1 where a=1 order by b limit 10" {
            // Register row R05: Go's findBestTask compares the TopN-pushed
            // index-lookup cop task through getTaskPlanCost, pricing its
            // row-id scan without isChildOfINL (a range scan); this planner
            // compares root tasks only, so that pricing call never happens.
            expected_vars = expected_vars.replace(" tidb_opt_table_range_scan_cost_factor", "");
        }
        assert_eq!(format!("[{}]", vars.join(" ")), expected_vars, "sql: {sql}");
        let fixes: Vec<String> = fixes.iter().map(ToString::to_string).collect();
        assert_eq!(format!("[{}]", fixes.join(" ")), expected["Fixes"].as_str().unwrap(), "sql: {sql}");
    }
}

#[test]
fn test_relevant_opt_vars_correlate_subquery() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table t1 (a int, b int, key(a))", "create table t2 (a int, b int, key(a))"]);
    let sql = "select * from t1 where a in (select a from t2)";
    // The alternative logical plans variable is relevant because the code
    // path where it affects plan choice (correlate-to-Apply) was reached.
    for enabled in ["OFF", "ON"] {
        exec_all(&mut tk, &[&format!("set tidb_opt_enable_alternative_logical_plans = {enabled}")]);
        let (vars, _) = tk
            .plan_explore_query(tidb_executor::record_relevant_opt_vars_and_fixes, &query_of(sql))
            .unwrap();
        assert!(
            vars.iter().any(|name| name == tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS),
            "enabled={enabled}: expected the variable in recorded vars {vars:?}"
        );
    }
}

#[test]
fn test_explain_explore_analyze() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table t (a int, b int, key(a))", "insert into t values (1, 2), (2, 3), (3, 4), (4, 5)"]);
    let check_exec_info = |tk: &mut Session, sql: &str, has_exec_info: bool| {
        for row in q(tk, sql) {
            let (latency, exec_times, ret_rows) = (&row[4], &row[5], &row[7]);
            if has_exec_info {
                assert_ne!(latency, "0", "sql: {sql}");
                assert_ne!(exec_times, "0", "sql: {sql}");
                assert_ne!(ret_rows, "0", "sql: {sql}");
            } else {
                assert_eq!(latency, "0", "sql: {sql}");
                assert_eq!(exec_times, "0", "sql: {sql}");
                assert_eq!(ret_rows, "0", "sql: {sql}");
            }
        }
    };
    check_exec_info(&mut tk, "explain explore select * from t where a=1", false);
    check_exec_info(&mut tk, "explain explore analyze select * from t where a=1", true);
    check_exec_info(&mut tk, "explain explore select * from t where b<10", false);
    check_exec_info(&mut tk, "explain explore analyze select * from t where b<10", true);
    check_exec_info(&mut tk, "explain explore select count(1) from t where b<10", false);
    check_exec_info(&mut tk, "explain explore analyze select count(1) from t where b<10", true);
}

#[test]
fn test_explain_explore_verify_and_bind() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(&mut tk, &["create table t (a int, b int, key(a))", "insert into t values (1, 2), (2, 3), (3, 4), (4, 5)"]);

    q(&mut tk, "select * from t");
    assert_eq!(q(&mut tk, "select @@last_plan_from_binding"), [["0"]]);
    assert!(q(&mut tk, "show global bindings").is_empty()); // no binding

    let rows = q(&mut tk, "explain explore select * from t");
    let run_stmt = rows[0][12].clone(); // "EXPLAIN ANALYZE <bind_sql>"
    let binding_sql = rows[0][13].clone(); // "CREATE GLOBAL BINDING USING <bind_sql>"
    assert!(run_stmt.starts_with("EXPLAIN ANALYZE"), "{run_stmt}");
    assert!(binding_sql.starts_with("CREATE GLOBAL BINDING USING"), "{binding_sql}");

    let rows = q(&mut tk, &run_stmt);
    assert!(rows[0][0].contains("TableReader"), "table scan and no error: {rows:?}");

    exec_all(&mut tk, &[&binding_sql]);
    q(&mut tk, "select * from t");
    assert_eq!(q(&mut tk, "select @@last_plan_from_binding"), [["1"]]);
    assert_eq!(q(&mut tk, "show global bindings").len(), 1);
}

#[test]
fn test_plan_generation() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t (a int, b int, c int, key(a))",
            "create table t1 (a int, b int, c int, key(a), key(b))",
            "create table t2 (a int, b int, c int, key(a), key(b))",
            "create table t3 (a int, b int, c int, key(a), key(b))",
        ],
    );
    let (input, output) = suite_case("TestPlanGeneration");
    for (sql, expected) in input.iter().zip(&output) {
        let expected: Vec<Vec<String>> = expected["Plan"]
            .as_array()
            .unwrap()
            .iter()
            .map(|plan| plan.as_array().unwrap().iter().map(|line| line.as_str().unwrap().to_owned()).collect())
            .collect();
        let rows = q(&mut tk, sql);
        let plans: Vec<Vec<String>> = rows
            .iter()
            .map(|row| row[2].replace('\t', "  ").split('\n').map(str::to_owned).collect())
            .collect();
        assert_eq!(plans, expected, "sql: {sql}");
    }
}
