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

//! Go `pkg/executor/prepared_test.go` (`TestPlanCacheWithDifferentVariableTypes`,
//! `TestParameterPushDown`), over Go's own `testdata/prepare_suite_{in,out}.json`,
//! on the mock store through the session, as Go's testkit runs them.

use crate::support::testkit::*;
use serde_json::Value;

const PREPARE_SUITE_IN: &str = include_str!("../../../../pkg/executor/testdata/prepare_suite_in.json");
const PREPARE_SUITE_OUT: &str = include_str!("../../../../pkg/executor/testdata/prepare_suite_out.json");

/// Go `prepareMergeSuiteData.LoadTestCases(t, &input, &output)` for `name`.
fn load_test_cases(name: &str) -> (Vec<Value>, Vec<Value>) {
    let cases = |text: &str, key: &str| -> Vec<Value> {
        let suites: Vec<Value> = serde_json::from_str(text).expect("prepare suite json");
        suites
            .into_iter()
            .find(|suite| suite.get("name").or_else(|| suite.get("Name")).and_then(Value::as_str) == Some(name))
            .and_then(|suite| suite.get(key).and_then(Value::as_array).cloned())
            .unwrap_or_else(|| panic!("no {name} cases"))
    };
    (cases(PREPARE_SUITE_IN, "cases"), cases(PREPARE_SUITE_OUT, "Cases"))
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| items.iter().map(|item| item.as_str().unwrap_or_default().to_owned()).collect())
        .unwrap_or_default()
}

/// Go `TestPlanCacheWithDifferentVariableTypes`.
#[test]
fn test_plan_cache_with_different_variable_types() {
    let mut tk = TestKit::new();
    tk.must_exec("set tidb_enable_prepared_plan_cache=1");
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("set @@tidb_enable_collect_execution_info=0;");
    tk.must_exec("create table t1(a varchar(20), b int, c float, key(b, a))");
    tk.must_exec("insert into t1 values('1',1,1.1),('2',2,222),('3',3,333)");
    tk.must_exec("create table t2(a varchar(20), b int, c float, key(b, a))");
    tk.must_exec("insert into t2 values('3',3,3.3),('2',2,222),('3',3,333)");
    let (input, output) = load_test_cases("TestPlanCacheWithDifferentVariableTypes");
    for (i, tt) in input.iter().enumerate() {
        let prepare_stmt = tt["PrepareStmt"].as_str().expect("PrepareStmt");
        tk.must_exec(prepare_stmt);
        assert_eq!(prepare_stmt, output[i]["PrepareStmt"].as_str().unwrap_or_default());
        for (j, exec) in tt["Executes"].as_array().expect("Executes").iter().enumerate() {
            let expected = &output[i]["Executes"][j];
            for v in exec["Vars"].as_array().into_iter().flatten() {
                tk.must_exec(&format!(
                    "set @{} = {}",
                    v["Name"].as_str().unwrap_or_default(),
                    v["Value"].as_str().unwrap_or_default()
                ));
            }
            let execute_sql = exec["ExecuteSQL"].as_str().expect("ExecuteSQL");
            let res = tk.must_query(execute_sql);
            let last_plan_use_cache = tk.must_query("select @@last_plan_from_cache").rows()[0][0].clone();
            tk.must_query(execute_sql);
            let plan = tk.must_query(&format!("explain for connection {}", tk.connection_id()));
            assert_eq!(execute_sql, expected["SQL"].as_str().unwrap_or_default());
            plan.check(strings(&expected["Plan"]));
            assert_eq!(exec["Vars"], expected["Vars"]);
            assert_eq!(last_plan_use_cache, expected["LastPlanUseCache"].as_str().unwrap_or_default(), "{execute_sql}");
            res.check(strings(&expected["Result"]));
        }
    }
}

/// Go `TestParameterPushDown`.
#[test]
fn test_parameter_push_down() {
    let mut tk = TestKit::new();
    tk.must_exec("set tidb_enable_prepared_plan_cache=1");
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int, c int, key(a))");
    tk.must_exec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6)");
    tk.must_exec("set @@tidb_enable_collect_execution_info=0;");
    tk.must_exec("set @x1=1,@x5=5,@x10=10,@x20=20");
    let (input, output) = load_test_cases("TestParameterPushDown");
    for (i, tt) in input.iter().enumerate() {
        let sql = tt["SQL"].as_str().expect("SQL");
        if sql.starts_with("execute") {
            let res = tk.must_query(sql).sort();
            let from_cache = tk.must_query("select @@last_plan_from_cache").rows()[0][0].clone();
            tk.must_query(sql);
            let plan = tk.must_query(&format!("explain for connection {}", tk.connection_id()));
            res.check(strings(&output[i]["Result"]));
            plan.check(strings(&output[i]["Plan"]));
            assert_eq!(from_cache, output[i]["FromCache"].as_str().unwrap_or_default(), "{sql}");
        } else {
            tk.must_exec(sql);
        }
    }
}
