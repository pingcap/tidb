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

//! Literal assertions extracted from Go windows/window_sql_test.go.
//! Regenerate upstream.json with rust/scripts/generate-go-window-tests.py.

use crate::tests_support::cell_text;
use crate::{Session, StmtOutput, StmtResult};

fn run_suite(session: &mut Session, name: &str) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("upstream.json")).unwrap();
    for action in fixture["suites"][name].as_array().unwrap() {
        let location = format!("{name}:{}", action["line"]);
        if let Some(size) = action["chunk"].as_u64() {
            // Go writes MaxChunkSize directly, below the SQL variable's minimum.
            session
                .vars
                .restore_system(vec![("tidb_max_chunk_size".into(), Some(size.to_string()))]);
            assert_eq!(
                session.statement_context(false).executor_chunk_sizes().1,
                size as usize
            );
        } else if let Some(function) = action["nullable"].as_str() {
            let sql = format!(
                "SELECT {function} OVER (PARTITION BY p ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a FROM agg"
            );
            let StmtOutput::Rows { columns, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{location}: expected rows");
            };
            assert_eq!(
                columns[0].1.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0,
                action["flag"].as_bool().unwrap(),
                "{location}: {sql}"
            );
        } else {
            let sql = action["sql"].as_str().unwrap();
            let result = session
                .run(sql)
                .unwrap_or_else(|error| panic!("{location}: {sql}: {error:?}"));
            if let Some(expected) = action["rows"].as_array() {
                let StmtResult::Rows(rows) = result else {
                    panic!("{location}: expected rows");
                };
                let mut actual: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| {
                                if value.is_null() {
                                    "<nil>".to_owned()
                                } else {
                                    cell_text(value)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect();
                if action["sort"].as_bool().unwrap() {
                    actual.sort();
                }
                let expected: Vec<_> = expected
                    .iter()
                    .map(|row| row.as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(actual, expected, "{location}: {sql}");
            }
        }
    }
}

#[test]
fn upstream_window_functions() {
    for pipelined in [0, 1] {
        for concurrency in [1, 4] {
            let mut session = Session::new();
            session
                .run(&format!(
                    "SET tidb_enable_pipelined_window_function={pipelined}"
                ))
                .unwrap();
            session
                .run(&format!("SET tidb_window_concurrency={concurrency}"))
                .unwrap();
            run_suite(&mut session, "doTestWindowFunctions");
        }
    }
}

#[test]
fn upstream_window_data_reference() {
    for pipelined in [0, 1] {
        let mut session = Session::new();
        session
            .run(&format!(
                "SET tidb_enable_pipelined_window_function={pipelined}"
            ))
            .unwrap();
        run_suite(&mut session, "TestWindowFunctionsDataReference");
    }
}

#[test]
fn upstream_sliding_window_functions() {
    for pipelined in [0, 1] {
        for data_type in ["FLOAT", "DOUBLE"] {
            for precision in ["ON", "OFF"] {
                let mut session = Session::new();
                session
                    .run(&format!(
                        "SET tidb_enable_pipelined_window_function={pipelined}"
                    ))
                    .unwrap();
                session
                    .run(&format!("SET windowing_use_high_precision={precision}"))
                    .unwrap();
                session
                    .run(&format!("CREATE TABLE t (id {data_type}, sex CHAR(1))"))
                    .unwrap();
                run_suite(&mut session, "baseTestSlidingWindowFunctions");
            }
        }
    }
}

#[test]
fn upstream_window_nullable_and_empty_input() {
    for name in ["TestIssue45964And46050", "TestVarSampAsAWindowFunction"] {
        run_suite(&mut Session::new(), name);
    }
}

/// Go window_executor_test.go: TestWindowExecutorsBasic and
/// TestWindowReturnColumnNullableAttribute.
#[test]
fn upstream_window_executor_basic_and_nullable() {
    use crate::tests_support::row_text;
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(1,2),(2,1),(2,2)")
        .unwrap();
    for pipelined in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_pipelined_window_function={pipelined}"
            ))
            .unwrap();
        assert_eq!(row_text(session.run("SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM t ORDER BY a, rn")), [["1", "1"], ["1", "2"], ["2", "1"], ["2", "2"]]);
        assert_eq!(row_text(session.run("SELECT a, SUM(b) OVER (ORDER BY a,b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t")), [["1", "1"], ["1", "3"], ["2", "3"], ["2", "3"]]);
    }
    session
        .run("CREATE TABLE agg (p INT NOT NULL, o INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    session
        .run("INSERT INTO agg VALUES (0,0,1),(1,1,2),(1,2,3),(1,3,4)")
        .unwrap();
    for (function, nullable) in [
        ("SUM(v)", true),
        ("COUNT(v)", false),
        ("ROW_NUMBER()", false),
        ("RANK()", false),
        ("DENSE_RANK()", false),
    ] {
        let sql = format!(
            "SELECT {function} OVER (PARTITION BY p ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a FROM agg"
        );
        let StmtOutput::Rows { columns, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            columns[0].1.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0,
            nullable,
            "{sql}"
        );
    }
}
