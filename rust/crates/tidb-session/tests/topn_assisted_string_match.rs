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

// aggregate-test: standalone
//! Go's `TestTopNAssistedEstimationWithoutNewCollation` and
//! `TestTopNAssistedEstimationWithNewCollation` change a process-global
//! collation mode. `TestCollationColumnEstimate` also requires the new mode.
//! Keep these fixtures in one test process so the temporary mode cannot leak
//! into other session tests.

use serde_json::Value;
use tidb_datatype::{new_collation_enabled, set_new_collation_enabled};
use tidb_session::{Session, StmtResult};

const GO_INPUT: &str =
    include_str!("../../../../pkg/planner/cardinality/testdata/cardinality_suite_in.json");
const GO_OUTPUT: &str =
    include_str!("../../../../pkg/planner/cardinality/testdata/cardinality_suite_out.json");

struct RestoreCollationMode(bool);

impl Drop for RestoreCollationMode {
    fn drop(&mut self) {
        set_new_collation_enabled(self.0);
    }
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|datum| {
                        String::from_utf8_lossy(
                            &datum.to_bytes().expect("EXPLAIN cell is wire encodable"),
                        )
                        .into_owned()
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn fixture_cases<'a>(fixture: &'a Value, section_name: &str, key: &str) -> &'a [Value] {
    fixture
        .as_array()
        .unwrap()
        .iter()
        .find(|section| {
            section["name"].as_str() == Some(section_name)
                || section["Name"].as_str() == Some(section_name)
        })
        .unwrap_or_else(|| panic!("missing Go fixture section {section_name}"))[key]
        .as_array()
        .unwrap()
}

fn run_collation_column_fixture(input: &Value, output: &Value) {
    set_new_collation_enabled(true);
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE t(a VARCHAR(20) COLLATE utf8mb4_general_ci)",
        "INSERT INTO t VALUES ('aaa'), ('bbb'), ('AAA'), ('BBB')",
        "SET tidb_analyze_version = 2",
        "ANALYZE TABLE t ALL COLUMNS",
        "EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 'aaa'",
    ] {
        session.run(sql).unwrap();
    }
    let inputs = fixture_cases(input, "TestCollationColumnEstimate", "cases");
    let outputs = fixture_cases(output, "TestCollationColumnEstimate", "Cases");
    assert_eq!(inputs.len(), 3);
    assert_eq!(inputs.len(), outputs.len());
    for (sql, expected) in inputs.iter().zip(outputs) {
        let sql = sql.as_str().unwrap();
        let actual = rows(&mut session, sql);
        let actual = actual.iter().map(|row| row.join(" ")).collect::<Vec<_>>();
        let expected = expected
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{sql}");
    }
}

fn run_topn_fixture(enabled: bool, input: &Value, output: &Value) {
    set_new_collation_enabled(enabled);
    let mut session = Session::new();
    session
        .run("SET tidb_default_string_match_selectivity = 0")
        .unwrap();
    session.run("SET tidb_stats_load_sync_wait = 3000").unwrap();
    session
        .run(
            "CREATE TABLE t(
                a VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
                b VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                c VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                d VARCHAR(100) CHARACTER SET gbk COLLATE gbk_bin,
                e VARCHAR(100) CHARACTER SET gbk COLLATE gbk_chinese_ci,
                f VARBINARY(100)
            )",
        )
        .unwrap();

    let mut values = Vec::with_capacity(40);
    for _ in 0..10 {
        for value in ["111abc111", "111cba111", "111234111"] {
            values.push(format!(
                "('{value}', '{value}', '{value}', '{value}', '{value}', '{value}')"
            ));
        }
    }
    values.extend((0..3).map(|_| "(NULL, NULL, NULL, NULL, NULL, NULL)".to_owned()));
    values.extend(
        [
            "tttttt", "uuuuuu", "vvvvvv", "wwwwww", "xxxxxx", "yyyyyy", "zzzzzz",
        ]
        .into_iter()
        .map(|value| format!("('{value}', '{value}', '{value}', '{value}', '{value}', '{value}')")),
    );
    assert_eq!(values.len(), 40);
    session
        .run(&format!("INSERT INTO t VALUES {}", values.join(",")))
        .unwrap();
    session
        .run("ANALYZE TABLE t ALL COLUMNS WITH 3 TOPN")
        .unwrap();

    let section_name = if enabled {
        "TestTopNAssistedEstimationWithNewCollation"
    } else {
        "TestTopNAssistedEstimationWithoutNewCollation"
    };
    let input_cases = fixture_cases(input, section_name, "cases");
    let output_cases = fixture_cases(output, section_name, "Cases");
    assert_eq!(input_cases.len(), 28);
    assert_eq!(output_cases.len(), input_cases.len());

    for (input_case, output_case) in input_cases.iter().zip(output_cases) {
        let go_sql = input_case.as_str().unwrap();
        assert_eq!(go_sql, output_case["SQL"].as_str().unwrap());
        let sql = go_sql
            .strip_prefix("explain format = 'brief' ")
            .expect("Go TopN fixture uses brief EXPLAIN");
        let actual = rows(&mut session, &format!("EXPLAIN {sql}"));
        let expected = output_case["Result"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{go_sql}");

        for (actual_row, expected_line) in actual.iter().zip(expected) {
            let actual_words = actual_row
                .iter()
                .flat_map(|column| column.split_whitespace())
                .map(|word| {
                    if let Some((operator, id)) = word.rsplit_once('_') {
                        if !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()) {
                            return operator.to_owned();
                        }
                    }
                    word.to_owned()
                })
                .collect::<Vec<_>>();
            // EXPLAIN's generated plan IDs are runtime allocation details;
            // Go's text format omits them, so compare the semantic operator.
            let expected_words = expected_line
                .as_str()
                .unwrap()
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            assert_eq!(
                actual_words, expected_words,
                "{go_sql}; Rust EXPLAIN row: {actual_row:?}"
            );
        }
    }
}

#[test]
fn topn_assisted_string_match_estimates_match_go_in_both_collation_modes() {
    let _restore = RestoreCollationMode(new_collation_enabled());
    let input: Value = serde_json::from_str(GO_INPUT).unwrap();
    let output: Value = serde_json::from_str(GO_OUTPUT).unwrap();

    run_topn_fixture(false, &input, &output);
    run_topn_fixture(true, &input, &output);
    run_collation_column_fixture(&input, &output);
}
