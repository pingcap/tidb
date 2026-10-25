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

//! Go `pkg/planner/util/fixcontrol::TestFixControl` through Rust's real
//! session-variable writer. The two included JSON artifacts are unchanged Go
//! testdata; every SQL case checks the raw variable, parsed map, typed getters,
//! exact error, and `SHOW WARNINGS` row.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::{
    tests_support::{row_text, store_requests},
    Session,
};

#[derive(Deserialize)]
struct InputSuite {
    name: String,
    cases: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
struct ExpectedFix {
    value_in_map: String,
    get_str: String,
    get_bool: bool,
    get_int: i64,
    get_float: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputCase {
    #[serde(rename = "SQL")]
    sql: String,
    fix_control: BTreeMap<String, ExpectedFix>,
    error: String,
    warnings: Vec<Vec<String>>,
    variable: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputSuite {
    name: String,
    cases: Vec<OutputCase>,
}

#[test]
fn go_fix_control_fixture_runs_through_the_session_writer() {
    let input: Vec<InputSuite> = serde_json::from_str(include_str!(
        "../../../../pkg/planner/util/fixcontrol/testdata/fix_control_suite_in.json"
    ))
    .expect("the pinned Go input fixture is valid JSON");
    let output: Vec<OutputSuite> = serde_json::from_str(include_str!(
        "../../../../pkg/planner/util/fixcontrol/testdata/fix_control_suite_out.json"
    ))
    .expect("the pinned Go output fixture is valid JSON");
    assert_eq!(input.len(), 1);
    assert_eq!(output.len(), 1);
    assert_eq!(input[0].name, output[0].name);
    assert_eq!(input[0].cases.len(), output[0].cases.len());

    let mut session = Session::new();
    for (sql, expected) in input[0].cases.iter().zip(&output[0].cases) {
        session
            .run("SET @@tidb_opt_fix_control = ''")
            .expect("the package test resets the control before every case");
        assert_eq!(sql, &expected.sql);

        let actual_error = match session.run(sql) {
            Ok(_) => String::new(),
            Err(error) => {
                let mysql = error.to_mysql_error();
                assert_eq!(mysql.code, 1105, "{sql}");
                assert_eq!(mysql.state, *b"HY000", "{sql}");
                mysql.message
            }
        };
        assert_eq!(actual_error, expected.error, "{sql}");
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            expected.warnings,
            "{sql}"
        );

        let raw: Vec<String> = row_text(session.run("SELECT @@tidb_opt_fix_control"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(raw, expected.variable, "{sql}");

        let controls = session.vars().optimizer_fix_control();
        assert_eq!(controls.as_map().len(), expected.fix_control.len(), "{sql}");
        for (key, expected_fix) in &expected.fix_control {
            let key = key
                .parse::<u64>()
                .expect("fixture map keys are issue numbers");
            let actual = ExpectedFix {
                value_in_map: controls.get_str(key).unwrap_or_default().to_owned(),
                get_str: controls.get_str_with_default(key, "default").to_owned(),
                get_bool: controls.get_bool_with_default(key, false),
                get_int: controls.get_int_with_default(key, 12345),
                get_float: controls.get_float_with_default(key, 1234.5),
            };
            assert_eq!(&actual, expected_fix, "{sql}; fix {key}");
        }
    }
}

#[test]
fn statement_set_var_is_first_wins_warns_on_invalid_and_covers_dml() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_opt_fix_control = '52592:OFF'")
        .unwrap();

    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
             @@tidb_opt_fix_control"
        )),
        [["52592:ON"]]
    );
    assert_eq!(
        row_text(session.run("SELECT @@tidb_opt_fix_control")),
        [["52592:OFF"]]
    );
    assert_eq!(
        session.vars().optimizer_fix_control().get_bool(52592),
        Some(false)
    );

    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='invalid') */ \
             @@tidb_opt_fix_control"
        )),
        [["52592:OFF"]]
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "invalid fix control: expected colon not found"
        ]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(no_such_fix_variable='x') \
             SET_VAR(no_such_fix_variable='y') */ 1"
        )),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [
            [
                "Warning",
                "3128",
                "Unresolved name 'no_such_fix_variable' for SET_VAR hint"
            ],
            [
                "Warning",
                "3128",
                "Unresolved name 'no_such_fix_variable' for SET_VAR hint"
            ]
        ],
        "Go validates each unknown name before duplicate detection"
    );
    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='invalid') \
             SET_VAR(tidb_opt_fix_control='52592:ON') */ @@tidb_opt_fix_control"
        )),
        [["52592:OFF"]],
        "the first invalid value still occupies Go's first-hint-wins slot"
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [
            [
                "Warning",
                "3126",
                "Hint SET_VAR(tidb_opt_fix_control=52592:ON) is ignored as conflicting/duplicated."
            ],
            [
                "Warning",
                "1105",
                "invalid fix control: expected colon not found"
            ]
        ],
        "Go parses duplicate hints before it validates and applies the first value"
    );
    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:OFF') \
             SET_VAR(tidb_opt_fix_control='52592:ON') */ @@tidb_opt_fix_control"
        )),
        [["52592:OFF"]],
        "the first valid value also wins"
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "3126",
            "Hint SET_VAR(tidb_opt_fix_control=52592:ON) is ignored as conflicting/duplicated."
        ]]
    );

    session
        .run(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ * \
             FROM missing_fix_control_table",
        )
        .unwrap_err();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_opt_fix_control")),
        [["52592:OFF"]]
    );
    assert_eq!(
        session.vars().optimizer_fix_control().get_bool(52592),
        Some(false),
        "the parsed authority is restored even when the hinted statement fails"
    );

    session
        .run("CREATE TABLE hint_dml (id BIGINT PRIMARY KEY, v VARCHAR(64))")
        .unwrap();
    session
        .run("INSERT INTO hint_dml VALUES (1,''),(2,'')")
        .unwrap();
    session
        .run(
            "UPDATE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ hint_dml \
             SET v='updated' WHERE id=1",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT v FROM hint_dml WHERE id=1")),
        [["updated"]]
    );
    session
        .run(
            "DELETE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ FROM hint_dml \
             WHERE id=2",
        )
        .unwrap();
    assert!(row_text(session.run("SELECT id FROM hint_dml WHERE id=2")).is_empty());

    assert_eq!(
        row_text(session.run("SELECT @@tidb_opt_fix_control")),
        [["52592:OFF"]],
        "every AST-owned overlay is restored at statement end"
    );
}

fn plan_has(session: &mut Session, sql: &str, operator: &str) -> bool {
    row_text(session.run(sql))
        .iter()
        .any(|row| row[0].contains(operator))
}

#[test]
fn fix_52592_disables_point_and_batch_paths_for_select_update_and_delete() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE fix_path (a BIGINT PRIMARY KEY, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO fix_path VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
        .run("CREATE TABLE fix_unsigned (a BIGINT UNSIGNED PRIMARY KEY)")
        .unwrap();
    session
        .run(
            "CREATE TABLE fix_unique (a BIGINT PRIMARY KEY, b BIGINT, c BIGINT, \
             UNIQUE KEY idx_bc(b,c))",
        )
        .unwrap();
    session
        .run("INSERT INTO fix_unique VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
        .run("SET @@tidb_opt_fix_control = '52592:OFF'")
        .unwrap();

    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_path WHERE a=1",
        "Point_Get"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_path WHERE a IN (1,2)",
        "Batch_Point_Get"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN UPDATE fix_path SET b=b+1 WHERE a=1",
        "Point_Get"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN DELETE FROM fix_path WHERE a=1",
        "Point_Get"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_unique WHERE b=2 AND c=2",
        "Point_Get"
    ));

    session
        .run("SET @@tidb_opt_fix_control = '52592:ON'")
        .unwrap();
    for sql in [
        "EXPLAIN SELECT * FROM fix_path WHERE a=1",
        "EXPLAIN SELECT * FROM fix_path WHERE 1=a",
        "EXPLAIN SELECT * FROM fix_path WHERE a IN (1,2)",
        "EXPLAIN UPDATE fix_path SET b=b+1 WHERE a=1",
        "EXPLAIN DELETE FROM fix_path WHERE a=1",
    ] {
        assert!(plan_has(&mut session, sql, "TableRangeScan"), "{sql}");
        assert!(!plan_has(&mut session, sql, "Point_Get"), "{sql}");
    }
    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT a FROM fix_unsigned WHERE a=-1",
        "TableDual"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_path WHERE a>1",
        "TableRangeScan"
    ));
    assert!(plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_unique WHERE b=2 AND c=2",
        "IndexRangeScan"
    ));
    assert!(!plan_has(
        &mut session,
        "EXPLAIN SELECT * FROM fix_unique WHERE b=2 AND c=2",
        "Point_Get"
    ));

    session
        .run("SET @@tidb_opt_fix_control = '52592:OFF'")
        .unwrap();
    for sql in [
        "EXPLAIN SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
         * FROM fix_path WHERE a=1",
        "EXPLAIN UPDATE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
         fix_path SET b=b+1 WHERE a=1",
        "EXPLAIN DELETE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
         FROM fix_path WHERE a=1",
    ] {
        assert!(plan_has(&mut session, sql, "TableRangeScan"), "{sql}");
        assert!(!plan_has(&mut session, sql, "Point_Get"), "{sql}");
    }
    assert_eq!(
        row_text(session.run("SELECT @@tidb_opt_fix_control")),
        [["52592:OFF"]]
    );

    let (rows, point) = store_requests(&mut session, |session| {
        session.run("SELECT b FROM fix_path WHERE a=1")
    });
    assert_eq!(row_text(rows), [["1"]]);
    assert_eq!((point.gets, point.coprocessor), (1, 0));
    // Go's BatchPointGet is ONE BatchGet request.
    let (rows, batch) = store_requests(&mut session, |session| {
        session.run("SELECT a FROM fix_path WHERE a IN (1,2)")
    });
    assert_eq!(row_text(rows), [["1"], ["2"]]);
    assert_eq!((batch.batch_gets, batch.coprocessor), (1, 0));
    let (updated, point_update) = store_requests(&mut session, |session| {
        session.run("UPDATE fix_path SET c=c WHERE a=3")
    });
    updated.unwrap();
    assert_eq!(point_update.coprocessor, 0);

    // Fix 52592 turns the point plans into table readers: no point get, a
    // coprocessor request instead.
    session
        .run("SET @@tidb_opt_fix_control = '52592:ON'")
        .unwrap();
    let (rows, range) = store_requests(&mut session, |session| {
        session.run("SELECT b FROM fix_path WHERE a=1")
    });
    assert_eq!(row_text(rows), [["1"]]);
    assert_eq!(range.gets, 0);
    assert!(range.coprocessor > 0, "{range:?}");
    let (rows, batch_ranges) = store_requests(&mut session, |session| {
        session.run("SELECT a FROM fix_path WHERE a IN (1,2)")
    });
    assert_eq!(row_text(rows), [["1"], ["2"]]);
    assert_eq!(batch_ranges.batch_gets, 0);
    assert!(batch_ranges.coprocessor > 0, "{batch_ranges:?}");
    let (updated, update_range) = store_requests(&mut session, |session| {
        session.run("UPDATE fix_path SET b=b+10 WHERE a=1")
    });
    updated.unwrap();
    assert!(update_range.coprocessor > 0, "{update_range:?}");
    assert_eq!(
        row_text(session.run("SELECT b FROM fix_path WHERE a=1")),
        [["11"]]
    );

    session
        .run("SET @@tidb_opt_fix_control = '52592:OFF'")
        .unwrap();
    let (updated, hinted_update_range) = store_requests(&mut session, |session| {
        session.run(
            "UPDATE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
             fix_path SET c=9 WHERE a=2",
        )
    });
    updated.unwrap();
    assert!(
        hinted_update_range.coprocessor > 0,
        "{hinted_update_range:?}"
    );
    let (deleted, hinted_delete_range) = store_requests(&mut session, |session| {
        session.run(
            "DELETE /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ \
             FROM fix_path WHERE a=3",
        )
    });
    assert!(
        hinted_delete_range.coprocessor > 0,
        "{hinted_delete_range:?}"
    );
    deleted.unwrap();
    assert_eq!(
        row_text(session.run("SELECT a,c FROM fix_path ORDER BY a")),
        [["1", "1"], ["2", "9"]]
    );
    assert_eq!(
        row_text(session.run("SELECT @@tidb_opt_fix_control")),
        [["52592:OFF"]]
    );
}
