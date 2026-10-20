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

//! `executor/aggregate.test`'s GROUP BY / ORDER BY name resolution and group
//! keys: Go's `resolveFromSelectFields`, `gbyResolver`, `aggOrderByResolver`
//! and `aggregate.GetGroupKey`.

use crate::tests_support::row_text;
use crate::Session;

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// Go keys an ENUM group item by its value (issue #26885): the invalid
/// value 0 and the member '' share the name '' but are different groups.
#[test]
fn an_enum_groups_by_its_value_not_its_name() {
    let mut session = Session::new();
    session
        .run("set sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    session
        .run("create table t1 (c1 enum('a', '', 'b'))")
        .unwrap();
    for value in ["'b'", "''", "'a'", "''", "0"] {
        session
            .run(&format!("insert into t1 (c1) values ({value})"))
            .unwrap();
    }
    assert_eq!(
        row_text(session.run("select c1 + 0, count(c1) from t1 group by c1 order by c1")),
        [["0", "1"], ["1", "1"], ["2", "2"], ["3", "1"]]
    );
}

/// `resolveFromSelectFields`: a non-column alias answers at once (ORDER BY
/// `d` is `1-d as d`), and GROUP BY over two different columns aliased the
/// same name is 1052 in 'group statement', even when the source has the name.
#[test]
fn select_field_names_resolve_as_go_does() {
    let mut session = Session::new();
    session.run("create table t (c int, d int)").unwrap();
    session
        .run("insert into t values (1, -1), (1, 0), (1, 1)")
        .unwrap();
    assert_eq!(
        row_text(session.run("select d, 1-d as d, c as d from t order by d")),
        [["1", "0", "1"], ["0", "1", "1"], ["-1", "2", "1"]]
    );
    let ambiguous = (
        1052,
        "Column 'd' in group statement is ambiguous".to_owned(),
    );
    assert_eq!(
        error_of(&mut session, "select d as d, c as d from t group by d"),
        ambiguous
    );
    assert_eq!(
        error_of(&mut session, "select t.d, c as d from t group by d"),
        ambiguous
    );
}

/// `aggOrderByResolver`: a GROUP_CONCAT ORDER BY position indexes the call's
/// own arguments, and one past them is 1054 naming the literal.
#[test]
fn a_group_concat_order_position_names_its_arguments() {
    let mut session = Session::new();
    session.run("create table test (id int, name int)").unwrap();
    session
        .run("insert into test values (1, 10), (2, 20), (3, 30)")
        .unwrap();
    assert_eq!(
        row_text(
            session.run("select group_concat(name, id order by 2 desc separator '+') from test")
        ),
        [["303+202+101"]]
    );
    assert_eq!(
        error_of(
            &mut session,
            "select group_concat(name, id order by 3 desc separator '+') from test"
        ),
        (1054, "Unknown column '3' in 'order clause'".to_owned())
    );
}

/// Go `buildProjectionField` records a projected outer column's
/// `CorrelatedColUniqueID`, and `LogicalApply.ExtractFD` equates it with the
/// outer column, so the new ONLY_FULL_GROUP_BY check sees
/// `(select t1.a from t1 as t2 limit 1)` determined by `t1.pk`
/// (planner/funcdep/only_full_group_by).
#[test]
fn a_projected_outer_column_is_determined_by_the_group_key() {
    let mut session = Session::new();
    session
        .run("set @@sql_mode = 'ONLY_FULL_GROUP_BY'")
        .unwrap();
    session
        .run("set @@tidb_enable_new_only_full_group_by_check = 'on'")
        .unwrap();
    session
        .run("create table t1 (pk int primary key, a int)")
        .unwrap();
    session
        .run("insert into t1 values (1, 10), (2, 20)")
        .unwrap();
    assert_eq!(
        row_text(
            session
                .run("select (select t1.a from t1 as t2 limit 1) from t1 group by pk order by pk")
        ),
        [["10"], ["20"]]
    );
}

/// Go `parseByItems` makes a position of every by-item whose value is an
/// int64, and TRUE/FALSE are int64 1/0: `ORDER BY true desc` sorts by the
/// first field, and `ORDER BY false` names position 0 (gorun, corpus
/// `table/group_by_position`).
#[test]
fn an_order_by_boolean_literal_is_a_position() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    session
        .run("insert into t values (1,10),(2,20),(1,30)")
        .unwrap();
    assert_eq!(
        row_text(session.run("select a from t order by true desc")),
        [["2"], ["1"], ["1"]]
    );
    assert_eq!(
        error_of(&mut session, "select a from t order by false"),
        (1054, "Unknown column '0' in 'order clause'".to_owned())
    );
}
