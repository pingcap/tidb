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

//! Expressions over a `VECTOR` column, as a Go testkit session answers them.

use crate::tests_support::*;
use crate::*;

fn vector_table() -> Session {
    let mut session = Session::new();
    session.run("create table vt(v vector, b int, s varchar(20))").unwrap();
    session
        .run("insert into vt values ('[1,2]',1,'[3,4]'),('[]',2,'[]'),(NULL,3,NULL)")
        .unwrap();
    session
}

/// Go's function classes over a vector argument: `+`, `-` and `*` are
/// element-wise when both operands are vectors (`WrapWithCastAsVectorFloat32`),
/// `IS [NOT] TRUE|FALSE` reads an empty vector as false, and a vector is a
/// condition through `istrue_with_null`.
#[test]
fn vector_expressions_answer_as_go() {
    let mut session = vector_table();
    for (sql, want) in [
        ("select v - v from vt", vec![vec!["[0,0]"], vec!["[]"], vec!["NULL"]]),
        (
            "select v + v, v * v from vt",
            vec![vec!["[2,4]", "[1,4]"], vec!["[]", "[]"], vec!["NULL", "NULL"]],
        ),
        (
            "select v is true, v is false, v is not true, v is not false from vt",
            vec![vec!["1", "0", "0", "1"], vec!["0", "1", "1", "0"], vec!["0", "0", "1", "1"]],
        ),
        ("select any_value(v) from vt", vec![vec!["[1,2]"], vec!["[]"], vec!["NULL"]]),
        ("select cast(s as vector) from vt", vec![vec!["[3,4]"], vec!["[]"], vec!["NULL"]]),
        ("select cast(v as char(3)) from vt", vec![vec!["[1,"], vec!["[]"], vec!["NULL"]]),
        ("select cast(v as binary) from vt", vec![vec!["[1,2]"], vec!["[]"], vec!["NULL"]]),
        ("select v and 1 from vt", vec![vec!["1"], vec!["0"], vec!["NULL"]]),
        ("select * from vt where v", vec![vec!["[1,2]", "1", "[3,4]"]]),
    ] {
        assert_eq!(row_text(session.run(sql)), want, "{sql}");
    }
}

/// The calls Go refuses over a vector, each with Go's text: a cast Go has no
/// signature for fails when it is evaluated, before its argument is read, and
/// `NOT` when it is built.
#[test]
fn vector_expressions_fail_as_go() {
    let mut session = vector_table();
    let mut wrong = Vec::new();
    for (sql, want) in [
        ("select v / 2 from vt", "cannot cast from vector to double"),
        ("select v % 2 from vt", "cannot cast from vector to double"),
        ("select 2 / v from vt", "cannot cast from vector to double"),
        ("select v div 2 from vt", "cannot cast from vector to decimal"),
        ("select -v from vt", "cannot cast from vector to double"),
        ("select v + 1 from vt", "cannot cast from bigint to vector"),
        ("select v * 2 from vt", "cannot cast from bigint to vector"),
        ("select cast(v as signed) from vt", "cannot cast from vector to bigint"),
        ("select cast(v as double) from vt", "cannot cast from vector to double"),
        ("select cast(v as json) from vt", "cannot cast from vector to json"),
        ("select cast(b as vector) from vt", "cannot cast from int to vector"),
        (
            "select cast(v as char), cast(v as vector(2)) from vt",
            "vector has 0 dimensions, does not fit VECTOR(2)",
        ),
        ("select cast(v as vector(3)) from vt", "vector has 2 dimensions, does not fit VECTOR(3)"),
        ("select not v from vt", "VectorFloat32 is not supported for unary not operator"),
    ] {
        let got = session.run(sql).map(|_| ()).map_err(|error| error.to_mysql_error().message);
        if got != Err(want.to_owned()) {
            wrong.push(format!("{sql}: Go {want}, got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // An empty table reads no row: only the build-time refusal remains.
    session.run("delete from vt").unwrap();
    assert!(row_text(session.run("select v / 2, -v, cast(v as json) from vt")).is_empty());
    assert_err(
        session.run("select not v from vt"),
        1105,
        "VectorFloat32 is not supported for unary not operator",
    );
}
