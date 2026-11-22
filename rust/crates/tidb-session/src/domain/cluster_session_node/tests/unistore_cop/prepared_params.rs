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


//! Prepared statement parameters reach every scope -- projection, filter,
//! ORDER BY, aggregate output and HAVING, join conditions, derived tables --
//! and the result metadata follows the parameter's type as Go's does, over
//! the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;
use tidb_datatype::FieldTypeCode as C;

fn prepared_table(tk: &mut Session) {
    exec_all(tk, &["CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)", "INSERT INTO p VALUES (1,10),(2,20),(3,30)"]);
}

/// Prepares `sql` and executes it with integer parameters.
fn run_prepared(tk: &mut Session, sql: &str, values: &[&str]) -> Vec<Vec<String>> {
    rows(tk, &format!("PREPARE st FROM '{sql}'"));
    let mut using = Vec::new();
    for (position, value) in values.iter().enumerate() {
        rows(tk, &format!("SET @p{position} = {value}"));
        using.push(format!("@p{position}"));
    }
    q(tk, &format!("EXECUTE st USING {}", using.join(", ")))
}

#[test]
fn prepared_typed_markers_reach_projection_filter_and_order() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 810);
    prepared_table(&mut tk);
    let sql = "SELECT IF(?, v + ?, v - ?) FROM p WHERE id BETWEEN ? AND ? ORDER BY id DESC LIMIT 2";
    assert_eq!(run_prepared(&mut tk, sql, &["1", "3", "4", "1", "3"]), [["33"], ["23"]]);
    assert_eq!(run_prepared(&mut tk, sql, &["0", "3", "4", "1", "2"]), [["16"], ["6"]]);
}

#[test]
fn prepared_typed_markers_reach_aggregate_output_and_having() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 811);
    prepared_table(&mut tk);
    let sql = "SELECT SUM(v) + ? AS total FROM p WHERE id >= ? HAVING SUM(v) > ?";
    assert_eq!(run_prepared(&mut tk, sql, &["5", "2", "40"]), [["55"]]);
    assert!(run_prepared(&mut tk, sql, &["5", "2", "60"]).is_empty());
}

#[test]
fn prepared_typed_markers_reach_join_scopes() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 812);
    prepared_table(&mut tk);
    assert_eq!(
        run_prepared(&mut tk, "SELECT l.v + ?, r.v - ? FROM p l JOIN p r ON r.id = l.id + ? WHERE l.id >= ? ORDER BY l.id", &["3", "4", "1", "1"]),
        [["13", "16"], ["23", "26"]]
    );
}

#[test]
fn prepared_typed_markers_reach_derived_scopes() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 813);
    prepared_table(&mut tk);
    assert_eq!(run_prepared(&mut tk, "SELECT v + ? FROM (SELECT id, v + ? AS v FROM p WHERE id >= ?) d ORDER BY id", &["3", "4", "2"]), [["27"], ["37"]]);
}

/// Go infers each marker's result type from the bound value: an integer
/// parameter keeps integer arithmetic, a NULL one turns it into DOUBLE and
/// the bare marker into a NULL-typed column.
#[test]
fn prepared_typed_markers_match_go_result_metadata_on_type_changes() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 814);
    const NOT_NULL: u16 = 1;
    const BINARY: u16 = 128;
    rows(&mut tk, "PREPARE st FROM 'SELECT POW(?,?), IFNULL(?,11), ? + 11, ? AS marker, ? * 2, 11 - ?, ? div 3, ? % 3, ? / 2'");
    let width = |flen: i64| u32::try_from(flen).ok();
    let scale = |decimal: i64| u8::try_from(decimal).ok();
    for (param, expected) in [
        (
            "2",
            [
                (C::Double, 23, -1, NOT_NULL | BINARY),
                (C::LongLong, 24, 0, NOT_NULL | BINARY),
                (C::LongLong, 20, 0, NOT_NULL | BINARY),
                (C::LongLong, -1, 0, NOT_NULL | BINARY),
                (C::LongLong, 20, 0, NOT_NULL | BINARY),
                (C::LongLong, 20, 0, NOT_NULL | BINARY),
                (C::LongLong, 20, 0, NOT_NULL | BINARY),
                (C::LongLong, 20, 0, NOT_NULL | BINARY),
                (C::NewDecimal, 25, 4, NOT_NULL | BINARY),
            ],
        ),
        (
            "NULL",
            [
                (C::Double, 23, -1, NOT_NULL | BINARY),
                (C::LongLong, 2, 0, NOT_NULL | BINARY),
                (C::Double, -1, -1, BINARY),
                (C::Null, 0, 0, 0),
                (C::Double, -1, -1, BINARY),
                (C::Double, -1, -1, BINARY),
                (C::LongLong, 20, 0, BINARY),
                (C::Double, 1, 0, BINARY),
                (C::Double, 23, -1, BINARY),
            ],
        ),
    ] {
        rows(&mut tk, "SET @a = 2, @b = 3");
        rows(&mut tk, &format!("SET @x = {param}"));
        let got = fields(&mut tk, "EXECUTE st USING @a, @b, @x, @x, @x, @x, @x, @x, @x, @x");
        assert_eq!(got.len(), expected.len());
        for (field, (code, flen, decimal, flags)) in got.iter().zip(expected) {
            let ft = &field.field_type;
            assert_eq!((ft.code, ft.flen, ft.decimal, ft.flags), (code, width(flen), scale(decimal), flags), "{}, parameter {param}", field.name);
            let collation = if code == C::Null { tidb_charset::Collation::Utf8Mb4Bin } else { tidb_charset::Collation::Binary };
            assert_eq!(ft.collation, collation, "{}, parameter {param}", field.name);
        }
    }
}
