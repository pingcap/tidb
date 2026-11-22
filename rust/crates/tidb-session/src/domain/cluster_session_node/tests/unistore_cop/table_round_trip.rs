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


//! A table's rows round-trip through SQL, and result columns keep Go's
//! written labels, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn base_table(tk: &mut Session) {
    exec_all(tk, &["CREATE TABLE t (a BIGINT, b BIGINT)", "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)"]);
}

#[test]
fn select_from_table() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 656);
    base_table(&mut tk);
    assert_eq!(q(&mut tk, "SELECT a FROM t"), [["1"], ["2"], ["3"]]);
    assert_eq!(q(&mut tk, "SELECT * FROM t WHERE t.a > 1"), [["2", "20"], ["3", "10"]]);
    assert_eq!(q(&mut tk, "SELECT a + b FROM t WHERE a = 2"), [["22"]]);
}

/// Go labels an unaliased aggregate with its written text; GROUP BY a
/// position naming an aggregate is "Can't group on".
#[test]
fn count_star_field_keeps_its_written_label() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 657);
    base_table(&mut tk);
    for (sql, label) in [
        ("SELECT count(*) FROM t", "count(*)"),
        ("SELECT count(1) FROM t", "count(1)"),
        ("SELECT count(a) FROM t", "count(a)"),
        ("SELECT count(DISTINCT a) FROM t", "count(DISTINCT a)"),
        ("SELECT count(*) AS n FROM t", "n"),
        ("SELECT * FROM (SELECT count(*) FROM t) d", "count(*)"),
    ] {
        assert_eq!(field_names(&mut tk, sql), [label], "{sql}");
    }
    assert_eq!(error(&mut tk, "SELECT count(*) FROM t GROUP BY 1").message, "Can't group on 'count(*)'");
    assert_eq!(error(&mut tk, "SELECT count(1) FROM t GROUP BY 1").message, "Can't group on 'count(1)'");
}

#[test]
fn insert_then_select_round_trip() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 658);
    base_table(&mut tk);
    assert_eq!(affected(&mut tk, "INSERT INTO t VALUES (4, 40), (5, 50)"), 2);
    assert_eq!(affected(&mut tk, "INSERT INTO t (a) VALUES (6)"), 1);
    assert_eq!(q(&mut tk, "SELECT a, b FROM t WHERE a > 3 ORDER BY a"), [["4", "40"], ["5", "50"], ["6", "NULL"]]);
    assert_eq!(error(&mut tk, "INSERT INTO t (a) VALUES (1, 2)").code, 1136);
    assert_eq!(error(&mut tk, "INSERT INTO missing VALUES (1)").code, 1146);
}

fn label_of(tk: &mut Session, sql: &str) -> String {
    field_names(tk, sql).remove(0)
}

/// Go `buildProjectionField`: a NULL literal column is named `NULL`.
#[test]
fn a_null_literal_column_is_named_uppercase_null() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    assert_eq!(label_of(&mut tk, "select null"), "NULL");
    assert_eq!(label_of(&mut tk, "select (null)"), "NULL");
}

/// A string literal column is named by its decoded value with leading
/// whitespace and control characters trimmed, the first of adjacent literals.
#[test]
fn a_string_literal_column_is_named_by_its_decoded_value() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    for (sql, label) in [
        ("select 'abc'", "abc"),
        (r"select '\N'", "N"),
        (r#"select "\N""#, "N"),
        (r"select '\t   col'", "col"),
        (r"select ' \r\n  .col'", ".col"),
        ("select 'abc   '", "abc   "),
        ("select '  abc   123   '", "abc   123   "),
        (r"select '\n\t   中文 col'", "中文 col"),
        ("select '   \u{1f606}col'", "\u{1f606}col"),
        ("select '\u{200b}col'", "col"),
        ("select (('abc'))", "abc"),
        (r#"select +"aaa""#, "aaa"),
        ("select 'a' 'b'", "a"),
        ("select 'é' 'x'", "é"),
    ] {
        assert_eq!(label_of(&mut tk, sql), label, "{sql}");
    }
}

#[test]
fn a_numeric_literal_column_is_named_by_its_trimmed_source_text() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    for (sql, label) in [("select +1", "1"), ("select +0", "0"), ("select +0.999", "0.999"), ("select +(-9)", "+(-9)"), ("select +(-0.001)", "+(-0.001)")] {
        assert_eq!(label_of(&mut tk, sql), label, "{sql}");
    }
}

#[test]
fn a_boolean_keyword_column_is_named_true_or_false() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    for (sql, label) in [("select true", "TRUE"), ("select FaLsE", "FALSE"), ("select 1", "1")] {
        assert_eq!(label_of(&mut tk, sql), label, "{sql}");
    }
}

#[test]
fn a_hex_or_bit_literal_column_keeps_its_source_text() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    assert_eq!(label_of(&mut tk, "select 0x41"), "0x41");
    assert_eq!(label_of(&mut tk, "select b'101'"), "b'101'");
}

#[test]
fn name_const_names_its_column_by_its_first_argument() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    assert_eq!(label_of(&mut tk, "select name_const('test_int', 1)"), "test_int");
    assert_eq!(label_of(&mut tk, "SELECT NAME_CONST('come', -1)"), "come");
}

#[test]
fn a_parenthesized_or_unary_plus_column_is_named_like_the_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    rows(&mut tk, "CREATE TABLE t (a INT)");
    for sql in ["select (a) from t", "select + a from t", "select all + a from t"] {
        assert_eq!(label_of(&mut tk, sql), "a", "{sql}");
    }
}

#[test]
fn a_folded_literal_is_not_a_written_literal() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 659);
    assert_eq!(label_of(&mut tk, "select (select 1)"), "(select 1)");
}
