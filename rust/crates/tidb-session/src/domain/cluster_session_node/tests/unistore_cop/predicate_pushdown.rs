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


//! A WHERE split between TiKV and the root answers what the whole predicate
//! answers: comparisons, MySQL's three-valued logic, pushed math builtins and
//! calls TiKV cannot evaluate, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn table_t(tk: &mut Session, values: &str) {
    exec_all(tk, &["CREATE TABLE t (a BIGINT, b BIGINT)", &format!("INSERT INTO t VALUES {values}")]);
}

fn column(tk: &mut Session, sql: &str) -> Vec<String> {
    q(tk, sql).into_iter().map(|row| row[0].clone()).collect()
}

#[test]
fn splitting_a_where_does_not_change_its_result() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 830);
    table_t(&mut tk, "(1, 10), (5, 50), (7, 70), (9, 90)");
    for (sql, want) in [
        ("SELECT a FROM t WHERE a > 5", &["7", "9"][..]),
        ("SELECT a FROM t WHERE 5 < a", &["7", "9"][..]),
        ("SELECT a FROM t WHERE a > 5 AND b < 80", &["7"][..]),
        ("SELECT a FROM t WHERE a = 1 OR a = 9", &["1", "9"][..]),
        ("SELECT a FROM t WHERE a > 1 AND b + 1 > 60", &["7", "9"][..]),
    ] {
        assert_eq!(column(&mut tk, sql), want, "{sql}");
    }
}

#[test]
fn the_composed_predicates_keep_mysqls_three_valued_answers() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 831);
    table_t(&mut tk, "(1, 10), (2, NULL), (NULL, 30), (4, 40)");
    for (sql, want) in [
        ("SELECT a FROM t WHERE a IS NULL", &["NULL"][..]),
        ("SELECT a FROM t WHERE a IS NOT NULL", &["1", "2", "4"][..]),
        ("SELECT a FROM t WHERE b IS NULL", &["2"][..]),
        ("SELECT a FROM t WHERE a IN (1, 4)", &["1", "4"][..]),
        ("SELECT a FROM t WHERE a NOT IN (1, 4)", &["2"][..]),
        ("SELECT a FROM t WHERE a = 1 OR a = 4", &["1", "4"][..]),
        ("SELECT a FROM t WHERE a = 1 OR a IS NULL", &["1", "NULL"][..]),
        ("SELECT a FROM t WHERE NOT a = 1", &["2", "4"][..]),
        ("SELECT a FROM t WHERE a > -1 AND a < 3", &["1", "2"][..]),
        ("SELECT a FROM t WHERE (a = 1 OR a = 4) AND b + 1 > 20", &["4"][..]),
    ] {
        assert_eq!(column(&mut tk, sql), want, "{sql}");
    }
}

#[test]
fn a_pushed_math_builtin_answers_what_the_same_selection_answered() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 832);
    table_t(&mut tk, "(0, 3), (1, 0), (2, 7), (7, 7), (NULL, 1)");
    for (sql, want) in [
        ("SELECT a FROM t WHERE sin(a)", &["1", "2", "7"][..]),
        ("SELECT a FROM t WHERE mod(a, 3)", &["1", "2", "7"][..]),
        ("SELECT a FROM t WHERE mod(a, 0)", &[][..]),
        ("SELECT a FROM t WHERE acos(a)", &["0"][..]),
        ("SELECT a FROM t WHERE round(a)", &["1", "2", "7"][..]),
        ("SELECT a FROM t WHERE pi()", &["0", "1", "2", "7", "NULL"][..]),
        ("SELECT a FROM t WHERE pow(a, 2)", &["1", "2", "7"][..]),
        ("SELECT a FROM t WHERE sin(a) AND a > 1 AND b + 1 > 7", &["2", "7"][..]),
    ] {
        assert_eq!(column(&mut tk, sql), want, "{sql}");
    }
}

#[test]
fn a_call_outside_the_catalog_stays_above_the_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 833);
    table_t(&mut tk, "(1, 10), (2, 20), (3, 30)");
    for sql in ["SELECT a FROM t WHERE tan(a)", "SELECT a FROM t WHERE abs(a)", "SELECT a FROM t WHERE sin(a + 1)", "SELECT a FROM t WHERE mod(a, b)"] {
        assert_eq!(column(&mut tk, sql), ["1", "2", "3"], "{sql}");
    }
}
