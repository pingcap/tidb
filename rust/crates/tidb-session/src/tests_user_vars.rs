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

//! User-defined session variables (`SET @name = value`, read back as `@name`).
//!
//! The Go mechanism is `SessionVars.SetUserVarVal` /
//! `GetUserVarVal` (`pkg/sessionctx/variable/session.go`), which stores a
//! `types.Datum` -- a value WITH A TYPE, not text. That is the rule this file
//! pins: `SET @i = 5` stores an integer, so `@i + 1` is integer arithmetic,
//! while `SET @y = 'hello'` stores a string.
//!
//! Every expectation below is captured from real TiDB via
//! `rust/difftests/gorun`:
//!
//! ```text
//! set @i = 5                                  OK
//! select @i + 1, @i, @i + 0.5, @i * '2'       RS:6|5|5.5|10
//! set @s = '5'                                OK
//! select @s + 1, @s, concat(@s,'x')           RS:6|5|5x
//! set @d = 1.5                                OK
//! select @d + 1, @d                           RS:2.5|1.5
//! set @f = 1e3                                OK
//! select @f, @f+1                             RS:1000|1001
//! select @unset, @unset + 1, concat(@unset)   RS:<nil>|<nil>|<nil>
//! set @n = null                               OK
//! select @n, @n+1                             RS:<nil>|<nil>
//! select @i, @I, @__i                         RS:5|5|<nil>
//! set @h = x'41'                              OK
//! select @h, @h+0                             RS:A|65
//! set @x = ANSI_QUOTES                        OK
//! select @x                                   RS:ANSI_QUOTES
//! ```

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn one_row(session: &mut Session, sql: &str) -> Vec<String> {
    let rows = row_text(session.run(sql));
    assert_eq!(rows.len(), 1, "{sql} returned {} rows", rows.len());
    rows.into_iter().next().unwrap()
}

/// The type survives the round trip: an integer variable is an integer
/// operand, not the text of one. Reading it back through arithmetic is what
/// distinguishes a typed store from a stringly one -- text would either error
/// or coerce differently.
#[test]
fn a_user_variable_keeps_the_type_it_was_assigned() {
    let mut session = Session::new();
    session.run("SET @i = 5").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @i, @i + 1"), ["5", "6"]);
    session.run("SET @d = 1.5").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @d, @d + 1"), ["1.5", "2.5"]);
    session.run("SET @f = 1e3").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @f, @f + 1"), ["1000", "1001"]);
    session.run("SET @y = 'hello'").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @y"), ["hello"]);
}

/// The value expression may read OTHER user variables, including with
/// arithmetic: Go evaluates the `SET` right-hand side through the ordinary
/// expression evaluator, so `@x` is bound to its value before the `+` runs.
#[test]
fn a_set_value_may_reference_other_user_variables() {
    let mut session = Session::new();
    session.run("SET @x = 5").unwrap();
    session.run("SET @z = @x + 1").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @z"), ["6"]);
    // ... including the SAME name, which reads the OLD value first.
    session.run("SET @x = @x + 10").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @x"), ["15"]);
}

/// Names are case-insensitive (Go lowercases the key), and an unset name is
/// NULL rather than an error -- the opposite of an unknown `@@sysvar`.
#[test]
fn names_are_case_insensitive_and_an_unset_variable_is_null() {
    let mut session = Session::new();
    session.run("SET @i = 5").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @I, @i"), ["5", "5"]);
    assert_eq!(
        one_row(&mut session, "SELECT @unset, @unset + 1"),
        ["NULL", "NULL"]
    );
    // An explicit NULL assignment reads back as NULL too.
    session.run("SET @n = NULL").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @n, @n + 1"), ["NULL", "NULL"]);
}

/// A bare word right-hand side is taken literally, as it is for a system
/// variable: `SET @x = ANSI_QUOTES` stores the string.
#[test]
fn a_bare_word_value_is_stored_as_its_text() {
    let mut session = Session::new();
    session.run("SET @x = ANSI_QUOTES").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @x"), ["ANSI_QUOTES"]);
}

/// A user variable is an ordinary operand anywhere a literal could be,
/// including a WHERE clause over a real table, and it is session-scoped
/// rather than transactional -- a later ROLLBACK does not take it back.
#[test]
fn a_user_variable_is_an_operand_and_is_not_transactional() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uv (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO uv VALUES (1,10),(2,20)").unwrap();
    session.run("SET @x = 5").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM uv WHERE v > @x ORDER BY id")),
        [vec!["1".to_owned()], vec!["2".to_owned()]]
    );
    session.run("BEGIN").unwrap();
    session.run("SET @w = 99").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @w"), ["99"]);
}

/// A scalar subquery is a legal value expression, and a cardinality violation
/// in one is the ordinary 1242 it is anywhere else.
#[test]
fn a_scalar_subquery_value_is_evaluated_and_its_cardinality_enforced() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uvs (id INT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO uvs VALUES (1),(2)").unwrap();
    session.run("SET @c = (SELECT COUNT(*) FROM uvs)").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @c"), ["2"]);
    let reported = session
        .run("SET @bad = (SELECT id FROM uvs)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(reported.code, 1242);
    assert_eq!(reported.message, "Subquery returns more than 1 row");
}

/// The inline `@x := expr` assignment expression, evaluated LEFT TO RIGHT
/// within a row: a later select-list item sees what an earlier one assigned.
/// Captured from Go:
///
/// ```text
/// set @i = 3                 OK
/// select @i := @i + 1, @i    RS:4|4
/// select @i                  RS:4
/// select @A := 7             RS:7
/// select @a                  RS:7
/// select @n2 := 5            RS:5
/// select @n2 + 1             RS:6
/// ```
#[test]
fn an_inline_assignment_is_visible_to_the_rest_of_its_own_row() {
    let mut session = Session::new();
    session.run("SET @i = 3").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @i := @i + 1, @i"), ["4", "4"]);
    // The assignment OUTLIVES the statement.
    assert_eq!(one_row(&mut session, "SELECT @i"), ["4"]);
    // NOT asserted, and not a user-variable gap: Go answers
    // `select @c := 0, @c := @c + 1, @c` with `RS:0|1|1` even though `@c` was
    // unset when the statement was built, because an unset variable is typed
    // as a string (Go does the same) and Go's arithmetic COERCES a string
    // operand. This tier's arithmetic refuses one outright -- see
    // `tidb_expr::ops`'s "string operand" -- which is a separate gap in
    // numeric coercion, reachable with no user variable in sight.
    //
    // The assigned name is case-insensitive, and the assigned value keeps its
    // type for a LATER statement's arithmetic. (Within the SAME statement the
    // read was already typed from the pre-statement value -- as Go's own
    // build-time signature choice is -- which is why `@n2 := 5, @n2 + 1`
    // lands on the string-operand gap noted above rather than here.)
    assert_eq!(one_row(&mut session, "SELECT @A := 7"), ["7"]);
    assert_eq!(one_row(&mut session, "SELECT @a, @a + 1"), ["7", "8"]);
}

/// `@x := NULL` returns NULL and LEAVES THE VARIABLE ALONE -- Go's
/// `builtinSetVar*Sig` skips the write for a NULL value. This is the opposite
/// of the top-level `SET @x = NULL`, which clears it. Captured from Go:
///
/// ```text
/// set @i = 3                 OK
/// select @i := @i + 1, @i    RS:4|4
/// select @i := null          RS:<nil>
/// select @i                  RS:4      <- still 4
/// ```
#[test]
fn an_inline_assignment_of_null_leaves_the_variable_alone() {
    let mut session = Session::new();
    session.run("SET @i = 4").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @i := NULL"), ["NULL"]);
    assert_eq!(one_row(&mut session, "SELECT @i"), ["4"]);
    // ... while the statement form clears it.
    session.run("SET @i = NULL").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @i"), ["NULL"]);
}

/// Assigning FROM a column runs once per row, so after the statement the
/// variable holds the LAST row's value in the statement's own row order.
/// Captured from Go: `select @s := v, @s from t2 order by v` =>
/// `RS:a|a;b|b;c|c`, then `select @s` => `RS:c`.
#[test]
fn an_inline_assignment_from_a_column_runs_once_per_row() {
    let mut session = Session::new();
    session.run("CREATE TABLE uvc (v VARCHAR(8))").unwrap();
    session
        .run("INSERT INTO uvc VALUES ('a'),('b'),('c')")
        .unwrap();
    session.run("SET @s = 'seed'").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @s := v, @s FROM uvc ORDER BY v")),
        [
            vec!["a".to_owned(), "a".to_owned()],
            vec!["b".to_owned(), "b".to_owned()],
            vec!["c".to_owned(), "c".to_owned()]
        ]
    );
    assert_eq!(one_row(&mut session, "SELECT @s"), ["c"]);
}
