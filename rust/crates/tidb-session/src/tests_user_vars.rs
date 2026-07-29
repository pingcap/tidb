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
