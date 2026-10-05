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
//! The original examples below were captured from real TiDB via
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
    // The first inline assignment publishes its integer type during planning,
    // so later expressions use that type before any row executes. Captured from Go:
    // `select @c := 0, @c := @c + 1, @c` -> `RS:0|1|1`.
    let mut fresh = Session::new();
    assert_eq!(
        one_row(&mut fresh, "SELECT @c := 0, @c := @c + 1, @c"),
        ["0", "1", "1"]
    );

    // The assigned name is case-insensitive, and the assigned value keeps its
    // type for a LATER statement's arithmetic.
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

// Go SessionVars preserves values and declared types independently across migration.
#[test]
fn user_variable_batch_migrates_independent_declared_types() {
    let mut source = Session::new();
    source.run("SET @value_only = 7").unwrap();
    let mut state = source.encode_session_states().unwrap();
    let mut declared = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
    declared.set_flen(18);
    declared.set_decimal(5);
    state["user-var-types"] = serde_json::json!({"type_only": declared});
    let mut target = Session::new();
    target.decode_session_states(&state.to_string()).unwrap();
    let restored = target.encode_session_states().unwrap();
    assert_eq!(restored["user-var-types"], state["user-var-types"]);
    assert_eq!(restored["user-var-values"], state["user-var-values"]);
    target.run("SET @type_only = NULL").unwrap();
    assert!(target.encode_session_states().unwrap()["user-var-types"]
        .get("type_only")
        .is_none());
}

#[test]
fn user_variable_batch_plans_inline_types_without_executing_values() {
    let mut session = Session::new();
    session.run("CREATE TABLE test.empty_vars (a INT)").unwrap();
    session
        .run("SELECT @planned := CAST(a AS DECIMAL(18,5)) FROM test.empty_vars")
        .unwrap();
    let state = session.encode_session_states().unwrap();
    assert!(state["user-var-values"].get("planned").is_none());
    let declared: tidb_datatype::FieldType =
        serde_json::from_value(state["user-var-types"]["planned"].clone()).unwrap();
    assert_eq!(
        declared.code(),
        tidb_datatype::FieldTypeCode::NewDecimal,
        "{state}"
    );
    assert_eq!((declared.flen(), declared.decimal()), (18, 5));
    assert_eq!(
        one_row(&mut session, "SELECT @fresh := 2, @fresh + 1"),
        ["2", "3"]
    );
}

#[test]
fn user_variable_batch_set_retains_declared_width_and_scale() {
    let mut session = Session::new();
    session
        .run("SET @d = CAST(1.25 AS DECIMAL(18,5)), @s = CAST('x' AS CHAR(12))")
        .unwrap();
    let state = session.encode_session_states().unwrap();
    let decimal: tidb_datatype::FieldType =
        serde_json::from_value(state["user-var-types"]["d"].clone()).unwrap();
    let string: tidb_datatype::FieldType =
        serde_json::from_value(state["user-var-types"]["s"].clone()).unwrap();
    assert_eq!((decimal.flen(), decimal.decimal()), (18, 5));
    assert_eq!(string.flen(), 12);
}

#[test]
fn user_variable_batch_uses_migrated_types_and_source_string_assignments() {
    let mut session = Session::new();
    session.run("SET @number = '1.25', @word = 'abc'").unwrap();
    let mut state = session.encode_session_states().unwrap();
    let mut decimal = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
    decimal.set_flen(18);
    decimal.set_decimal(5);
    state["user-var-types"]["number"] = serde_json::to_value(decimal).unwrap();
    session.decode_session_states(&state.to_string()).unwrap();
    assert_eq!(
        one_row(&mut session, "SELECT @number, @number + 1"),
        ["1.25", "2.25"]
    );
    assert_eq!(one_row(&mut session, "SELECT COERCIBILITY(@word)"), ["2"]);
    session
        .run("SELECT @json := CAST('[1,2]' AS JSON), @duration := CAST('12:34:56' AS TIME)")
        .unwrap();
    use tidb_expr::user_vars::UserVarsReader;
    for name in ["json", "duration"] {
        assert!(matches!(
            session.user_vars.get_user_var_val(name),
            Some(Datum::String(_))
        ));
    }
    session.run("SET @Ä = 11").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @ä"), ["11"]);
    session.run("SET @ä = NULL").unwrap();
    assert_eq!(one_row(&mut session, "SELECT @Ä"), ["NULL"]);
}

#[test]
fn user_variable_batch_set_plans_all_assignments_before_execution() {
    let mut session = Session::new();
    session.run("SET @first = 5, @second = @first * 2").unwrap();
    use tidb_expr::user_vars::UserVarsReader;
    assert!(
        matches!(session.user_vars.get_user_var_val("second"), Some(Datum::Real(value)) if value == 10.0)
    );
    session.run("SET @first = 4").unwrap();
    session
        .run("SELECT @first := CAST(NULL AS CHAR(12))")
        .unwrap();
    assert_eq!(
        session.user_vars.get_user_var_val("first"),
        Some(Datum::Int(4))
    );
    assert_eq!(
        session.user_vars.get_user_var_type("first").unwrap().flen(),
        12
    );
    assert_eq!(
        one_row(&mut session, "SELECT @first, @first + 1"),
        ["4", "5"]
    );
}

#[test]
fn user_variable_batch_reads_integer_carrier_and_running_totals() {
    let mut session = Session::new();
    session.run("SET @carrier = '12'").unwrap();
    let mut state = session.encode_session_states().unwrap();
    state["user-var-types"]["carrier"] = serde_json::to_value(tidb_datatype::FieldType::new(
        tidb_datatype::FieldTypeCode::LongLong,
    ))
    .unwrap();
    session.decode_session_states(&state.to_string()).unwrap();
    assert_eq!(one_row(&mut session, "SELECT @carrier"), ["0"]);
    session
        .run("CREATE TABLE totals (a INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO totals VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @t := @t + v FROM totals ORDER BY a")),
        [
            vec!["NULL".to_owned()],
            vec!["NULL".to_owned()],
            vec!["NULL".to_owned()]
        ]
    );
    session.run("SET @t = 0").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @t := @t + v FROM totals ORDER BY a")),
        [
            vec!["10".to_owned()],
            vec!["30".to_owned()],
            vec!["60".to_owned()]
        ]
    );
}
