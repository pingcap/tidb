#![cfg(test)]

//! Builtin results the differential survey (every Go builtin over a pool of
//! edge arguments, `tools/gen_bsurvey.py`) found different from Go. Each
//! expectation was captured from the Go oracle.

use crate::tests_support::*;
use crate::*;

fn one(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{sql} returned no row"))
}

fn error_text(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .expect_err("the statement fails")
        .to_mysql_error()
        .message
}

#[test]
fn makedate_wraps_a_huge_day_count_into_null_instead_of_overflowing() {
    let mut session = Session::new();
    // Go adds in `int`, so the day number wraps negative and TimeFromDays
    // answers the zero date, which MAKEDATE reports as NULL.
    assert_eq!(
        one(
            &mut session,
            "SELECT MAKEDATE(1.5, 1e308), MAKEDATE(2020, 9223372036854775807), \
             MAKEDATE(2020, 365), MAKEDATE(9999, 366), MAKEDATE(9999, 365)"
        ),
        ["NULL", "NULL", "2020-12-30", "NULL", "9999-12-31"]
    );
}

#[test]
fn degrees_follows_go_association_and_never_overflows() {
    let mut session = Session::new();
    // `val * 180 / math.Pi`, not `val * (180 / Pi)`: the last digit differs.
    assert_eq!(
        one(&mut session, "SELECT DEGREES(-8385959)"),
        ["-480480057.86974835"]
    );
}

#[test]
fn exp_and_cot_overflow_quote_the_evaluated_argument_like_go() {
    let mut session = Session::new();
    assert_eq!(
        error_text(&mut session, "SELECT EXP(1e308)"),
        "DOUBLE value is out of range in 'exp(1e+308)'"
    );
    assert_eq!(
        error_text(&mut session, "SELECT EXP(9223372036854775807)"),
        "DOUBLE value is out of range in 'exp(9.223372036854776e+18)'"
    );
    assert_eq!(
        error_text(&mut session, "SELECT COT(CAST('{\"a\":[1,2]}' AS JSON))"),
        "DOUBLE value is out of range in 'cot(0)'"
    );
}

#[test]
fn time_keeps_the_partial_duration_and_warns() {
    let mut session = Session::new();
    assert_eq!(
        one(
            &mut session,
            "SELECT TIME('12abc'), TIME(9223372036854775807)"
        ),
        ["00:00:12", "838:59:59"]
    );
    let warnings = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(
        warnings,
        [
            ["Warning", "1292", "Truncated incorrect time value: '12abc'"],
            [
                "Warning",
                "1292",
                "Truncated incorrect time value: '9223372036854775807'"
            ],
        ]
    );
}

#[test]
fn wrong_value_for_function_text_is_cut_like_the_error_template() {
    let mut session = Session::new();
    // "Incorrect %-.32s value: '%-.128s' for function %-.32s".
    let message = error_text(&mut session, "SELECT BIN_TO_UUID(1e308)");
    let quoted = message
        .split('\'')
        .nth(1)
        .expect("the message quotes the value");
    assert_eq!(quoted.chars().count(), 128, "{message}");
    assert!(message.ends_with("' for function bin_to_uuid"), "{message}");
}

#[test]
fn pow_follows_go_math_pow_and_its_overflow_text() {
    let mut session = Session::new();
    assert_eq!(
        one(&mut session, "SELECT POW(2, 0.5), POW(2, -2), POWER(0, 3), POW(-2, 3)"),
        ["1.4142135623730951", "0.25", "0", "-8"]
    );
    // The message always names `pow` and prints both operands as
    // `FormatFloat(v, 'f', -1, 64)`, also for POWER.
    assert_eq!(
        error_text(&mut session, "SELECT POWER(-1, 1.5)"),
        "DOUBLE value is out of range in 'pow(-1, 1.5)'"
    );
    assert_eq!(
        error_text(&mut session, "SELECT POW(time '-838:59:59', 0x6162)"),
        "DOUBLE value is out of range in 'pow(-8385959, 24930)'"
    );
}

#[test]
fn regexp_failures_are_go_err_regexp_and_stray_braces_are_literals() {
    let mut session = Session::new();
    assert_eq!(
        error_text(&mut session, "SELECT REGEXP_LIKE('12abc', '')"),
        "Got error 'Empty pattern is invalid' from regexp"
    );
    assert_eq!(
        error_text(&mut session, "SELECT REGEXP_LIKE('a', '(')"),
        "Got error 'error parsing regexp: missing closing ): `(`' from regexp"
    );
    // Go reads a `{` that opens no `{n}`, `{n,}` or `{n,m}` as a literal.
    assert_eq!(
        one(
            &mut session,
            "SELECT REGEXP_LIKE('abc', CAST('{\"a\":[1,2]}' AS JSON)), \
             REGEXP_LIKE('a{,2}', 'a{,2}'), REGEXP_LIKE('aa', 'a{2}')"
        ),
        ["0", "1", "1"]
    );
}
