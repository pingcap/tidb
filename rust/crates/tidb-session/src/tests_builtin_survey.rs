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

#[test]
fn operator_and_cast_survey_findings() {
    let mut session = Session::new();
    // MIN % u64::MAX negated the remainder and panicked in debug builds; Go's
    // `-int64(uint64(-a) % uint64(b))` wraps to MIN.
    assert_eq!(
        one(
            &mut session,
            "SELECT -9223372036854775808 % 18446744073709551615, \
             18446744073709551615 % -9223372036854775808"
        ),
        ["-9223372036854775808", "9223372036854775807"]
    );
    // CASE v WHEN x is `v = x`, the ordinary comparison with its coercions: a
    // string that is not a JSON document compares as a JSON string.
    assert_eq!(
        one(
            &mut session,
            "SELECT CASE CAST('{\"a\":1}' AS JSON) WHEN 'abc' THEN 1 ELSE 2 END, \
             CASE CAST('{\"a\":1}' AS JSON) WHEN 1 THEN 'a' WHEN 'abc' THEN 'b' ELSE 'c' END"
        ),
        ["2", "c"]
    );
    // A negative number is never a date, however small (Go parses the text).
    assert_eq!(
        one(&mut session, "SELECT CAST(-1e-320 AS DATE), CAST(-0.0 AS DATE)"),
        ["NULL", "0000-00-00"]
    );
}

#[test]
fn round_with_an_extreme_scale_answers_at_once() {
    let mut session = Session::new();
    // A scale deeper than the integer digits is the zero decimal (Go
    // `MyDecimal.Round`); building the digits first never finished.
    assert_eq!(
        one(
            &mut session,
            "SELECT ROUND(12.345, -100), ROUND(12.345, -100000000), \
             ROUND(12.345, -9223372036854775808), ROUND(12345, -2)"
        ),
        ["0", "0", "0", "12300"]
    );
}

#[test]
fn binary_column_values_reach_the_time_and_string_builtins_as_bytes() {
    let mut session = Session::new();
    session.run("CREATE TABLE tb (id INT PRIMARY KEY, b VARBINARY(20), i BIGINT, s VARCHAR(20))").expect("setup statement");
    session.run("INSERT INTO tb VALUES (1, x'ff', -9223372036854775808, 'a')").expect("setup statement");
    // Go strings are byte strings: an invalid UTF-8 value fails to parse as a
    // date (NULL) instead of aborting the statement.
    assert_eq!(
        one(
            &mut session,
            "SELECT date(b), month(b), to_days(b), str_to_date(b, '%Y'), inet_aton(b), is_ipv4(b) FROM tb"
        ),
        ["NULL", "NULL", "NULL", "NULL", "NULL", "0"]
    );
    // `conv` negates its bases with Go's wrapping `-`, `locate` decrements a
    // huge position the same way; neither may overflow.
    assert_eq!(
        one(&mut session, "SELECT conv(s, i, 10), locate('a', s, i) FROM tb"),
        ["NULL", "0"]
    );
    assert_eq!(
        error_text(&mut session, "SELECT abs(i) FROM tb"),
        "BIGINT value is out of range in 'abs(-9223372036854775808)'"
    );
}

#[test]
fn regexp_argument_failures_are_go_err_regexp() {
    let mut session = Session::new();
    session.run("CREATE TABLE tr (b VARBINARY(20))").expect("setup statement");
    session.run("INSERT INTO tr VALUES (x'ff')").expect("setup statement");
    assert_eq!(
        error_text(&mut session, "SELECT regexp_like('a', b) FROM tr"),
        "Got error 'error parsing regexp: invalid UTF-8: `\u{fffd}`' from regexp"
    );
    // Only the INSTR and REPLACE classes wrap the arity failure in ErrRegexp.
    assert_eq!(
        error_text(&mut session, "SELECT regexp_instr(1)"),
        "Got error '[expression:1582]Incorrect parameter count in the call to native function 'regexp_instr'' from regexp"
    );
}

#[test]
fn date_format_mask_zero_and_zero_in_date_follow_go() {
    let mut session = Session::new();
    // MySQL compatibility (#11203): the mask "0" answers "0" before the
    // zero-date check, but any other mask on a zero-in-date value is NULL
    // with a 1292 warning naming the time.
    assert_eq!(
        one(&mut session, "SELECT date_format(0, '0'), date_format('2020-00-00', '0')"),
        ["0", "0"]
    );
    assert_eq!(
        one(&mut session, "SELECT date_format('2020-00-00', '%Y')"),
        ["NULL"]
    );
    assert_eq!(
        one(&mut session, "SHOW WARNINGS"),
        ["Warning", "1292", "Incorrect datetime value: '2020-00-00 00:00:00.000000'"]
    );
}
