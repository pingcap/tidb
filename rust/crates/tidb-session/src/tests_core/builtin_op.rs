//! Go `builtin_op.go`'s function classes through SQL. Results, warnings and
//! plans are captured from Go with `zzprobe/opclass`.

use crate::tests_support::*;
use crate::*;

const JSON_IN_BOOLEAN_CONTEXT: (u16, &str) = (
    3986,
    "Evaluating a JSON value in SQL boolean context does an implicit comparison against JSON \
     integer 0; if this is not what you want, consider converting JSON to a SQL numeric type with \
     JSON_VALUE RETURNING",
);
const FLOAT_JSON: (u16, &str) = (1292, "Truncated incorrect FLOAT value: '{\"a\": 1}'");
const DOUBLE_1X: (u16, &str) = (1292, "Truncated incorrect DOUBLE value: '1x'");
const INTEGER_1X: (u16, &str) = (1292, "Truncated incorrect INTEGER value: '1x'");

fn op_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table t(a bigint, b bigint, j json, s varchar(10), dt datetime, d decimal(10,2), \
         f double, ti time, u bigint unsigned)",
        "insert into t values (null, 9223372036854775807, '0', '1x', '2024-01-02 03:04:05', 1.50, \
         2.5, '01:02:03', 5)",
        "insert into t values (1, 2, '{\"a\":1}', '0', null, 0.00, 0, '00:00:00', 0)",
        "create table tv(v vector(3))",
        "insert into tv values ('[1,2,3]')",
    ] {
        session.run(sql).unwrap();
    }
    session
}

fn rows(text: &[&[&str]]) -> Vec<Vec<String>> {
    text.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect()
}

#[test]
fn op_classes_wrap_and_evaluate_like_go() {
    let mut session = op_session();
    let cases: &[(&str, &[&[&str]], &[(u16, &str)])] = &[
        ("select not j from t", &[&["1"], &["0"]], &[JSON_IN_BOOLEAN_CONTEXT]),
        (
            "select not j, !j from t",
            &[&["1", "1"], &["0", "0"]],
            &[JSON_IN_BOOLEAN_CONTEXT, JSON_IN_BOOLEAN_CONTEXT],
        ),
        (
            "select j is true, j is false, j is null, j is not null from t",
            &[&["0", "1", "0", "1"], &["0", "1", "0", "1"]],
            &[FLOAT_JSON, FLOAT_JSON],
        ),
        ("select 1 from t where j", &[&["1"]], &[]),
        (
            "select j and 1, j or 0, j xor 1 from t",
            &[&["0", "0", "1"], &["0", "0", "1"]],
            &[FLOAT_JSON, FLOAT_JSON, FLOAT_JSON],
        ),
        ("select v is true, v is false, v is null from tv", &[&["1", "0", "0"]], &[]),
        ("select v and 1 from tv", &[&["1"]], &[]),
        ("select --9223372036854775808", &[&["9223372036854775808"]], &[]),
        ("select -(-9223372036854775808)", &[&["9223372036854775808"]], &[]),
        ("select -9223372036854775808", &[&["-9223372036854775808"]], &[]),
        ("select -9223372036854775809", &[&["-9223372036854775809"]], &[]),
        ("select -18446744073709551615", &[&["-18446744073709551615"]], &[]),
        (
            "select -u, -a, -b, -d, -f, -s, -dt, -ti, -j from t",
            &[
                &["-5", "NULL", "-9223372036854775807", "-1.50", "-2.5", "-1", "-20240102030405", "-10203", "-0"],
                &["0", "-1", "-2", "0.00", "-0", "-0", "NULL", "0", "-0"],
            ],
            &[DOUBLE_1X, FLOAT_JSON],
        ),
        (
            "select ~u, ~a, ~d, ~f, ~s, ~dt, ~ti from t",
            &[
                &[
                    "18446744073709551610",
                    "NULL",
                    "18446744073709551613",
                    "18446744073709551613",
                    "18446744073709551614",
                    "18446723833607521210",
                    "18446744073709541412",
                ],
                &[
                    "18446744073709551615",
                    "18446744073709551614",
                    "18446744073709551615",
                    "18446744073709551615",
                    "18446744073709551615",
                    "NULL",
                    "18446744073709551615",
                ],
            ],
            &[INTEGER_1X],
        ),
        (
            "select not u, not a, not d, not f, not s, not dt, not ti from t",
            &[&["0", "NULL", "0", "0", "0", "0", "0"], &["1", "0", "1", "1", "1", "NULL", "1"]],
            &[DOUBLE_1X],
        ),
        (
            "select s is true, dt is true, ti is true, d is true, f is true, s is false, dt is false from t",
            &[&["1", "1", "1", "1", "1", "0", "0"], &["0", "0", "0", "0", "0", "1", "0"]],
            &[DOUBLE_1X, DOUBLE_1X],
        ),
        (
            "select a is not null, a is not true, a is not false, s is not true from t",
            &[&["0", "1", "1", "0"], &["1", "0", "1", "1"]],
            &[DOUBLE_1X],
        ),
        (
            "select a and s, s or dt, d xor f, s && ti from t",
            &[&["NULL", "1", "0", "1"], &["0", "NULL", "0", "0"]],
            &[DOUBLE_1X, DOUBLE_1X, DOUBLE_1X],
        ),
        (
            "select a | s, s & dt, d ^ f, s << 1, dt >> 2 from t",
            &[&["NULL", "1", "0", "2", "5060025507601"], &["1", "NULL", "0", "0", "NULL"]],
            &[INTEGER_1X, INTEGER_1X, INTEGER_1X],
        ),
        (
            "select case when s then 1 when dt then 2 else 3 end, if(s, 1, 2), if(j, 1, 2) from t",
            &[&["1", "1", "2"], &["3", "2", "2"]],
            &[DOUBLE_1X, DOUBLE_1X, FLOAT_JSON],
        ),
    ];
    for (sql, expected, warnings) in cases {
        assert_eq!(row_text(session.run(sql)), rows(expected), "{sql}");
        let expected_warnings: Vec<(u16, String)> =
            warnings.iter().map(|(code, text)| (*code, (*text).to_owned())).collect();
        assert_eq!(warnings_of(&session), expected_warnings, "{sql}");
    }

    assert_eq!(row_text(session.run("select * from t where not j")).len(), 2);
    assert_eq!(
        warnings_of(&session),
        [
            (JSON_IN_BOOLEAN_CONTEXT.0, JSON_IN_BOOLEAN_CONTEXT.1.to_owned()),
            (FLOAT_JSON.0, FLOAT_JSON.1.to_owned())
        ]
    );

    let error = session.run("select not v from tv").unwrap_err().to_mysql_error();
    assert_eq!(error.message, "VectorFloat32 is not supported for unary not operator");
    let error = session
        .run("select a xor (b+1) from t where a is null")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1690, "BIGINT value is out of range in '(test.t.b + 1)'")
    );
}

/// The probe's testkit prints an integer cast's display width
/// (`bigint(19) BINARY`); a server, which deprecates it, prints
/// `bigint BINARY`, as the recorded integration results do.
#[test]
fn op_classes_print_gos_plans() {
    let mut session = op_session();
    for (sql, info, warnings) in [
        ("select * from t where not dt", "not(istrue_with_null(cast(test.t.dt, bigint BINARY)))", vec![]),
        ("select * from t where not s", "not(istrue_with_null(cast(test.t.s, double BINARY)))", vec![]),
        (
            "select * from t where not j",
            "not(istrue_with_null(cast(test.t.j, double BINARY)))",
            vec![(JSON_IN_BOOLEAN_CONTEXT.0, JSON_IN_BOOLEAN_CONTEXT.1.to_owned())],
        ),
        ("select * from t where d is true", "istrue(test.t.d)", vec![]),
        ("select * from t where s is true", "istrue(cast(test.t.s, double BINARY))", vec![]),
        ("select * from t where s is not true", "not(istrue(cast(test.t.s, double BINARY)))", vec![]),
        ("select * from t where j is null", "isnull(cast(test.t.j, var_string(4294967295)))", vec![]),
        ("select * from t where dt is not null", "not(isnull(test.t.dt))", vec![]),
        ("select * from t where isnull(j)", "isnull(cast(test.t.j, var_string(4294967295)))", vec![]),
        ("select * from t where isnull(s) xor a", "xor(isnull(test.t.s), test.t.a)", vec![]),
        ("select * from t where -s > 1", "gt(unaryminus(cast(test.t.s, double BINARY)), 1)", vec![]),
        ("select * from t where ~s > 1", "gt(bitneg(cast(test.t.s, bigint BINARY)), 1)", vec![]),
        ("select * from t where s and dt", "test.t.dt, test.t.s", vec![]),
        (
            "select * from t where s xor d",
            "xor(istrue_with_null(cast(test.t.s, double BINARY)), istrue_with_null(test.t.d))",
            vec![],
        ),
        ("select * from t where a | s", "bitor(test.t.a, cast(test.t.s, bigint BINARY))", vec![]),
        (
            "select -u, -a, -dt, not ti, a is false from t",
            "unaryminus(test.t.u)->Column#12, unaryminus(test.t.a)->Column#13, \
             unaryminus(cast(test.t.dt, decimal(19,0) BINARY))->Column#14, \
             not(cast(test.t.ti, bigint BINARY))->Column#15, isfalse(test.t.a)->Column#16",
            vec![],
        ),
    ] {
        let plan = row_text(session.run(&format!("explain format='brief' {sql}")));
        let actual_warnings = warnings_of(&session);
        let operator = &plan[if sql.starts_with("select *") { 1 } else { 0 }];
        assert_eq!(operator.last().map(String::as_str), Some(info), "{sql}: {plan:?}");
        assert_eq!(actual_warnings, warnings, "{sql}");
    }
}

/// Go `unaryMinusFunctionClass.handleIntOverflow` reads a parameter's value
/// as it reads a literal's.
#[test]
fn unary_minus_of_a_parameter_overflows_into_decimal_like_go() {
    let mut session = Session::new();
    session.run("prepare st from 'select -?, -(?)'").unwrap();
    for (x, y, expected) in [
        ("-9223372036854775808", "1", ["9223372036854775808", "-1"]),
        ("9223372036854775808", "18446744073709551615", ["-9223372036854775808", "-18446744073709551615"]),
        ("5", "-9223372036854775808", ["-5", "9223372036854775808"]),
        ("'7'", "1.5", ["-7", "-1.5"]),
    ] {
        session.run(&format!("set @x={x}, @y={y}")).unwrap();
        assert_eq!(row_text(session.run("execute st using @x, @y")), [expected], "{x}/{y}");
    }
}
