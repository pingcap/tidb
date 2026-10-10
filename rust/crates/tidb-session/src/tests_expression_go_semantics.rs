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

//! Expression construction and evaluation answers the `expression/*` and
//! `executor/executor` topics record: argument casts built the way Go's
//! function classes build them, the `from_binary` charset boundary, the
//! row/vectorized body split, and typed literals.

use crate::tests_support::{row_text, warnings_of};
use crate::Session;

fn cell(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql)).remove(0).remove(0)
}

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

/// The operator info of the plan's `Selection`.
fn selection_info(session: &mut Session, sql: &str) -> String {
    row_text(session.run(&format!("explain {sql}")))
        .into_iter()
        .find(|row| row[0].contains("Selection"))
        .unwrap_or_else(|| panic!("{sql} has no Selection"))[4]
        .clone()
}

/// IF / IFNULL / CASE cast their value arguments to the merged result type
/// before folding, COALESCE casts to the eval type only, and the condition
/// folds first, so a discarded `1/0` never warns.
#[test]
fn control_functions_cast_and_fold_like_go() {
    let mut session = Session::new();
    for (sql, want) in [
        ("select if(1, 1, 1/0)", "1.0000"),
        ("select ifnull(1, 1/0)", "1.0000"),
        ("select case when 1 then 1 else 1/0 end", "1.0000"),
        ("select coalesce(1, 1/0)", "1"),
        ("select coalesce(1, 2.55, 3)", "1.00"),
        ("select if(1, 1, 1.0)", "1.0"),
    ] {
        assert_eq!(cell(&mut session, sql), want, "{sql}");
        assert_eq!(warnings_of(&session), Vec::new(), "{sql}");
    }
}

/// `castAsIntFunctionClass` reads a hex literal through
/// `BinaryLiteral.ToInt`: the unsigned value reinterpreted as int64, and a
/// literal wider than eight bytes is `MaxUint64` plus a truncation warning.
#[test]
fn hex_literal_cast_to_signed_wraps_like_go() {
    let mut session = Session::new();
    assert_eq!(
        cell(&mut session, "select cast(0xffffffffffffffff as signed)"),
        "-1"
    );
    assert_eq!(
        cell(&mut session, "select cast(0x8fffffffffffffff as signed)"),
        "-8070450532247928833"
    );
    assert_eq!(
        cell(
            &mut session,
            "select cast(0x9999999999999999999999999999999999999999999 as signed)"
        ),
        "-1"
    );
    assert_eq!(
        warnings_of(&session),
        vec![(
            1292,
            "Truncated incorrect BINARY value: '0x09999999999999999999999999999999999999999999'"
                .to_owned()
        )]
    );
}

/// A FLOAT cast evaluates in float64; only a FLOAT column stores float32.
#[test]
fn float_cast_keeps_float64_during_evaluation() {
    let mut session = Session::new();
    for sql in [
        "select cast(1.1 as float) = 1.1",
        "select cast(-1.1 as float) = -1.1",
        "select cast('123.321' as float) = 123.321",
        "select cast(12345678901234567890 as float) = 1.2345678901234567e19",
    ] {
        assert_eq!(cell(&mut session, sql), "1", "{sql}");
    }
    let (code, message) = code(&mut session, "select cast('1e300' as float)");
    assert_eq!(
        (code, message.as_str()),
        (1690, "constant 1e+300 overflows float")
    );
}

/// REPEAT and PASSWORD have Go's row/vectorized split: a column-reading call
/// takes `vecEvalString`, a folded constant call the row body.
#[test]
fn repeat_and_password_follow_the_vectorized_body_for_columns() {
    let mut session = Session::new();
    session.run("create table rp (a varchar(10))").unwrap();
    session
        .run("insert into rp values ('a'), (null), ('')")
        .unwrap();
    // An empty string repeats to '' -- zero bytes never exceed the flen.
    assert_eq!(
        row_text(session.run("select repeat(a, 16777217) from rp")),
        vec![
            vec!["NULL".to_owned()],
            vec!["NULL".to_owned()],
            vec![String::new()]
        ]
    );
    assert_eq!(warnings_of(&session), Vec::new());
    assert_eq!(
        cell(&mut session, "select length(repeat('a', 16777217))"),
        "16777217"
    );
    assert_eq!(
        row_text(session.run("select password(a) from rp where a is null or a = ''")),
        vec![vec![String::new()], vec![String::new()]]
    );
    assert_eq!(warnings_of(&session), Vec::new());
    assert_eq!(cell(&mut session, "select password(null) is null"), "1");
}

/// `WrapWithCastAsTime` gives a string argument fsp 6, which is what the
/// NO_ZERO_IN_DATE warning prints.
#[test]
fn datetime_argument_cast_carries_the_string_fsp() {
    let mut session = Session::new();
    session.run("set sql_mode = 'NO_ZERO_IN_DATE'").unwrap();
    assert_eq!(cell(&mut session, "select date('2024-00-01')"), "NULL");
    assert_eq!(
        warnings_of(&session),
        vec![(
            1292,
            "Incorrect datetime value: '2024-00-01 00:00:00.000000'".to_owned()
        )]
    );
}

/// `timeDiffFunctionClass` picks its signature from the argument types; a
/// string side goes through `StrToDuration`, and mismatched kinds are NULL.
#[test]
fn timediff_follows_go_signatures() {
    let mut session = Session::new();
    assert_eq!(
        cell(
            &mut session,
            "select timediff('0.003475670307845084', '2012-01-16')"
        ),
        "-00:20:11.996524"
    );
    assert_eq!(
        cell(
            &mut session,
            "select 1 from dual where timediff((7/'2014-07-07 02:30:02'), '2012-01-16') is true"
        ),
        "1"
    );
    session
        .run("create table td (dc datetime(3), tc time(2), sc varchar(30))")
        .unwrap();
    session
        .run("insert into td values ('2020-01-01 10:00:00.123', '10:00:00.5', '2020-01-01 09:00:00')")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "select timediff(dc, sc), timediff(tc, sc), timediff(dc, tc), timediff(tc, '09:00') from td"
        )),
        vec![vec![
            "01:00:00.123000".to_owned(),
            "NULL".to_owned(),
            "NULL".to_owned(),
            "01:00:00.50".to_owned(),
        ]]
    );
}

/// `castAsStringFunctionClass` decodes a binary (or BIT) argument through an
/// explicit `from_binary`, which warns; the coprocessor rebuilds it without
/// that flag, so the pushed-down copy errors with 1105.
#[test]
fn explicit_cast_from_binary_warns_and_errors_when_pushed_down() {
    let mut session = Session::new();
    session.run("create table fb (a bit(24))").unwrap();
    session.run("insert into fb values (0xffffff)").unwrap();
    assert_eq!(
        cell(&mut session, "select hex(convert(a, char)) from fb"),
        "NULL"
    );
    assert_eq!(
        warnings_of(&session),
        vec![(
            3854,
            r"Cannot convert string '\xFF\xFF\xFF' from binary to utf8mb4".to_owned()
        )]
    );
    let sql = "select a from fb where false not like convert(a, char)";
    assert_eq!(
        selection_info(&mut session, sql),
        r#"not(like("0", cast(from_binary(cast(test.fb.a, binary(3))), var_string(3)), 92))"#
    );
    assert_eq!(
        code(&mut session, sql),
        (
            1105,
            r"Cannot convert string '\xFF\xFF\xFF' from binary to utf8mb4".to_owned()
        )
    );
}

/// An ETString comparison or string-function argument meets
/// `HandleBinaryLiteral` under the derived collation: a hex literal folds to
/// a string, and an undecodable one stays an implicit `from_binary`.
#[test]
fn implicit_from_binary_folds_hex_literals_like_go() {
    let mut session = Session::new();
    session
        .run("create table hb (u varchar(10), i int, primary key (u))")
        .unwrap();
    session.run("insert into hb values ('ab', 1)").unwrap();
    assert_eq!(
        selection_info(&mut session, "select * from hb where concat(u, 0x61) = 'x'"),
        r#"eq(concat(test.hb.u, "a"), "x")"#
    );
    assert_eq!(
        selection_info(&mut session, "select * from hb where concat(u, 0x80) = 'x'"),
        r#"eq(concat(test.hb.u, from_binary("0x80")), "x")"#
    );
    assert_eq!(
        selection_info(&mut session, "select * from hb where i like '1%'"),
        r#"like(cast(test.hb.i, var_string(20)), "1%", 92)"#
    );
    assert_eq!(
        code(&mut session, "select * from hb where concat(u, 0x80) = 'x'"),
        (
            1105,
            r"Cannot convert string '\x80' from binary to utf8mb4".to_owned()
        )
    );
    assert_eq!(
        cell(&mut session, "select u from hb where u = 0x6162"),
        "ab"
    );
}

/// Go keeps `EscapeExplicit`: NO_BACKSLASH_ESCAPES drops only an implicit
/// backslash escape.
#[test]
fn explicit_backslash_escape_survives_no_backslash_escapes() {
    let mut session = Session::new();
    session
        .run("set sql_mode = 'NO_BACKSLASH_ESCAPES'")
        .unwrap();
    assert_eq!(cell(&mut session, r"select 'David_' like 'David\_'"), "0");
    assert_eq!(
        cell(&mut session, r"select 'David_' like 'David\_' escape '\'"),
        "1"
    );
}

/// `inFunctionClass.verifyArgs` drops negative integers for a BIT column,
/// and `GeneratePlanCacheStmtWithAST` refuses to prepare a loader.
#[test]
fn bit_in_list_and_unpreparable_statements() {
    let mut session = Session::new();
    session
        .run("create table bi (id bit(16), key id(id))")
        .unwrap();
    session.run("insert into bi values (65)").unwrap();
    assert_eq!(
        row_text(session.run("select hex(id) from bi where id not in (-1, 2)")).len(),
        1
    );
    assert_eq!(
        code(&mut session, "select * from bi where id in (-1, -2)"),
        (
            1582,
            "Incorrect parameter count in the call to native function 'in'".to_owned()
        )
    );
    for sql in [
        "prepare st from \"load data local infile '/tmp/x.csv' into table bi\"",
        "prepare st from \"import into bi from 'xx' format 'delimited'\"",
    ] {
        assert_eq!(code(&mut session, sql).0, 1295, "{sql}");
    }
}

/// Result types Go sizes after the argument casts, and the FROM_UNIXTIME
/// string argument's DECIMAL(flen + 6, 6).
#[test]
fn result_types_and_from_unixtime_string_argument() {
    let mut session = Session::new();
    session
        .run("create table ta (myid int(11), varc varchar(100))")
        .unwrap();
    session
        .run("create view va as select aes_encrypt(myid, 'k') from ta")
        .unwrap();
    assert_eq!(row_text(session.run("desc va"))[0][1], "varbinary(32)");
    session
        .run("insert into ta values (1, '111111111111111111111111111111111111111111111111111111111111111111111111111')")
        .unwrap();
    assert_eq!(
        cell(&mut session, "select from_unixtime(varc) from ta"),
        "NULL"
    );
}

/// `timeLiteralFunctionClass`: the duration regex gates the literal, and a
/// parse failure is a statement error.
#[test]
fn time_literal_is_gated_like_go() {
    let mut session = Session::new();
    assert_eq!(
        code(&mut session, "select time '2017-01-01 00:00:00'"),
        (
            1292,
            "Incorrect time value: '2017-01-01 00:00:00'".to_owned()
        )
    );
    assert_eq!(code(&mut session, "select time '12:61:00'").0, 1292);
    assert_eq!(
        cell(&mut session, "select time '12:30:45.123'"),
        "12:30:45.123"
    );
}

/// NULLIF is `IF(a = b, NULL, a)`: its result is nullable even over a NOT
/// NULL column.
#[test]
fn nullif_result_is_nullable() {
    let mut session = Session::new();
    session.run("create table nf (a int not null)").unwrap();
    session.run("insert into nf values (1), (2)").unwrap();
    assert_eq!(
        row_text(session.run("select a from nf where nullif(a, a) is null")).len(),
        2
    );
}
