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

//! Time-function answers `expression/time.test` records.

use crate::tests_support::{row_text, warnings_of};
use crate::Session;

/// The single cell a one-row, one-column query returns, as a client reads it.
fn cell(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql)).remove(0).remove(0)
}

/// Go `CanImplicitEvalInt`/`CanImplicitEvalReal`: DAYNAME is its weekday
/// index (Monday 0) wherever it is cast to a number, compared with one, or
/// tested for truth. 1962-03-01 is a Thursday.
#[test]
fn dayname_is_its_weekday_index_in_numeric_contexts() {
    let mut session = Session::new();
    for (sql, want) in [
        ("select dayname('1962-03-01')+0", "3"),
        ("select dayname('1962-03-04')+0", "6"),
        ("select dayname('1962-03-05')+0", "0"),
        ("select dayname('1962-03-01')+2.333", "5.333"),
        ("select dayname('1962-03-01')>2", "1"),
        ("select dayname('1962-03-01')=3", "1"),
        ("select dayname('1962-03-01')!=3", "0"),
        ("select !dayname('1962-03-01')", "0"),
        ("select dayname('1962-03-01')&3", "3"),
        ("select dayname('1962-03-01')|7", "7"),
        ("select dayname('1962-03-01')^1", "2"),
        ("select cast(dayname('1962-03-01') as signed)", "3"),
        ("select dayname('1962-03-01')", "Thursday"),
    ] {
        assert_eq!(cell(&mut session, sql), want, "{sql}");
        assert_eq!(warnings_of(&session), Vec::new(), "{sql}");
    }
}

/// Go `parseTimeValue` aligns a composite interval's microsecond field
/// (`alignFrac`), so "-2" SECOND_MICROSECOND is 200000 microseconds; and
/// `ParseTimeFromDecimal` keeps a datetime-shaped number's fraction.
#[test]
fn interval_fractions_follow_go() {
    let mut session = Session::new();
    for (sql, want) in [
        (
            "select date_add('2007-03-28 22:08:28', interval -2 second_microsecond)",
            "2007-03-28 22:08:27.800000",
        ),
        (
            "select date_add('2007-03-28 22:08:28', interval -2 day_microsecond)",
            "2007-03-28 22:08:27.800000",
        ),
        (
            "select 19000101000000.0005 + interval 0.0005 second",
            "1900-01-01 00:00:00.001000",
        ),
        (
            "select 19000101001843.456789 - interval 1.123456789e3 second",
            "1900-01-01 00:00:00",
        ),
    ] {
        assert_eq!(cell(&mut session, sql), want, "{sql}");
    }
}

/// Go `intervalReformatString` hands a single-unit amount that is not a
/// clean integer to `ec.HandleError`: a SELECT warns, a strict INSERT fails.
#[test]
fn a_garbage_interval_amount_fails_a_strict_insert() {
    let mut session = Session::new();
    session
        .run("create table t2(a decimal(65, 2), d datetime)")
        .unwrap();
    assert_eq!(
        session
            .run(r#"insert into t2 values('0', "1000-01-01 00:00:00" + INTERVAL "XXX" YEAR)"#)
            .unwrap_err()
            .to_string(),
        "Truncated incorrect DECIMAL value: 'XXX'"
    );
    session.run("set @@sql_mode=''").unwrap();
    session
        .run(r#"insert into t2 values('0', "1000-01-01 00:00:00" + INTERVAL "XXX" YEAR)"#)
        .unwrap();
    assert_eq!(
        cell(&mut session, "select d from t2"),
        "1000-01-01 00:00:00"
    );
    assert_eq!(
        cell(&mut session, r#"select "1000-01-01" + interval "1x" day"#),
        "1000-01-02"
    );
    assert_eq!(
        warnings_of(&session),
        vec![(1292, "Truncated incorrect DECIMAL value: '1x'".to_owned())]
    );
}

/// Go `addUnitToTime`'s overflow (`validAddTime`/`validAddMonth`) is
/// `ErrDatetimeFunctionOverflow` through `handleInvalidTimeError`.
#[test]
fn an_overflowing_timestampadd_warns_1441() {
    let mut session = Session::new();
    for sql in [
        "select timestampadd(year, 1.212208e+308, '1995-01-05 06:32:20.859724')",
        "select timestampadd(month, 3, '9999-10-29')",
        "select timestampadd(second, -1, '0001-01-01 00:00:00')",
    ] {
        assert_eq!(cell(&mut session, sql), "NULL", "{sql}");
        assert_eq!(
            warnings_of(&session),
            vec![(
                1441,
                "Datetime function: datetime field overflow".to_owned()
            )],
            "{sql}"
        );
    }
    assert_eq!(
        cell(
            &mut session,
            "select timestampadd(second, 1, '9999-12-31 23:59:58')"
        ),
        "9999-12-31 23:59:59"
    );
}

/// Go `builtinDateLiteralSig.evalTime` reads the statement's SQL mode: a
/// zero date literal is legal without NO_ZERO_DATE. The planner's resolver
/// had answered TiDB's default mode.
#[test]
fn a_zero_date_literal_follows_the_session_sql_mode() {
    let mut session = Session::new();
    assert_eq!(
        session.run("select date '0-0-0'").unwrap_err().to_string(),
        "Incorrect date value: '0000-00-00'"
    );
    session
        .run("set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    assert_eq!(cell(&mut session, "select date '0-0-0'"), "0000-00-00");
    assert_eq!(
        cell(
            &mut session,
            "select addtime(date '0-0-0', '12:00:01.341300')"
        ),
        "NULL"
    );
}

/// Go `handleInvalidTimeError`: a STR_TO_DATE failure warns in a SELECT
/// and fails a strict write. The DATETIME signature names the parsed time;
/// the DATE signature names the input (1411).
#[test]
fn a_str_to_date_failure_fails_a_strict_write() {
    let mut session = Session::new();
    assert_eq!(
        cell(&mut session, "select str_to_date('1980-01-01', '%m-%d')"),
        "NULL"
    );
    assert_eq!(
        crate::tests_support::warnings_of(&session),
        vec![(
            1292,
            "Incorrect datetime value: '0000-00-00 00:00:00'".to_owned()
        )]
    );
    session
        .run("create table t (c int, c1 varchar(32) default (str_to_date('1980-01-01','%m-%d')))")
        .unwrap();
    assert_eq!(
        session
            .run("insert into t(c) values (1)")
            .unwrap_err()
            .to_string(),
        "Incorrect datetime value: '0000-00-00 00:00:00'"
    );
    assert_eq!(
        session
            .run("select str_to_date('01-01', '%Y-%m-%d %H:%i:%s') + 0")
            .map(|_| crate::tests_support::warnings_of(&session))
            .unwrap(),
        vec![(
            1292,
            "Incorrect datetime value: '2001-01-00 00:00:00'".to_owned()
        )]
    );
}

/// Go reads `@@timestamp` through its `GetSession` hook, which answers the
/// CURRENT statement's time unless `SET timestamp` overrides it, so `NOW()`
/// moves from statement to statement. Caching the hook's first answer pinned
/// every later `NOW()` of the session -- and made `AS OF TIMESTAMP @a` read a
/// snapshot from before `@a` was set.
#[test]
fn now_advances_per_statement_unless_timestamp_is_set() {
    let mut session = Session::new();
    let first = cell(&mut session, "select now(6)");
    session.run("select sleep(0.05)").unwrap();
    session.run("set @a = now(6)").unwrap();
    let assigned = cell(&mut session, "select @a");
    let second = cell(&mut session, "select now(6)");
    assert!(
        first < assigned && assigned < second,
        "{first} {assigned} {second}"
    );
    session.run("set timestamp = 1700000000.654321").unwrap();
    let pinned = cell(&mut session, "select unix_timestamp(now(6))");
    session.run("select sleep(0.05)").unwrap();
    assert_eq!(cell(&mut session, "select unix_timestamp(now(6))"), pinned);
    assert_eq!(pinned, "1700000000.654320");
}
