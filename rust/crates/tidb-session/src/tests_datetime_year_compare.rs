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

//! `<date/datetime/timestamp column> <cmp> <YEAR column>`: the pair compares
//! in the DATETIME domain, with the YEAR read as `YYYY-00-00 00:00:00`.
//!
//! # The silent row loss this closes
//!
//! `select * from t where t.a < t.b` over `t(a datetime, b year)` returned
//! ZERO rows here and two in TiDB, and its mirror `t.a > t.b` returned ALL
//! FOUR rows here and two in TiDB -- a predicate that is neither true nor
//! false but CONSTANT, which no error and no warning reported. The cause was
//! the numeric fallback: a YEAR column reaches the value evaluator as a plain
//! `Datum::Int`, so `20000503164444 < 2018` decided every row.
//!
//! # The Go that decides it
//!
//! `getBaseCmpType` (`pkg/expression/builtin_compare.go:1424-1427`) has one
//! arm for this pair and it is the LAST one before the ETReal fallback:
//!
//! ```go
//! } else if lft != nil && rft != nil && (types.IsTemporalWithDate(lft.GetType()) && rft.GetType() == mysql.TypeYear ||
//!     lft.GetType() == mysql.TypeYear && types.IsTemporalWithDate(rft.GetType())) {
//!     return types.ETDatetime
//! }
//! ```
//!
//! `types.IsTemporalWithDate` is `IsTypeTime` (`pkg/types/etc.go:119-121`),
//! i.e. exactly `TypeDatetime`, `TypeDate`, `TypeTimestamp`.
//! `GetAccurateCmpType` leaves that ETDatetime alone (its later arms fire only
//! for ETString/ETReal/JSON/vector), and `generateCmpSigs` then builds the
//! signature with `newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt,
//! tp, tp)` -- which wraps each argument in `WrapWithCastAsTime`.
//!
//! `WrapWithCastAsTime` (`pkg/expression/builtin_cast.go:2817-2823`) inserts
//! NO cast on the date/timestamp side (`(exprTp == mysql.TypeDate || exprTp ==
//! mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime` returns `expr`),
//! and a `builtinCastIntAsTimeSig` on the YEAR side, whose evalTime
//! (`builtin_cast.go:1127-1131`) routes by the ARGUMENT'S TYPE, not by the
//! integer's digits:
//!
//! ```go
//! if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
//!     res, err = types.ParseTimeFromYear(val)
//! } else {
//!     res, err = types.ParseTimeFromNum(typeCtx(ctx), val, b.tp.GetType(), b.tp.GetDecimal())
//! }
//! ```
//!
//! and `ParseTimeFromYear` (`pkg/types/time.go:2072-2081`) is a FIELD
//! injection, not a parse of the digits:
//!
//! ```go
//! if year == 0 {
//!     return NewTime(ZeroCoreTime, mysql.TypeDate, DefaultFsp), nil
//! }
//! dt := FromDate(int(year), 0, 0, 0, 0, 0, 0)
//! return NewTime(dt, mysql.TypeDatetime, DefaultFsp), nil
//! ```
//!
//! So `2018` is `2018-00-00 00:00:00` -- month and day ZERO -- and `0` is the
//! zero date. Routing the same `2018` through `ParseTimeFromNum` (the digit
//! parser every other INT source takes) fails outright and yields NULL, which
//! is the second way this comparison loses rows and is covered below.

use super::Session;
use crate::tests_support::row_text;

/// `TestIssue20121`'s three tables, verbatim from
/// `tests/integrationtest/t/expression/issues.test:592-613`.
fn issue20121_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table t(a datetime, b year)",
        "insert into t values('2000-05-03 16:44:44', 2018)",
        "insert into t values('2020-10-01 11:11:11', 2000)",
        "insert into t values('2020-10-01 11:11:11', 2070)",
        "insert into t values('2020-10-01 11:11:11', 1999)",
        "create table tt(a date, b year)",
        "insert into tt values('2019-11-11', 2000)",
        "insert into tt values('2019-11-11', 2020)",
        "insert into tt values('2019-11-11', 2022)",
        "create table ttt(a timestamp, b year)",
        "insert into ttt values('2019-11-11 11:11:11', 2019)",
        "insert into ttt values('2019-11-11 11:11:11', 2000)",
        "insert into ttt values('2019-11-11 11:11:11', 2022)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    session
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The six recorded row sets, quoted from
/// `tests/integrationtest/r/expression/issues.result:978-1007`.
///
/// Every one of the six is a PROPER SUBSET of the table: no query here is
/// satisfied by "both sides empty" or by "both sides everything", which is
/// exactly what the numeric fallback produced for the `<`/`>` pair.
#[test]
fn datetime_date_timestamp_compared_with_year_returns_tidbs_rows() {
    let mut session = issue20121_session();

    // DATETIME vs YEAR. 2000-05-03 < 2018-00-00 and 2020-10-01 < 2070-00-00.
    assert_eq!(
        rows(&mut session, "select * from t where t.a < t.b"),
        vec![
            vec!["2000-05-03 16:44:44".to_string(), "2018".to_string()],
            vec!["2020-10-01 11:11:11".to_string(), "2070".to_string()],
        ]
    );
    assert_eq!(
        rows(&mut session, "select * from t where t.a > t.b"),
        vec![
            vec!["2020-10-01 11:11:11".to_string(), "2000".to_string()],
            vec!["2020-10-01 11:11:11".to_string(), "1999".to_string()],
        ]
    );

    // DATE vs YEAR: no cast on the DATE side, so `2019-11-11 00:00:00` is
    // compared against `2020-00-00 00:00:00` -- month 11 loses to month 0 of
    // the LATER year, which only a field-wise time compare gets right.
    assert_eq!(
        rows(&mut session, "select * from tt where tt.a > tt.b"),
        vec![vec!["2019-11-11".to_string(), "2000".to_string()]]
    );
    assert_eq!(
        rows(&mut session, "select * from tt where tt.a < tt.b"),
        vec![
            vec!["2019-11-11".to_string(), "2020".to_string()],
            vec!["2019-11-11".to_string(), "2022".to_string()],
        ]
    );

    // TIMESTAMP vs YEAR, including the EQUAL-YEAR boundary: `2019` becomes
    // `2019-00-00 00:00:00`, which is STRICTLY LESS than `2019-11-11
    // 11:11:11`, so the same-year row lands in `>` and not in `<`.
    assert_eq!(
        rows(&mut session, "select * from ttt where ttt.a > ttt.b"),
        vec![
            vec!["2019-11-11 11:11:11".to_string(), "2019".to_string()],
            vec!["2019-11-11 11:11:11".to_string(), "2000".to_string()],
        ]
    );
    assert_eq!(
        rows(&mut session, "select * from ttt where ttt.a < ttt.b"),
        vec![vec!["2019-11-11 11:11:11".to_string(), "2022".to_string()]]
    );

    // `getBaseCmpType`'s arm is symmetric (`lft.GetType() == mysql.TypeYear &&
    // types.IsTemporalWithDate(rft.GetType())` is its second half), so writing
    // the YEAR on the LEFT selects the same domain and must return the mirror
    // of `t.a < t.b` exactly.
    assert_eq!(
        rows(&mut session, "select * from t where t.b > t.a"),
        vec![
            vec!["2000-05-03 16:44:44".to_string(), "2018".to_string()],
            vec!["2020-10-01 11:11:11".to_string(), "2070".to_string()],
        ]
    );
    assert_eq!(
        rows(&mut session, "select * from tt where tt.b < tt.a"),
        vec![vec!["2019-11-11".to_string(), "2000".to_string()]]
    );
}

/// The boundaries the recorded script does not reach: YEAR 0, a NULL YEAR, the
/// SAME-YEAR row, and the year immediately below.
///
/// Row identity is asserted through an `id` column so that nothing here
/// depends on how a YEAR renders -- only on WHICH ROWS SURVIVE, which is the
/// property that was silently wrong.
///
/// Two of these four separate Go's conversion from every plausible near-miss:
///
///   * `id = 1` (`2020-01-01 00:00:00` vs `2020`) is on the `>` side and in
///     NEITHER `=` nor `<`. A conversion that produced `2020-01-01 00:00:00`
///     -- the obvious "start of the year" reading -- would put it in `=`
///     instead. Only `FromDate(2020, 0, 0, ...)`'s month-0/day-0 injection
///     answers `>`.
///   * `id = 3` (`1990-01-01 00:00:00` vs YEAR `0`) is on the `>` side.
///     `AdjustYear(0, false)` returns `0` unchanged (`time.go:1279-1281`), and
///     `ParseTimeFromYear` maps that `0` to the ZERO DATE, below every real
///     datetime. The datetime is deliberately BEFORE 2000: a `0` misread as
///     the two-digit year 2000 -- the reading `AdjustYear` would give it if
///     `adjustZero` were true -- puts this row on the `<` side instead, and a
///     `0` that failed to convert drops it from both.
#[test]
fn year_zero_null_same_year_and_the_year_below() {
    let mut session = Session::new();
    session
        .run("create table b(id int, a datetime, y year)")
        .unwrap();
    for sql in [
        "insert into b values(1, '2020-01-01 00:00:00', 2020)",
        "insert into b values(2, '2019-12-31 23:59:59', 2020)",
        "insert into b values(3, '1990-01-01 00:00:00', 0)",
        "insert into b values(4, '2000-01-01 00:00:00', null)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    assert_eq!(
        rows(&mut session, "select id from b where a > y"),
        vec![vec!["1".to_string()], vec!["3".to_string()]]
    );
    assert_eq!(
        rows(&mut session, "select id from b where a < y"),
        vec![vec!["2".to_string()]]
    );
    // No datetime in the table IS a `YYYY-00-00 00:00:00`, so equality is
    // empty -- and the NULL row is absent from all four directions.
    assert_eq!(
        rows(&mut session, "select id from b where a = y"),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        rows(&mut session, "select id from b where a <> y"),
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );
}

/// `CAST(<year column> AS DATETIME)` on its own, which is where the conversion
/// the comparison depends on is observable without a comparison.
///
/// This is `builtinCastIntAsTimeSig`'s own YEAR branch: routing `2018` through
/// the digit parser (`ParseTimeFromNum`, which reads an int as a packed
/// `YYYYMMDD`) fails and yields NULL, so a NULL here is the very defect the
/// comparison hits.
#[test]
fn cast_year_column_as_datetime_injects_the_year_field() {
    let mut session = Session::new();
    session.run("create table y(v year)").unwrap();
    session
        .run("insert into y values(2018), (1999), (2155)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "select cast(v as datetime) from y"),
        vec![
            vec!["2018-00-00 00:00:00".to_string()],
            vec!["1999-00-00 00:00:00".to_string()],
            vec!["2155-00-00 00:00:00".to_string()],
        ]
    );
}
