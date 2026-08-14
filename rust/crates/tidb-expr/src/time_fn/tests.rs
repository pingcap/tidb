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

//! The time family's unit tests, split out of `time_fn/mod.rs` purely for
//! file size (the `source_size_ratchet` gate). Every assertion, Go citation
//! and doc comment is unchanged from its prior home in that file's
//! `#[cfg(test)] mod tests`.

use super::*;

fn string_datum(value: &str) -> Datum {
    Datum::new_string(value.to_string())
}

/// A single argument as an `types.ETDatetime`-declaring signature
/// actually receives it: through Go's build-time `WrapWithCastAsTime`
/// (`crate::arg_eval_type`), which both evaluators apply before dispatch.
/// A source vector written against `builtin_time_test.go` states the SQL
/// input, so it must enter through the same layer a query does.
fn datetime_arg(value: Datum) -> Vec<Datum> {
    crate::arg_eval_type::wrap_datetime_args("MONTH", vec![value], &[], &crate::NoColumns)
        .expect("the ETDatetime argument cast")
}

/// [`datetime_arg`] for a string-valued argument.
fn datetime_str_arg(value: &str) -> Vec<Datum> {
    datetime_arg(string_datum(value))
}

/// `builtinGetFormatSig.getFormat` table: every (type, location) pair,
/// case-insensitive location, empty for unknown, TIMESTAMP shares DATETIME.
#[test]
fn get_format_table() {
    assert_eq!(get_format("DATE", "USA"), "%m.%d.%Y");
    assert_eq!(get_format("DATE", "JIS"), "%Y-%m-%d");
    assert_eq!(get_format("DATE", "ISO"), "%Y-%m-%d");
    assert_eq!(get_format("DATE", "EUR"), "%d.%m.%Y");
    assert_eq!(get_format("DATE", "INTERNAL"), "%Y%m%d");
    assert_eq!(get_format("DATETIME", "USA"), "%Y-%m-%d %H.%i.%s");
    assert_eq!(get_format("DATETIME", "JIS"), "%Y-%m-%d %H:%i:%s");
    assert_eq!(get_format("DATETIME", "ISO"), "%Y-%m-%d %H:%i:%s");
    assert_eq!(get_format("DATETIME", "EUR"), "%Y-%m-%d %H.%i.%s");
    assert_eq!(get_format("DATETIME", "INTERNAL"), "%Y%m%d%H%i%s");
    assert_eq!(get_format("TIMESTAMP", "eur"), "%Y-%m-%d %H.%i.%s");
    assert_eq!(get_format("TIME", "USA"), "%h:%i:%s %p");
    assert_eq!(get_format("TIME", "JIS"), "%H:%i:%s");
    assert_eq!(get_format("TIME", "ISO"), "%H:%i:%s");
    assert_eq!(get_format("TIME", "EUR"), "%H.%i.%s");
    assert_eq!(get_format("TIME", "INTERNAL"), "%H%i%s");
    // Location is case-insensitive.
    assert_eq!(get_format("TIME", "usa"), "%h:%i:%s %p");
    // Unknown location / type -> empty.
    assert_eq!(get_format("DATE", "unknown"), "");
    assert_eq!(get_format("YEAR", "USA"), "");
}

/// `builtinWeekOfYearSig` = `date.Week(3)`; zero/invalid dates are NULL.
#[test]
fn week_of_year_source_vectors() {
    assert_eq!(
        week_of_year_builtin(&[string_datum("2024-03-15")]).unwrap(),
        Datum::Int(11)
    );
    assert_eq!(
        week_of_year_builtin(&[string_datum("2024-01-01")]).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        week_of_year_builtin(&[string_datum("2020-12-31")]).unwrap(),
        Datum::Int(53)
    );
    assert_eq!(
        week_of_year_builtin(&[string_datum("0000-00-00")]).unwrap(),
        Datum::Null
    );
    assert_eq!(week_of_year_builtin(&[Datum::Null]).unwrap(), Datum::Null);
}

/// `builtinTidbParseTsoLogicalSig` = low 18 bits; non-positive/NULL -> NULL.
#[test]
fn tidb_parse_tso_logical_vectors() {
    let cases = [
        (Datum::Int(452_605_852_463_012_352), Datum::Int(137_728)),
        (Datum::Int(262_144), Datum::Int(0)),
        (Datum::Int(262_143), Datum::Int(262_143)),
        (Datum::Int(1), Datum::Int(1)),
        (Datum::Int(i64::MAX), Datum::Int(262_143)),
        (Datum::Int(0), Datum::Null),
        (Datum::Int(-1), Datum::Null),
        (Datum::Null, Datum::Null),
    ];
    for (arg, want) in cases {
        assert_eq!(
            tidb_parse_tso_logical(std::slice::from_ref(&arg)).unwrap(),
            want,
            "{arg:?}"
        );
    }
}

#[test]
fn calendar_part_source_vectors() {
    // TestDayOfWeek, TestDayOfMonth, TestDayOfYear, TestQuarter, and
    // the directly shared function classes in TestDate from
    // pkg/expression/builtin_time_test.go. The IgnoreZeroInDate-only
    // DAYOFMONTH/QUARTER rows stay outside this value-only evaluator;
    // their exact StatementContext blocker is recorded in the ledger.
    let day_of_week_cases = [
        ("2017-12-01", Datum::Int(6)),
        ("0000-00-00", Datum::Null),
        ("2018-00-00", Datum::Null),
        ("2017-00-00 12:12:12", Datum::Null),
        ("0000-00-00 12:12:12", Datum::Null),
        ("2000-01-01", Datum::Int(7)),
        ("2011-11-11", Datum::Int(6)),
        ("0000-01-01", Datum::Int(7)),
    ];
    for (input, want) in day_of_week_cases {
        assert_eq!(
            day_of_week(&[string_datum(input)]).unwrap(),
            want,
            "{input}"
        );
    }

    let day_of_year_cases = [
        ("2017-12-01", Datum::Int(335)),
        ("0000-00-00", Datum::Null),
        ("2018-00-00", Datum::Null),
        ("2017-00-00 12:12:12", Datum::Null),
        ("0000-00-00 12:12:12", Datum::Null),
        ("2000-01-01", Datum::Int(1)),
        ("2011-11-11", Datum::Int(315)),
        ("0000-01-01", Datum::Int(1)),
    ];
    for (input, want) in day_of_year_cases {
        assert_eq!(
            day_of_year(&[string_datum(input)]).unwrap(),
            want,
            "{input}"
        );
    }

    let day_of_month_cases = [
        ("2017-12-01", Datum::Int(1)),
        ("2000-01-01", Datum::Int(1)),
        ("2011-11-11", Datum::Int(11)),
        ("0000-01-01", Datum::Int(1)),
        ("2008-13-01", Datum::Null),
    ];
    for (input, want) in day_of_month_cases {
        assert_eq!(
            day_of_month(&datetime_str_arg(input)).unwrap(),
            want,
            "{input}"
        );
    }

    let quarter_cases = [
        ("2008-04-01", 2),
        ("2008-01-01", 1),
        ("2008-03-31", 1),
        ("2008-06-30", 2),
        ("2008-07-01", 3),
        ("2008-09-30", 3),
        ("2008-10-01", 4),
        ("2008-12-31", 4),
        ("0000-01-01", 1),
    ];
    for (input, want) in quarter_cases {
        assert_eq!(
            quarter(&datetime_str_arg(input)).unwrap(),
            Datum::Int(want),
            "{input}"
        );
    }
    assert_eq!(
        quarter(&datetime_str_arg("2008-13-01")).unwrap(),
        Datum::Null
    );

    let weekday_cases = [
        ("2000-01-01", Datum::Int(5)),
        ("2011-11-11", Datum::Int(4)),
        ("0000-01-01", Datum::Int(5)),
        ("0000-00-00", Datum::Null),
    ];
    for (input, want) in weekday_cases {
        assert_eq!(weekday(&[string_datum(input)]).unwrap(), want, "{input}");
    }

    assert_eq!(
        day_of_month(&datetime_arg(Datum::Null)).unwrap(),
        Datum::Null
    );
    assert_eq!(day_of_week(&[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(day_of_year(&[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(weekday(&[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(quarter(&datetime_arg(Datum::Null)).unwrap(), Datum::Null);
    assert_eq!(
        day_of_month(&datetime_arg(Datum::Int(20_240_315))).unwrap(),
        Datum::Int(15)
    );
    assert_eq!(
        day_of_week(&[Datum::Int(20_240_315)]).unwrap(),
        Datum::Int(6)
    );
    assert_eq!(
        day_of_year(&[Datum::Int(20_240_315)]).unwrap(),
        Datum::Int(75)
    );
    assert_eq!(weekday(&[Datum::Int(20_240_315)]).unwrap(), Datum::Int(4));
    assert_eq!(
        quarter(&datetime_arg(Datum::Int(20_240_315))).unwrap(),
        Datum::Int(1)
    );
    assert!(day_of_week(&[]).is_err());
    assert!(quarter(&[string_datum("2008-01-01"), Datum::Int(1)]).is_err());
}

/// Exact scalar rows from `TestQuarter` at
/// `pkg/expression/builtin_time_test.go:2781`.  The source context enables
/// `IgnoreZeroInDate`, so the month-zero row is retained as quarter zero;
/// typed temporal warnings and session mode state remain outside Datum.
#[test]
fn quarter_source_vectors() {
    for (input, want) in [
        ("2008-04-01", 2),
        ("2008-01-01", 1),
        ("2008-03-31", 1),
        ("2008-06-30", 2),
        ("2008-07-01", 3),
        ("2008-09-30", 3),
        ("2008-10-01", 4),
        ("2008-12-31", 4),
        ("2008-00-01", 0),
    ] {
        assert_eq!(
            quarter(&datetime_str_arg(input)).unwrap(),
            Datum::Int(want),
            "QUARTER({input:?})"
        );
    }
    assert_eq!(
        quarter(&datetime_str_arg("2008-13-01")).unwrap(),
        Datum::Null
    );
    assert_eq!(quarter(&datetime_arg(Datum::Null)).unwrap(), Datum::Null);
}

/// `TestZeroDateTimeCompatibility` in `r/executor/executor.result`: a
/// zero-datetime COLUMN (`insert ignore into t values(0,0)`) reaches these
/// builtins as an already-typed `Datum::Time`, which is Go's own early
/// return in `WrapWithCastAsTime` (`builtin_cast.go:2821`) — a DATETIME
/// expression is handed to the signature unwrapped. The stored-component extractors return the stored
/// `0`; the day-of-week family still rejects the zero date as NULL. This is
/// distinct from the string form `YEAR("0000-00-00")`, which is NULL
/// because the string fails NO_ZERO_DATE parsing — both are asserted
/// absolutely so a "both NULL" (or "both 0") regression fails.
#[test]
fn zero_datetime_column_matches_recorded_tidb() {
    use tidb_datatype::{CoreTime, Time, TimeType};

    let zero = Datum::Time(Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap());
    // Stored-component extractors return the stored 0 for a typed zero
    // datetime (Go: `date.Year()`/`Month()`/`Day()`, `(Month()+2)/3`).
    assert_eq!(
        calendar::date_part(std::slice::from_ref(&zero), |d| d.0).unwrap(),
        Datum::Int(0),
        "YEAR(zero-datetime)"
    );
    assert_eq!(month(std::slice::from_ref(&zero)).unwrap(), Datum::Int(0));
    assert_eq!(
        day_of_month(std::slice::from_ref(&zero)).unwrap(),
        Datum::Int(0)
    );
    assert_eq!(quarter(std::slice::from_ref(&zero)).unwrap(), Datum::Int(0));

    // The day-of-week family rejects a zero date as NULL (Go:
    // `InvalidZero()` -> NULL + warning 1292), even when typed.
    assert_eq!(
        day_of_week(std::slice::from_ref(&zero)).unwrap(),
        Datum::Null
    );
    assert_eq!(
        day_of_year(std::slice::from_ref(&zero)).unwrap(),
        Datum::Null
    );
    assert_eq!(weekday(std::slice::from_ref(&zero)).unwrap(), Datum::Null);
    assert_eq!(dayname(std::slice::from_ref(&zero)).unwrap(), Datum::Null);
    assert_eq!(monthname(std::slice::from_ref(&zero)).unwrap(), Datum::Null);

    // The string form is NULL, NOT 0 — and the split is now decided by ONE
    // rule instead of by each signature: the ETDatetime argument cast
    // (`crate::arg_eval_type`) rejects `"0000-00-00"` under NO_ZERO_DATE
    // before the signature runs, exactly as Go's `WrapWithCastAsTime` does,
    // while the typed zero above is Go's early return and reaches the
    // signature intact.
    assert_eq!(
        calendar::date_part(&datetime_str_arg("0000-00-00"), |d| d.0).unwrap(),
        Datum::Null,
        "YEAR(\"0000-00-00\")"
    );
    assert_eq!(month(&datetime_str_arg("0000-00-00")).unwrap(), Datum::Null);
    assert_eq!(
        quarter(&datetime_str_arg("0000-00-00")).unwrap(),
        Datum::Null
    );

    // A non-zero typed datetime still reads its real components.
    let valid = Datum::Time(
        Time::new(
            CoreTime::from_date(2024, 3, 15, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap(),
    );
    assert_eq!(
        calendar::date_part(std::slice::from_ref(&valid), |d| d.0).unwrap(),
        Datum::Int(2024)
    );
    assert_eq!(month(std::slice::from_ref(&valid)).unwrap(), Datum::Int(3));
    assert_eq!(
        day_of_month(std::slice::from_ref(&valid)).unwrap(),
        Datum::Int(15)
    );
    assert_eq!(
        quarter(std::slice::from_ref(&valid)).unwrap(),
        Datum::Int(1)
    );
}

#[test]
fn month_and_monthname_source_vectors() {
    // TestMonthName and the directly shared MONTH/MONTHNAME rows in
    // TestDate. MONTH's SQL-mode-dependent zero result and TestVecMonth's
    // vector/warning assertions remain explicit ledger gaps.
    let month_cases = [
        ("2000-01-01", Datum::Int(1)),
        ("2011-11-11", Datum::Int(11)),
        ("0000-01-01", Datum::Int(1)),
        ("2008-13-01", Datum::Null),
    ];
    for (input, want) in month_cases {
        assert_eq!(month(&datetime_str_arg(input)).unwrap(), want, "{input}");
    }

    let monthname_cases = [
        ("2017-12-01", Datum::new_string("December".to_string())),
        ("2017-00-01", Datum::Null),
        ("0000-00-00", Datum::Null),
        ("0000-00-00 00:00:00.000000", Datum::Null),
        ("0000-00-00 00:00:11.000000", Datum::Null),
        ("2000-01-01", Datum::new_string("January".to_string())),
        ("2011-11-11", Datum::new_string("November".to_string())),
        ("0000-01-01", Datum::new_string("January".to_string())),
        ("2008-13-01", Datum::Null),
    ];
    for (input, want) in monthname_cases {
        assert_eq!(monthname(&[string_datum(input)]).unwrap(), want, "{input}");
    }

    assert_eq!(month(&datetime_arg(Datum::Null)).unwrap(), Datum::Null);
    assert_eq!(monthname(&[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        month(&datetime_arg(Datum::Int(20_240_315))).unwrap(),
        Datum::Int(3)
    );
    assert_eq!(
        monthname(&[Datum::Int(20_240_315)]).unwrap(),
        Datum::new_string("March".to_string())
    );
    assert!(month(&[]).is_err());
    assert!(monthname(&[string_datum("2008-01-01"), Datum::Int(1)]).is_err());
}

struct FractionalClock;

impl Columns for FractionalClock {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        // SET timestamp = 1700000000.654321 is a TypeFloat in Go and
        // materializes this binary-float nanosecond value.
        Some((1_700_000_000, 654_320_955, 0))
    }
}

#[test]
fn current_time_truncates_to_microseconds_before_fsp_rounding() {
    let clock = FractionalClock;
    assert_eq!(
        current_time(&[Datum::Int(6)], &clock).unwrap(),
        Datum::new_string("22:13:20.654320".to_string())
    );
    assert_eq!(
        utc_time(&[Datum::Int(6)], &clock).unwrap(),
        Datum::new_string("22:13:20.654320".to_string())
    );
    assert_eq!(
        utc_timestamp(&[Datum::Int(6)], &clock).unwrap(),
        Datum::new_string("2023-11-14 22:13:20.654321".to_string()),
        "UTC_TIMESTAMP rounds raw nanoseconds instead of using the duration path"
    );
}

#[test]
fn current_clock_null_fsp_follows_each_go_signature() {
    let clock = FractionalClock;
    assert_eq!(now(&[Datum::Null], &clock), now(&[Datum::Int(0)], &clock));
    assert_eq!(
        utc_timestamp(&[Datum::Null], &clock),
        utc_timestamp(&[Datum::Int(0)], &clock)
    );
    assert_eq!(
        current_time(&[Datum::Null], &clock),
        current_time(&[Datum::Int(0)], &clock)
    );
    assert_eq!(utc_time(&[Datum::Null], &clock), Ok(Datum::Null));
}

#[test]
fn go_time_vectors_cover_duration_scale_and_clamp() {
    assert_eq!(
        sec_to_time(&[Datum::new_string("123.4".to_string())]).unwrap(),
        Datum::new_string("00:02:03.400000".to_string())
    );
    assert_eq!(
        sec_to_time(&[Datum::Real(86_401.4)]).unwrap(),
        Datum::new_string("24:00:01.4".to_string())
    );
    assert_eq!(
        maketime(&[
            Datum::Int(1_000),
            Datum::Int(1),
            Datum::Decimal(crate::Decimal::from_literal("1.0")),
        ])
        .unwrap(),
        Datum::new_string("838:59:59.0".to_string())
    );
    assert_eq!(
        time_format(&[
            Datum::new_string("1990-05-07 19:30:10".to_string()),
            Datum::new_string("%H %i %s".to_string()),
        ])
        .unwrap(),
        Datum::new_string("19 30 10".to_string())
    );
    assert_eq!(
        time_format(&[
            Datum::new_string("12:34:56".to_string()),
            Datum::new_string(String::new()),
        ])
        .unwrap(),
        Datum::Null
    );
}

/// Exact scalar rows from `TestTimeToSec` at
/// `pkg/expression/builtin_time_test.go:3117`.  The source's typed
/// duration result is represented here by its integer seconds; NULL and
/// the accepted delimited/compact forms remain directly comparable.
#[test]
fn time_to_sec_source_vectors() {
    for (input, want) in [
        ("22:23:00", 80_580),
        ("00:39:38", 2_378),
        ("23:00", 82_800),
        ("00:00", 0),
        ("00:00:00", 0),
        ("23:59:59", 86_399),
        ("1:0", 3_600),
        ("1:00", 3_600),
        ("1:0:0", 3_600),
        ("-02:00", -7_200),
        ("-02:00:05", -7_205),
        ("020005", 7_205),
    ] {
        assert_eq!(
            time_to_sec(&[string_datum(input)]).unwrap(),
            Datum::Int(want),
            "TIME_TO_SEC({input:?})"
        );
    }
    assert_eq!(time_to_sec(&[Datum::Null]).unwrap(), Datum::Null);
}

/// Exact value-domain rows from `TestSecToTime` at
/// `pkg/expression/builtin_time_test.go:3162`.  String FSP and natural
/// scalar float precision are preserved; the source's explicit
/// expression decimal override (`inputDecimal == -1`) is a typed metadata
/// path and remains partial.
#[test]
fn sec_to_time_source_vectors() {
    for (input, want) in [
        (Datum::Int(2_378), "00:39:38"),
        (Datum::Int(3_864_000), "838:59:59"),
        (Datum::Int(-3_864_000), "-838:59:59"),
        (Datum::Real(86_401.4), "24:00:01.4"),
        (Datum::Real(-86_401.4), "-24:00:01.4"),
        (Datum::Real(86_401.543_21), "24:00:01.54321"),
        (string_datum("123.4"), "00:02:03.400000"),
        (string_datum("123.4567891"), "00:02:03.456789"),
        (string_datum("123"), "00:02:03.000000"),
        (string_datum("abc"), "00:00:00.000000"),
        (string_datum("1e-4"), "00:00:00.000100"),
        (string_datum("1e-5"), "00:00:00.000010"),
        (string_datum("1e-6"), "00:00:00.000001"),
        (string_datum("1e-7"), "00:00:00.000000"),
    ] {
        assert_eq!(
            sec_to_time(std::slice::from_ref(&input)).unwrap(),
            Datum::new_string(want.to_string()),
            "SEC_TO_TIME({input:?})"
        );
    }
    assert_eq!(sec_to_time(&[Datum::Null]).unwrap(), Datum::Null);
}

#[test]
fn go_week_vectors_cover_year_boundaries() {
    assert_eq!(week_of_year(2008, 2, 20, 0, false), (2008, 7));
    assert_eq!(week_of_year(2008, 2, 20, 1, false), (2008, 8));
    assert_eq!(week_of_year(2020, 1, 1, 3, true), (2020, 1));
    assert_eq!(
        yearweek(&[Datum::new_string("2000-01-01".to_string()), Datum::Int(0)]).unwrap(),
        Datum::Int(199_952)
    );
    assert_eq!(
        calendar::date_format(
            &Datum::new_string("2020-01-01".to_string()),
            &Datum::new_string("%U %u %V %v %X %x".to_string()),
        )
        .unwrap(),
        Datum::new_string("00 01 52 01 2019 2020".to_string())
    );
}

/// Exact representable rows from `TestDayName` in
/// `pkg/expression/builtin_time_test.go:458`.  The source evaluates an
/// `ETDatetime` argument, so the value-only seed keeps ordinary calendar
/// strings and NULL/arity domains while leaving zero-component handling to
/// the StatementContext boundary documented by the evidence ledger.
#[test]
fn dayname_source_vectors() {
    let cases = [
        ("2017-12-01", Datum::new_string("Friday".to_string())),
        ("0000-12-01", Datum::new_string("Friday".to_string())),
        ("2017-00-01", Datum::Null),
        ("2017-01-00", Datum::Null),
        ("0000-00-00", Datum::Null),
        ("0000-00-00 00:00:00.000000", Datum::Null),
        ("0000-00-00 00:00:11.000000", Datum::Null),
    ];
    for (input, want) in cases {
        assert_eq!(dayname(&[string_datum(input)]).unwrap(), want, "{input}");
    }
    assert_eq!(dayname(&[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        dayname(&[Datum::Int(20_171_201)]).unwrap(),
        Datum::new_string("Friday".to_string())
    );
    assert!(dayname(&[]).is_err());
}

/// Full finite source table from `TestDateFormat` at line 604.  This is
/// deliberately a string-valued temporal boundary: typed MySQL `Time`,
/// invalid-zero SQL modes, and the warning/error path are not represented
/// by `Datum` and remain explicit partial evidence rather than guessed.
#[test]
fn date_format_source_vectors() {
    let cases = [
        (
            "2010-01-07 23:12:34.12345",
            "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
            "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %",
        ),
        (
            "2012-12-21 23:12:34.123456",
            "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
            "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
        ),
        (
            "0000-01-01 00:00:00.123456",
            "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y %y %%",
            "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000 00 %",
        ),
        (
            "2016-09-3 00:59:59.123456",
            "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
            "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
        ),
        (
            "2012-10-01 00:00:00",
            "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%",
            "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %",
        ),
    ];
    for (date, format, want) in cases {
        assert_eq!(
            calendar::date_format(&string_datum(date), &string_datum(format)).unwrap(),
            Datum::new_string(want.to_string()),
            "DATE_FORMAT({date}, {format})"
        );
    }
    assert_eq!(
        calendar::date_format(&Datum::Null, &string_datum("%Y-%M-%D")).unwrap(),
        Datum::Null
    );
}

/// Representable rows from `TestStrToDate` at
/// `pkg/expression/builtin_time_test.go:1792`.  The Go function class
/// chooses typed DATE/DATETIME/DURATION signatures from the format; this
/// seed exposes the same parsed value as its canonical string while
/// retaining NULL/invalid input, fractional-second, AM/PM, and skip-token
/// behavior.
#[test]
fn str_to_date_source_vectors() {
    let cases = [
        (
            "10/28/2011 9:46:29 pm",
            "%m/%d/%Y %l:%i:%s %p",
            Some("2011-10-28 21:46:29"),
        ),
        (
            "10/28/2011 9:46:29 Pm",
            "%m/%d/%Y %l:%i:%s %p",
            Some("2011-10-28 21:46:29"),
        ),
        (
            "2011/10/28 9:46:29 am",
            "%Y/%m/%d %l:%i:%s %p",
            Some("2011-10-28 09:46:29"),
        ),
        (
            "20161122165022",
            "%Y%m%d%H%i%s",
            Some("2016-11-22 16:50:22"),
        ),
        (
            "2016 11 22 16 50 22",
            "%Y%m%d%H%i%s",
            Some("2016-11-22 16:50:22"),
        ),
        (
            "16-50-22 2016 11 22",
            "%H-%i-%s%Y%m%d",
            Some("2016-11-22 16:50:22"),
        ),
        ("16-50 2016 11 22", "%H-%i-%s%Y%m%d", None),
        (
            "15-01-2001 1:59:58.999",
            "%d-%m-%Y %I:%i:%s.%f",
            Some("2001-01-15 01:59:58.999000"),
        ),
        (
            "15-01-2001 1:59:58.1",
            "%d-%m-%Y %H:%i:%s.%f",
            Some("2001-01-15 01:59:58.100000"),
        ),
        (
            "15-01-2001 1:59:58.",
            "%d-%m-%Y %H:%i:%s.%f",
            Some("2001-01-15 01:59:58.000000"),
        ),
        (
            "15-01-2001 1:9:8.999",
            "%d-%m-%Y %H:%i:%s.%f",
            Some("2001-01-15 01:09:08.999000"),
        ),
        (
            "15-01-2001 1:9:8.999",
            "%d-%m-%Y %H:%i:%S.%f",
            Some("2001-01-15 01:09:08.999000"),
        ),
        (
            "2003-01-02 10:11:12.0012",
            "%Y-%m-%d %H:%i:%S.%f",
            Some("2003-01-02 10:11:12.001200"),
        ),
        ("2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", None),
        ("10:20:10AM", "%H:%i:%S%p", None),
        ("2020-10-10ABCD", "%Y-%m-%d%@", Some("2020-10-10")),
        ("2020-10-101234", "%Y-%m-%d%#", Some("2020-10-10")),
        ("2020-10-10....", "%Y-%m-%d%.", Some("2020-10-10")),
        ("2020-10-10.1", "%Y-%m-%d%.%#%@", Some("2020-10-10")),
        ("abcd2020-10-10.1", "%@%Y-%m-%d%.%#%@", Some("2020-10-10")),
        ("abcd-2020-10-10.1", "%@-%Y-%m-%d%.%#%@", Some("2020-10-10")),
        ("2020-10-10", "%Y-%m-%d%@", Some("2020-10-10")),
        (
            "2020-10-10abcde123abcdef",
            "%Y-%m-%d%@%#",
            Some("2020-10-10"),
        ),
        (
            "12:3:56pm  13/05/2019",
            "%r %d/%c/%Y",
            Some("2019-05-13 12:03:56"),
        ),
        ("11:13:56 am", "%r", Some("11:13:56")),
        (
            "12:13:56 13/05/2019",
            "%T %d/%c/%Y",
            Some("2019-05-13 12:13:56"),
        ),
        (
            "19:3:56  13/05/2019",
            "%T %d/%c/%Y",
            Some("2019-05-13 19:03:56"),
        ),
        ("21:13:24", "%T", Some("21:13:24")),
    ];
    for (date, format, want) in cases {
        let got = calendar::str_to_date(
            &[string_datum(date), string_datum(format)],
            &crate::NoColumns,
        )
        .unwrap();
        let want = want.map_or(Datum::Null, |want| Datum::new_string(want.to_string()));
        assert_eq!(got, want, "STR_TO_DATE({date:?}, {format:?})");
    }
    assert_eq!(
        calendar::str_to_date(&[Datum::Null, string_datum("%Y")], &crate::NoColumns).unwrap(),
        Datum::Null
    );
    assert!(calendar::str_to_date(&[string_datum("2020")], &crate::NoColumns).is_err());
}

/// A session whose `sql_mode` bits are chosen per test; everything else
/// is [`crate::NoColumns`]' defaults.
struct Modes(tidb_datatype::DateModes);

impl Columns for Modes {
    fn get(&self, _path: &[String]) -> Option<Datum> {
        None
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.0
    }
}

/// `TestIssue9732`, recorded VERBATIM in
/// `tests/integrationtest/r/expression/issues.result`: the SAME six
/// partial-format calls answer NULL under the default `sql_mode` and a
/// zero-component VALUE once `NO_ZERO_DATE` is dropped from it.
///
/// ```text
/// select str_to_date(1, '%m');
/// str_to_date(1, '%m')
/// NULL
/// ...
/// set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
/// select str_to_date(1, '%m');
/// str_to_date(1, '%m')
/// 0000-01-00
/// ```
///
/// Note that the second `sql_mode` still carries `NO_ZERO_IN_DATE` --
/// which is exactly why the deciding bit has to be `NO_ZERO_DATE` and
/// not the "in date" one that reads like the obvious candidate.
#[test]
fn str_to_date_partial_formats_follow_no_zero_date() {
    let relaxed = Modes(tidb_datatype::DateModes {
        no_zero_date: false,
        no_zero_in_date: true,
        allow_invalid_dates: false,
    });
    let cases = [
        ("1", "%m", "0000-01-00"),
        ("01", "%d", "0000-00-01"),
        ("2019", "%Y", "2019-00-00"),
        ("5,2019", "%m,%Y", "2019-05-00"),
        ("01,2019", "%d,%Y", "2019-00-01"),
        ("01,5", "%d,%m", "0000-05-01"),
    ];
    for (date, format, want) in cases {
        let args = [string_datum(date), string_datum(format)];
        assert_eq!(
            calendar::str_to_date(&args, &crate::NoColumns).unwrap(),
            Datum::Null,
            "STR_TO_DATE({date:?}, {format:?}) under the default sql_mode"
        );
        assert_eq!(
            calendar::str_to_date(&args, &relaxed).unwrap(),
            Datum::new_string(want.to_string()),
            "STR_TO_DATE({date:?}, {format:?}) without NO_ZERO_DATE"
        );
    }
}

/// `builtinStrToDateDurationSig.evalDuration` has NO `NO_ZERO_DATE`
/// rejection at all, so a TIME-only format keeps its value under the
/// mode that nulls a partial DATE. Recorded in
/// `tests/integrationtest/r/expression/issues.result` under a `sql_mode`
/// that DOES set `NO_ZERO_DATE`:
///
/// ```text
/// select str_to_date(substr(dest,1,6),'%H%i%s') from sun;
/// str_to_date(substr(dest,1,6),'%H%i%s')
/// 20:23:10
/// ```
#[test]
fn str_to_date_duration_signature_ignores_no_zero_date() {
    assert_eq!(
        calendar::str_to_date(
            &[string_datum("202310"), string_datum("%H%i%s")],
            &crate::NoColumns
        )
        .unwrap(),
        Datum::new_string("20:23:10".to_string()),
    );
}

/// `ALLOW_INVALID_DATES` is Go `checkMonthDay`'s `maxDay = 31` branch,
/// the only calendar rejection `Time.Check` still applies once
/// `IgnoreZeroInDate` is on.
#[test]
fn str_to_date_day_of_month_follows_allow_invalid_dates() {
    let args = [string_datum("2021-02-30"), string_datum("%Y-%m-%d")];
    assert_eq!(
        calendar::str_to_date(&args, &crate::NoColumns).unwrap(),
        Datum::Null,
        "February 30 is out of range by default"
    );
    assert_eq!(
        calendar::str_to_date(
            &args,
            &Modes(tidb_datatype::DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: true,
            })
        )
        .unwrap(),
        Datum::new_string("2021-02-30".to_string()),
        "ALLOW_INVALID_DATES keeps Go's maxDay = 31"
    );
}

/// Exact source rows from `TestFromDays` at
/// `pkg/expression/builtin_time_test.go:1864`.  The evaluator keeps the
/// result as a date-shaped string; the Go function's typed DATE result and
/// warning/SQL-mode state remain outside the value-only boundary.
#[test]
fn from_days_source_vectors() {
    for (day, want) in [
        (-140, "0000-00-00"),
        (140, "0000-00-00"),
        (735_000, "2012-05-12"),
        (735_030, "2012-06-11"),
        (735_130, "2012-09-19"),
        (734_909, "2012-02-11"),
        (734_878, "2012-01-11"),
        (734_927, "2012-02-29"),
        (734_634, "2011-05-12"),
        (734_664, "2011-06-11"),
        (734_764, "2011-09-19"),
        (734_544, "2011-02-11"),
        (734_513, "2011-01-11"),
        (3_652_424, "9999-12-31"),
    ] {
        assert_eq!(
            calendar::from_days(&[Datum::Int(day)]).unwrap(),
            Datum::new_string(want.to_string()),
            "FROM_DAYS({day})"
        );
    }
    assert_eq!(
        calendar::from_days(&[Datum::Int(3_652_425)]).unwrap(),
        Datum::Null
    );
    for (input, want) in [
        ("z550z", "0000-00-00"),
        ("6500z", "0017-10-18"),
        ("440", "0001-03-16"),
    ] {
        assert_eq!(
            calendar::from_days(&[string_datum(input)]).unwrap(),
            Datum::new_string(want.to_string()),
            "FROM_DAYS({input:?})"
        );
    }
    assert_eq!(calendar::from_days(&[Datum::Null]).unwrap(), Datum::Null);
}

/// Exact scalar rows from `TestDateDiff` at
/// `pkg/expression/builtin_time_test.go:1932`.  DATE/TIME typed datum
/// conversion and warning state are outside this value-only boundary;
/// the source's valid and invalid string pairs remain directly
/// representable here.
#[test]
fn date_diff_source_vectors() {
    for ((left, right), want) in [
        (("2004-05-21", "2004:01:02"), 140),
        (("2004-04-21", "2000:01:02"), 1_571),
        (
            ("2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002"),
            1,
        ),
        (("1010-11-30 23:59:59", "2010-12-31"), -365_274),
        (("1010-11-30", "2210-11-01"), -438_262),
    ] {
        assert_eq!(
            calendar::date_diff(&[string_datum(left), string_datum(right)]).unwrap(),
            Datum::Int(want),
            "DATEDIFF({left:?}, {right:?})"
        );
    }
    for (left, right) in [
        ("2004-05-21", "abcdefg"),
        ("2007-12-31 23:59:59", "23:59:59"),
        ("2007-00-31 23:59:59", "2016-01-13"),
        ("2007-10-31 23:59:59", "2016-01-00"),
        ("2007-10-31 23:59:59", "99999999-01-00"),
    ] {
        assert_eq!(
            calendar::date_diff(&[string_datum(left), string_datum(right)]).unwrap(),
            Datum::Null,
            "DATEDIFF({left:?}, {right:?})"
        );
    }
    assert_eq!(
        calendar::date_diff(&[Datum::Null, string_datum("2004-01-01")]).unwrap(),
        Datum::Null
    );
}

/// Exact scalar rows from `TestTimestampDiff` at
/// `pkg/expression/builtin_time_test.go:2130`.  The source evaluates
/// typed DATETIME arguments and StatementContext zero-date flags; these
/// string-valued rows preserve the integer results and NULL boundary
/// without inventing warning or SQL-mode state.
#[test]
fn timestamp_diff_source_vectors() {
    for ((unit, left, right), want) in [
        (("MONTH", "2003-02-01", "2003-05-01"), 3),
        (("YEAR", "2002-05-01", "2001-01-01"), -1),
        (("MINUTE", "2003-02-01", "2003-05-01 12:05:55"), 128_885),
    ] {
        assert_eq!(
            calendar::timestamp_diff(&[
                string_datum(unit),
                string_datum(left),
                string_datum(right),
            ])
            .unwrap(),
            Datum::Int(want),
            "TIMESTAMPDIFF({unit:?}, {left:?}, {right:?})"
        );
    }
    for (unit, left, right) in [
        ("MONTH", "2003-00-01", "2003-05-01"),
        ("MONTH", "2003-02-01", "2003-05-00"),
    ] {
        assert_eq!(
            calendar::timestamp_diff(&[
                string_datum(unit),
                string_datum(left),
                string_datum(right),
            ])
            .unwrap(),
            Datum::Null,
            "TIMESTAMPDIFF({unit:?}, {left:?}, {right:?})"
        );
    }
    assert_eq!(
        calendar::timestamp_diff(&[string_datum("DAY"), Datum::Null, string_datum("2017-01-01"),])
            .unwrap(),
        Datum::Null
    );
}

/// Exact scalar rows from `TestToSeconds` at
/// `pkg/expression/builtin_time_test.go:2860`.  The source evaluates a
/// typed DATETIME and enables `IgnoreZeroInDate`; this keeps ordinary
/// numeric/string dates, two-digit-year expansion, invalid temporal
/// strings, and NULL results while leaving warnings and type metadata to
/// the explicit partial boundary.
#[test]
fn to_seconds_source_vectors() {
    for (input, want) in [
        (Datum::Int(950501), 62_966_505_600),
        (string_datum("2009-11-29"), 63_426_672_000),
        (string_datum("2009-11-29 13:43:32"), 63_426_721_412),
        (string_datum("09-11-29 13:43:32"), 63_426_721_412),
        (string_datum("99-11-29 13:43:32"), 63_111_102_212),
    ] {
        assert_eq!(calendar::to_seconds(&[input]).unwrap(), Datum::Int(want),);
    }
    for input in [
        "0000-00-00",
        "1992-13-00",
        "2007-10-07 23:59:61",
        "1998-10-00",
        "1998-00-11",
        "123456789",
    ] {
        assert_eq!(
            calendar::to_seconds(&[string_datum(input)]).unwrap(),
            Datum::Null,
            "TO_SECONDS({input:?})"
        );
    }
    assert_eq!(calendar::to_seconds(&[Datum::Null]).unwrap(), Datum::Null);
}

/// Exact scalar rows from `TestToDays` at
/// `pkg/expression/builtin_time_test.go:2903`.  The source uses the
/// zero-date `TimestampDiff("DAY", ...)` path, so year-zero January 1 is
/// retained while all invalid-zero/malformed temporal inputs stay NULL.
#[test]
fn to_days_source_vectors() {
    for (input, want) in [
        (Datum::Int(950501), 728_779),
        (string_datum("2007-10-07"), 733_321),
        (string_datum("2008-10-07"), 733_687),
        (string_datum("08-10-07"), 733_687),
        (string_datum("0000-01-01"), 1),
        (string_datum("2007-10-07 00:00:59"), 733_321),
    ] {
        assert_eq!(calendar::to_days(&[input]).unwrap(), Datum::Int(want));
    }
    for input in [
        "0000-00-00",
        "1992-13-00",
        "2007-10-07 23:59:61",
        "1998-10-00",
        "123456789",
    ] {
        assert_eq!(
            calendar::to_days(&[string_datum(input)]).unwrap(),
            Datum::Null,
            "TO_DAYS({input:?})"
        );
    }
    assert_eq!(calendar::to_days(&[Datum::Null]).unwrap(), Datum::Null);
}

/// Exact scalar rows from `TestTimeDiff` at
/// `pkg/expression/builtin_time_test.go:1985`.  The Go suite also checks
/// typed result FSP and StatementContext warnings; those metadata paths
/// remain explicit partial evidence rather than being guessed here.
#[test]
fn time_diff_source_vectors() {
    for ((left, right), want) in [
        (
            ("2000:01:01 00:00:00", "2000:01:01 00:00:00.000001"),
            "-00:00:00.000001",
        ),
        (
            ("2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002"),
            "46:58:57.999999",
        ),
        (("2016-12-00 12:00:00", "2016-12-01 12:00:00"), "-24:00:00"),
        (("10:10:10", "10:9:0"), "00:01:10"),
        (("00:00:00.000000", "00:00:00.000001"), "-00:00:00.000001"),
    ] {
        assert_eq!(
            time_diff(&[string_datum(left), string_datum(right)]).unwrap(),
            Datum::new_string(want.to_string()),
            "TIMEDIFF({left:?}, {right:?})"
        );
    }
    for (left, right) in [
        ("2016-12-00 12:00:00", "10:9:0"),
        ("2016-12-00 12:00:00", ""),
    ] {
        assert_eq!(
            time_diff(&[string_datum(left), string_datum(right)]).unwrap(),
            Datum::Null,
            "TIMEDIFF({left:?}, {right:?})"
        );
    }
    assert_eq!(
        time_diff(&[Datum::Null, string_datum("00:00:00")]).unwrap(),
        Datum::Null
    );
}

/// Explicit-mode rows from `TestWeek` at line 2035, including the source
/// NULL-mode normalization to mode zero.
#[test]
fn week_source_vectors() {
    for ((date, mode), want) in [
        (("2008-02-20", 0), 7),
        (("2008-02-20", 1), 8),
        (("2008-12-31", 1), 53),
    ] {
        assert_eq!(
            week(&[string_datum(date), Datum::Int(mode)], 0).unwrap(),
            Datum::Int(want),
            "WEEK({date}, {mode})"
        );
    }
    assert_eq!(
        week(&[string_datum("2023-01-01"), Datum::Null], 0,).unwrap(),
        Datum::Int(1)
    );
}

/// Go `TestWeekWithoutModeSig`: the zero-mode arity reads the session's
/// `default_week_format` for every evaluation, including a value changed
/// after the function was built. An empty sysvar is normalized to mode zero
/// by the session before it reaches this interface.
#[test]
fn test_week_without_mode_sig() {
    struct Mode(i64);

    impl Columns for Mode {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn default_week_format(&self) -> i64 {
            self.0
        }
    }

    for (date, mode, expected) in [
        ("2008-02-20", 0, 7),
        ("2000-12-31", 0, 53),
        ("2000-12-31", 6, 1),
        ("2005-12-3", 6, 48),
        ("2008-02-20", 0, 7),
    ] {
        assert_eq!(
            dispatch("WEEK", &[string_datum(date)], &Mode(mode))
                .expect("WEEK must dispatch")
                .expect("source row must evaluate"),
            Datum::Int(expected),
            "WEEK({date}) with default_week_format={mode}"
        );
    }
}

/// Normal string/numeric rows from `TestLastDay` at line 3371.  The
/// source's day-zero result changes with SQLMode and therefore stays
/// outside this value-only test; malformed time-of-day input is still
/// representable and must not be silently accepted.
#[test]
fn last_day_source_vectors() {
    for (input, want) in [
        ("2003-02-05", "2003-02-28"),
        ("2004-02-05", "2004-02-29"),
        ("2004-01-01 01:01:01", "2004-01-31"),
    ] {
        assert_eq!(
            last_day(&[string_datum(input)]).unwrap(),
            Datum::new_string(want.to_string()),
            "LAST_DAY({input})"
        );
    }
    assert_eq!(
        last_day(&[Datum::Int(950501)]).unwrap(),
        Datum::new_string("1995-05-31".to_string())
    );
    for input in [
        "0000-00-00",
        "1992-13-00",
        "2007-10-07 23:59:61",
        "2005-00-00",
        "2005-00-01",
        "2243-01 00:00:00",
        "123456789",
    ] {
        assert_eq!(
            last_day(&[string_datum(input)]).unwrap(),
            Datum::Null,
            "LAST_DAY({input})"
        );
    }
    assert_eq!(last_day(&[Datum::Null]).unwrap(), Datum::Null);
}

#[test]
fn period_arithmetic_matches_go_vectors_and_null_ordering() {
    // `TestPeriodAdd` and `TestPeriodDiff` in
    // `pkg/expression/builtin_time_test.go`.
    let add_cases = [
        ((201611, 2), 201701),
        ((201611, 3), 201702),
        ((201611, -13), 201510),
        ((1611, 3), 201702),
        ((7011, 3), 197102),
    ];
    for ((period, months), want) in add_cases {
        assert_eq!(
            period_add(&[Datum::Int(period), Datum::Int(months)]).unwrap(),
            Datum::Int(want)
        );
    }
    assert!(period_add(&[Datum::Int(0), Datum::Int(3)]).is_err());
    assert_eq!(
        period_add(&[Datum::Int(0), Datum::Null]).unwrap(),
        Datum::Null,
        "both arguments are evaluated before TiDB validates the period"
    );

    let diff_cases = [
        ((201611, 201611), 0),
        ((200802, 200703), 11),
        ((201701, 201611), 2),
        ((201702, 201611), 3),
        ((201510, 201611), -13),
        ((201702, 1611), 3),
        ((197102, 7011), 3),
    ];
    for ((period1, period2), want) in diff_cases {
        assert_eq!(
            period_diff(&[Datum::Int(period1), Datum::Int(period2)]).unwrap(),
            Datum::Int(want)
        );
    }
    assert!(period_diff(&[Datum::Int(0), Datum::Int(201611)]).is_err());
    assert_eq!(
        period_diff(&[Datum::Null, Datum::Int(201611)]).unwrap(),
        Datum::Null
    );
}

#[test]
fn period_arithmetic_retains_go_unsigned_wrapping() {
    // Direct `goeval` probes for the Go uint64 helper / int64 conversion
    // boundary in `builtinPeriodAddSig` and `builtinPeriodDiffSig`.
    assert_eq!(
        period_add(&[Datum::Int(i64::MAX), Datum::Int(1)]).unwrap(),
        Datum::Int(i64::MIN)
    );
    assert_eq!(
        period_diff(&[Datum::Int(i64::MAX), Datum::Int(197001)]).unwrap(),
        Datum::Int(1_106_804_644_422_549_462)
    );
}
