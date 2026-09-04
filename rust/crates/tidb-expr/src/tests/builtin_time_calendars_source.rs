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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Remaining `pkg/expression/builtin_time_test.go` rows (the alphabetical
//! tail after part6's `TestUTCTime` item: `TestUTCDate` … `TestCurrentTso`
//! at origin/master lines 1779-3723) that sibling carriers do not already
//! pin. Where an earlier carrier exists (`time_fn::tests`,
//! `time_fn/convert_tz`, `time_fn/session_tz`, `tests/go_time_values`) its
//! rows were re-read from `origin/master` and are cited in the batch receipt
//! rather than duplicated here.

use super::*;
use crate::context::SessionTimeZone;
use crate::time_fn::dispatch;
use std::cell::RefCell;

/// A session pinned to UTC with a FIXED statement clock: everything Go's
/// `mock.Context` + `resetStmtContext` gives a time test without host-clock
/// flakiness.
struct UtcClock {
    utc_secs: i64,
    sysdate_is_now: bool,
}

impl Columns for UtcClock {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((self.utc_secs, 0, 0))
    }

    fn sysdate_is_now(&self) -> bool {
        self.sysdate_is_now
    }
}

/// Evaluates one builtin through the time family's dispatch seam and unwraps
/// both the dispatch hit and the evaluation result.
fn dispatched(name: &str, vals: &[Datum], cols: &dyn Columns) -> Datum {
    dispatch(name, vals, cols)
        .expect("the name belongs to the time family")
        .expect("source row must evaluate")
}

/// GO PORT of `builtin_time_test.go:1779 TestUTCDate`. Go evaluates
/// `builtinUTCDateSig` (`pkg/expression/builtin_time.go:2520 evalTime`) and
/// requires the rendered date to be `>=` the date captured at test start --
/// monotonic against the statement clock. A fixed clock pins the SAME
/// contract deterministically: the result is exactly the statement instant's
/// UTC calendar date, independent of any session zone.
#[test]
fn utc_date_answers_the_utc_statement_date() {
    // 2021-03-01 00:00:30 UTC (one instant inside Go's own assertion band).
    let clock = UtcClock {
        utc_secs: 1_614_556_830,
        sysdate_is_now: false,
    };
    assert_eq!(
        dispatched("UTC_DATE", &[], &clock),
        Datum::new_string("2021-03-01")
    );
    // Midnight crossing picks up the new UTC day; nothing about a session
    // zone can shift it.
    let clock = UtcClock {
        utc_secs: 1_614_556_800 + 86_400,
        sysdate_is_now: false,
    };
    assert_eq!(
        dispatched("UTC_DATE", &[], &clock),
        Datum::new_string("2021-03-02")
    );
}

/// GO PORT of `builtin_time_test.go:2099 TestYearWeek`'s three unpinned
/// source rows. `builtinYearWeekSig` keeps `2016-00-05` NULL like every
/// invalid-zero temporal (`week_of_year(..., ignoreIsZero = true)`), and the
/// two dev.mysql.com rows answer `198652` / `199952`.
#[test]
fn yearweek_source_rows_pin_zero_month_null_and_boundary_years() {
    let string_arg = |text: &str| Datum::new_string(text);
    assert_eq!(
        dispatched(
            "YEARWEEK",
            &[string_arg("1987-01-01"), Datum::Int(0)],
            &NoColumns,
        ),
        Datum::Int(198_652)
    );
    assert_eq!(
        dispatched(
            "YEARWEEK",
            &[string_arg("2000-01-01"), Datum::Int(0)],
            &NoColumns
        ),
        Datum::Int(199_952),
    );
    assert_eq!(
        dispatched("YEARWEEK", &[string_arg("2016-00-05")], &NoColumns),
        Datum::Null
    );
}

/// GO PORT of `builtin_time_test.go:2130 TestTimestampDiff`'s trailing flag
/// block: with `IgnoreTruncateErr | IgnoreZeroInDate` the month-zero DAY row
/// still answers NULL, and a plain NULL operand propagates.
#[test]
fn timestamp_diff_flag_block_rows_stay_null() {
    for args in [
        vec![
            Datum::new_string("DAY"),
            Datum::new_string("2017-01-00"),
            Datum::new_string("2017-01-01"),
        ],
        vec![
            Datum::new_string("DAY"),
            Datum::Null,
            Datum::new_string("2017-01-01"),
        ],
    ] {
        assert_eq!(dispatched("TIMESTAMPDIFF", &args, &NoColumns), Datum::Null);
    }
}

/// GO PORT of the representable rows of
/// `builtin_time_test.go:2185 TestUnixTimestamp`, evaluated under the table's
/// own `time.UTC` session zone. Go selects one of THREE signatures from the
/// argument FieldType (`builtinUnixTimestamp*Sig`,
/// `pkg/expression/builtin_time.go:4421-4427`): Int results for fsp-0 inputs,
/// DECIMAL otherwise, and 0 -- never NULL -- for out-of-range instants.
#[test]
fn unix_timestamp_value_table_under_utc() {
    let s = |text: &str| Datum::new_string(text);
    let utc = ZonedNoColumns(SessionTimeZone::utc());

    for (arg, want_label) in [
        (Datum::Int(151_113), "INT:1447372800"),
        (Datum::Int(20_151_113), "INT:1447372800"),
        (Datum::new_string("2015-11-13 10:20:19"), "INT:1447410019"),
        (
            Datum::new_string("2015-11-13 10:20:19.012"),
            "DEC:1447410019.012",
        ),
        (s("1970-01-01 00:00:00"), "INT:0"),
        (s("3001-01-18 23:59:59.999999"), "DEC:32536771199.999999"),
        // The two out-of-range rows answer decimal ZERO; Go compares through
        // MyDecimal ToString, which trims the all-zero fraction to "0", so
        // the zero-valued results are compared on their numeric content.
        (s("1969-12-31 23:59:59.999999"), "zero-decimal"),
        (s("3001-01-19 00:00:00.000000"), "zero-decimal"),
    ] {
        let got = dispatched("UNIX_TIMESTAMP", &[arg.clone()], &utc);
        if want_label == "zero-decimal" {
            match &got {
                Datum::Decimal(value) => {
                    assert!(value.is_zero(), "{arg:?} must answer decimal zero");
                }
                other => panic!("{arg:?} must answer a DECIMAL, got {other:?}"),
            }
        } else {
            assert_eq!(got.label(), want_label, "{arg:?}");
        }
    }

    // go-parity-gap: this evaluator parses COMPACT NUMERIC datetime texts
    // only up to the eight-digit YYYYMMDD form. Go's table rows
    // `{Int(151113102019)}`, `{Real(151113102019)}` rely on
    // `types.ParseTimeFromNum`'s 12-digit YYMMDDHHMMSS reading and expect
    // `1447410019`, `{Decimal(7-scale "...1234567")}` additionally needs
    // fsp-7 half-up rounding (`...123457`), and `{'2017-00-02'}` needs the
    // IgnoreZeroInDate month-zero reader Go answers INT:0 through; none of
    // those readers exist at this evaluator seam, so they stay unproven
    // rather than approximated.
}

/// GO PORT of `builtin_time_test.go:2275 TestDateArithFuncs` (the DAY/HOUR/
/// MONTH/YEAR, overflow, and NULL-interval blocks). Rows whose expectation is
/// the empty string are Go printing a NULL result via `ToString`; they are
/// asserted as NULL here under their own names.
#[test]
fn date_arith_day_month_year_overflow_tables_match_master() {
    for (sql, want) in [
        ("date_add('2016-12-31', interval 1 day)", Some("2017-01-01")),
        (
            "date_add('2017-01-01', interval -1 day)",
            Some("2016-12-31"),
        ),
        (
            "date_add('2017-01-01', interval -0.5 day)",
            Some("2016-12-31"),
        ),
        (
            "date_add('2017-01-01', interval -1.4 day)",
            Some("2016-12-31"),
        ),
        ("date_add('1998-10-00', interval 1 day)", None),
        ("date_add('2004-00-01', interval 1 day)", None),
        (
            "date_add('20111111', interval '-123' day)",
            Some("2011-07-11"),
        ),
        ("date_sub('2017-01-01', interval 1 day)", Some("2016-12-31")),
        (
            "date_sub('2016-12-31', interval -1 day)",
            Some("2017-01-01"),
        ),
        (
            "date_sub('2016-12-31', interval -0.5 day)",
            Some("2017-01-01"),
        ),
        (
            "date_sub('2016-12-31', interval -1.4 day)",
            Some("2017-01-01"),
        ),
        ("date_sub('1998-10-00', interval 31 day)", None),
        ("date_sub('2004-00-01', interval 31 day)", None),
        (
            "date_sub('20111111', interval '-123' day)",
            Some("2012-03-13"),
        ),
        // The NULL-interval block.
        ("date_add('2016-12-31', interval null day)", None),
        ("date_sub('2017-01-01', interval null day)", None),
    ] {
        assert_eq!(
            e(sql),
            match want {
                Some(text) => format!("STR:{text}"),
                None => "NULL".to_string(),
            },
            "{sql}"
        );
    }

    // TestIssue11645 HOUR block. Go's year-zero answers keep TIME OF DAY on
    // the zero date (`0000-00-00 22:00:00`); `-8785` hours leaves it.
    for (input, hours, expect) in [
        ("1000-01-01 00:00:00", -2, Some("0999-12-31 22:00:00")),
        ("1000-01-01 00:00:00", -200, Some("0999-12-23 16:00:00")),
        ("0001-01-01 00:00:00", -2, Some("0000-00-00 22:00:00")),
        ("0001-01-01 00:00:00", -25, Some("0000-00-00 23:00:00")),
        ("0001-01-01 00:00:00", -8784, Some("0000-00-00 00:00:00")),
        ("0001-01-01 00:00:00", -8785, None),
        ("0001-01-02 00:00:00", -2, Some("0001-01-01 22:00:00")),
        ("0001-01-02 00:00:00", -24, Some("0001-01-01 00:00:00")),
        ("0001-01-02 00:00:00", -25, Some("0000-00-00 23:00:00")),
        ("0001-01-02 00:00:00", -8785, Some("0000-00-00 23:00:00")),
    ] {
        let sql = format!("date_add('{input}', interval {hours} hour)");
        assert_eq!(
            e(&sql),
            match expect {
                Some(text) => format!("STR:{text}"),
                None => "NULL".to_string(),
            },
            "{sql}"
        );
    }

    // MONTH end-of-month clamps (Go's `types.AddDate`).
    for (input, months, expect) in [
        ("1900-01-31", 1, "1900-02-28"),
        ("2000-01-31", 1, "2000-02-29"),
        ("2016-01-31", 1, "2016-02-29"),
        ("2018-07-31", 1, "2018-08-31"),
        ("2018-08-31", 1, "2018-09-30"),
        ("2018-07-31", 2, "2018-09-30"),
        ("2016-01-31", 27, "2018-04-30"),
        ("2000-02-29", 12, "2001-02-28"),
        ("2000-11-30", 1, "2000-12-30"),
    ] {
        let sql = format!("date_add('{input}', interval {months} month)");
        assert_eq!(e(&sql), format!("STR:{expect}"), "{sql}");
    }

    // YEAR additions pin leap-day folding onto Feb 28.
    for (input, years, expect) in [
        ("1899-02-28", 1, "1900-02-28"),
        ("1901-02-28", -1, "1900-02-28"),
        ("2000-02-29", 1, "2001-02-28"),
        ("2001-02-28", -1, "2000-02-28"),
        ("2004-02-29", 1, "2005-02-28"),
        ("2005-02-28", -1, "2004-02-28"),
    ] {
        let sql = format!("date_add('{input}', interval {years} year)");
        assert_eq!(e(&sql), format!("STR:{expect}"), "{sql}");
    }

    // Overflow block: ±1465647104 YEARs leave the range on DATE and DATETIME
    // inputs alike; ±266076160 QUARTERs leave it on the datetime row. Both
    // directions of both units answer NULL.
    for sign in ["date_add", "date_sub"] {
        for amount in [-1_465_647_104i64, 1_465_647_104] {
            for input in ["2008-11-23", "2000-04-13 07:17:02"] {
                let sql = format!("{sign}('{input}', interval {amount} year)");
                assert_eq!(e(&sql), "NULL", "{sql}");
            }
        }
        for amount in [-266_076_160i64, 266_076_160] {
            let sql = format!("{sign}('2008-11-23 22:47:31', interval {amount} quarter)");
            assert_eq!(e(&sql), "NULL", "{sql}");
        }
    }

    // go-parity-gap: TestDateArithFuncs' trailing DURATION block feeds a
    // typed DURATION datum as DATE_ADD's first argument
    // (`types.StrToDuration(...) => MakeDatums(dur, format, unit)`); a
    // duration-shaped first argument selects Go's duration result signature,
    // which needs the argument FieldType seam that only live SQL carries, so
    // the `00:00:00.000100`-style duration answers stay unproven here.
}

/// GO PORT of the DELIMITED-input rows of
/// `builtin_time_test.go:2563 TestTimestamp`: one- and two-argument string
/// forms. Numeric and DECIMAL source rows are covered by the source-type
/// regressions immediately below.
#[test]
fn timestamp_delimited_argument_rows_match_master() {
    let cases = [
        ("timestamp('2017-01-18')", "2017-01-18 00:00:00"),
        ("timestamp('2017-01-18 12:30:56')", "2017-01-18 12:30:56"),
        // Two arguments add the duration to the base.
        ("timestamp('2017-01-18', '12:30:59')", "2017-01-18 12:30:59"),
        (
            "timestamp('2017-01-18 01:01:01', '12:30:50')",
            "2017-01-18 13:31:51",
        ),
        (
            "timestamp('2017-01-18 01:01:01', '838:59:59')",
            "2017-02-22 00:01:00",
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(e(sql), format!("STR:{want}"), "{sql}");
    }
    for sql in [
        "timestamp(null)",
        "timestamp(0.9999999)",
        "timestamp(1.234)",
        "timestamp('0000-01-01', '1')",
    ] {
        assert_eq!(e(sql), "NULL", "{sql}");
    }
}

/// The delimiter-free STRING rows from Go's `TestTimestamp`. Integer-shaped
/// dates and datetimes use the same 6/8/12/14-digit reader as
/// `types.ParseTimeWithString`; a fractional suffix on the full 14-digit
/// datetime is rounded to Go's six-digit datetime precision. Date-only
/// compact fractions use Go's hour-suffix rule in the string signature.
#[test]
fn timestamp_compact_string_rows_match_master() {
    for (sql, want) in [
        ("timestamp('20170118')", "STR:2017-01-18 00:00:00"),
        ("timestamp('170118')", "STR:2017-01-18 00:00:00"),
        ("timestamp('11111111111')", "STR:2011-11-11 11:11:01"),
        ("timestamp('20170118123056')", "STR:2017-01-18 12:30:56"),
        (
            "timestamp('20170118123050.999')",
            "STR:2017-01-18 12:30:50.999",
        ),
        (
            "timestamp('20170118123050.1234567')",
            "STR:2017-01-18 12:30:50.123457",
        ),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
}

/// Numeric/DECIMAL `TIMESTAMP` rows from Go's `TestTimestamp`. These remain
/// separate from the floating-point carrier so the source-type parser choice
/// is explicit in the regression inventory.
#[test]
fn timestamp_numeric_integer_and_decimal_rows_match_master() {
    for (sql, want) in [
        ("timestamp(170118)", "STR:2017-01-18 00:00:00"),
        ("timestamp(20170118)", "STR:2017-01-18 00:00:00"),
        (
            "timestamp(20170118123950.123)",
            "STR:2017-01-18 12:39:50.123",
        ),
        (
            "timestamp(20170118123950.999)",
            "STR:2017-01-18 12:39:50.999",
        ),
        ("timestamp(0.4352)", "STR:0000-00-00 00:00:00.4352"),
        ("timestamp(0.12345678)", "STR:0000-00-00 00:00:00.123457"),
        ("timestamp(101.234)", "STR:2000-01-01 00:00:00.000"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    for sql in ["timestamp(0.9999999)", "timestamp(1.234)"] {
        assert_eq!(e(sql), "NULL", "{sql}");
    }
}

/// Source-type-specific floating-point rows that use Go's
/// `ParseTimeFromFloatString` timestamp reader.
#[test]
fn timestamp_float_rows_match_master() {
    assert_eq!(e("timestamp(20170118.999)"), "STR:2017-01-18 00:00:00.000");
}

/// Fraction-only DECIMAL rows from Go's `TestIssue25093`; these exercise the
/// source numeric parser's zero-date conversion rather than compact digits.
#[test]
fn timestamp_fraction_only_decimal_rows_match_master() {
    assert_eq!(e("timestamp(0.123)"), "STR:0000-00-00 00:00:00.123");
}

/// INTEGER-second MAKETIME rows of
/// `builtin_time_test.go:2677 TestMakeTime`: domain overflow past minute/
/// second rollover, garbage strings, mid-table NULL arguments, and Go's
/// CAST(-1 AS UNSIGNED) case. Value-level twins sit in
/// `tests/go_time_values.rs`.
#[test]
fn maketime_integer_second_master_rows_overflow_garbage_and_null_arguments() {
    for (sql, want) in [
        ("maketime(12, 15, 60)", "NULL"),
        ("maketime(12, 15, '60')", "NULL"),
        ("maketime(12, 60, 0)", "NULL"),
        ("maketime(12, '60', 0)", "NULL"),
        ("maketime(12, 15, null)", "NULL"),
        ("maketime(12, null, 0)", "NULL"),
        ("maketime(null, 15, 0)", "NULL"),
        ("maketime(null, null, null)", "NULL"),
        ("maketime('', '', '')", "DUR:00:00:00.000000"),
        ("maketime('h', 'm', 's')", "DUR:00:00:00.000000"),
        ("maketime(1000, 1, 1)", "DUR:838:59:59"),
        ("maketime(1000, 59.5, 1)", "NULL"),
    ] {
        assert_eq!(chunk_e(sql), want, "{sql}");
    }
}

// The remaining floating-second and unsigned-hour MAKETIME rows are active
// in `tests::go_time_values::go_test_maketime_float_seconds_and_unsigned_hour`,
// alongside the value-level MakeTime table.  Keep this calendar source file
// focused on its date-arithmetic and timestamp carriers.

/// GO PORT of `builtin_time_test.go:2946 TestTimestampAdd`'s delimited-date
/// rows: second/microsecond fractions, minute rounding, WEEK/DAY spans, the
/// QUARTER overflow-over-year case, both issued month-clamp sweeps (#41052
/// forward, #54908 backward), and the range-exit rows whose empty Go strings
/// are NULL results here. The `{MICROSECOND,1,950501}`-style integer-date
/// inputs remain an explicit TIMESTAMPADD source-type gap, and the `10000*365
/// +/- 1` MONTH amounts are additionally rejected outright by this evaluator's
/// unit dispatcher rather than answering NULL (recorded as a gap in the batch
/// receipt, not approximated).
#[test]
fn timestamp_add_delimited_rows_match_master() {
    let cases = [
        (
            "timestampadd(MINUTE, 1, '2003-01-02')",
            "2003-01-02 00:01:00",
        ),
        (
            "timestampadd(WEEK, 1, '2003-01-02 23:59:59')",
            "2003-01-09 23:59:59",
        ),
        (
            "timestampadd(QUARTER, 3, '1995-05-01')",
            "1996-02-01 00:00:00",
        ),
        // Fractional SECOND: microsecond carry into whole seconds, truncation
        // of the sub-microsecond tail, and the below-one-microsecond floor.
        (
            "timestampadd(SECOND, 1.1, '1995-05-01')",
            "1995-05-01 00:00:01.100000",
        ),
        (
            "timestampadd(SECOND, -1, '1995-05-01')",
            "1995-04-30 23:59:59",
        ),
        (
            "timestampadd(SECOND, -1.1, '1995-05-01')",
            "1995-04-30 23:59:58.900000",
        ),
        (
            "timestampadd(SECOND, 0.0000099999, '1995-05-01')",
            "1995-05-01 00:00:00.000009",
        ),
        (
            "timestampadd(SECOND, -0.0000099999, '1995-05-01')",
            "1995-04-30 23:59:59.999991",
        ),
        // MINUTE rounds half-away-from-zero onto the whole count; both
        // source spellings of midnight agree.
        (
            "timestampadd(MINUTE, 1.5, '1995-05-01 00:00:00')",
            "1995-05-01 00:02:00",
        ),
        (
            "timestampadd(MINUTE, 1.5, '1995-05-01 00:00:00.000000')",
            "1995-05-01 00:02:00",
        ),
        // A negative MICROSECOND span that zeroes the result drops the
        // fraction entirely (Go keeps DefaultFsp there).
        (
            "timestampadd(MICROSECOND, -100, '1995-05-01 00:00:00.0001')",
            "1995-05-01 00:00:00",
        ),
        // Issue #41052: one-month additions clamp February/30-day tails.
        (
            "timestampadd(MONTH, 1, '2024-01-31')",
            "2024-02-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, 1, '2024-01-28')",
            "2024-02-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, 1, '2024-10-31')",
            "2024-11-30 00:00:00",
        ),
        (
            "timestampadd(MONTH, 3, '2024-01-31')",
            "2024-04-30 00:00:00",
        ),
        (
            "timestampadd(MONTH, 15, '2024-01-31')",
            "2025-04-30 00:00:00",
        ),
        (
            "timestampadd(MONTH, 10, '2024-10-31')",
            "2025-08-31 00:00:00",
        ),
        (
            "timestampadd(MONTH, 1, '2024-11-30')",
            "2024-12-30 00:00:00",
        ),
        (
            "timestampadd(MONTH, 13, '2024-11-30')",
            "2025-12-30 00:00:00",
        ),
        // Issue #54908: negative multi-month walks and their February folds.
        (
            "timestampadd(MONTH, 0, '2024-09-01')",
            "2024-09-01 00:00:00",
        ),
        (
            "timestampadd(MONTH, -10, '2024-09-01')",
            "2023-11-01 00:00:00",
        ),
        (
            "timestampadd(MONTH, -2, '2024-04-28')",
            "2024-02-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -2, '2024-04-29')",
            "2024-02-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, -2, '2024-04-30')",
            "2024-02-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, -1, '2024-03-31')",
            "2024-02-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, -1, '2024-03-25')",
            "2024-02-25 00:00:00",
        ),
        (
            "timestampadd(MONTH, -12, '2024-03-31')",
            "2023-03-31 00:00:00",
        ),
        (
            "timestampadd(MONTH, -13, '2024-03-31')",
            "2023-02-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -14, '2024-03-31')",
            "2023-01-31 00:00:00",
        ),
        (
            "timestampadd(MONTH, -11, '2025-02-28')",
            "2024-03-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -13, '2025-02-28')",
            "2024-01-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -11, '2024-02-29')",
            "2023-03-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, -12, '2024-02-29')",
            "2023-02-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -13, '2024-02-29')",
            "2023-01-29 00:00:00",
        ),
        (
            "timestampadd(MONTH, -11, '2023-02-28')",
            "2022-03-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -15, '2023-03-20')",
            "2021-12-20 00:00:00",
        ),
        (
            "timestampadd(MONTH, -15, '2023-03-31')",
            "2021-12-31 00:00:00",
        ),
        (
            "timestampadd(MONTH, 12, '2020-02-29')",
            "2021-02-28 00:00:00",
        ),
        (
            "timestampadd(MONTH, -12, '2020-02-29')",
            "2019-02-28 00:00:00",
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(e(sql), format!("STR:{want}"), "{sql}");
    }
    // Range exits answer NULL under a 1292-shaped warning (Go's "" rows);
    // leap-day folding stays symmetric across the pivot.
    for sql in [
        "timestampadd(MONTH, 3, '9999-10-29')",
        "timestampadd(MONTH, -3, '0001-01-29')",
    ] {
        assert_eq!(e(sql), "NULL", "{sql}");
    }
}

/// GO PORT of `builtin_time_test.go:3035 TestPeriodAdd`'s failing row and
/// `:3298 TestPeriodDiff`'s error table SHAPE: periods outside `[1, 9999999]`
/// reject the call outright (both rows in the source error as
/// "[expression:1210]Incorrect arguments to period_diff").
#[test]
fn period_invalid_period_reject_the_call() {
    for name_args in [
        ("PERIOD_ADD", vec![Datum::Int(12_323), Datum::Int(10)]),
        ("PERIOD_ADD", vec![Datum::Int(0), Datum::Int(3)]),
        ("PERIOD_DIFF", vec![Datum::Int(0), Datum::Int(999_999_999)]),
        ("PERIOD_DIFF", vec![Datum::Int(9_999_999), Datum::Int(0)]),
        ("PERIOD_DIFF", vec![Datum::Int(411), Datum::Int(200_413)]),
        (
            "PERIOD_DIFF",
            vec![Datum::Int(197_000), Datum::Int(207_700)],
        ),
        ("PERIOD_DIFF", vec![Datum::Int(12_509), Datum::Int(12_323)]),
    ] {
        let (name, args) = name_args;
        let outcome = dispatch(name, &args, &NoColumns);
        assert!(
            outcome.map(|result| result.is_err()).unwrap_or(false),
            "{name}({args:?}) must fail"
        );
    }
    // go-parity-gap: Go's exact message is "[expression:1210]Incorrect
    // arguments to period_diff"; this evaluator raises
    // EvalError::Unsupported("invalid PERIOD_DIFF period") --
    // the rejection matches, the surfaced text does not.
    // The nil blocks of TestPeriodAdd/TestPeriodDiff.
    assert_eq!(
        dispatched("PERIOD_DIFF", &[Datum::Null, Datum::Int(0)], &NoColumns),
        Datum::Null
    );
    assert_eq!(
        dispatched("PERIOD_DIFF", &[Datum::Int(0), Datum::Null], &NoColumns),
        Datum::Null
    );
    assert_eq!(
        dispatched("PERIOD_ADD", &[Datum::Int(0), Datum::Null], &NoColumns),
        Datum::Null,
        "both arguments evaluate before TiDB validates the period"
    );
}

/// GO PORT of `builtin_time_test.go:3071 TestTimeFormat`'s remaining table
/// rows (the `%H %k %h %I %l` hour-family variants and the `%r`/`%T`/
/// `%h:%i%p` composite). The `'12:34:56' => ''` issue-59445 NULL row and the
/// NULL-format row already sit in `go_time_vectors_cover_duration_scale_and_clamp`.
#[test]
fn time_format_hour_family_rows_match_master() {
    for (time, format, want) in [
        ("23:00:00", "%H %k %h %I %l", "23 23 11 11 11"),
        ("11:00:00", "%H %k %h %I %l", "11 11 11 11 11"),
        (
            "17:42:03.000001",
            "%r %T %h:%i%p %h:%i:%s %p %H %i %s",
            "05:42:03 PM 17:42:03 05:42PM 05:42:03 PM 17 42 03",
        ),
        ("07:42:03.000001", "%f", "000001"),
    ] {
        let got = dispatched(
            "TIME_FORMAT",
            &[Datum::new_string(time), Datum::new_string(format)],
            &NoColumns,
        );
        assert_eq!(
            got,
            Datum::new_string(want),
            "TIME_FORMAT({time:?}, {format:?})"
        );
    }
    // SELECT TIME_FORMAT(null,'%H %k %h %I %l').
    assert_eq!(
        dispatched(
            "TIME_FORMAT",
            &[Datum::Null, Datum::new_string("%H %k %h %I %l")],
            &NoColumns
        ),
        Datum::Null
    );
}

/// GO PORT of `builtin_time_test.go:3426 TestWithTimeZone`: SYSDATE(fsp),
/// CURDATE, CURRENT_TIME(fsp) and CURTIME all read the SESSION zone's local
/// wall clock. Go asserts each result sits within two seconds of `now` in
/// Asia/Tokyo; routing SYSDATE through the statement clock makes the
/// identical shape deterministic -- every rendered value is exactly the
/// Tokyo rendering of the fixed instant, truncated/rounded per its fsp.
#[test]
fn with_time_zone_clock_builtins_render_the_session_zone() {
    struct TokyoClock(i64);

    impl Columns for TokyoClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            // tz_offset_seconds mirrors Go's session Location: +09:00.
            Some((self.0, 0, 9 * 3600))
        }

        fn sysdate_is_now(&self) -> bool {
            true
        }
    }

    // 2021-03-01 08:01:09 UTC == 2021-03-01 17:01:09 Tokyo.
    let ctx = TokyoClock(1_614_585_669);

    // SYSDATE(2): timezone-local, fsp-2 truncation.
    assert_eq!(
        dispatched("SYSDATE", &[Datum::Int(2)], &ctx),
        Datum::new_string("2021-03-01 17:01:09.00"),
        "SYSDATE(2) in Asia/Tokyo"
    );
    assert_eq!(
        dispatched("CURDATE", &[], &ctx),
        Datum::new_string("2021-03-01")
    );
    // CURRENT_TIME(2) renders a Duration at fsp 2; CURTIME() stays default.
    assert_eq!(
        dispatched("CURRENT_TIME", &[Datum::Int(2)], &ctx),
        Datum::new_string("17:01:09.00")
    );
    assert_eq!(
        dispatched("CURTIME", &[], &ctx),
        Datum::new_string("17:01:09")
    );
}

/// GO PORT of `builtin_time_test.go:3471 TestTidbParseTso`: positive TS
/// values decode their physical half in the (UTC) session zone, a STRING
/// argument coerces, TSO 1 is the epoch, and non-positive inputs -- whether
/// int or string -- are NULL.
#[test]
fn tidb_parse_tso_master_vectors_under_utc() {
    let utc = ZonedNoColumns(SessionTimeZone::utc());
    for (arg, want) in [
        (
            Datum::Int(404_411_537_129_996_288),
            "STR:2018-11-20 09:53:04.877000",
        ),
        (
            Datum::new_string("404411537129996288"),
            "STR:2018-11-20 09:53:04.877000",
        ),
        (Datum::Int(1), "STR:1970-01-01 00:00:00.000000"),
    ] {
        assert_eq!(
            dispatched("TIDB_PARSE_TSO", &[arg.clone()], &utc).label(),
            want,
            "{arg:?}"
        );
    }
    for arg in [
        Datum::Int(0),
        Datum::Int(-1),
        Datum::new_string("-1"),
        Datum::Null,
    ] {
        assert_eq!(
            dispatched("TIDB_PARSE_TSO", &[arg.clone()], &utc),
            Datum::Null,
            "{arg:?}"
        );
    }
}

/// GO PORT of `builtin_time_test.go:3509 TestTidbParseTsoLogical`'s own three
/// positive rows: consecutive TS values expose consecutive logical counters
/// under the low-18-bits mask. The NULL domain and the goeval carrier's wider
/// row set live in `time_fn::tests::tidb_parse_tso_logical_vectors`.
#[test]
fn tidb_parse_tso_logical_consecutive_tso_counters() {
    for (tso, want) in [
        (404_411_537_129_996_288, 0),
        (404_411_537_129_996_289, 1),
        (404_411_537_129_996_290, 2),
    ] {
        assert_eq!(
            dispatched("TIDB_PARSE_TSO_LOGICAL", &[Datum::Int(tso)], &NoColumns),
            Datum::Int(want),
            "{tso}"
        );
    }
    for arg in [
        Datum::Int(0),
        Datum::Int(-1),
        Datum::new_string("-1"),
        Datum::Null,
    ] {
        assert_eq!(
            dispatched("TIDB_PARSE_TSO_LOGICAL", &[arg], &NoColumns),
            Datum::Null
        );
    }
}

/// GO PORT of `builtin_time_test.go:3546 TestTiDBBoundedStaleness`.
#[test]
#[ignore = "go-parity-gap: TIDB_BOUNDED_STALENESS needs Go's injectSafeTS failpoint seam plus oracle.GoTimeToTS epoch arithmetic (builtinTiDBBoundedStalenessSig, builtin_time.go:7090); neither exists in this evaluator"]
fn tidb_bounded_staleness_safets_windows_and_monotonicity() {}

/// GO PORT of `builtin_time_test.go:3669 TestStrDatetimeAddDurationFreezesWarningArg`:
/// a first argument that fails datetime parsing appends EXACTLY ONE warning
/// carrying the argument text (Go: `strDatetimeAddDuration`,
/// `pkg/expression/builtin_time.go:4827` warns "regardless of sql_mode" and
/// answers NULL) -- and the warning text stays frozen even though Go captured
/// the raw bytes behind a shared buffer. Rust captures immutable text, so the
/// freeze property holds trivially; what is asserted here is the one-warning
/// count and content.
#[test]
fn str_datetime_add_duration_warns_once_with_frozen_arg_text() {
    struct WarnSink(RefCell<Vec<String>>);

    impl Columns for WarnSink {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, _code: u16, message: &str) {
            self.0.borrow_mut().push(message.to_string());
        }
    }

    let sink = WarnSink(RefCell::new(Vec::new()));
    let zero_duration = v("'00:00:00'");
    let result = dispatch("ADDTIME", &[Datum::new_string("abc"), zero_duration], &sink)
        .unwrap()
        .unwrap();
    assert!(result.is_null());
    let warnings = sink.0.borrow();
    assert_eq!(warnings.len(), 1, "exactly one warning: {warnings:?}");
    assert!(
        warnings[0].contains("abc"),
        "warning carries the argument: {}",
        warnings[0]
    );
    assert!(
        warnings[0].starts_with("Incorrect datetime value: "),
        "Go's strDatetimeAddDuration message shape: {}",
        warnings[0]
    );
}

/// GO PORT of `builtin_time_test.go:3690 TestCurrentTso`: the zero-argument
/// builtin reports the transaction start TSO the session exposes
/// (`builtinTiDBCurrentTsoSig.evalInt`, `pkg/expression/builtin_time.go:7259`),
/// and a session-less resolver reports Go's zero TSO state.
#[test]
fn current_tso_reports_session_transaction_tso() {
    struct SessionTso(i64);

    impl Columns for SessionTso {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn current_tso(&self) -> i64 {
            self.0
        }
    }

    let ctx = SessionTso(452_605_852_463_012_352);
    assert_eq!(
        dispatched("TIDB_CURRENT_TSO", &[], &ctx),
        Datum::Int(452_605_852_463_012_352)
    );
    // No active transaction (the trait default): Go's zero value.
    assert_eq!(
        dispatched("TIDB_CURRENT_TSO", &[], &NoColumns),
        Datum::Int(0)
    );
}
