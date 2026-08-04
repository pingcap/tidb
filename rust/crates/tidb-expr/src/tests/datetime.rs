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

//! Date/time, cast/convert, collation, and regexp-dispatch tests, split out
//! of `tests/mod.rs` purely for file size. This mirrors `pkg/expression`'s
//! own adjacency of `builtin_time.go`, `builtin_cast.go`, and the regexp
//! family in `builtin_string.go`/`builtin_like.go`; every assertion, Go
//! citation, and doc comment is unchanged from its prior home in `mod.rs`.

use super::*;
use tidb_ast::{QueryStmt, SelectField, Stmt};

/// A [`Columns`] fixture with a fixed clock but no columns — for
/// testing `NOW()`/`CURRENT_TIMESTAMP()` and friends directly, since
/// `e`/`v`'s `NoColumns` has no session by design (see
/// `now_current_timestamp` below for that boundary case). Fields:
/// `(utc_secs, nanos, tz_offset_seconds)`.
struct FixedClock(i64, u32, i32);
impl Columns for FixedClock {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((self.0, self.1, self.2))
    }
}

fn e_at(expr: &str, clock: &FixedClock) -> String {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("expected Query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected Select")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    match eval_in(expr, clock) {
        Ok(v) => v.label(),
        Err(e) => format!("{e:?}"),
    }
}

#[test]
fn case_when() {
    // Simple form: WHEN compares `value = cond` via ordinary `=`.
    assert_eq!(e("case 1 when 1 then 'a' when 2 then 'b' end"), "STR:a");
    assert_eq!(e("case 2 when 1 then 'a' when 2 then 'b' end"), "STR:b");
    // No match and no ELSE is NULL.
    assert_eq!(e("case 3 when 1 then 'a' when 2 then 'b' end"), "NULL");
    assert_eq!(e("case 3 when 1 then 'a' else 'c' end"), "STR:c");
    // Searched form: no compare value, each WHEN is truthiness-tested
    // directly -- the same three-valued logic IF/WHERE already use.
    assert_eq!(e("case when 1=1 then 10 else 20 end"), "INT:10");
    assert_eq!(e("case when 1=0 then 10 else 20 end"), "INT:20");
    assert_eq!(e("case when 1=0 then 10 end"), "NULL");
    // A NULL searched condition is neither true nor false -- ELSE wins.
    assert_eq!(e("case when null then 1 else 2 end"), "INT:2");
    // A NULL simple-CASE value never matches ANY WHEN (NULL = x is
    // NULL, matching ordinary `=` propagation) -- confirmed via
    // goeval, not assumed: `CASE NULL WHEN NULL THEN 1 ELSE 2 END`
    // is `2`, not `1`.
    assert_eq!(e("case null when null then 1 else 2 end"), "INT:2");
    assert_eq!(e("case null when 1 then 'a' else 'b' end"), "STR:b");
    // The FIRST matching WHEN wins, even if a later one would also
    // match.
    assert_eq!(
        e("case when 1=1 then 1 when 1=1 then 2 else 3 end"),
        "INT:1"
    );
    assert_eq!(e("case 1 when 1 then 10 when 1 then 20 end"), "INT:10");
    // LAZY evaluation: only the taken branch is ever evaluated,
    // matching real MySQL's short-circuit CASE -- a load-bearing SQL
    // idiom for guarding against errors. `1/0` in the untaken branch
    // must NOT raise `IntOverflow`/division-by-zero here.
    assert_eq!(e("case when 1=0 then 1/0 else 5 end"), "INT:5");
    assert_eq!(e("case 1 when 2 then 1/0 else 5 end"), "INT:5");
    // Nests, and composes with ordinary operators.
    assert_eq!(
        e("case when 1=1 then case when 2=2 then 'nested' else 'no' end else 'outer' end"),
        "STR:nested"
    );
    assert_eq!(e("1 + case when 1=1 then 10 else 20 end"), "INT:11");
}

#[test]
fn now_current_timestamp() {
    // `NoColumns` (used by plain constant-expression `eval`) has no
    // session clock by design -- this evaluator never falls back to
    // the live wall clock, which would be non-deterministic.
    assert_eq!(
        e("now()"),
        "Unsupported(\"no session clock (SET timestamp)\")"
    );
    assert_eq!(
        e("current_timestamp"),
        "Unsupported(\"no session clock (SET timestamp)\")"
    );

    // 1700000000.123456 (Unix epoch, UTC) -- matches the exact value
    // probed via `gorun` for the `rust/difftests/corpus/table/
    // now_current_timestamp.txt` topic.
    let clock = FixedClock(1_700_000_000, 123_456_000, 0);
    assert_eq!(e_at("now()", &clock), "STR:2023-11-14 22:13:20");
    // NOW and CURRENT_TIMESTAMP are true synonyms; CURRENT_TIMESTAMP
    // also parses with no `()` at all.
    assert_eq!(e_at("current_timestamp", &clock), "STR:2023-11-14 22:13:20");
    assert_eq!(
        e_at("current_timestamp()", &clock),
        "STR:2023-11-14 22:13:20"
    );
    // The fractional part TRUNCATES (never rounds) to the requested
    // 0-6 precision.
    assert_eq!(e_at("now(3)", &clock), "STR:2023-11-14 22:13:20.123");
    assert_eq!(e_at("now(6)", &clock), "STR:2023-11-14 22:13:20.123456");
    // TestNowAndUTCTimestamp's exact invalid source values: only 0-6 is a
    // valid precision.
    assert_eq!(
        e_at("now(8)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(
        e_at("now(-2)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    // Additional boundary neighbors retain the same contract.
    assert_eq!(
        e_at("now(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(
        e_at("now(-1)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
}

#[test]
fn curdate_curtime() {
    // A nonzero `time_zone` offset (+05:30, matching the epoch/offset
    // probed via `gorun`) shifts CURDATE/CURTIME's local rendering.
    let clock = FixedClock(1_700_000_000, 654_321_000, 19_800);
    assert_eq!(e_at("curdate()", &clock), "STR:2023-11-15");
    assert_eq!(e_at("current_date", &clock), "STR:2023-11-15");
    assert_eq!(e_at("current_date()", &clock), "STR:2023-11-15");
    assert_eq!(e_at("curtime()", &clock), "STR:03:43:20");
    assert_eq!(e_at("current_time", &clock), "STR:03:43:20");
    // TestCurrentTime's explicit precision table.
    assert_eq!(e_at("current_time(3)", &clock), "STR:03:43:20.654");
    assert_eq!(e_at("current_time(6)", &clock), "STR:03:43:20.654321");
    // Go's parser rejects a signed precision before execution sees it.
    assert!(tidb_parser::parse("select current_time(-1)").is_err());
    assert_eq!(
        e_at("current_time(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(e_at("curtime(3)", &clock), "STR:03:43:20.654");
    // CURDATE takes no argument at all -- confirmed via `godump
    // restore`: `CURDATE(1)` is a real parse error, not just an
    // out-of-range value.
    assert!(tidb_parser::parse("select curdate(1)").is_err());

    // A genuine SPLIT rule, confirmed via `gorun`: the 0-arg form
    // TRUNCATES, but an EXPLICIT argument (even literally `0`)
    // ROUNDS. UTC offset 0 here isolates the effect from the
    // time_zone shift above.
    let utc = FixedClock(1_700_000_000, 654_321_000, 0);
    assert_eq!(e_at("curtime()", &utc), "STR:22:13:20"); // truncates
    assert_eq!(e_at("curtime(0)", &utc), "STR:22:13:21"); // rounds up
}

#[test]
fn utc_date_time_timestamp() {
    // The RAW UTC clock, ignoring `time_zone` entirely -- with a
    // nonzero offset, `UTC_TIMESTAMP()` still reports the SAME value
    // it would at offset 0 (confirmed via `gorun`).
    let clock = FixedClock(1_700_000_000, 654_321_000, 19_800);
    assert_eq!(e_at("utc_date()", &clock), "STR:2023-11-14");
    // UTC_TIMESTAMP() ALWAYS ROUNDS (ties away from zero), for BOTH
    // the 0-arg and explicit-arg forms alike -- unlike NOW's uniform
    // truncation, and unlike CURTIME/UTC_TIME's 0-arg/explicit-arg
    // split. Confirmed via reading `evalUTCTimestampWithFsp` in
    // `pkg/expression/builtin_time.go`, not assumed.
    assert_eq!(e_at("utc_timestamp()", &clock), "STR:2023-11-14 22:13:21");
    assert_eq!(e_at("utc_timestamp(0)", &clock), "STR:2023-11-14 22:13:21");
    assert_eq!(
        e_at("utc_timestamp(3)", &clock),
        "STR:2023-11-14 22:13:20.654"
    );
    assert_eq!(
        e_at("utc_timestamp(6)", &clock),
        "STR:2023-11-14 22:13:20.654321"
    );
    assert_eq!(
        e_at("utc_timestamp(8)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    // Signed precision is a parse error in Go's FuncDatetimePrecListOpt.
    assert!(tidb_parser::parse("select utc_timestamp(-2)").is_err());
    // UTC_TIME has the SAME 0-arg-truncates/explicit-arg-rounds split
    // as CURTIME.
    assert_eq!(e_at("utc_time()", &clock), "STR:22:13:20");
    assert_eq!(e_at("utc_time(0)", &clock), "STR:22:13:21");
    assert_eq!(e_at("utc_time(3)", &clock), "STR:22:13:20.654");
    assert_eq!(e_at("utc_time(6)", &clock), "STR:22:13:20.654321");
    assert!(tidb_parser::parse("select utc_time(-1)").is_err());
    assert_eq!(
        e_at("utc_time(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
}

#[test]
fn date_parts() {
    // A DATE/DATETIME value is a plain string; these extract its
    // calendar components directly, ignoring any time-of-day part.
    assert_eq!(e("year('2021-03-15')"), "INT:2021");
    assert_eq!(e("month('2021-03-15')"), "INT:3");
    assert_eq!(e("day('2021-03-15')"), "INT:15");
    assert_eq!(e("dayofmonth('2021-03-15')"), "INT:15");
    assert_eq!(e("quarter('2021-03-15')"), "INT:1");
    assert_eq!(e("quarter('2021-12-31')"), "INT:4");
    assert_eq!(e("year('2021-03-15 10:30:00')"), "INT:2021");
    // Lenient separators (any run of non-digit characters) and
    // whitespace trimming, matching real TiDB's own leniency.
    assert_eq!(e("year('2021-3-5')"), "INT:2021"); // no zero-padding required
    assert_eq!(e("year('  2021-01-15  ')"), "INT:2021");
    assert_eq!(e("year('2021/01/15')"), "INT:2021");
    // Calendar validation: month 1-12, day valid for that month/year.
    assert_eq!(e("year('not a date')"), "NULL");
    assert_eq!(e("year('2021-13-01')"), "NULL"); // no month 13
    assert_eq!(e("year('2021-01-32')"), "NULL"); // no day 32
    assert_eq!(e("year('2021-02-30')"), "NULL"); // Feb never has 30 days
    assert_eq!(e("year('2020-02-29')"), "INT:2020"); // 2020 is a leap year
    assert_eq!(e("year('2021-02-29')"), "NULL"); // 2021 is not
    assert_eq!(e("year('10:30:00')"), "NULL"); // no date part at all
    assert_eq!(e("year(NULL)"), "NULL");

    // A bare, separator-less digit run of EXACTLY 6 or 8 digits (an
    // integer literal argument coerces to this same string form) is a
    // SEPARATE positional YYMMDD/YYYYMMDD reading, confirmed via
    // `goeval` -- NOT the same algorithm as the lenient
    // separator-based path above.
    assert_eq!(e("year(20240315)"), "INT:2024");
    assert_eq!(e("month(20240315)"), "INT:3");
    assert_eq!(e("day(20240315)"), "INT:15");
    assert_eq!(e("quarter(20240315)"), "INT:1");
    assert_eq!(e("year('20240315')"), "INT:2024"); // quoted string, same reading
                                                   // The 6-digit form's 2-digit year is CENTURY-PIVOTED: 00-69 ->
                                                   // 2000-2069, 70-99 -> 1970-1999 (real MySQL/TiDB convention,
                                                   // confirmed via `goeval`, not invented).
    assert_eq!(e("year(240315)"), "INT:2024");
    assert_eq!(e("year(690101)"), "INT:2069"); // pivot boundary
    assert_eq!(e("year(700101)"), "INT:1970"); // pivot boundary, other side
                                               // The SAME century pivot applies to a separator-based date's own
                                               // 1- or 2-digit year (a 1-digit year is indistinguishable from a
                                               // 2-digit one once parsed, and pivots identically) -- but NOT to
                                               // a 3-or-more-digit year, which is taken LITERALLY even when its
                                               // own value happens to be under 100 (confirmed via `goeval`, a
                                               // real asymmetry that couldn't be guessed from the value alone).
    assert_eq!(e("year('24-03-15')"), "INT:2024");
    assert_eq!(e("year('99-03-15')"), "INT:1999");
    assert_eq!(e("year('1-03-15')"), "INT:2001");
    assert_eq!(e("year('099-03-15')"), "INT:99");
    // Calendar validation still applies after century-pivoting.
    assert_eq!(e("year(20241332)"), "NULL"); // no month 13
    assert_eq!(e("year(230229)"), "NULL"); // 2023 (pivoted) is not a leap year

    // DATEDIFF: day count between two dates' DATE parts, ignoring any
    // time-of-day component and honoring leap years.
    assert_eq!(e("datediff('2021-03-15', '2021-03-10')"), "INT:5");
    assert_eq!(e("datediff('2021-03-10', '2021-03-15')"), "INT:-5");
    assert_eq!(e("datediff('2021-01-01', '2020-01-01')"), "INT:366"); // 2020 is a leap year
    assert_eq!(
        e("datediff('2021-03-15 23:59:59', '2021-03-15 00:00:01')"),
        "INT:0"
    ); // same calendar day, time ignored
    assert_eq!(e("datediff('2021-03-15', '2021-03-15')"), "INT:0");
    assert_eq!(e("datediff('not a date', '2021-01-01')"), "NULL");
    assert_eq!(e("datediff('2021-01-01', NULL)"), "NULL");

    // DAYOFYEAR: 1-based day count within the year, leap-year aware.
    assert_eq!(e("dayofyear('2021-01-01')"), "INT:1");
    assert_eq!(e("dayofyear('2021-12-31')"), "INT:365");
    assert_eq!(e("dayofyear('2020-12-31')"), "INT:366"); // 2020 is a leap year
    assert_eq!(e("dayofyear('2020-02-29')"), "INT:60");

    // DAYOFWEEK (1=Sunday..7=Saturday) / WEEKDAY (0=Monday..6=Sunday)
    // over a full week starting 2021-01-01, a Friday.
    assert_eq!(e("dayofweek('2021-01-01')"), "INT:6"); // Friday
    assert_eq!(e("dayofweek('2021-01-03')"), "INT:1"); // Sunday
    assert_eq!(e("dayofweek('2021-01-04')"), "INT:2"); // Monday
    assert_eq!(e("weekday('2021-01-01')"), "INT:4"); // Friday
    assert_eq!(e("weekday('2021-01-04')"), "INT:0"); // Monday
    assert_eq!(e("weekday('2021-02-30')"), "NULL"); // invalid calendar date

    // TO_DAYS: an absolute day number (days_from_civil plus a fixed
    // offset solved from real TiDB's own answer); ignores time-of-day.
    assert_eq!(e("to_days('1970-01-01')"), "INT:719528"); // days_from_civil's own epoch
    assert_eq!(e("to_days('2021-01-01')"), "INT:738156");
    assert_eq!(e("to_days('2021-03-15 10:30:00')"), "INT:738229");
    assert_eq!(e("to_days('not a date')"), "NULL");
    assert_eq!(e("to_days(NULL)"), "NULL");

    // FROM_DAYS: the inverse of TO_DAYS. Outside the valid range
    // (year 0001-9999) real TiDB returns the "zero date" string.
    assert_eq!(e("from_days(719528)"), "STR:1970-01-01");
    assert_eq!(e("from_days(738156)"), "STR:2021-01-01");
    assert_eq!(e("from_days(366)"), "STR:0001-01-01"); // lower boundary
    assert_eq!(e("from_days(3652424)"), "STR:9999-12-31"); // upper boundary
    assert_eq!(e("from_days(365)"), "STR:0000-00-00"); // just below the valid range
    assert_eq!(e("from_days(0)"), "STR:0000-00-00");
    assert_eq!(e("from_days(NULL)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n DAY: exact day arithmetic, so
    // month/year rollover and leap days are handled correctly for free.
    assert_eq!(
        e("date_add('2021-01-01', interval 5 day)"),
        "STR:2021-01-06"
    );
    assert_eq!(
        e("date_sub('2021-01-01', interval 5 day)"),
        "STR:2020-12-27"
    );
    assert_eq!(
        e("date_add('2021-01-31', interval 1 day)"),
        "STR:2021-02-01"
    ); // month rollover
    assert_eq!(
        e("date_add('2020-02-28', interval 1 day)"),
        "STR:2020-02-29"
    ); // leap day
    assert_eq!(
        e("date_add('2021-01-01', interval -5 day)"),
        "STR:2020-12-27"
    ); // negative interval = subtraction
       // `date_expr + INTERVAL n unit` / `date_expr - INTERVAL n unit`
       // desugar to `DATE_ADD`/`DATE_SUB` at PARSE time (`tidb-parser`'s
       // own `fold_interval_arith`), so evaluation here needs no new
       // logic at all -- confirmed end-to-end (not just restore-checked)
       // against `gorun`.
    assert_eq!(e("'2020-01-01' + interval 5 day"), "STR:2020-01-06");
    assert_eq!(e("'2020-01-01' - interval 5 day"), "STR:2019-12-27");
    // A time-of-day suffix is preserved verbatim in the output.
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 day)"),
        "STR:2021-01-06 10:30:00"
    );
    assert_eq!(e("date_add('not a date', interval 5 day)"), "NULL");
    assert_eq!(e("date_add(NULL, interval 5 day)"), "NULL");
    assert_eq!(e("date_add('2021-01-01', interval NULL day)"), "NULL");
    // A decimal interval value rounds to the nearest day, ties away
    // from zero (matching Decimal::round_to_i64's existing rule).
    assert_eq!(
        e("date_add('2021-01-10', interval 5.5 day)"),
        "STR:2021-01-16"
    );
    assert_eq!(
        e("date_add('2021-01-10', interval -5.5 day)"),
        "STR:2021-01-04"
    );
    // DAY's year-range boundary: a computed year of exactly 0 is the
    // "zero date" string; any other out-of-range year is NULL. A real
    // bug in an earlier version of this function never checked this.
    assert_eq!(e("date_add('9999-12-31', interval 1 day)"), "NULL");
    assert_eq!(
        e("date_add('0001-01-01', interval -1 day)"),
        "STR:0000-00-00"
    );
    assert_eq!(e("date_add('0001-01-01', interval -367 day)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n MONTH/YEAR: calendar-field
    // arithmetic, clamping the day to the target month's own length
    // rather than overflowing into the next month.
    assert_eq!(
        e("date_add('2021-01-31', interval 1 month)"),
        "STR:2021-02-28"
    );
    // The clamp applies once against the FINAL target month, not
    // iteratively re-clamped one month at a time (would give 03-28).
    assert_eq!(
        e("date_add('2021-01-31', interval 2 month)"),
        "STR:2021-03-31"
    );
    assert_eq!(
        e("date_add('2020-01-31', interval 1 month)"),
        "STR:2020-02-29"
    ); // leap year: clamps to 29, not 28
    assert_eq!(
        e("date_add('2021-01-31', interval -1 month)"),
        "STR:2020-12-31"
    );
    assert_eq!(
        e("date_add('2020-02-29', interval 1 year)"),
        "STR:2021-02-28"
    ); // leap day, target year not leap
    assert_eq!(
        e("date_add('2021-01-31 10:30:00', interval 1 month)"),
        "STR:2021-02-28 10:30:00"
    );
    assert_eq!(e("date_add('2021-01-31', interval NULL month)"), "NULL");
    // A decimal amount rounds to the nearest whole unit first, ties
    // away from zero, then the (rounded) calendar arithmetic applies.
    assert_eq!(
        e("date_add('2021-01-31', interval 1.5 month)"),
        "STR:2021-03-31"
    );
    // MONTH/YEAR's year-range boundary: the same rule as DAY's.
    assert_eq!(e("date_add('9999-12-01', interval 1 month)"), "NULL");
    assert_eq!(
        e("date_add('0001-02-01', interval -2 month)"),
        "STR:0000-00-00"
    );
    assert_eq!(e("date_add('0003-06-15', interval -4 year)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n WEEK: exact day arithmetic,
    // WEEK being DAY with the (already-rounded) amount pre-multiplied
    // by 7.
    assert_eq!(
        e("date_add('2021-01-01', interval 1 week)"),
        "STR:2021-01-08"
    );
    assert_eq!(
        e("date_sub('2021-01-01', interval 1 week)"),
        "STR:2020-12-25"
    );
    // A fractional WEEK amount rounds to the nearest whole WEEK FIRST,
    // then multiplies by 7 (not the reverse order: round(1.5*7)=11
    // would give Jan 12, not Jan 15).
    assert_eq!(
        e("date_add('2021-01-01', interval 1.5 week)"),
        "STR:2021-01-15"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 1 week)"),
        "STR:2021-01-08 10:30:00"
    );
    assert_eq!(e("date_add('2021-01-01', interval NULL week)"), "NULL");
    assert_eq!(e("date_add('9999-12-25', interval 1 week)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n HOUR/MINUTE/SECOND: unlike
    // DAY/WEEK/MONTH/YEAR, these ALWAYS render a time-of-day
    // component, treating a DATE-only input as midnight.
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 hour)"),
        "STR:2021-01-01 15:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01', interval 5 hour)"),
        "STR:2021-01-01 05:00:00"
    );
    // Overflow carries into the day (and, via civil_from_days, month).
    assert_eq!(
        e("date_add('2021-01-01 22:00:00', interval 5 hour)"),
        "STR:2021-01-02 03:00:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval -15 hour)"),
        "STR:2020-12-31 19:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 minute)"),
        "STR:2021-01-01 10:35:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:45', interval 20 second)"),
        "STR:2021-01-01 10:31:05"
    ); // second->minute carry
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 1.5 hour)"),
        "STR:2021-01-01 12:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval NULL hour)"),
        "NULL"
    );
    assert_eq!(
        e("date_add('9999-12-31 23:00:00', interval 2 hour)"),
        "NULL"
    );
    // The zero-date special case replaces ONLY the date portion; the
    // computed time still shows through.
    assert_eq!(
        e("date_add('0001-01-01 00:00:00', interval -1 hour)"),
        "STR:0000-00-00 23:00:00"
    );
    assert_eq!(
        e("date_add('0001-01-01', interval -1 second)"),
        "STR:0000-00-00 23:59:59"
    );

    // Composite `INTERVAL` units -- ported from `parseTimeValue`/
    // `parseSingleTimeValue` (`pkg/types/time.go`), every case confirmed via
    // `pkg/executor` capture against `'2024-01-31 10:20:30'`.
    let dt = "'2024-01-31 10:20:30'";
    assert_eq!(
        e(&format!("date_add({dt}, interval '1:30' hour_minute)")),
        "STR:2024-01-31 11:50:30"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1:2:3' hour_second)")),
        "STR:2024-01-31 11:22:33"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1 2' day_hour)")),
        "STR:2024-02-01 12:20:30"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1 2:3' day_minute)")),
        "STR:2024-02-01 12:23:30"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1 2:3:4' day_second)")),
        "STR:2024-02-01 12:23:34"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1:30' minute_second)")),
        "STR:2024-01-31 10:22:00"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '1-2' year_month)")),
        "STR:2025-03-31 10:20:30"
    );
    // SHORT-STRING rule: a lone number with no separator fills only the
    // RIGHTMOST (smallest) field of the unit -- `'30' HOUR_MINUTE` is `+30
    // minutes`, not `+30 hours`; `'30' DAY_HOUR` is `+30 hours` (which
    // carries into the day), not `+30 days`.
    assert_eq!(
        e(&format!("date_add({dt}, interval '30' hour_minute)")),
        "STR:2024-01-31 10:50:30"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '30' day_hour)")),
        "STR:2024-02-01 16:20:30"
    );
    assert_eq!(
        e(&format!("date_add({dt}, interval '30' year_month)")),
        "STR:2026-07-31 10:20:30"
    );
    // A NUMBER (not a string) amount is formatted to its plain decimal
    // string first (matching Go's `getIntervalFromInt`), then split the
    // SAME way: `130` has one digit run, so it fills the rightmost field.
    assert_eq!(
        e(&format!("date_add({dt}, interval 130 hour_minute)")),
        "STR:2024-01-31 12:30:30"
    );
    // Negative: the leading `-` negates EVERY parsed field, not just the
    // first.
    assert_eq!(
        e(&format!("date_add({dt}, interval '-1:30' hour_minute)")),
        "STR:2024-01-31 08:50:30"
    );
    // Month overflow is NOT re-normalized before adding -- `13` months
    // simply adds to `1` year's 12, landing on total month 25.
    assert_eq!(
        e(&format!("date_add({dt}, interval '1-13' year_month)")),
        "STR:2026-02-28 10:20:30"
    );
    // MORE numeric groups than the unit's field count is a malformed-value
    // WARNING in real TiDB (even under `STRICT_TRANS_TABLES`), not a hard
    // error -- the date comes back UNCHANGED rather than `NULL`.
    assert_eq!(
        e(&format!("date_add({dt}, interval '1:2:3' hour_minute)")),
        "STR:2024-01-31 10:20:30"
    );
    assert_eq!(
        e(&format!("date_sub({dt}, interval '1:30' hour_minute)")),
        "STR:2024-01-31 08:50:30"
    );
    assert_eq!(
        e(&format!("date_sub({dt}, interval '30' hour_minute)")),
        "STR:2024-01-31 09:50:30"
    );

    // `QUARTER`: `parseSingleTimeValue`'s `3 * riv` MONTHs, so it shares
    // MONTH/YEAR's calendar-field clamp through the same code path
    // (confirmed via `goeval`): `2024-01-31 + 1 QUARTER` clamps into
    // April's 30 days, and `2024-11-30 + 1 QUARTER` clamps into a
    // non-leap February.
    assert_eq!(
        e("date_add('2024-01-31', interval 1 quarter)"),
        "STR:2024-04-30"
    );
    assert_eq!(
        e("date_add('2024-11-30', interval 1 quarter)"),
        "STR:2025-02-28"
    );
    assert_eq!(
        e("date_add('2024-01-31', interval 2 quarter)"),
        "STR:2024-07-31"
    );

    // A STRING `INTERVAL` amount for a non-composite unit is NOT the same
    // as a numeric `Decimal` amount: `intervalReformatString`
    // (`pkg/expression/builtin_time.go`) keeps only the string's LEADING
    // `[+-]?[0-9]+` digit run and throws the fraction away entirely --
    // this is a hard truncation of the STRING before any rounding could
    // happen, not a round. `'5.9' DAY` and `'5.4' DAY` both become `5`
    // (confirmed via `goeval`: `'5.99' DAY` doesn't round to `6` either),
    // unlike a NUMERIC `5.5 DAY`, which rounds to `6` (ties away from
    // zero, tested above) -- the string and numeric paths take genuinely
    // different rules for the exact same interval unit.
    assert_eq!(
        e("date_add('2024-01-01', interval '5.9' day)"),
        "STR:2024-01-06"
    );
    assert_eq!(
        e("date_add('2024-01-01', interval '5.4' day)"),
        "STR:2024-01-06"
    );
    assert_eq!(
        e("date_add('2024-01-01', interval '-5.5' day)"),
        "STR:2023-12-27"
    );
    assert_eq!(
        e("date_add('2024-01-01', interval '' day)"),
        "STR:2024-01-01"
    ); // no leading digit run: Go's "0" fallback.
       // `SECOND` is the one single unit whose STRING amount is parsed as a
       // full decimal (Go routes it through `MyDecimal.FromString`,
       // preserving the fraction so the real engine can render a sub-second
       // time-of-day) -- captured via `goeval`:
       // `DATE_ADD('...10:00:00', INTERVAL '-5.5' SECOND)` is
       // `...09:59:54.500000`, the fraction kept exactly, not rounded and not
       // truncated like `DAY`'s string amount above. This tier's
       // `date_add_time` has no fractional-second representation at all
       // (the same limitation the numeric `Decimal` amount already has), so
       // the closest amount it can still act on is the nearest whole second.
    assert_eq!(
        e("date_add('2024-01-15 10:00:00', interval '5' second)"),
        "STR:2024-01-15 10:00:05"
    );
    assert_eq!(
        e("date_add('2024-01-15 10:00:00', interval '5.5' second)"),
        "STR:2024-01-15 10:00:06"
    );
}

#[test]
fn hour_minute_second() {
    // Real TiDB's own two-path algorithm (confirmed via `goeval`, not
    // assumed), depending on whether the argument contains a `:`.
    //
    // WITH a `:`: an optional `[DATE ]` prefix followed by a required
    // `H:M[:S]` time-of-day (`S` defaults to `0`). `H` may exceed 23 --
    // TiDB's `TIME` domain is elapsed-time, not wall-clock.
    assert_eq!(e("hour('10:30:45')"), "INT:10");
    assert_eq!(e("minute('10:30:45')"), "INT:30");
    assert_eq!(e("second('10:30:45')"), "INT:45");
    assert_eq!(e("hour('2024-01-15 10:30:45')"), "INT:10");
    assert_eq!(e("minute('2024-01-15 10:30:45')"), "INT:30");
    assert_eq!(e("hour('100:30:45')"), "INT:100"); // elapsed time, not wall-clock
    assert_eq!(e("hour('10:30')"), "INT:10"); // seconds default to 0
    assert_eq!(e("second('10:30')"), "INT:0");
    assert_eq!(e("hour('1:2:3')"), "INT:1"); // single-digit components
    assert_eq!(e("hour(' 10:30:45 ')"), "INT:10"); // whitespace trimmed
                                                   // A negative sign is stripped before parsing -- HOUR/MINUTE/SECOND
                                                   // always return a NON-NEGATIVE magnitude, never the sign itself.
    assert_eq!(e("hour('-10:30:45')"), "INT:10");
    assert_eq!(e("minute('-10:30:45')"), "INT:30");
    // A JUNK date-like prefix invalidates the WHOLE value, not just
    // gets ignored.
    assert_eq!(e("hour('junk 10:30:45')"), "NULL");
    // `838:59:59` is TiDB's real documented `TIME` maximum; an `H`
    // exceeding it clamps the WHOLE value to exactly `838:59:59` --
    // not just the hour component -- even when `M`/`S` were
    // individually valid.
    assert_eq!(e("hour('838:59:59')"), "INT:838");
    assert_eq!(e("hour('900:30:15')"), "INT:838");
    assert_eq!(e("minute('900:30:15')"), "INT:59"); // NOT 30
    assert_eq!(e("second('900:30:15')"), "INT:59"); // NOT 15
                                                    // An out-of-range `M`/`S` invalidates the WHOLE value, regardless
                                                    // of `H`'s own magnitude.
    assert_eq!(e("hour('839:70:80')"), "NULL");
    assert_eq!(e("minute('839:70:80')"), "NULL");
    assert_eq!(e("second('839:70:80')"), "NULL");
    assert_eq!(e("hour(NULL)"), "NULL");
    assert_eq!(e("hour('not a time')"), "NULL");

    // WITHOUT a `:` (including a bare `DATE`-only value, NOT `0` --
    // a genuinely surprising real behavior): the value's OWN leading
    // digit run decodes as a right-aligned HHMMSS number, the SAME
    // rule an integer literal already uses.
    assert_eq!(e("hour(103045)"), "INT:10");
    assert_eq!(e("hour(-103045)"), "INT:10"); // sign stripped here too
    assert_eq!(e("minute(-103045)"), "INT:30");
    assert_eq!(e("hour(103045.789)"), "INT:10"); // fractional part truncated
    assert_eq!(e("second(103045.789)"), "INT:45");
    // A bare DATE (no `:` at all) takes ONLY its leading digit run
    // ('2024', stopping at the first non-digit '-') -- NOT the
    // calendar date's own values.
    assert_eq!(e("hour('2024-01-15')"), "INT:0");
    assert_eq!(e("minute('2024-01-15')"), "INT:20");
    assert_eq!(e("second('2024-01-15')"), "INT:24");
    assert_eq!(e("minute('2024-02-15')"), "INT:20"); // same leading run, rest ignored
    assert_eq!(e("hour('12')"), "INT:0");
    assert_eq!(e("minute('12')"), "INT:0");
    assert_eq!(e("second('12')"), "INT:12");
    assert_eq!(e("hour('12abc')"), "INT:0"); // digit run stops at the first non-digit
    assert_eq!(e("hour('abc123')"), "NULL"); // must START with a digit
    assert_eq!(e("hour('-')"), "NULL"); // sign with no digits at all
    assert_eq!(e("hour('')"), "NULL");
    // The SAME `0..=59`-for-`M`/`S`-or-invalid rule applies to the
    // decoded digit run too.
    assert_eq!(e("hour(999999999)"), "NULL");
    assert_eq!(e("minute(999999999)"), "NULL");
    assert_eq!(e("second(999999999)"), "NULL");
}

#[test]
fn extract() {
    // `EXTRACT(unit FROM expr)` is sugar for calling the SAME
    // single-argument function `unit` already names -- every simple
    // unit this project's evaluator already supports as a standalone
    // function works identically through `EXTRACT`.
    assert_eq!(e("extract(year from '2024-03-15')"), "INT:2024");
    assert_eq!(e("extract(month from '2024-03-15')"), "INT:3");
    assert_eq!(e("extract(day from '2024-03-15')"), "INT:15");
    assert_eq!(e("extract(quarter from '2024-03-15')"), "INT:1");
    assert_eq!(e("extract(hour from '2024-03-15 10:30:45')"), "INT:10");
    assert_eq!(e("extract(minute from '2024-03-15 10:30:45')"), "INT:30");
    assert_eq!(e("extract(second from '2024-03-15 10:30:45')"), "INT:45");
    // NULL propagates, same as an ordinary function call.
    assert_eq!(e("extract(year from NULL)"), "NULL");
    // The unit keyword is case-insensitive (lexed as an ordinary
    // keyword token, then canonically uppercased by the parser).
    assert_eq!(e("extract(YeAr from '2024-03-15')"), "INT:2024");
    // `WEEK` is now a differentially verified standalone function, so its
    // EXTRACT spelling takes the exact same path. (The no-mode spelling uses
    // the evaluator's documented default-week-format capability boundary.)
    assert_eq!(e("extract(week from '2024-03-15')"), "INT:10");
    // Composite units (`HOUR_MINUTE`, `DAY_SECOND`, `YEAR_MONTH`, ...) --
    // ported from `ExtractDatetimeNum`/`ExtractDurationNum`
    // (`pkg/types/time.go`); every value below confirmed via `pkg/executor`
    // capture against `'2024-01-31 10:20:30'`.
    let dt = "'2024-01-31 10:20:30'";
    assert_eq!(e(&format!("extract(hour_minute from {dt})")), "INT:1020");
    assert_eq!(e(&format!("extract(day_second from {dt})")), "INT:31102030");
    assert_eq!(e(&format!("extract(day_minute from {dt})")), "INT:311020");
    assert_eq!(e(&format!("extract(day_hour from {dt})")), "INT:3110");
    assert_eq!(
        e(&format!("extract(day_microsecond from {dt})")),
        "INT:31102030000000"
    );
    assert_eq!(e(&format!("extract(year_month from {dt})")), "INT:202401");
    assert_eq!(e(&format!("extract(hour_second from {dt})")), "INT:102030");
    assert_eq!(
        e(&format!("extract(hour_microsecond from {dt})")),
        "INT:102030000000"
    );
    assert_eq!(e(&format!("extract(minute_second from {dt})")), "INT:2030");
    assert_eq!(
        e(&format!("extract(minute_microsecond from {dt})")),
        "INT:2030000000"
    );
    assert_eq!(
        e(&format!("extract(second_microsecond from {dt})")),
        "INT:30000000"
    );
    // A bare TIME/duration literal has no day-of-month: `ExtractDurationNum`
    // drops the day component and applies the duration's own sign to the
    // WHOLE result, unlike the datetime formulas above.
    assert_eq!(e("extract(hour_minute from '-01:02:03')"), "INT:-102");
    assert_eq!(e("extract(hour_second from '-01:02:03')"), "INT:-10203");
    assert_eq!(e("extract(day_second from '-01:02:03')"), "INT:-10203");
}

/// `CAST(... AS type)` / `CONVERT(...)` evaluation — one `(expr, want)`
/// pair per rule confirmed via `goeval` (see `crate::cast`'s own doc for
/// the rules themselves); table-driven since there's no shared setup
/// between cases, unlike most of this file's other tests.
#[test]
fn cast_and_convert() {
    let cases: &[(&str, &str)] = &[
        ("cast('123' as signed)", "INT:123"),
        ("cast(1.5 as signed)", "INT:2"),
        ("cast(1.9 as signed)", "INT:2"),
        ("cast(-1.9 as signed)", "INT:-2"),
        ("cast('abc' as signed)", "INT:0"),
        ("cast(NULL as signed)", "NULL"),
        ("cast(-1 as unsigned)", "UINT:18446744073709551615"),
        ("cast(1 as unsigned)", "UINT:1"),
        ("cast(-1.5 as unsigned)", "UINT:0"),
        ("cast('123.45' as decimal)", "DEC:123"),
        ("cast('123.45' as decimal(10,2))", "DEC:123.45"),
        ("cast(1 as decimal(5,2))", "DEC:1.00"),
        ("cast(123 as char)", "STR:123"),
        ("cast(123.45 as char)", "STR:123.45"),
        ("cast('  123  ' as char)", "STR:  123  "),
        ("cast(1 as char(1))", "STR:1"),
        ("cast('hello' as char(3))", "STR:hel"),
        ("cast('2021-01-01' as date)", "STR:2021-01-01"),
        ("cast('2021-01-01 10:30:00' as date)", "STR:2021-01-01"),
        ("cast('2021-01-01' as datetime)", "STR:2021-01-01 00:00:00"),
        ("cast('not a date' as date)", "NULL"),
        ("cast(NULL as date)", "NULL"),
        ("cast('2021-01-01' as year)", "INT:2021"),
        ("cast(2021 as year)", "INT:2021"),
        ("cast('99' as year)", "INT:99"),
        ("cast(1 as double)", "FLOAT:1"),
        ("cast('1.5' as double)", "FLOAT:1.5"),
        ("cast(1 as float)", "FLOAT:1"),
        ("cast(1 as binary)", "STR:1"),
        ("cast('hi' as binary(5))", "STR:hi\0\0\0"),
        // `TestCastFunctions` truncates BINARY by bytes (`str[:5]`), not
        // UTF-8 characters.  The fifth byte lands inside `好`; the raw
        // result remains observable through Datum's lossless hex label.
        ("cast('你好world' as binary(5))", "STR_HEX:E4BDA0E5A5"),
        ("cast('hi' as binary)", "STR:hi"),
        ("cast(123 as binary)", "STR:123"),
        ("convert('123', signed)", "INT:123"),
        ("convert('hello' using utf8)", "STR:hello"),
        ("cast(true as signed)", "INT:1"),
        ("cast(3.5 as decimal)", "DEC:4"),
        ("cast(-5 as unsigned)", "UINT:18446744073709551611"),
        ("cast(-100 as unsigned)", "UINT:18446744073709551516"),
        ("cast('-5' as unsigned)", "UINT:18446744073709551611"),
        ("cast('hi' as char(5))", "STR:hi"),
        // `CHAR(N) CHARSET binary` restores identically to `BINARY(N)`
        // (confirmed via `godump restore`), but does NOT evaluate the
        // same way — it stays a plain truncating `CHAR` cast (no
        // right-padding), confirmed directly via `goeval`: `LENGTH(CAST(
        // 'hi' AS CHAR(5) CHARSET binary))` is `2`, not `5`. `charset` is
        // ignored entirely at evaluation time (see `crate::cast`'s own
        // `CastType::Char` arm), so this is really just re-confirming
        // `cast('hi' as char(5))`'s own behavior above under a
        // charset-qualified spelling that could easily be mistaken for
        // `binary(5)`'s padding behavior instead.
        ("cast('hi' as char(5) charset binary)", "STR:hi"),
        ("cast(99 as year)", "INT:99"),
        ("cast(0 as year)", "INT:0"),
        ("cast(2000 as year)", "INT:2000"),
        ("cast(123456 as decimal(5,2))", "DEC:999.99"),
        ("cast(123.456 as decimal(5,2))", "DEC:123.46"),
        ("cast(-123.456 as decimal(5,2))", "DEC:-123.46"),
        ("cast('  42abc' as signed)", "INT:42"),
        ("cast('   4.5e1  ' as signed)", "INT:4"),
        ("cast(1e300 as signed)", "INT:9223372036854775807"),
        ("cast(1 as unsigned) + 1", "UINT:2"),
        (
            "cast('9223372036854775807' as unsigned) + 1",
            "UINT:9223372036854775808",
        ),
        ("cast(1.5 as decimal)", "DEC:2"),
        ("cast(-1 as char)", "STR:-1"),
        ("cast(-5 as year)", "INT:-5"),
        ("cast(2.5e0 as signed)", "INT:2"),
        ("cast(-2.5e0 as signed)", "INT:-2"),
        ("cast(0.5e0 as unsigned)", "UINT:0"),
        ("cast('3.5abc' as decimal)", "DEC:4"),
        ("cast('3.5e1abc' as double)", "FLOAT:35"),
        ("cast('1e2' as decimal)", "DEC:100"),
        ("cast('10:30:00' as time)", "Unsupported(\"CAST AS TIME\")"),
        // `CAST(x AS JSON)` produces this tier's canonical JSON TEXT (the
        // documented BinaryJSON-as-string divergence); only the STRING
        // signature parses, so a malformed document is TiDB's 3140.
        ("cast('{}' as json)", "STR:{}"),
        (
            "cast('{\"b\":1,\"aa\":2}' as json)",
            r#"STR:{"aa": 2, "b": 1}"#,
        ),
        ("cast(3 as json)", "STR:3"),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// `expr COLLATE name` is a pure passthrough — the value is unaffected,
/// unlike `CONVERT ... USING`'s own stringification (see
/// `tidb_ast::Expr::Collate`'s own doc). Confirmed via `gorun`: real TiDB
/// itself treats it identically since this crate models no collation
/// domain at all.
#[test]
fn collate_expr() {
    let cases: &[(&str, &str)] = &[
        ("'a' collate utf8mb4_bin", "STR:a"),
        ("'a' collate utf8mb4_bin = 'a'", "INT:1"),
        (
            "'a' collate utf8mb4_bin collate utf8mb4_general_ci",
            "STR:a",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// `expr [NOT] REGEXP pattern` — case-sensitive (`utf8mb4_bin`,
/// matching this crate's own established collation convention), a
/// substring/partial match (no implicit `^`/`$` anchoring), `NULL`
/// from either operand propagates, and a non-string operand is
/// coerced the SAME way `LIKE` already does — all confirmed via
/// `gorun`. See `crate::regexp::regexp_match`'s own doc for the
/// empty-pattern/malformed-pattern error rules, also exercised here.
#[test]
fn regexp_expr_eval() {
    let cases: &[(&str, &str)] = &[
        ("'abc' regexp 'a.c'", "INT:1"),
        ("'ABC' regexp 'a.c'", "INT:0"), // case-sensitive
        ("'abc' regexp 'xyz'", "INT:0"),
        ("'abc' not regexp 'a.c'", "INT:0"),
        ("'abc' not regexp 'xyz'", "INT:1"),
        ("null regexp 'a.c'", "NULL"),
        ("'abc' regexp null", "NULL"),
        ("5 regexp '5'", "INT:1"),             // non-string operand coerced
        ("'abc123' regexp '[0-9]+'", "INT:1"), // substring match, no anchors needed
        ("'hello world' regexp '^hello'", "INT:1"),
        ("'hello world' regexp 'world$'", "INT:1"),
        (
            "'abc' regexp '['",
            "Unsupported(\"invalid regular expression pattern\")",
        ),
        (
            "'abc' regexp ''",
            "Unsupported(\"empty regular expression pattern\")",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// The original two-argument `[NOT] REGEXP` function is registered separately
/// from `REGEXP_LIKE` in Go (`pkg/expression/builtin_like_test.go:64
/// TestRegexp`).  Keep every successful source row running through the real
/// parser and `Expr::Regexp` dispatch as well as through the leaf builder
/// tests in `regexp.rs`.
#[test]
fn regexp_source_rows_through_dispatch() {
    let rows: &[(&str, &str)] = &[
        ("'a' regexp '^$'", "INT:0"),
        ("'a' regexp 'a'", "INT:1"),
        ("'b' regexp 'a'", "INT:0"),
        ("'aA' regexp 'aA'", "INT:1"),
        ("'a' regexp '.'", "INT:1"),
        ("'ab' regexp '^.$'", "INT:0"),
        ("'b' regexp '..'", "INT:0"),
        ("'aab' regexp '.ab'", "INT:1"),
        ("'abcd' regexp '.*'", "INT:1"),
        ("'a' not regexp 'a'", "INT:0"),
        ("'a' not regexp 'b'", "INT:1"),
    ];
    for (expr, want) in rows {
        assert_eq!(&e(expr), want, "expression: {expr}");
    }
}

/// `MATCH(col, ...) AGAINST(expr [modifier])` evaluates as `Unsupported` —
/// no fulltext index or scoring is modelled at all (see
/// `tidb_ast::Expr::MatchAgainst`'s own doc for the same "parse/restore
/// fidelity only" boundary `Expr::Regexp` already established).
#[test]
fn match_against_unsupported() {
    let cases: &[(&str, &str)] = &[
        (
            "match(a) against('x')",
            "Unsupported(\"unsupported expression\")",
        ),
        (
            "match(a) against('x' in boolean mode)",
            "Unsupported(\"unsupported expression\")",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// `ADDTIME`/`SUBTIME`, pinned to what a real TiDB session ANSWERED for each
/// statement (captured with a `gorun`-shaped oracle that also prints the
/// result column's `FieldType`).
///
/// These are the CONSTANT calls, which Go constant-folds and therefore
/// evaluates through `builtinAdd*Sig.evalString` -- its row body, not the
/// vectorized one. The two differ, and the difference is in this table: the
/// `'2020-01-01 10:00:00'` second argument is NULL under `ADDTIME` (the
/// `parser.Number`/`parser.Char('-')` guard at the end of
/// `builtinAddStringAndStringSig.evalString`) and a real value under
/// `SUBTIME`, whose row body has no such guard.
#[test]
fn addtime_and_subtime_match_the_captured_session_answers() {
    for (expr, want) in [
        (
            "addtime('2020-01-01 10:00:00','01:00:00')",
            "STR:2020-01-01 11:00:00",
        ),
        (
            "subtime('2020-01-01 10:00:00','01:00:00')",
            "STR:2020-01-01 09:00:00",
        ),
        ("addtime('10:00:00','01:00:00')", "STR:11:00:00"),
        ("subtime('10:00:00','01:00:00')", "STR:09:00:00"),
        (
            "addtime('2020-01-01','01:00:00')",
            "STR:2020-01-01 01:00:00",
        ),
        (
            "addtime('2020-01-01 10:00:00','-01:00:00')",
            "STR:2020-01-01 09:00:00",
        ),
        (
            "subtime('2020-01-01 10:00:00','-01:00:00')",
            "STR:2020-01-01 11:00:00",
        ),
        (
            "addtime('2020-01-01 10:00:00','3')",
            "STR:2020-01-01 10:00:03",
        ),
        (
            "subtime('2020-01-01 10:00:00','3')",
            "STR:2020-01-01 09:59:57",
        ),
        // The row-body guard, and its absence in SUBTIME.
        (
            "addtime('2020-01-01 10:00:00','2020-01-01 10:00:00')",
            "NULL",
        ),
        (
            "subtime('2020-01-01 10:00:00','2020-01-01 10:00:00')",
            "STR:2020-01-01 00:00:00",
        ),
        ("addtime('10:00:00','2020-01-01 10:00:00')", "NULL"),
        // `types.ParseDuration` fails first, so this one is NULL in BOTH.
        ("addtime('2020-01-01 10:00:00','xyz')", "NULL"),
        ("subtime('2020-01-01 10:00:00','xyz')", "NULL"),
        ("addtime('2020-01-01 10:00:00','3-1')", "NULL"),
        ("addtime(null,'01:00:00')", "NULL"),
        ("addtime('2020-01-01 10:00:00',null)", "NULL"),
        // `strDatetimeAddDuration` raises the result to MaxFsp exactly when
        // the sum carries a microsecond, so these two differ in WIDTH.
        (
            "addtime('2020-01-01 10:00:00','01:02:03.4567')",
            "STR:2020-01-01 11:02:03.456700",
        ),
        ("addtime('10:00:00','01:02:03.4567')", "STR:11:02:03.456700"),
        // `matchDayHHMMSS`: a leading day count folded into the hours.
        (
            "addtime('2020-01-01 10:00:00','1 01:00:00')",
            "STR:2020-01-02 11:00:00",
        ),
        // A duration result can be negative.
        ("subtime('10:00:00','20:00:00')", "STR:-10:00:00"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
        assert_eq!(chunk_e(expr), want, "{expr} (chunk tier)");
    }
}

/// `TIMESTAMP`, `TIMESTAMPADD`, captured the same way.
#[test]
fn timestamp_and_timestampadd_match_the_captured_session_answers() {
    for (expr, want) in [
        ("timestamp('2020-01-01')", "STR:2020-01-01 00:00:00"),
        (
            "timestamp('2020-01-01 10:00:00.123')",
            "STR:2020-01-01 10:00:00.123",
        ),
        (
            "timestamp('2020-01-01','01:00:00')",
            "STR:2020-01-01 01:00:00",
        ),
        (
            "timestamp('2020-01-01 10:00:00','01:00:00.5')",
            "STR:2020-01-01 11:00:00.5",
        ),
        (
            "timestamp('2020-01-01','-01:00:00')",
            "STR:2019-12-31 23:00:00",
        ),
        ("timestamp('bad')", "NULL"),
        ("timestamp('2020-01-01','bad')", "NULL"),
        // `builtinTimestamp2ArgsSig` gates the second argument on
        // `isDuration` BEFORE parsing it, so a datetime-shaped one is NULL
        // even though `types.ParseDuration` would happily fall back to a
        // datetime and answer 05:00:00 for it.
        ("timestamp('2020-01-01','2020-01-01 05:00:00')", "NULL"),
        // The `D HH:MM:SS` and over-24-hour duration forms, which do pass
        // that gate.
        (
            "timestamp('2020-01-01','1 05:00:00')",
            "STR:2020-01-02 05:00:00",
        ),
        (
            "timestamp('2020-01-01','100:00:00')",
            "STR:2020-01-05 04:00:00",
        ),
        // "MySQL won't evaluate add for date with zero year."
        ("timestamp('0000-00-00','01:00:00')", "NULL"),
        // The zero-year gate is not redundant with the range check: this
        // pair straddles it. Year 0 plus 838 hours would land in a VALID
        // year 1 datetime, and Go still answers NULL because the gate fires
        // on the PARSED value before the addition.
        ("timestamp('0000-12-31 00:00:00','838:00:00')", "NULL"),
        (
            "timestamp('0001-01-01 00:00:00','838:00:00')",
            "STR:0001-02-04 22:00:00",
        ),
        ("timestamp(null)", "NULL"),
        (
            "timestampadd(minute, 5, '2020-01-01 10:00:00')",
            "STR:2020-01-01 10:05:00",
        ),
        (
            "timestampadd(microsecond, 5, '2020-01-01 10:00:00')",
            "STR:2020-01-01 10:00:00.000005",
        ),
        (
            "timestampadd(second, 5.4, '2020-01-01 10:00:00')",
            "STR:2020-01-01 10:00:05.400000",
        ),
        // `types.AddDate` CLAMPS for MONTH ...
        (
            "timestampadd(month, 1, '2020-01-31')",
            "STR:2020-02-29 00:00:00",
        ),
        // ... while QUARTER and YEAR go through Go's own `time.AddDate`,
        // which OVERFLOWS into the next month.
        (
            "timestampadd(year, 1, '2020-02-29')",
            "STR:2021-03-01 00:00:00",
        ),
        (
            "timestampadd(quarter, 1, '2020-01-01')",
            "STR:2020-04-01 00:00:00",
        ),
        (
            "timestampadd(day, -1, '2020-01-01')",
            "STR:2019-12-31 00:00:00",
        ),
        (
            "timestampadd(week, 2, '2020-01-01')",
            "STR:2020-01-15 00:00:00",
        ),
        (
            "timestampadd(hour, 1, '2020-01-01')",
            "STR:2020-01-01 01:00:00",
        ),
        ("timestampadd(minute, 5, null)", "NULL"),
        ("timestampadd(second, 1, '9999-12-31 23:59:59')", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
        assert_eq!(chunk_e(expr), want, "{expr} (chunk tier)");
    }
}
