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

//! A zero, zero-in, or invalid date READ back through `CAST` in a `SELECT`,
//! under every SQL mode that changes the answer -- the mirror of
//! `crate::tests_zero_date`, which covers the same values on the WRITE side.
//!
//! The engine half is `tidb_expr::cast`'s `cast_to_time`, which mirrors Go
//! `builtinCastStringAsTimeSig.evalTime` and converts through
//! `tidb_datatype::parse_time` (Go `types.ParseTime`) under the flags Go's
//! `ResetContextOfStmt` gives an `*ast.SelectStmt`. Both sides now read the
//! mode bits from ONE place, `tidb_datatype::DateModes`.
//!
//! Captured from real TiDB (mock store, `SHOW WARNINGS` after each
//! statement). `->` is the value the column reads as; `W` is warning 1292
//! `Incorrect datetime value: '<input>'` alongside a NULL result.
//!
//! ```text
//! SELECT CAST(<value> AS DATE)
//!                    default        sql_mode=''       NO_ZERO_IN_DATE   NO_ZERO_DATE      ALLOW_INVALID_DATES
//! 'not-a-date'       W NULL         W NULL            W NULL            W NULL            W NULL
//! '2024-00-01'       -> 2024-00-01  -> 2024-00-01     -> 2024-00-01     -> 2024-00-01     -> 2024-00-01
//! '2024-01-00'       -> 2024-01-00  -> 2024-01-00     -> 2024-01-00     -> 2024-01-00     -> 2024-01-00
//! '0000-00-00'       W NULL         -> 0000-00-00     -> 0000-00-00     W NULL            -> 0000-00-00
//! '2024-02-31'       W NULL         W NULL            W NULL            W NULL            -> 2024-02-31
//! '2024-13-01'       W NULL         W NULL            W NULL            W NULL            W NULL
//! ''                 W NULL         W NULL            W NULL            W NULL            W NULL
//! '2024-01-15'       -> 2024-01-15  -> 2024-01-15     -> 2024-01-15     -> 2024-01-15     -> 2024-01-15
//! ```
//!
//! `CAST(... AS DATETIME)` is the same table with ` 00:00:00` appended to
//! every non-NULL value. `STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE`
//! is byte-identical to `default` -- strict mode changes NOTHING here, which
//! is the invariant [`a_read_never_fails_the_statement`] pins.
//!
//! # The three rules the table encodes, each a separate line of Go
//!
//! 1. `IgnoreZeroInDate` is set UNCONDITIONALLY for a `SELECT`, so a zero
//!    MONTH or DAY reads back INTACT in every column above -- including the
//!    default mode, which refuses to STORE one.
//! 2. `NO_ZERO_DATE` alone rejects the ALL-zero value, and it does so AFTER
//!    the parse, in `evalTime` itself rather than in the flags.
//! 3. `ALLOW_INVALID_DATES` decides which dates EXIST: it makes
//!    `'2024-02-31'` real, while `'2024-13-01'` stays wrong because a 13th
//!    month is not a date at all.
//!
//! # What this deliberately does NOT change
//!
//! The same capture run measured the OTHER date builtins, and they are NOT
//! the same function. They are recorded here so a future reader does not
//! assume the CAST rule generalizes, and so the enumeration that scoped this
//! change is visible rather than remembered:
//!
//! ```text
//!                                   default    sql_mode=''    ALLOW_INVALID_DATES
//! DATE('2024-00-01')                W NULL     -> 2024-00-01  -> 2024-00-01
//! YEAR('2024-00-01')                -> 2024    -> 2024        -> 2024
//! MONTH('2024-00-01')               -> 0       -> 0           -> 0
//! DATEDIFF('2024-00-01', ...)       W NULL     W NULL         W NULL
//! DATE_ADD('2024-00-01', 1 DAY)     W NULL     W NULL         W NULL
//! ```
//!
//! `DATE_ADD` and `DATEDIFF` REFUSE a zero-in-date under every mode, so
//! `tidb_expr::time_fn::calendar::parse_date_ymd` -- which every one of those
//! builtins still calls, and which rejects a zero month unconditionally --
//! stays strict on purpose. Relaxing it would have leaked a permissive parse
//! into comparison paths, and the controls below show why that matters.

use super::Session;
use crate::tests_support::row_text;

fn warnings(session: &Session) -> Vec<(u16, String)> {
    session
        .warnings()
        .iter()
        .map(|w| (w.code, w.message.clone()))
        .collect()
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// One captured `SELECT CAST(...)`: either a value with no warning, or NULL
/// with warning 1292.
#[derive(Clone, Copy)]
enum Read {
    /// The cast produced this text and left no warning.
    Value(&'static str),
    /// The cast produced NULL and left warning 1292.
    WarnedNull,
}

fn check_cast(session: &mut Session, sql_mode: &str, target: &str, value: &str, expected: Read) {
    let sql = format!("SELECT CAST('{value}' AS {target})");
    let got = rows(session, &sql);
    let context = format!("{sql} under sql_mode='{sql_mode}'");
    match expected {
        Read::Value(text) => {
            assert_eq!(got, [[text]], "{context}");
            assert_eq!(warnings(session), Vec::new(), "{context}");
        }
        Read::WarnedNull => {
            assert_eq!(got, [["NULL"]], "{context}");
            assert_eq!(
                warnings(session),
                [(1292, format!("Incorrect datetime value: '{value}'"))],
                "{context}"
            );
        }
    }
}

/// The whole captured matrix, both target types, every mode.
#[test]
fn a_cast_reads_a_zero_in_date_back_intact_and_warns_on_the_rest() {
    use Read::{Value, WarnedNull};

    // `None` marks the default sql_mode, which is not spellable as a literal
    // (`SET sql_mode=DEFAULT` is the statement TiDB accepts for it).
    let matrix: [(&str, [Read; 8]); 5] = [
        (
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE",
            [
                WarnedNull,
                Value("2024-00-01"),
                Value("2024-01-00"),
                WarnedNull,
                WarnedNull,
                WarnedNull,
                WarnedNull,
                Value("2024-01-15"),
            ],
        ),
        (
            "",
            [
                WarnedNull,
                Value("2024-00-01"),
                Value("2024-01-00"),
                Value("0000-00-00"),
                WarnedNull,
                WarnedNull,
                WarnedNull,
                Value("2024-01-15"),
            ],
        ),
        (
            "NO_ZERO_IN_DATE",
            [
                WarnedNull,
                Value("2024-00-01"),
                Value("2024-01-00"),
                Value("0000-00-00"),
                WarnedNull,
                WarnedNull,
                WarnedNull,
                Value("2024-01-15"),
            ],
        ),
        (
            "NO_ZERO_DATE",
            [
                WarnedNull,
                Value("2024-00-01"),
                Value("2024-01-00"),
                WarnedNull,
                WarnedNull,
                WarnedNull,
                WarnedNull,
                Value("2024-01-15"),
            ],
        ),
        (
            "ALLOW_INVALID_DATES",
            [
                WarnedNull,
                Value("2024-00-01"),
                Value("2024-01-00"),
                Value("0000-00-00"),
                Value("2024-02-31"),
                WarnedNull,
                WarnedNull,
                Value("2024-01-15"),
            ],
        ),
    ];
    let values = [
        "not-a-date",
        "2024-00-01",
        "2024-01-00",
        "0000-00-00",
        "2024-02-31",
        "2024-13-01",
        "",
        "2024-01-15",
    ];

    for (sql_mode, expected) in matrix {
        let mut session = Session::new();
        session.run(&format!("SET sql_mode='{sql_mode}'")).unwrap();
        for (value, expected) in values.iter().zip(expected) {
            check_cast(&mut session, sql_mode, "DATE", value, expected);
            // DATETIME is the same answer with the midnight clock appended.
            let datetime = match expected {
                Read::WarnedNull => Read::WarnedNull,
                Read::Value(text) => Read::Value(match text {
                    "2024-00-01" => "2024-00-01 00:00:00",
                    "2024-01-00" => "2024-01-00 00:00:00",
                    "0000-00-00" => "0000-00-00 00:00:00",
                    "2024-02-31" => "2024-02-31 00:00:00",
                    "2024-01-15" => "2024-01-15 00:00:00",
                    other => unreachable!("unmapped captured value {other}"),
                }),
            };
            check_cast(&mut session, sql_mode, "DATETIME", value, datetime);
        }
    }
}

/// THE INVARIANT the write-path unit pinned first and this one must keep: a
/// read never fails the statement, in any mode. Strict mode turns a bad write
/// into an error; it leaves a bad READ a warning.
#[test]
fn a_read_never_fails_the_statement() {
    for sql_mode in [
        "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE",
        "",
        "ALLOW_INVALID_DATES",
    ] {
        let mut session = Session::new();
        session.run(&format!("SET sql_mode='{sql_mode}'")).unwrap();
        for value in ["not-a-date", "0000-00-00", "2024-02-31", "2024-13-01", ""] {
            for target in ["DATE", "DATETIME"] {
                let sql = format!("SELECT CAST('{value}' AS {target})");
                session.run(&sql).unwrap_or_else(|error| {
                    panic!("{sql} failed under sql_mode='{sql_mode}': {error:?}")
                });
            }
        }
    }
}

/// THE CONTROL against an over-broad relaxation. `parse_date_ymd` is shared
/// by `DATE_ADD` and by the comparison paths, and letting a zero-in-date
/// through THERE would change row sets rather than one scalar. Every
/// assertion here is captured from the same run as the matrix above, and all
/// of them held BEFORE the cast fix as well -- that is the point.
#[test]
fn the_relaxed_parse_does_not_reach_date_arithmetic_or_comparisons() {
    let mut session = Session::new();
    session.run("CREATE TABLE d (v DATE)").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("INSERT INTO d VALUES ('2024-01-15'), ('0000-00-00'), ('2024-00-01')")
        .unwrap();
    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();

    // A stored zero-in-date is a real row and compares equal to its own text.
    assert_eq!(
        rows(&mut session, "SELECT v FROM d WHERE v = '2024-00-01'"),
        [["2024-00-01"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT v FROM d WHERE v BETWEEN '2024-00-01' AND '2024-12-31' ORDER BY v"
        ),
        [["2024-00-01"], ["2024-01-15"]]
    );
    // And the cast's own value agrees with the column's.
    assert_eq!(
        rows(
            &mut session,
            "SELECT '2024-00-01' = CAST('2024-00-01' AS DATE)"
        ),
        [["1"]]
    );

    // DATE_ADD, which shares `parse_date_ymd`, REFUSES a zero-in-date under
    // every mode -- captured, and the reason that parser stays strict.
    for value in ["2024-00-01", "2024-01-00"] {
        assert_eq!(
            rows(
                &mut session,
                &format!("SELECT DATE_ADD('{value}', INTERVAL 1 DAY)")
            ),
            [["NULL"]],
            "DATE_ADD('{value}', INTERVAL 1 DAY)"
        );
    }
}
