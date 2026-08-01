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

//! `CAST(<string> AS SIGNED/UNSIGNED)` when the string is not entirely a
//! number: the VALUE was already right, the DIAGNOSTIC was missing entirely.
//!
//! `select cast('x' as signed), cast('y' as signed)` answered `0, 0` with
//! ZERO warnings, where TiDB answers `0, 0` with TWO. A client could not
//! distinguish "the string WAS a number" from "the string was garbage":
//! both returned a value and said nothing.
//!
//! # Which Go line decides this, and why it is NOT `HandleTruncate`'s level
//!
//! `types.getValidIntPrefix` (`pkg/types/convert.go`, `isFuncCast` arm)
//! scans `[+-]?[0-9]*` and, when the scan does not consume the WHOLE
//! (space-trimmed) string, raises
//! `ErrTruncatedWrongVal("INTEGER", str)` through `Context.HandleTruncate`.
//! The prefix it already scanned is still the answer -- the value never
//! depended on the diagnostic.
//!
//! `HandleTruncate` then consults `TypeFlags`, and this is the part that
//! matters for the trap: `ResetContextOfStmt`'s `*ast.SelectStmt` arm
//! (`pkg/executor/select.go`) sets `WithTruncateAsWarning(true)`
//! UNCONDITIONALLY -- not from the SQL mode. So a SELECT warns under strict
//! mode and under `sql_mode=''` alike, and CANNOT be turned into an error by
//! any mode. That invariant is [`a_cast_read_never_fails_the_statement`].
//!
//! The write side is the one that differs: `util.GetTypeFlagsForInsert` uses
//! `WithTruncateAsWarning(!strictSQLMode || ignoreErr)`, so the SAME
//! condition is an error inside a strict `INSERT`. That is why the level is
//! carried on the statement context rather than decided at the cast site.
//!
//! # Captured shape (read from the Go source above, both sql_modes)
//!
//! ```text
//! SELECT CAST(<v> AS SIGNED)      value   warning
//! 'x'                             0       1292 Truncated incorrect INTEGER value: 'x'
//! ''                              0       1292 Truncated incorrect INTEGER value: ''
//! '12abc'                         12      1292 Truncated incorrect INTEGER value: '12abc'
//! '3.5'                           3       1292 Truncated incorrect INTEGER value: '3.5'
//! '-7xy'                          -7      1292 Truncated incorrect INTEGER value: '-7xy'
//! '12'                            12      (none)
//! '  12  '                        12      (none)   -- Go TrimSpace's BOTH ends first
//! '-12'                           -12     (none)
//! ```

use super::Session;
use crate::tests_support::row_text;

fn warnings(session: &Session) -> Vec<(u16, String)> {
    session
        .warnings()
        .iter()
        .map(|w| (w.code, w.message.clone()))
        .collect()
}

/// The truncation warning one input leaves, or `None` when the whole string
/// was consumed and the cast is exact.
fn expected_warning(input: &str) -> Option<(u16, String)> {
    let trimmed = input.trim();
    let consumed = {
        let body = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
        let digits = body.chars().take_while(char::is_ascii_digit).count();
        !trimmed.is_empty() && digits + (trimmed.len() - body.len()) == trimmed.len()
    };
    (!consumed).then(|| {
        (
            1292,
            format!("Truncated incorrect INTEGER value: '{trimmed}'"),
        )
    })
}

/// Every row of the captured table, in BOTH sql_modes, through BOTH the
/// `Session::warnings` buffer (what `SHOW WARNINGS` reads) and
/// `Session::wire_warning_count` (what a driver reads off the packet).
#[test]
fn a_partly_numeric_string_cast_to_int_warns_1292_and_keeps_the_prefix() {
    let table: [(&str, &str); 8] = [
        ("x", "0"),
        ("", "0"),
        ("12abc", "12"),
        ("3.5", "3"),
        ("-7xy", "-7"),
        ("12", "12"),
        ("  12  ", "12"),
        ("-12", "-12"),
    ];

    for sql_mode in ["STRICT_TRANS_TABLES", ""] {
        let mut session = Session::new();
        session.run(&format!("SET sql_mode='{sql_mode}'")).unwrap();
        for (input, value) in table {
            let sql = format!("SELECT CAST('{input}' AS SIGNED)");
            let context = format!("{sql} under sql_mode='{sql_mode}'");
            let got = row_text(session.run(&sql));
            assert_eq!(got, [[value]], "{context}");

            let expected: Vec<_> = expected_warning(input).into_iter().collect();
            assert_eq!(warnings(&session), expected, "{context}");
            // The second channel: a driver learns the count from the OK/EOF
            // packet, never from the warning buffer it cannot see.
            assert_eq!(
                session.wire_warning_count(),
                u16::try_from(expected.len()).unwrap(),
                "wire count for {context}"
            );
        }
    }
}

/// The reported unit itself: two bad casts in one statement leave two
/// warnings, not one and not zero -- the count is per evaluation, so a
/// deduplicating sink would still read as "it warns".
#[test]
fn two_bad_casts_in_one_select_leave_two_warnings() {
    let mut session = Session::new();
    let got = row_text(session.run("select cast('x' as signed), cast('y' as signed)"));
    assert_eq!(got, [["0", "0"]]);
    assert_eq!(
        warnings(&session),
        [
            (1292, "Truncated incorrect INTEGER value: 'x'".to_owned()),
            (1292, "Truncated incorrect INTEGER value: 'y'".to_owned()),
        ]
    );
    assert_eq!(session.wire_warning_count(), 2);

    // And `SHOW WARNINGS` renders the same two rows for a human client.
    let shown = row_text(session.run("SHOW WARNINGS"));
    assert_eq!(
        shown,
        [
            ["Warning", "1292", "Truncated incorrect INTEGER value: 'x'"],
            ["Warning", "1292", "Truncated incorrect INTEGER value: 'y'"],
        ]
    );
}

/// THE INVARIANT that scopes this change: no SQL mode may turn a bad cast in
/// a SELECT into an error. Go's `*ast.SelectStmt` arm sets
/// `WithTruncateAsWarning(true)` with no mode input at all, so the value must
/// still come back in every mode.
#[test]
fn a_cast_read_never_fails_the_statement() {
    for sql_mode in [
        "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE",
        "STRICT_ALL_TABLES",
        "",
    ] {
        let mut session = Session::new();
        session.run(&format!("SET sql_mode='{sql_mode}'")).unwrap();
        for input in ["x", "", "12abc", "-7xy"] {
            for target in ["SIGNED", "UNSIGNED"] {
                let sql = format!("SELECT CAST('{input}' AS {target})");
                assert!(
                    session.run(&sql).is_ok(),
                    "{sql} under sql_mode='{sql_mode}' must not fail"
                );
            }
        }
    }
}

/// `UNSIGNED` reaches the same scan, so it warns on the same inputs. The
/// negative rows are the interesting ones: the value is the low-64-bit
/// reinterpretation Go produces, and the warning is still about the string.
#[test]
fn an_unsigned_cast_warns_on_the_same_strings() {
    let mut session = Session::new();
    for (input, value) in [
        ("x", "0"),
        ("12abc", "12"),
        ("-7xy", "18446744073709551609"),
        ("12", "12"),
    ] {
        let sql = format!("SELECT CAST('{input}' AS UNSIGNED)");
        let got = row_text(session.run(&sql));
        assert_eq!(got, [[value]], "{sql}");
        let expected: Vec<_> = expected_warning(input).into_iter().collect();
        assert_eq!(warnings(&session), expected, "{sql}");
    }
}
