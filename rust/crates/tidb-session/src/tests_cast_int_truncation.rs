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

/// The OTHER half of the trap, measured rather than assumed: the same cast
/// inside a WRITE is the one place Go's level reads the SQL mode, so a
/// strict `INSERT` must fail where the SELECT above must not.
///
/// Captured from a run of this port, after the fix:
///
/// ```text
/// INSERT INTO t VALUES (CAST('x' AS SIGNED))
///   STRICT_TRANS_TABLES  Err  SHOW WARNINGS: Error   1292 ...  rows []
///   sql_mode=''          Ok   SHOW WARNINGS: Warning 1292 ...  rows [0]
/// ```
///
/// The strict row is `Error`-level, not a second stray warning: a statement
/// that fails records its own error in the same buffer, which is what MySQL
/// shows for a failed statement and is pre-existing machinery here.
#[test]
fn a_strict_write_fails_on_the_same_cast_a_read_only_warns_about() {
    let mut strict = Session::new();
    strict.run("SET sql_mode='STRICT_TRANS_TABLES'").unwrap();
    strict.run("CREATE TABLE t (a INT)").unwrap();
    assert!(strict
        .run("INSERT INTO t VALUES (CAST('x' AS SIGNED))")
        .is_err());
    assert_eq!(
        warnings(&strict),
        [(1292, "Truncated incorrect INTEGER value: 'x'".to_owned())]
    );
    assert_eq!(strict.warnings()[0].level, crate::WarningLevel::Error);
    // The row never landed, so the refusal is real rather than cosmetic.
    assert_eq!(
        row_text(strict.run("SELECT a FROM t")),
        Vec::<Vec<String>>::new()
    );

    let mut permissive = Session::new();
    permissive.run("SET sql_mode=''").unwrap();
    permissive.run("CREATE TABLE t (a INT)").unwrap();
    assert!(permissive
        .run("INSERT INTO t VALUES (CAST('x' AS SIGNED))")
        .is_ok());
    assert_eq!(
        warnings(&permissive),
        [(1292, "Truncated incorrect INTEGER value: 'x'".to_owned())]
    );
    assert_eq!(permissive.warnings()[0].level, crate::WarningLevel::Warning);
    // The best-effort prefix is what Go stores, so the row IS there as 0.
    assert_eq!(row_text(permissive.run("SELECT a FROM t")), [["0"]]);
}

/// One warning per evaluation on the write path too -- a two-row permissive
/// INSERT leaves two, and the strict one stops at the first bad row rather
/// than pre-scanning every value.
#[test]
fn the_write_path_warns_once_per_evaluated_value() {
    let mut permissive = Session::new();
    permissive.run("SET sql_mode=''").unwrap();
    permissive.run("CREATE TABLE t (a INT)").unwrap();
    permissive
        .run("INSERT INTO t VALUES (CAST('y' AS SIGNED)),(CAST('z' AS SIGNED))")
        .unwrap();
    assert_eq!(
        warnings(&permissive),
        [
            (1292, "Truncated incorrect INTEGER value: 'y'".to_owned()),
            (1292, "Truncated incorrect INTEGER value: 'z'".to_owned()),
        ]
    );

    let mut strict = Session::new();
    strict.run("SET sql_mode='STRICT_TRANS_TABLES'").unwrap();
    strict.run("CREATE TABLE t (a INT)").unwrap();
    assert!(strict
        .run("INSERT INTO t VALUES (CAST('y' AS SIGNED)),(CAST('z' AS SIGNED))")
        .is_err());
    assert_eq!(
        warnings(&strict),
        [(1292, "Truncated incorrect INTEGER value: 'y'".to_owned())],
        "the second value is never reached, so it must not be reported"
    );
}

/// `CAST(<negative real> AS UNSIGNED)` answers the LOW 64 BITS, not zero.
///
/// Go `ConvertFloatToUint` (`pkg/types/convert.go:169-183`) rounds first and
/// then, for a negative result, takes the `AllowNegativeToUnsigned` arm:
/// `return uint64(int64(val)), overflow(val, tp)` -- the value AND an overflow
/// event, not a clamp to zero. This engine answered 0 for every negative real,
/// and a unit test asserted that wrong answer.
///
/// The DECIMAL source really is the opposite (`MyDecimal.ToUint`: negative ->
/// 0, with a DECIMAL-worded 1292), so the two arms must stay different.
///
/// Captured from Go (`gorun`, default sql_mode):
///
/// ```text
/// select cast(-1.5e0 as unsigned)   18446744073709551614
///   Warning|1690|constant -2 overflows bigint
/// select cast(-1e300 as unsigned)   9223372036854775808
///   Warning|1690|constant -1e+300 overflows bigint
/// select cast(-0.4e0 as unsigned)   0            (no warning)
/// select cast(-1.5 as unsigned)     0
///   Warning|1292|Truncated incorrect DECIMAL value: '-1.5'
/// ```
#[test]
fn a_negative_real_cast_to_unsigned_keeps_its_low_64_bits() {
    let mut session = Session::new();
    for (sql, value, warning) in [
        (
            "SELECT CAST(-1.5e0 AS UNSIGNED)",
            "18446744073709551614",
            Some((1690, "constant -2 overflows bigint")),
        ),
        (
            "SELECT CAST(-1e0 AS UNSIGNED)",
            "18446744073709551615",
            Some((1690, "constant -1 overflows bigint")),
        ),
        (
            "SELECT CAST(-1e300 AS UNSIGNED)",
            "9223372036854775808",
            Some((1690, "constant -1e+300 overflows bigint")),
        ),
        // -0.4 rounds to -0.0, which is not `< 0`: the one negative input that
        // really is 0, and the one that says nothing about it.
        ("SELECT CAST(-0.4e0 AS UNSIGNED)", "0", None),
        // The DECIMAL source keeps Go's own opposite rule.
        (
            "SELECT CAST(-1.5 AS UNSIGNED)",
            "0",
            Some((1292, "Truncated incorrect DECIMAL value: '-1.5'")),
        ),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], value, "{sql}");
        assert_eq!(
            warnings(&session),
            warning
                .map(|(code, text)| vec![(code, text.to_owned())])
                .unwrap_or_default(),
            "{sql}"
        );
    }
}
