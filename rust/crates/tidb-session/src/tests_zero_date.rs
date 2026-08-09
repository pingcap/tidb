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

//! A zero, zero-in, or invalid DATE/DATETIME/TIMESTAMP written into a column,
//! end to end, under every SQL mode that changes the answer.
//!
//! The engine half is `tidb_executor::zero_date`, which mirrors Go
//! `pkg/table/column.go`'s `handleZeroDatetime` and the conversion flags
//! `pkg/util/misc.go`'s `GetTypeFlagsForInsert` derives from the SQL mode.
//!
//! Captured from real TiDB (mock store, `SHOW WARNINGS` after each statement)
//! before any of this was written down. `->` is what the row reads back as;
//! `1292` is `ErrWrongValue`, whose write-path message carries the column and
//! row that `completeInsertErr` appends.
//!
//! ```text
//! INSERT INTO t(v DATE) VALUES (...)
//!                    default    sql_mode=''  NO_ZERO_DATE  NO_ZERO_IN_DATE  ALLOW_INVALID_DATES  STRICT
//! '0000-00-00'       ERR 1292   ok  zero     WARN zero     ok  zero         ok  zero             ok  zero
//! '2024-00-01'       ERR 1292   ok  as-is    ok  as-is     WARN zero        ok  as-is            ok  as-is
//! '2024-01-00'       ERR 1292   ok  as-is    ok  as-is     WARN zero        ok  as-is            ok  as-is
//! '2024-02-31'       ERR 1292   WARN zero    WARN zero     WARN zero        ok  as-is            ERR 1292
//! 'not-a-date'       ERR 1292   WARN zero    WARN zero     WARN zero        WARN zero            ERR 1292
//! '2024-13-01'       ERR 1292   WARN zero    WARN zero     WARN zero        WARN zero            ERR 1292
//! ''                 ERR 1292   WARN zero    WARN zero     WARN zero        WARN zero            ERR 1292
//! '2024-01-15'       ok         ok           ok            ok               ok                   ok
//! ```
//!
//! DATETIME behaves identically. TIMESTAMP is STRICTER and has its own table
//! in Go: only the all-zero value gets the `NO_ZERO_DATE` treatment, while
//! EVERY other bad value -- including a zero-in-date, and including an
//! otherwise-invalid date under `ALLOW_INVALID_DATES` -- is a warning plus
//! the zero timestamp without strict mode, and an error with it:
//!
//! ```text
//! INSERT INTO t(v TIMESTAMP) VALUES (...)
//!                    default    sql_mode=''  NO_ZERO_DATE  ALLOW_INVALID_DATES  STRICT  STRICT,ALLOW_INVALID_DATES
//! '0000-00-00'       ERR 1292   ok  zero     WARN zero     ok  zero             ok zero ok  zero
//! '2024-00-01'       ERR 1292   WARN zero    WARN zero     WARN zero            ERR     ERR 1292
//! '2024-02-31'       ERR 1292   WARN zero    WARN zero     WARN zero            ERR     ERR 1292
//! ```
//!
//! # The controls
//!
//! Every table above carries `'2024-01-15'` and, under the modes that permit
//! them, `'0000-00-00'` and `'2024-00-01'` stored AS THEY WERE WRITTEN. They
//! are the statements TiDB ACCEPTS, and they are here because the failure
//! this file fixes could be "fixed" in the wrong direction: erroring on every
//! zero date would turn eleven accepted statements into errors, which is a
//! worse bug than the one being repaired.
//!
//! The READ path is a separate control ([`cast_in_a_select_only_warns`]): Go
//! gives a `SELECT`'s conversion `IgnoreZeroInDate`, and a failed CAST there
//! yields NULL plus a warning under EVERY mode, strict included. The write
//! fix must not reach it.

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

/// Applies one DATETIME literal default through every DDL entry point that
/// Go routes through `SetDefaultValue`. Keeping these four together prevents
/// CREATE from becoming mode-aware while ADD, MODIFY, or SET DEFAULT quietly
/// retain a hard-coded strict conversion context.
fn check_datetime_default_ddl(
    sql_mode: &str,
    literal: &str,
    accepted: bool,
    set_default_accepted: bool,
) {
    let cases = [
        (
            "create",
            None,
            format!("CREATE TABLE d (v DATETIME DEFAULT '{literal}')"),
        ),
        (
            "add",
            Some("CREATE TABLE d (id INT)"),
            format!("ALTER TABLE d ADD COLUMN v DATETIME DEFAULT '{literal}'"),
        ),
        (
            "modify",
            Some("CREATE TABLE d (v DATETIME)"),
            format!("ALTER TABLE d MODIFY COLUMN v DATETIME DEFAULT '{literal}'"),
        ),
        (
            "set default",
            Some("CREATE TABLE d (v DATETIME)"),
            format!("ALTER TABLE d ALTER COLUMN v SET DEFAULT '{literal}'"),
        ),
    ];

    for (name, setup, ddl) in cases {
        // Go's DDL owner revalidates ALTER COLUMN ... SET DEFAULT under
        // newReorgExprCtx (ModeNone + DefaultStmtFlags), after the session
        // pass. In particular, ALLOW_INVALID_DATES can admit the spelling in
        // CREATE/ADD/MODIFY while that second owner pass still returns 1067.
        let accepted = if name == "set default" {
            set_default_accepted
        } else {
            accepted
        };
        let mut session = Session::new();
        if let Some(setup) = setup {
            session.run(setup).expect("setup table");
        }
        session
            .run(&format!("SET sql_mode='{sql_mode}'"))
            .unwrap_or_else(|error| panic!("SET sql_mode='{sql_mode}' failed: {error:?}"));
        let result = session.run(&ddl);
        if accepted {
            result.unwrap_or_else(|error| {
                panic!("{name} rejected DATETIME DEFAULT '{literal}' in {sql_mode}: {error:?}")
            });
            let stored = session
                .with_catalog_mut(|catalog| {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_mut_in("test", "d")
                    else {
                        panic!("d is not storage-backed");
                    };
                    Ok(table
                        .columns
                        .iter()
                        .find(|column| column.name == "v")
                        .expect("default column v")
                        .default_value
                        .clone())
                })
                .expect("read stored default");
            let shown = rows(&mut session, "SHOW COLUMNS FROM d");
            let displayed = shown
                .iter()
                .find(|row| row[0] == "v")
                .expect("SHOW row for v");
            assert_ne!(
                displayed[4], "NULL",
                "{name} dropped an accepted default in {sql_mode}; metadata was {stored:?}"
            );
        } else {
            let error = result
                .err()
                .unwrap_or_else(|| {
                    panic!("{name} accepted DATETIME DEFAULT '{literal}' in {sql_mode}")
                })
                .to_mysql_error();
            assert_eq!(error.code, 1067, "{name}: {ddl}");
        }
    }
}

/// One captured INSERT: the value written, and what TiDB did with it.
enum Outcome {
    /// Accepted with no warning, and the column reads back as this text.
    Stored(&'static str),
    /// Accepted with warning 1292, and the column reads back as this text.
    WarnedAndStored(&'static str),
    /// The statement failed with 1292 and wrote nothing.
    Refused,
}

/// Runs one `INSERT INTO <table> VALUES (<id>, '<value>')` and asserts the
/// captured outcome, including the value the row reads back as.
fn check_insert(
    session: &mut Session,
    table: &str,
    column_type: &str,
    id: u32,
    value: &str,
    outcome: &Outcome,
) {
    let sql = format!("INSERT INTO {table} VALUES ({id}, '{value}')");
    let result = session.run(&sql);
    match outcome {
        Outcome::Refused => {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{sql} was accepted, TiDB refuses it"))
                .to_mysql_error();
            assert_eq!(
                (error.code, error.message.as_str()),
                (
                    1292,
                    format!("Incorrect {column_type} value: '{value}' for column 'v' at row 1")
                        .as_str()
                ),
                "{sql}"
            );
            assert!(
                rows(session, &format!("SELECT v FROM {table} WHERE id = {id}")).is_empty(),
                "{sql} stored a row TiDB refuses"
            );
        }
        Outcome::Stored(expected) | Outcome::WarnedAndStored(expected) => {
            result.unwrap_or_else(|error| panic!("{sql} failed: {error:?}, TiDB accepts it"));
            let expected_warnings: Vec<(u16, String)> = match outcome {
                Outcome::WarnedAndStored(_) => vec![(
                    1292,
                    format!("Incorrect {column_type} value: '{value}' for column 'v' at row 1"),
                )],
                _ => Vec::new(),
            };
            assert_eq!(warnings(session), expected_warnings, "{sql}");
            assert_eq!(
                rows(session, &format!("SELECT v FROM {table} WHERE id = {id}")),
                [[expected.to_owned()]],
                "{sql}"
            );
        }
    }
}

/// Drives one column type through one SQL mode's captured column.
fn check_mode(column_type: &str, sql_mode: &str, cases: &[(&'static str, Outcome)]) {
    let mut session = Session::new();
    session
        .run(&format!("SET sql_mode='{sql_mode}'"))
        .unwrap_or_else(|error| panic!("SET sql_mode='{sql_mode}' failed: {error:?}"));
    session
        .run(&format!(
            "CREATE TABLE t (id INT PRIMARY KEY, v {column_type})"
        ))
        .unwrap();
    for (index, (value, outcome)) in cases.iter().enumerate() {
        check_insert(&mut session, "t", column_type, index as u32, value, outcome);
    }
}

/// The default mode (`STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE`)
/// refuses every one of them -- including `'0000-00-00'`, which was STORED
/// SILENTLY before this seam existed.
#[test]
fn default_mode_refuses_every_bad_date() {
    for column_type in ["date", "datetime", "timestamp"] {
        check_mode(
            column_type,
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE",
            &[
                ("0000-00-00", Outcome::Refused),
                ("2024-00-01", Outcome::Refused),
                ("2024-01-00", Outcome::Refused),
                ("2024-02-31", Outcome::Refused),
                ("not-a-date", Outcome::Refused),
                ("2024-13-01", Outcome::Refused),
                ("", Outcome::Refused),
                (
                    "2024-01-15",
                    Outcome::Stored(if column_type == "date" {
                        "2024-01-15"
                    } else {
                        "2024-01-15 00:00:00"
                    }),
                ),
            ],
        );
    }
}

/// DDL derives its date flags from the statement SQL mode before it settles
/// a literal default. These are the four independent bits in Go's
/// `ResetContextOfStmt` CREATE/ALTER arm, exercised through every caller of
/// the shared default pipeline.
#[test]
fn datetime_defaults_follow_ddl_sql_mode_in_every_entry_point() {
    check_datetime_default_ddl(
        "STRICT_TRANS_TABLES,NO_ZERO_DATE",
        "0000-00-00",
        false,
        false,
    );
    check_datetime_default_ddl("STRICT_TRANS_TABLES", "0000-00-00", true, true);
    check_datetime_default_ddl(
        "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE",
        "2999-00-00",
        false,
        false,
    );
    check_datetime_default_ddl("STRICT_TRANS_TABLES", "2999-00-00", true, false);
    check_datetime_default_ddl("STRICT_TRANS_TABLES", "2999-02-30", false, false);
    check_datetime_default_ddl(
        "STRICT_TRANS_TABLES,ALLOW_INVALID_DATES",
        "2999-02-30",
        true,
        false,
    );
}

/// A row written before `ADD COLUMN` has no bytes for the new column, so the
/// query path must cast its stored `OriginDefaultValue` with SELECT flags.
/// SELECT tolerates a partial-zero date even under strict SQL mode, through
/// both scan and point-read plans.
#[test]
fn origin_default_query_read_uses_query_statement_flags() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("CREATE TABLE origin_read (id INT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO origin_read VALUES (1)").unwrap();
    session
        .run("ALTER TABLE origin_read ADD COLUMN v DATE DEFAULT '2024-00-01'")
        .unwrap();

    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT v FROM origin_read"),
        vec![vec!["2024-00-01".to_owned()]],
        "a SELECT scan uses the SELECT conversion flags"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM origin_read WHERE id = 1"),
        vec![vec!["2024-00-01".to_owned()]],
        "a point read uses the same SELECT conversion flags as a scan"
    );
}

/// `sql_mode = ''`: nothing is an error, the zero and zero-in dates are
/// stored AS WRITTEN with no warning at all, and only a value that is not a
/// date warns and stores the zero date.
#[test]
fn empty_sql_mode_warns_and_stores_the_zero_date() {
    check_mode(
        "date",
        "",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00")),
            ("2024-00-01", Outcome::Stored("2024-00-01")),
            ("2024-01-00", Outcome::Stored("2024-01-00")),
            ("2024-02-31", Outcome::WarnedAndStored("0000-00-00")),
            ("not-a-date", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-13-01", Outcome::WarnedAndStored("0000-00-00")),
            ("", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
    check_mode(
        "datetime",
        "",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00 00:00:00")),
            ("2024-00-01", Outcome::Stored("2024-00-01 00:00:00")),
            (
                "not-a-date",
                Outcome::WarnedAndStored("0000-00-00 00:00:00"),
            ),
            ("2024-01-15", Outcome::Stored("2024-01-15 00:00:00")),
        ],
    );
}

/// `NO_ZERO_DATE` alone is the case the hypothesis got wrong twice: it warns
/// AND STORES the zero date, it does not refuse, and it leaves a zero-in-date
/// alone.
#[test]
fn no_zero_date_alone_warns_and_still_stores() {
    check_mode(
        "date",
        "NO_ZERO_DATE",
        &[
            ("0000-00-00", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-00-01", Outcome::Stored("2024-00-01")),
            ("2024-01-00", Outcome::Stored("2024-01-00")),
            ("not-a-date", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
}

/// `NO_ZERO_IN_DATE` is a DIFFERENT flag from `NO_ZERO_DATE`: it fires on a
/// zero month or day and leaves the all-zero date alone -- the exact mirror
/// of the test above, which is what proves the two are not one bit.
#[test]
fn no_zero_in_date_alone_fires_on_the_zero_part() {
    check_mode(
        "date",
        "NO_ZERO_IN_DATE",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00")),
            ("2024-00-01", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-01-00", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
}

/// `ALLOW_INVALID_DATES` accepts a well-formed date that does not exist, and
/// only that: a month of 13 or an unparseable string still warns.
#[test]
fn allow_invalid_dates_accepts_february_31st() {
    check_mode(
        "date",
        "ALLOW_INVALID_DATES",
        &[
            ("2024-02-31", Outcome::Stored("2024-02-31")),
            ("2024-13-01", Outcome::WarnedAndStored("0000-00-00")),
            ("not-a-date", Outcome::WarnedAndStored("0000-00-00")),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
    // With strict mode beside it the accepted case stays accepted and the
    // rejected ones become errors -- the flag is about which dates are VALID,
    // not about the level.
    check_mode(
        "date",
        "STRICT_TRANS_TABLES,ALLOW_INVALID_DATES",
        &[
            ("2024-02-31", Outcome::Stored("2024-02-31")),
            ("2024-13-01", Outcome::Refused),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
}

/// Strict mode WITHOUT `NO_ZERO_DATE`/`NO_ZERO_IN_DATE` still accepts both
/// zero forms. This is the control that keeps strict mode from being read as
/// "refuse every zero date".
#[test]
fn strict_mode_alone_still_accepts_the_zero_dates() {
    check_mode(
        "date",
        "STRICT_TRANS_TABLES",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00")),
            ("2024-00-01", Outcome::Stored("2024-00-01")),
            ("2024-01-00", Outcome::Stored("2024-01-00")),
            ("2024-02-31", Outcome::Refused),
            ("not-a-date", Outcome::Refused),
            ("2024-01-15", Outcome::Stored("2024-01-15")),
        ],
    );
}

/// TIMESTAMP's own table: a zero-in-date is NOT tolerated the way DATE's is,
/// and `ALLOW_INVALID_DATES` does not rescue `'2024-02-31'` either. Go says
/// so in `handleZeroDatetime`'s timestamp arms, and TiDB confirms it.
#[test]
fn timestamp_is_stricter_than_date() {
    check_mode(
        "timestamp",
        "",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00 00:00:00")),
            (
                "2024-00-01",
                Outcome::WarnedAndStored("0000-00-00 00:00:00"),
            ),
            (
                "2024-02-31",
                Outcome::WarnedAndStored("0000-00-00 00:00:00"),
            ),
            ("2024-01-15", Outcome::Stored("2024-01-15 00:00:00")),
        ],
    );
    check_mode(
        "timestamp",
        "NO_ZERO_DATE",
        &[
            (
                "0000-00-00",
                Outcome::WarnedAndStored("0000-00-00 00:00:00"),
            ),
            ("2024-01-15", Outcome::Stored("2024-01-15 00:00:00")),
        ],
    );
    check_mode(
        "timestamp",
        "STRICT_TRANS_TABLES,ALLOW_INVALID_DATES",
        &[
            ("0000-00-00", Outcome::Stored("0000-00-00 00:00:00")),
            ("2024-00-01", Outcome::Refused),
            ("2024-02-31", Outcome::Refused),
            ("2024-01-15", Outcome::Stored("2024-01-15 00:00:00")),
        ],
    );
}

/// The UPDATE half. Go derives the level per statement kind, so this is
/// asserted separately rather than assumed to follow the INSERT.
///
/// An UPDATE's message is `table.CastValue`'s OWN, with no column and no
/// row: Go's `handleUpdateError` re-titles `ErrDataTooLong` and `ErrOverflow`
/// and returns everything else unchanged. Both lines below were re-measured
/// against TiDB after this test asserted the INSERT's decorated form here by
/// mistake:
///
/// ```text
/// set sql_mode='NO_ZERO_DATE,NO_ZERO_IN_DATE';
/// update u2 set v='not-a-date';
///   Warning 1292 Incorrect date value: 'not-a-date'
/// set sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE';
/// update u set v='0000-00-00';
///   ERROR 1292 (22007): Incorrect date value: '0000-00-00'
/// ```
#[test]
fn update_follows_the_same_table() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("CREATE TABLE u (id INT PRIMARY KEY, v DATE)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (1, '2020-05-05')")
        .unwrap();

    session.run("UPDATE u SET v = 'not-a-date'").unwrap();
    assert_eq!(
        warnings(&session),
        [(1292, "Incorrect date value: 'not-a-date'".to_owned())]
    );
    assert_eq!(rows(&mut session, "SELECT v FROM u"), [["0000-00-00"]]);

    // The control: an accepted UPDATE under the same mode.
    session.run("UPDATE u SET v = '0000-00-00'").unwrap();
    assert_eq!(warnings(&session), []);
    assert_eq!(rows(&mut session, "SELECT v FROM u"), [["0000-00-00"]]);

    let mut strict = Session::new();
    strict
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE'")
        .unwrap();
    strict
        .run("CREATE TABLE u (id INT PRIMARY KEY, v DATE)")
        .unwrap();
    strict.run("SET sql_mode=''").unwrap();
    strict
        .run("INSERT INTO u VALUES (1, '2020-05-05')")
        .unwrap();
    strict
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE'")
        .unwrap();
    let error = strict
        .run("UPDATE u SET v = '0000-00-00'")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1292, "Incorrect date value: '0000-00-00'")
    );
    // A refused UPDATE leaves the row alone.
    assert_eq!(rows(&mut strict, "SELECT v FROM u"), [["2020-05-05"]]);
}

/// THE READ CONTROL. A `CAST` in a `SELECT` never fails the statement, in any
/// mode: it warns and yields NULL. Go reaches this through the `SelectStmt`
/// arm of `ResetContextOfStmt`, which sets `IgnoreZeroInDate` unconditionally
/// -- which is also why a zero-in-date reads back INTACT even under the
/// default mode that refuses to store one.
#[test]
fn cast_in_a_select_only_warns() {
    for sql_mode in [
        "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE",
        "",
        "ALLOW_INVALID_DATES",
    ] {
        let mut session = Session::new();
        session.run(&format!("SET sql_mode='{sql_mode}'")).unwrap();

        assert_eq!(
            rows(&mut session, "SELECT CAST('not-a-date' AS DATE)"),
            [["NULL"]],
            "sql_mode={sql_mode}"
        );
        // The two read-path gaps this control once only NAMED -- a missing
        // warning 1292, and a zero-in-date reading as NULL instead of itself
        // -- are closed, and the full read matrix now lives in
        // `crate::tests_read_cast`. What stays here is the property the WRITE
        // fix had to preserve and must keep preserving: the read path never
        // fails the statement, in any mode.
        session
            .run("SELECT CAST('2024-00-01' AS DATE)")
            .unwrap_or_else(|error| panic!("read path failed under {sql_mode}: {error:?}"));
    }
}
