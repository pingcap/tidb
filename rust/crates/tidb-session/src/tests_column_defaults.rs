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

//! A column `DEFAULT` that is COMPUTED rather than settled, end to end over a
//! real session: what it stores, what `SHOW CREATE TABLE` prints back, and
//! what an omitted column takes on `INSERT`.
//!
//! Every expectation here was CAPTURED from real TiDB through
//! `rust/difftests/gorun` before it was written down; the capture script is
//! reproduced statement by statement in the tests below, so a reader can see
//! which Go answer each assertion quotes. The captured `SHOW CREATE TABLE`
//! bodies were, verbatim:
//!
//! ```text
//! create table t1 (a int, b timestamp default current_timestamp)
//!   `b` timestamp DEFAULT CURRENT_TIMESTAMP
//! create table t3 (a int, b timestamp default now())
//!   `b` timestamp DEFAULT CURRENT_TIMESTAMP      -- `now()` stores the marker
//! create table t7 (a int, b timestamp(3) default current_timestamp(3))
//!   `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3)
//! create table t4 (a int, b varchar(64) default (uuid()))
//!   `b` varchar(64) DEFAULT (uuid())          -- accepted by Go; see below
//! create table t5 (a int, b double default (rand()))
//!   `b` double DEFAULT (rand())
//! create table t9 (a int, b int default (1+1))
//!   `b` int(11) DEFAULT '2'                      -- folded, and QUOTED
//! create table t8 (a int, b varchar(10) default (upper('ab')))
//!   ERR                                          -- not on Go's whitelist
//! ```
//!
//! Those bodies are `gorun`'s output verbatim, and `int(11)` is the one place
//! they do NOT describe a running node: `deprecate-integer-display-length`
//! defaults to true and only `cmd/tidb-server/main.go` applies it, so a real
//! server prints `int` there. The assertions below quote the DEFAULT, which
//! the switch does not touch.
//!
//! Mirrors Go `pkg/ddl/add_column.go` (`SetDefaultValue`, `getDefaultValue`,
//! `getFuncCallDefaultValue`), `pkg/table/column.go` (`GetColDefaultValue`,
//! `NewColDesc`) and `pkg/executor/show.go`'s default printer.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The body of `SHOW CREATE TABLE t`, which is its second cell.
fn show_create(session: &mut Session, table: &str) -> String {
    rows(session, &format!("SHOW CREATE TABLE {table}")).remove(0)[1].clone()
}

/// The error code a statement fails with, or `None` when it succeeded.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// `DEFAULT CURRENT_TIMESTAMP` stores Go's marker word, and every spelling of
/// the clock -- `current_timestamp`, `now()` -- stores the SAME one, so all of
/// them print back identically.
#[test]
fn current_timestamp_default_prints_as_the_marker() {
    let mut session = Session::new();
    for (table, written) in [
        ("t1", "b TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("t2", "b DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("t3", "b TIMESTAMP DEFAULT now()"),
    ] {
        session
            .run(&format!("CREATE TABLE {table} (a INT, {written})"))
            .unwrap();
        assert!(
            show_create(&mut session, table).contains("DEFAULT CURRENT_TIMESTAMP"),
            "{table} declared `{written}`: {}",
            show_create(&mut session, table)
        );
    }
}

/// Go `getFuncCallDefaultValue`'s whole rule for the clock marker on a
/// `TIMESTAMP`/`DATETIME` column: the fsp WRITTEN on the default -- 0 when it
/// is written bare -- must EQUAL the column's own fsp, and `ErrInvalidDefault`
/// (1067) is the answer when it does not. So the two spellings are not
/// interchangeable on a column that has an fsp: `DATETIME(3)` demands
/// `CURRENT_TIMESTAMP(3)` and refuses the bare word.
///
/// Captured from real TiDB, statement by statement:
///
/// ```text
/// create table a10 (ts timestamp(3) default current_timestamp(3))  OK
/// create table a3  (ts datetime(3)  default now(3))                OK
/// create table a1  (ts datetime(3)  default current_timestamp)     ERR
/// create table a2  (ts datetime     default current_timestamp(3))  ERR
/// create table a6  (ts datetime(3)  default current_timestamp(2))  ERR
/// ```
#[test]
fn the_clock_defaults_fsp_must_equal_the_columns_own() {
    let mut session = Session::new();
    for (table, written) in [
        ("t7", "b TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        ("t7b", "b DATETIME(3) DEFAULT now(3)"),
        ("t7c", "b TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
    ] {
        session
            .run(&format!("CREATE TABLE {table} (a INT, {written})"))
            .unwrap_or_else(|error| panic!("{written}: {error:?}"));
    }
    for written in [
        "b DATETIME(3) DEFAULT CURRENT_TIMESTAMP",
        "b DATETIME DEFAULT CURRENT_TIMESTAMP(3)",
        "b DATETIME(3) DEFAULT CURRENT_TIMESTAMP(2)",
        "b TIMESTAMP(6) DEFAULT now()",
    ] {
        assert_eq!(
            code(
                &mut session,
                &format!("CREATE TABLE bad (a INT, {written})")
            ),
            Some(1067),
            "{written}"
        );
    }
}

/// The fsp travels from the declared type through the stored default and into
/// the WRITE: an omitted `DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)` column
/// stores a clock reading with three fractional digits, not a whole second.
///
/// Captured from real TiDB:
///
/// ```text
/// create table t72 (id int primary key, ts datetime(3) default current_timestamp(3),
///                   d datetime(6) default current_timestamp(6),
///                   z datetime default current_timestamp)
/// insert into t72 (id) values (1)
/// select id, ts, d, z from t72
///   1|2026-08-01 00:40:17.093|2026-08-01 00:40:17.093391|2026-08-01 00:40:17
/// select length(ts), length(d), length(z) from t72
///   23|26|19
/// ```
#[test]
fn the_columns_fsp_reaches_the_value_a_clock_default_writes() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t72 (id INT PRIMARY KEY, \
             ts DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3), \
             d DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6), \
             z DATETIME DEFAULT CURRENT_TIMESTAMP)",
        )
        .unwrap();
    session.run("INSERT INTO t72 (id) VALUES (1)").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT LENGTH(ts), LENGTH(d), LENGTH(z) FROM t72"
        ),
        vec![vec!["23".to_owned(), "26".to_owned(), "19".to_owned()]]
    );
}

/// `SHOW CREATE TABLE` prints the marker with the column's fsp appended, and
/// `now(3)` prints back as `CURRENT_TIMESTAMP(3)` because Go stores the marker
/// word rather than the written spelling. Captured:
/// `` `ts` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ``.
#[test]
fn a_clock_default_with_an_fsp_prints_the_marker_with_it() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a3 (ts DATETIME(3) DEFAULT now(3))")
        .unwrap();
    assert!(
        show_create(&mut session, "a3").contains("`ts` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{}",
        show_create(&mut session, "a3")
    );
}

/// An omitted `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column takes a clock
/// reading, not NULL: `insert into t1 (a) values (1)` then
/// `select a, b is not null from t1` captured `1|1`.
#[test]
fn an_omitted_clock_default_reads_the_clock() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        .unwrap();
    session.run("INSERT INTO t1 (a) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, b IS NOT NULL FROM t1"),
        vec![vec!["1".to_owned(), "1".to_owned()]]
    );
}

/// A `DefaultIsExpr` default prints PARENTHESISED and unquoted, which is the
/// one visible difference from a literal default.
#[test]
fn an_expression_default_prints_parenthesised() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    assert!(
        show_create(&mut session, "t5").contains("DEFAULT (rand())"),
        "{}",
        show_create(&mut session, "t5")
    );
}

/// `DEFAULT (uuid())` is ON Go's whitelist and captured as
/// `` `b` varchar(64) DEFAULT (uuid()) ``, but `uuid` is not among the
/// builtins this tier evaluates over a chunk, so the default is refused at
/// DDL time rather than stored as one that would fail on every INSERT.
#[test]
fn a_uuid_default_is_refused_because_the_builtin_is_not_evaluable() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE t4 (a INT, b VARCHAR(64) DEFAULT (uuid()))")
        .is_err());
}

/// An omitted expression-default column is evaluated per row: `insert into t4
/// (a) values (1)` then `select a, length(b) from t4` captured `1|36`, the
/// length of a UUID.
#[test]
fn an_omitted_expression_default_is_evaluated() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    session.run("INSERT INTO t5 (a) VALUES (1)").unwrap();
    // `rand()` is in [0, 1): the assertion is that a value was COMPUTED for
    // the omitted column rather than the NULL a missing default would give.
    assert_eq!(
        rows(&mut session, "SELECT a, b >= 0 AND b < 1 FROM t5"),
        vec![vec!["1".to_owned(), "1".to_owned()]]
    );
}

/// `DEFAULT (1+1)` is NOT a function call, so Go folds it at DDL time and
/// stores `2` -- printed QUOTED, like every other literal default, and read
/// back as the integer 2.
#[test]
fn a_folded_default_stays_a_settled_literal() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t9 (a INT, b INT DEFAULT (1+1))")
        .unwrap();
    assert!(
        show_create(&mut session, "t9").contains("DEFAULT '2'"),
        "{}",
        show_create(&mut session, "t9")
    );
    session.run("INSERT INTO t9 (a) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t9"),
        vec![vec!["1".to_owned(), "2".to_owned()]]
    );
}

/// The whitelist is the whole rule: a function that is not on it is refused
/// even when it FOLDS to a constant. Captured from TiDB, `create table t8 (a
/// int, b varchar(10) default (upper('ab')))` is an error -- `upper` is
/// accepted only as `UPPER(SUBSTRING_INDEX(USER(), '@', 1))`.
#[test]
fn a_function_off_the_whitelist_is_refused_even_when_constant() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE t8 (a INT, b VARCHAR(10) DEFAULT (upper('ab')))"
        ),
        Some(3770)
    );
}

/// `SHOW COLUMNS` reports the STORED string, which for an expression default
/// is the text WITHOUT the parentheses `SHOW CREATE TABLE` adds -- Go
/// `NewColDesc` and `pkg/executor/show.go` read the same field and render it
/// differently, so both renderings are asserted here together.
#[test]
fn show_columns_reports_the_stored_default_text() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    session
        .run("CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        .unwrap();
    let expression = rows(&mut session, "SHOW COLUMNS FROM t5");
    assert_eq!(expression[1][4], "rand()");
    let clock = rows(&mut session, "SHOW COLUMNS FROM t1");
    assert_eq!(clock[1][4], "CURRENT_TIMESTAMP");
}

/// Transcreates Go `pkg/ddl/db_integration_test.go`'s `TestEnumAndSetDefaultValue`.
///
/// A HEX LITERAL is a legal way to spell an `ENUM`/`SET` element and its
/// `DEFAULT`, and it must be RESOLVED to the member string before the column
/// is stored -- `0x61` names the member `'a'`; it is not the number 97 and not
/// the text `0x61`. Go decides this in `pkg/ddl/add_column.go`
/// `getDefaultValue` -> `types.Datum.ConvertTo` against the enum's field type,
/// so the resolution happens once at CREATE time and everything downstream
/// reads a settled member.
///
/// This is the shape a wrong answer takes here: the hex is ACCEPTED by the
/// parser and then DISCARDED by whoever stores it, leaving a column whose
/// default is not a member of its own type. `0x61` is chosen over an ASCII
/// `'a'` for exactly that reason -- with `'a'` written directly, an engine
/// that never converts anything still passes.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table t (a enum(0x61, 'b') not null default 0x61,
///                 b set(0x61, 'b') not null default 0x61) character set latin1
///   `a` enum('a','b') NOT NULL DEFAULT 'a',
///   `b` set('a','b') NOT NULL DEFAULT 'a'
/// ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
/// insert into t values ()
/// select a, b from t   ->   a|a
/// ```
///
/// The charset is varied because Go's conversion runs against the column's
/// resolved charset: `latin1` and `utf8mb4` must reach the same member.
#[test]
fn a_hex_literal_enum_and_set_default_resolves_to_the_member_string() {
    for (table, charset, collation) in [
        ("t", "latin1", "latin1_bin"),
        ("t2", "utf8mb4", "utf8mb4_bin"),
    ] {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE {table} (a ENUM(0x61, 'b') NOT NULL DEFAULT 0x61, \
                 b SET(0x61, 'b') NOT NULL DEFAULT 0x61) CHARACTER SET {charset}"
            ))
            .unwrap();
        let body = show_create(&mut session, table);
        assert!(
            body.contains("`a` enum('a','b') NOT NULL DEFAULT 'a'"),
            "{charset}: enum column did not resolve its hex element/default: {body}"
        );
        assert!(
            body.contains("`b` set('a','b') NOT NULL DEFAULT 'a'"),
            "{charset}: set column did not resolve its hex element/default: {body}"
        );
        assert!(
            body.contains(&format!("DEFAULT CHARSET={charset} COLLATE={collation}")),
            "{charset}: table charset/collation: {body}"
        );

        session
            .run(&format!("INSERT INTO {table} VALUES ()"))
            .unwrap();
        assert_eq!(
            rows(&mut session, &format!("SELECT a, b FROM {table}")),
            vec![vec!["a".to_owned(), "a".to_owned()]],
            "{charset}: the omitted columns did not take the resolved member"
        );
    }
}

/// Transcreates Go `pkg/ddl/db_integration_test.go`'s `TestEnumDefaultValue`.
///
/// An `ENUM` `DEFAULT` is matched against the member list the way a VALUE of
/// that type is: trailing spaces are not significant, so `DEFAULT 'b '`
/// settles as the member `'b'` and prints back without its space. An engine
/// that stores the literal it was handed keeps a default that no member equals.
///
/// The empty-string member is kept from the Go case on purpose: it makes
/// "matched a member" distinguishable from "fell back to the first member",
/// which for this list is `''` and not `'b'`.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim -- both
/// the exact and the space-padded spelling produce the SAME body:
///
/// ```text
/// CREATE TABLE t3 ( a enum('','a','b') NOT NULL DEFAULT 'b' )
///   ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
///   `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'
/// CREATE TABLE t4 ( a enum('','a','b') NOT NULL DEFAULT 'b ' )  -- trailing space
///   `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'
/// ```
#[test]
fn a_space_padded_enum_default_settles_on_the_member_it_names() {
    let expected = "`a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'";
    for (table, written) in [("t3", "'b'"), ("t4", "'b '")] {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE {table} ( a ENUM('','a','b') NOT NULL DEFAULT {written} ) \
                 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            ))
            .unwrap();
        let body = show_create(&mut session, table);
        assert!(
            body.contains(expected),
            "declared DEFAULT {written}: {body}"
        );
    }
}
