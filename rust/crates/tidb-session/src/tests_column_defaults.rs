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

/// TiDB accepts `DEFAULT CURRENT_TIMESTAMP(3)` on a `TIMESTAMP(3)` column and
/// prints it back with the fsp (captured: `` `b` timestamp(3) DEFAULT
/// CURRENT_TIMESTAMP(3) ``). This tier stores a temporal type's written `(n)`
/// in `flen` rather than in `decimal`, so it cannot apply Go's
/// fsp-must-match rule; the form is REFUSED rather than accepted under a
/// guess. This test pins the refusal so that fixing the fsp storage is what
/// turns it into support.
#[test]
fn an_explicit_fsp_on_the_clock_default_is_refused() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE t7 (a INT, b TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))")
        .is_err());
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
