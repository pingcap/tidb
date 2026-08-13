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

//! The `sql_mode` flags the SCANNER consults, over a real session, through
//! every door a statement can take -- SELECT, INSERT, DDL.
//!
//! The lexer has modelled `NO_BACKSLASH_ESCAPES`, `ANSI_QUOTES`,
//! `HIGH_NOT_PRECEDENCE` and `REAL_AS_FLOAT` for a long time; what it did not
//! have was a caller. Every parse site hard-coded `SqlMode::default()`, so
//! `SET sql_mode = 'ANSI'` -- a composite clients really do set -- silently
//! bought four ignored flags.
//!
//! The reason the tests below run each flag through a DML or DDL statement as
//! well as a SELECT is that this tier RE-PARSES the raw statement text in the
//! executor tiers, where Go parses once in `session.ParseSQL` and passes the
//! AST. A mode honored in SELECT and dropped in INSERT is worse than a mode
//! ignored everywhere: it writes different bytes than it reads. The mode now
//! travels on `StmtContext`, which every executor entry already takes.
//!
//! Captured from real TiDB (`gorun`, unistore mock cluster) before any of this
//! was written:
//!
//! ```text
//! -- default sql_mode
//! select length('a\nb')                      3
//! insert into t values (1,'a\nb'); select length(s)   3
//! select "id" from t                         id      (a string literal)
//!
//! -- sql_mode = 'NO_BACKSLASH_ESCAPES'
//! select length('a\nb')                      4
//! insert into t values (2,'a\nb'); select length(s)   4
//!
//! -- sql_mode = 'ANSI_QUOTES' (and 'ANSI', which expands to it)
//! select "id" from t                         7       (the COLUMN id)
//! select @@sql_mode after SET sql_mode='ANSI'
//!     REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI
//!
//! -- a DDL expression created under NO_BACKSLASH_ESCAPES, read back after
//! -- the mode is cleared: the stored form is CANONICAL, so the value does
//! -- not change with the reader's mode
//! create table g (s varchar(20), c varchar(40) as (concat(s,'\c')) stored,
//!                 d varchar(20) default 'x\y');
//! insert into g (s) values ('a');   -> a | a\c | 3 | x\y | 3
//! set sql_mode=default; insert into g (s) values ('b');
//!                                   -> b | b\c | 3 | x\y | 3
//! ```

use super::{Session, StmtResult};
use crate::tests_support::{row_text, show_create};

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

fn one(session: &mut Session, sql: &str) -> String {
    rows(session, sql).remove(0).remove(0)
}

/// `NO_BACKSLASH_ESCAPES` reaches the SELECT door and the INSERT door alike.
/// The INSERT half is the one that matters: it is the re-parsing side, and it
/// is where both recorded divergences in
/// `tests/integrationtest/t/generated_columns.test` live.
#[test]
fn no_backslash_escapes_reaches_select_and_the_re_parsed_insert() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT, s VARCHAR(50))")
        .unwrap();

    assert_eq!(one(&mut session, r"SELECT LENGTH('a\nb')"), "3");
    session.run(r"INSERT INTO t VALUES (1, 'a\nb')").unwrap();
    assert_eq!(
        one(&mut session, "SELECT LENGTH(s) FROM t WHERE id = 1"),
        "3"
    );

    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();

    assert_eq!(one(&mut session, r"SELECT LENGTH('a\nb')"), "4");
    // The write door: the backslash is an ordinary character, so four bytes
    // are STORED, not three.
    session.run(r"INSERT INTO t VALUES (2, 'a\nb')").unwrap();
    assert_eq!(
        one(&mut session, "SELECT LENGTH(s) FROM t WHERE id = 2"),
        "4"
    );

    // ... and the row written under the default mode is untouched: the mode
    // decides how text is LEXED, not how stored bytes are read back.
    assert_eq!(
        one(&mut session, "SELECT LENGTH(s) FROM t WHERE id = 1"),
        "3"
    );
}

/// An UPDATE re-parses its own text too, so the same flag has to reach it.
#[test]
fn no_backslash_escapes_reaches_update_and_delete() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (id INT, s VARCHAR(50))")
        .unwrap();
    session.run("INSERT INTO u VALUES (1, 'x')").unwrap();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();

    session
        .run(r"UPDATE u SET s = 'a\nb' WHERE id = 1")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT LENGTH(s) FROM u"), "4");

    // The DELETE door lexes the same literal, so it matches the row the
    // UPDATE wrote.
    assert_eq!(
        session.run(r"DELETE FROM u WHERE s = 'a\nb'").unwrap(),
        StmtResult::Affected(1)
    );
}

/// `ANSI_QUOTES` turns a double-quoted token into an identifier -- captured:
/// `select "id" from t` reports the string `id` by default and the COLUMN
/// under the flag.
#[test]
fn ansi_quotes_makes_a_double_quoted_token_an_identifier() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT)").unwrap();
    session.run("INSERT INTO t VALUES (7)").unwrap();

    assert_eq!(one(&mut session, r#"SELECT "id" FROM t"#), "id");

    session.run("SET sql_mode='ANSI_QUOTES'").unwrap();
    assert_eq!(one(&mut session, r#"SELECT "id" FROM t"#), "7");

    // The write door -- a re-parsed statement -- reads the same token the same
    // way: the UPDATE names the COLUMN on both sides.
    session.run(r#"UPDATE t SET "id" = "id" + 1"#).unwrap();
    assert_eq!(one(&mut session, r#"SELECT "id" FROM t"#), "8");
}

/// `SET sql_mode = 'ANSI'` is the door a client walks through by accident: it
/// expands to five flags, four of them the scanner's. Captured `@@sql_mode`
/// after the SET is the expansion, and `select "id"` answers the column.
#[test]
fn the_ansi_composite_carries_its_scanner_flags_through() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT)").unwrap();
    session.run("INSERT INTO t VALUES (7)").unwrap();
    session.run("SET sql_mode='ANSI'").unwrap();

    assert_eq!(
        one(&mut session, "SELECT @@sql_mode"),
        "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
    );
    assert_eq!(one(&mut session, r#"SELECT "id" FROM t"#), "7");
}

/// `REAL_AS_FLOAT` is consulted while a COLUMN TYPE is parsed, which happens
/// on the DDL door -- the one that has no `StmtContext` of its own and takes
/// the mode as its own parameter.
#[test]
fn real_as_float_reaches_create_table() {
    let mut session = Session::new();
    session.run("CREATE TABLE d (a REAL)").unwrap();
    session.run("SET sql_mode='REAL_AS_FLOAT'").unwrap();
    session.run("CREATE TABLE f (a REAL)").unwrap();

    let double = show_create(&mut session, "d");
    let float = show_create(&mut session, "f");
    assert!(double.contains("double"), "{double}");
    assert!(float.contains("float"), "{float}");
}

/// `HIGH_NOT_PRECEDENCE` changes how `NOT` binds. Captured:
/// `SELECT NOT 1 BETWEEN 0 AND 3` is 0 by default and 1 under the flag.
#[test]
fn high_not_precedence_rebinds_not() {
    let mut session = Session::new();
    assert_eq!(one(&mut session, "SELECT NOT 1 BETWEEN 0 AND 3"), "0");
    session.run("SET sql_mode='HIGH_NOT_PRECEDENCE'").unwrap();
    assert_eq!(one(&mut session, "SELECT NOT 1 BETWEEN 0 AND 3"), "1");
}

/// The mode in force is the one AT PARSE TIME: a `SET sql_mode` changes the
/// statements after it and nothing already parsed. The clearing direction is
/// the interesting one, because a cached mode would keep answering 4.
#[test]
fn the_mode_that_counts_is_the_one_current_at_parse_time() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(one(&mut session, r"SELECT LENGTH('a\nb')"), "4");
    session.run("SET sql_mode=default").unwrap();
    assert_eq!(one(&mut session, r"SELECT LENGTH('a\nb')"), "3");
}

/// Go `expression_rewriter` stops supplying the implicit backslash escape
/// under `NO_BACKSLASH_ESCAPES` when this session switch is enabled.  The
/// scanner already preserves the backslash in both literals; the expression
/// builder must make the same decision for the omitted `ESCAPE` clause.
#[test]
fn no_backslash_escapes_changes_like_default_escape_only_when_enabled() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();

    assert_eq!(one(&mut session, r"SELECT 'a\b' LIKE 'a\b'"), "1");

    session
        .run("SET tidb_enable_no_backslash_escapes_in_like = OFF")
        .unwrap();
    assert_eq!(one(&mut session, r"SELECT 'a\b' LIKE 'a\b'"), "0");
}

/// The same statement snapshot reaches a table-backed `WHERE`, whose
/// expression resolver is built from `FromScope` rather than from the
/// table-dual path above.
#[test]
fn no_backslash_escapes_like_default_reaches_a_table_filter() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    session
        .run("CREATE TABLE l (s VARCHAR(20), KEY s (s))")
        .unwrap();
    session.run(r"INSERT INTO l VALUES ('a\b')").unwrap();

    assert_eq!(
        one(
            &mut session,
            r"SELECT COUNT(*) FROM l FORCE INDEX (s) WHERE s LIKE 'a\b'",
        ),
        "1"
    );

    session
        .run("SET tidb_enable_no_backslash_escapes_in_like = OFF")
        .unwrap();
    assert_eq!(
        one(
            &mut session,
            r"SELECT COUNT(*) FROM l FORCE INDEX (s) WHERE s LIKE 'a\b'",
        ),
        "0"
    );
}

/// Generated expressions are rewritten while DDL is admitted, so they must
/// capture the same omitted-escape policy as ordinary query expressions.
#[test]
fn no_backslash_escapes_like_default_reaches_a_generated_expression() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    session
        .run(r"CREATE TABLE g_like (s VARCHAR(20), m TINYINT AS (s LIKE 'a\b') STORED)")
        .unwrap();
    session
        .run(r"INSERT INTO g_like (s) VALUES ('a\b')")
        .unwrap();

    assert_eq!(one(&mut session, "SELECT m FROM g_like"), "1");
}

/// A virtual generated expression is rebuilt in the reading statement's
/// context, so changing the enabled session policy changes its result.
#[test]
fn no_backslash_escapes_like_default_rebuilds_virtual_generated_expressions() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    session
        .run(r"CREATE TABLE v_like (s VARCHAR(20), m TINYINT AS (s LIKE 'a\b') VIRTUAL)")
        .unwrap();
    session
        .run(r"INSERT INTO v_like (s) VALUES ('a\b')")
        .unwrap();

    assert_eq!(one(&mut session, "SELECT m FROM v_like"), "1");
    session
        .run("SET @@tidb_enable_no_backslash_escapes_in_like = 0")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT m FROM v_like"), "0");
}

/// A generated column and a DEFAULT written under `NO_BACKSLASH_ESCAPES` keep
/// their meaning after the mode is cleared, because the DDL stores the parsed
/// expression rather than the text. Captured from TiDB, where `SHOW CREATE
/// TABLE` prints the CANONICAL restore (`'\\c'`) in either mode and a row
/// inserted later gets the same value as one inserted before.
#[test]
fn a_ddl_expression_keeps_its_creation_mode_meaning() {
    let mut session = Session::new();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    session
        .run(
            r"CREATE TABLE g (s VARCHAR(20), c VARCHAR(40) AS (CONCAT(s, '\c')) STORED,
                              d VARCHAR(20) DEFAULT 'x\y')",
        )
        .unwrap();
    session.run("INSERT INTO g (s) VALUES ('a')").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT c, LENGTH(c), d, LENGTH(d) FROM g"),
        vec![vec![
            "a\\c".to_owned(),
            "3".to_owned(),
            "x\\y".to_owned(),
            "3".to_owned()
        ]]
    );

    session.run("SET sql_mode=default").unwrap();
    session.run("INSERT INTO g (s) VALUES ('b')").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT c, LENGTH(c) FROM g WHERE s = 'b'"),
        vec![vec!["b\\c".to_owned(), "3".to_owned()]]
    );
}

/// The default session is unchanged by all of the above: this is the control
/// for the widest blast radius in the tree, the shared parser.
#[test]
fn the_default_session_lexes_exactly_as_before() {
    let mut session = Session::new();
    assert_eq!(one(&mut session, r"SELECT LENGTH('a\nb')"), "3");
    assert_eq!(one(&mut session, r#"SELECT "id""#), "id");
    assert_eq!(one(&mut session, "SELECT NOT 1 BETWEEN 0 AND 3"), "0");
}
