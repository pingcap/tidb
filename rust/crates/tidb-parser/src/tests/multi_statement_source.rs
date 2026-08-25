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

//! Source-backed coverage for Go's multi-statement parser envelope.
//!
//! `Parser.Parse` returns every statement completed by a semicolon, while the
//! ordinary Rust [`parse`](crate::parse) entrypoint deliberately remains a
//! strict one-statement API. These rows are the exact multi-statement inputs
//! from the integration corpus that previously stopped at Rust's trailing
//! token check. The test keeps the source boundary and every canonical restore
//! visible so a future caller cannot accidentally implement this as "parse the
//! first statement and ignore the rest".

use super::*;

fn restores(sql: &str) -> Vec<String> {
    parse_multi(sql)
        .expect("Go accepts the multi-statement source row")
        .into_iter()
        .map(|statement| statement.restore())
        .collect()
}

#[test]
fn cte_fixture_multi_statement_rows_restore_each_statement() {
    assert_eq!(
        restores("create table t1 (a int); insert into t1 values(1), (2), (3);"),
        vec![
            "CREATE TABLE `t1` (`a` INT)",
            "INSERT INTO `t1` VALUES (1),(2),(3)",
        ]
    );
    assert_eq!(
        restores("drop table if exists t1; create table t1 (a int, b int);"),
        vec![
            "DROP TABLE IF EXISTS `t1`",
            "CREATE TABLE `t1` (`a` INT,`b` INT)",
        ]
    );
}

#[test]
fn session_and_expression_fixture_rows_restore_each_statement() {
    assert_eq!(
        restores("drop table if exists t1; create table t1(id int ); insert into t1 values (1);"),
        vec![
            "DROP TABLE IF EXISTS `t1`",
            "CREATE TABLE `t1` (`id` INT)",
            "INSERT INTO `t1` VALUES (1)",
        ]
    );
    assert_eq!(
        restores("set @a=3;set @b=20200414;set @c='a';set @d=20200414;set @e=3;set @f='a';"),
        vec![
            "SET @`a`=3",
            "SET @`b`=20200414",
            "SET @`c`=_UTF8MB4'a'",
            "SET @`d`=20200414",
            "SET @`e`=3",
            "SET @`f`=_UTF8MB4'a'",
        ]
    );
}

#[test]
fn session_and_plan_cache_fixture_rows_restore_each_statement() {
    assert_eq!(
        restores("set @@tidb_allow_mpp = 1; set @@tidb_enforce_mpp = 1;"),
        vec![
            "SET @@SESSION.`tidb_allow_mpp`=1",
            "SET @@SESSION.`tidb_enforce_mpp`=1",
        ]
    );
    assert_eq!(
        restores("update t set a=1 where a<10; update t set a=2 where a<12;"),
        vec![
            "UPDATE `t` SET `a`=1 WHERE `a`<10",
            "UPDATE `t` SET `a`=2 WHERE `a`<12",
        ]
    );
}

#[test]
fn empty_semicolon_is_an_empty_go_statement_slice() {
    assert!(restores(";").is_empty());
}

#[test]
fn executor_fixture_truncate_row_restores_each_statement() {
    assert_eq!(
        restores("truncate t1;truncate t2;truncate t3;truncate t4;"),
        vec![
            "TRUNCATE TABLE `t1`",
            "TRUNCATE TABLE `t2`",
            "TRUNCATE TABLE `t3`",
            "TRUNCATE TABLE `t4`",
        ]
    );
}

#[test]
fn generated_column_fixture_restores_all_insert_statements() {
    let sql = "insert into tbl1 (id) select null; insert into tbl1 (id) select null from tbl1; \
               insert into tbl1 (id) select null from tbl1; insert into tbl1 (id) select null from tbl1; \
               insert into tbl1 (id) select null from tbl1; insert into tbl1 (id) select null from tbl1; \
               insert into tbl1 (id) select null from tbl1; insert into tbl1 (id) select null from tbl1; \
               insert into tbl1 (id) select null from tbl1; insert into tbl1 (id) select null from tbl1; \
               insert into tbl1 (id) select null from tbl1; insert into tbl1 (id) select null from tbl1;";
    let expected = std::iter::once("INSERT INTO `tbl1` (`id`) SELECT NULL".to_owned())
        .chain(std::iter::repeat_n(
            "INSERT INTO `tbl1` (`id`) SELECT NULL FROM `tbl1`".to_owned(),
            11,
        ))
        .collect::<Vec<_>>();
    assert_eq!(restores(sql), expected);
}

/// The admission fast path's contract, against the parser itself: whenever
/// [`is_sole_statement`](crate::is_sole_statement) answers `true`, the
/// multi-statement parse must agree that the text holds exactly one
/// statement (or fail -- a failure the caller answers with the same
/// whole-text fallback). A `false` answer claims nothing and only costs the
/// caller the parse it would have run anyway.
#[test]
fn is_sole_statement_agrees_with_the_multi_statement_parse() {
    let single = [
        "SELECT 1",
        "SELECT 1;",
        "select c_discount from bmsql_customer where c_id = ?",
        "UPDATE t SET a = a + 1 WHERE b = 'x;y;z'",
        "INSERT INTO t VALUES ('a''b')",
        "INSERT INTO t VALUES (\"double;quoted\")",
        "SELECT `we;ird` FROM t",
        "/* leading comment */ SELECT 1",
        "SELECT 1 /* trailing ; comment */",
        "-- line comment with a semicolon;\nSELECT 1",
        "# hash comment with a semicolon;\nSELECT 1",
        // The `;` here sits INSIDE the hash comment and a newline is not a
        // delimiter, so the multi-parse fails ("expected ';' between
        // statements") -- which this tier answers with the text WHOLE, the
        // same answer the sole-statement fast path gives.
        "SELECT 1 # trailing;\nSELECT 2",
        "SELECT '--not-a-comment'",
        "BEGIN",
        "COMMIT",
    ];
    for sql in single {
        assert!(crate::is_sole_statement(sql), "{sql} is one statement");
        // One statement or a parse failure: either way the caller returns
        // the text whole.
        assert!(
            matches!(parse_multi_with_sql_mode(sql, SqlMode::default()), Ok(stmts) if stmts.len() == 1)
                || parse_multi_with_sql_mode(sql, SqlMode::default()).is_err(),
            "{sql} must not silently become zero statements"
        );
    }

    let multiple = [
        "",
        "   ",
        "/* only a comment */",
        ";",
        " ;; ",
        "SELECT 1; SELECT 2",
        "SELECT 'a;b'; SELECT 'c;d'",
        "SELECT 1;-- trailing\nSELECT 2",
    ];
    for sql in multiple {
        assert!(!crate::is_sole_statement(sql), "{sql} is not one statement");
    }

    // Conservative declines: escape sequences inside strings are
    // sql-mode dependent, so the scanner refuses to answer.
    for sql in [
        "SELECT 'a\\'; SELECT 2 -- b'",
        "INSERT INTO t VALUES ('\\'; ')",
    ] {
        assert!(!crate::is_sole_statement(sql), "{sql} must be declined");
    }
}
