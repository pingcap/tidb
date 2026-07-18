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

//! Direct CTAS rows from `pkg/parser/parser_test.go:TestDDL` plus the
//! result-source variants covered by the same Go parser production.
//!
//! The test deliberately inspects the attached `CreateTableAsQuery`: this
//! proves CTAS stays a single DDL AST payload, rather than being accepted by
//! parser glue and lowered into a synthetic INSERT.

use super::*;

#[test]
fn go_test_ddl_ctas_select_duplicate_and_parenthesized_rows() {
    // All seven rows below are contiguous CTAS rows from
    // `pkg/parser/parser_test.go:3031-3039` (current source checkout).
    for (sql, expected) in [
        (
            "create table a select * from b",
            "CREATE TABLE `a` AS SELECT * FROM `b`",
        ),
        (
            "create table a as select * from b",
            "CREATE TABLE `a` AS SELECT * FROM `b`",
        ),
        (
            "create table a (m int, n datetime) as select * from b",
            "CREATE TABLE `a` (`m` INT,`n` DATETIME) AS SELECT * FROM `b`",
        ),
        (
            "create table a (unique(n)) as select n from b",
            "CREATE TABLE `a` (UNIQUE(`n`)) AS SELECT `n` FROM `b`",
        ),
        (
            "create table a ignore as select n from b",
            "CREATE TABLE `a` IGNORE AS SELECT `n` FROM `b`",
        ),
        (
            "create table a replace as select n from b",
            "CREATE TABLE `a` REPLACE AS SELECT `n` FROM `b`",
        ),
        (
            "create table a (m int) replace as (select n as m from b union select n+1 as m from c group by 1 limit 2)",
            "CREATE TABLE `a` (`m` INT) REPLACE AS (SELECT `n` AS `m` FROM `b` UNION SELECT `n`+1 AS `m` FROM `c` GROUP BY 1 LIMIT 2)",
        ),
        (
            "create temporary table tmp as select 1",
            "CREATE TEMPORARY TABLE `tmp` AS SELECT 1",
        ),
        (
            "create global temporary table tmp as select 1 on commit delete rows",
            "CREATE GLOBAL TEMPORARY TABLE `tmp` AS SELECT 1 ON COMMIT DELETE ROWS",
        ),
    ] {
        let statement = parse(sql).expect("parse Go CTAS source row");
        assert_eq!(statement.restore(), expected, "source SQL: {sql}");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL statement");
        };
        let tidb_ast::DdlStmt::CreateTable(table) = *ddl else {
            panic!("expected CREATE TABLE");
        };
        assert!(table.ctas.is_some(), "source SQL: {sql}");
    }
}

#[test]
fn go_test_ddl_ctas_result_set_source_variants() {
    // `parseCreateTableStmt` dispatches these directly to the same
    // ResultSetNode field. `TABLE` rows are from parser_test.go:679-681;
    // `VALUES` rows are from parser_test.go:699-700. The WITH and wrapped
    // rows are source-probed against this checkout's Go parser because the
    // same production does not have one dedicated named test.
    for (sql, expected) in [
        (
            "create table ta table tb",
            "CREATE TABLE `ta` AS TABLE `tb`",
        ),
        (
            "create table ta (x int) table tb",
            "CREATE TABLE `ta` (`x` INT) AS TABLE `tb`",
        ),
        (
            "create table ta as values row(1)",
            "CREATE TABLE `ta` AS VALUES ROW(1)",
        ),
        (
            "create table ta as with q as (select 1) select * from q",
            "CREATE TABLE `ta` AS WITH `q` AS (SELECT 1) SELECT * FROM `q`",
        ),
        (
            "create table ta as (select 1 union select 2)",
            "CREATE TABLE `ta` AS (SELECT 1 UNION SELECT 2)",
        ),
        (
            "create table ta as (table tb)",
            "CREATE TABLE `ta` AS (TABLE `tb`)",
        ),
        (
            "create table ta as (values row(1))",
            "CREATE TABLE `ta` AS (VALUES ROW(1))",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn go_test_ddl_bare_create_and_dangling_ctas_markers_stay_parser_valid() {
    // Go accepts all three and Restore elides a duplicate policy/AS without a
    // populated Select field. They must not become `CREATE TABLE t ()`.
    for sql in [
        "create table a",
        "create table a as",
        "create table a ignore",
    ] {
        let statement = parse(sql).expect("source accepts bare CREATE form");
        assert_eq!(statement.restore(), "CREATE TABLE `a`", "source SQL: {sql}");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL statement");
        };
        let tidb_ast::DdlStmt::CreateTable(table) = *ddl else {
            panic!("expected CREATE TABLE");
        };
        assert!(table.ctas.is_none(), "source SQL: {sql}");
        assert!(table.columns.is_empty(), "source SQL: {sql}");
    }
}
