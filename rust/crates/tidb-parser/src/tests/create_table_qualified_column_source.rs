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

//! Source-owned qualified column-name branch from TiDB's CREATE TABLE parser.

use super::*;

#[test]
fn create_table_qualified_column_names_restore_as_name_paths() {
    // `pkg/parser/ddl_table_parser.go:parseColumnDef` accepts a ColumnName
    // path even though execution later rejects the invalid qualified column
    // name. Go's ColumnName.Restore quotes every path segment independently.
    for (sql, expected, qualifier, name) in [
        (
            "create table t(xxx.t.a bigint)",
            "CREATE TABLE `t` (`xxx`.`t`.`a` BIGINT)",
            vec!["xxx", "t"],
            "a",
        ),
        (
            "create table t(ddl__db_integration.tttt.a bigint)",
            "CREATE TABLE `t` (`ddl__db_integration`.`tttt`.`a` BIGINT)",
            vec!["ddl__db_integration", "tttt"],
            "a",
        ),
        (
            "create table t(t.tttt.a bigint)",
            "CREATE TABLE `t` (`t`.`tttt`.`a` BIGINT)",
            vec!["t", "tttt"],
            "a",
        ),
        (
            "create table t2(c1.c2 blob default null)",
            "CREATE TABLE `t2` (`c1`.`c2` BLOB DEFAULT NULL)",
            vec!["c1"],
            "c2",
        ),
        (
            "create table t1(t1.a char)",
            "CREATE TABLE `t1` (`t1`.`a` CHAR)",
            vec!["t1"],
            "a",
        ),
        (
            "create table t2(a char, t2.b int)",
            "CREATE TABLE `t2` (`a` CHAR,`t2`.`b` INT)",
            vec!["t2"],
            "b",
        ),
        (
            "create table t3(s.a char)",
            "CREATE TABLE `t3` (`s`.`a` CHAR)",
            vec!["s"],
            "a",
        ),
    ] {
        let statement = parse(sql).expect("qualified CREATE TABLE column must parse");
        assert_eq!(statement.restore(), expected, "source SQL: {sql}");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected DDL statement")
        };
        let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
            panic!("expected CREATE TABLE")
        };
        let column = table.columns.last().expect("column definition");
        assert_eq!(column.qualifier, qualifier);
        assert_eq!(column.name, name);
    }
}
