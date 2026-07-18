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

//! Source-owned SELECT-node restore rows from `pkg/parser/ast/dml_test.go`.
//! These tests keep LIMIT and select-field ownership in one leaf so changes to
//! AST restore formatting remain isolated from unrelated DML grammar tests.

use super::*;

fn field_list(sql: &str) -> String {
    let statement = parse(&format!("SELECT {sql}")).expect("parse select field list");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected query statement");
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("expected plain SELECT statement");
    };
    select.restore_field_list()
}

/// Go source: `TestLimitRestore` (`dml_test.go:134`).
#[test]
fn limit_restore_source_rows() {
    for (sql, expected) in [
        ("limit 10", "LIMIT 10"),
        ("limit 10,20", "LIMIT 10,20"),
        ("limit 20 offset 10", "LIMIT 10,20"),
    ] {
        let statement = format!("SELECT 1 {sql}");
        assert_eq!(r(&statement), format!("SELECT 1 {expected}"), "{statement}");
    }
}

/// Go source: `TestWildCardFieldRestore` (`dml_test.go:146`).
#[test]
fn wildcard_field_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
    ] {
        let statement = format!("SELECT {sql}");
        assert_eq!(r(&statement), format!("SELECT {expected}"), "{statement}");
    }
}

/// Go source: `TestSelectFieldRestore` (`dml_test.go:158`).
#[test]
fn select_field_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
        ("col as a", "`col` AS `a`"),
        ("col + 1 a", "`col`+1 AS `a`"),
    ] {
        let statement = format!("SELECT {sql}");
        assert_eq!(r(&statement), format!("SELECT {expected}"), "{statement}");
    }
}

/// Go source: `TestFieldListRestore` (`dml_test.go:172`).
#[test]
fn field_list_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
        ("col as a", "`col` AS `a`"),
        ("`t`.*, s.col as a", "`t`.*, `s`.`col` AS `a`"),
    ] {
        assert_eq!(field_list(sql), expected, "field list {sql}");
    }
}
