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

//! Direct source coverage for `ALTER TABLE ... AUTO_INCREMENT`.

use super::*;

/// `TestDDL` rows at `pkg/parser/parser_test.go:3944,3946` prove the
/// AUTO_INCREMENT prefix accepts optional `=` and restores it canonically.
/// Their following AUTO_RANDOM_BASE action belongs to a separate, explicitly
/// unported leaf, so this test keeps the source-owned prefix independent.
#[test]
fn alter_table_auto_increment_testddl_prefix_restores_like_go() {
    for (sql, expected) in [
        (
            "alter table t auto_increment 30",
            "ALTER TABLE `t` AUTO_INCREMENT = 30",
        ),
        (
            "alter table t auto_increment = 30",
            "ALTER TABLE `t` AUTO_INCREMENT = 30",
        ),
        (
            "alter table t auto_increment = 110, auto_increment = 90",
            "ALTER TABLE `t` AUTO_INCREMENT = 110, AUTO_INCREMENT = 90",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// Go `parseTableOptionUint` accepts `intLit` only; it neither accepts a
/// missing payload nor widens to decimal/string literals.
#[test]
fn alter_table_auto_increment_keeps_the_integer_payload_boundary() {
    for sql in [
        "alter table t auto_increment",
        "alter table t auto_increment = 1.5",
        "alter table t auto_increment = '1'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }

    let tidb_ast::Stmt::Ddl(ddl) = parse("alter table t auto_increment = 30").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::AutoIncrement("30".to_string())],
        }]
    );
}
