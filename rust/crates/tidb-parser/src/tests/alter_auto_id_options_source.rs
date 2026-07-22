// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct source coverage for ALTER TABLE AUTO_ID_CACHE/AUTO_RANDOM_BASE.

use super::*;

/// These rows mirror `pkg/parser/parser_test.go:3943-3946` and the
/// `parseTableOptionUint`/`parseForceAutoOption` branches in
/// `pkg/parser/ddl_table_option_parser.go`.
#[test]
fn alter_table_auto_id_options_restore_like_go() {
    for (sql, expected) in [
        (
            "alter table t auto_id_cache 10",
            "ALTER TABLE `t` AUTO_ID_CACHE = 10",
        ),
        (
            "alter table t auto_id_cache = 10",
            "ALTER TABLE `t` AUTO_ID_CACHE = 10",
        ),
        (
            "alter table t auto_random_base 50",
            "ALTER TABLE `t` AUTO_RANDOM_BASE = 50",
        ),
        (
            "alter table t auto_random_base = 50",
            "ALTER TABLE `t` AUTO_RANDOM_BASE = 50",
        ),
        (
            "alter table t force auto_random_base = 50",
            "ALTER TABLE `t` FORCE AUTO_RANDOM_BASE = 50",
        ),
        (
            "alter table t auto_increment 30, auto_random_base 40",
            "ALTER TABLE `t` AUTO_INCREMENT = 30, AUTO_RANDOM_BASE = 40",
        ),
        (
            "alter table t auto_increment 30, force auto_random_base 40",
            "ALTER TABLE `t` AUTO_INCREMENT = 30, FORCE AUTO_RANDOM_BASE = 40",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_table_auto_id_options_keep_integer_and_force_boundaries() {
    for sql in [
        "alter table t auto_id_cache",
        "alter table t auto_id_cache = 1.5",
        "alter table t auto_id_cache = '1'",
        "alter table t auto_random_base",
        "alter table t auto_random_base = 1.5",
        "alter table t auto_random_base = '1'",
        "alter table t force auto_id_cache = 10",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }

    let tidb_ast::Stmt::Ddl(ddl) =
        parse("alter table t force auto_random_base = 50").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::ForceAutoRandomBase("50".to_string())],
        }]
    );

    let tidb_ast::Stmt::Ddl(ddl) = parse("alter table t force auto_increment = 10").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::ForceAutoIncrement("10".to_string())],
        }]
    );
}
