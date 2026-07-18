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

//! Direct source coverage for root `ALTER TABLE ... ENGINE_ATTRIBUTE`.

use super::*;

/// Go's `parseTableOption` owns ENGINE_ATTRIBUTE for ALTER TABLE through the
/// same StringName production used by CREATE TABLE: `=` is optional and the
/// payload may be either a string literal or an identifier-like word.
#[test]
fn alter_engine_attribute_restores_go_rows() {
    for (sql, expected) in [
        (
            "alter table t ENGINE_ATTRIBUTE = '{\"key\": \"value\"}'",
            "ALTER TABLE `t` ENGINE_ATTRIBUTE = '{\"key\": \"value\"}'",
        ),
        (
            "alter table t engine_attribute '{\"key\":\"value\"}'",
            "ALTER TABLE `t` ENGINE_ATTRIBUTE = '{\"key\":\"value\"}'",
        ),
        (
            "alter table t engine_attribute = engine_value",
            "ALTER TABLE `t` ENGINE_ATTRIBUTE = 'engine_value'",
        ),
        (
            "alter table t engine_attribute = 'first' engine_attribute = second",
            "ALTER TABLE `t` ENGINE_ATTRIBUTE = 'first' ENGINE_ATTRIBUTE = 'second'",
        ),
        (
            "alter table t engine_attribute = 'first', engine_attribute = 'second'",
            "ALTER TABLE `t` ENGINE_ATTRIBUTE = 'first', ENGINE_ATTRIBUTE = 'second'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// The source grammar rejects a missing ENGINE_ATTRIBUTE payload and does not
/// widen StringName to numeric or expression values.
#[test]
fn alter_engine_attribute_keeps_string_name_boundary() {
    for sql in [
        "alter table t engine_attribute",
        "alter table t engine_attribute =",
        "alter table t engine_attribute = 1",
        "alter table t engine_attribute = (select 1)",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }

    let tidb_ast::Stmt::Ddl(ddl) = parse("alter table t engine_attribute = attr").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::EngineAttribute("attr".to_string())],
        }]
    );
}
