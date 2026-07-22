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

//! Direct Go `TestDDL` row for `ALTER TABLE ... DROP PRIMARY KEY`.

use super::*;

#[test]
fn alter_drop_primary_key_testddl_row_restores_and_types_distinctly() {
    assert_eq!(
        r("ALTER TABLE t DROP PRIMARY KEY"),
        "ALTER TABLE `t` DROP PRIMARY KEY"
    );

    let statement = parse("ALTER TABLE t DROP PRIMARY KEY").expect("parse Go TestDDL row");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::AlterTable(table) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        table.actions,
        vec![tidb_ast::AlterTableAction::DropPrimaryKey(
            tidb_ast::DropPrimaryKey
        )]
    );
}

#[test]
fn alter_drop_primary_key_composes_with_the_existing_multi_action_loop() {
    assert_eq!(
        r("ALTER TABLE t DROP INDEX i3, DROP PRIMARY KEY"),
        "ALTER TABLE `t` DROP INDEX `i3`, DROP PRIMARY KEY"
    );
}

#[test]
fn alter_drop_primary_key_keeps_go_grammar_boundary() {
    for sql in [
        "ALTER TABLE t DROP PRIMARY",
        "ALTER TABLE t DROP PRIMARY INDEX",
        "ALTER TABLE t DROP PRIMARY KEY IF EXISTS",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
