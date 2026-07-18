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

//! Direct Go `HandParser.parseAlterDrop` foreign-key rows.

use super::*;

#[test]
fn alter_drop_foreign_key_testddl_and_ast_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t DROP FOREIGN KEY a",
            "ALTER TABLE `t` DROP FOREIGN KEY `a`",
        ),
        (
            "ALTER TABLE d.t DROP FOREIGN KEY `fk``x`",
            "ALTER TABLE `d`.`t` DROP FOREIGN KEY `fk``x`",
        ),
        (
            "ALTER TABLE t DROP FOREIGN KEY fk, DROP FOREIGN KEY fk2",
            "ALTER TABLE `t` DROP FOREIGN KEY `fk`, DROP FOREIGN KEY `fk2`",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    let statement = parse("ALTER TABLE t DROP FOREIGN KEY fk_a").unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::AlterTable(table) = *ddl else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        table.actions,
        vec![tidb_ast::AlterTableAction::DropForeignKey(
            tidb_ast::DropForeignKey {
                name: "fk_a".to_owned(),
            }
        )]
    );
}

#[test]
fn alter_drop_foreign_key_keeps_go_parser_boundary() {
    for sql in [
        "ALTER TABLE t DROP FOREIGN KEY",
        "ALTER TABLE t DROP FOREIGN KEY IF EXISTS fk",
        "ALTER TABLE t DROP FOREIGN INDEX fk",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
