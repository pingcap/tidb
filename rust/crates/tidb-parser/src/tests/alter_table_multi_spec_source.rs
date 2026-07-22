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

//! Direct source rows for ordered `ALTER TABLE` multi-spec composition.
//!
//! Go's `parseAlterAdd` and `parseAlterDrop` retain IF NOT EXISTS / IF EXISTS
//! on each individual `AlterTableSpec`, while `AlterTableStmt.Restore` owns
//! the comma-space separators between specs. These rows exercise that seam in
//! source order, including the long mixed statement at
//! `tests/integrationtest/t/ddl/multi_schema_change.test:305`.

use super::*;

#[test]
fn multi_spec_column_actions_preserve_go_metadata_and_order() {
    for (sql, expected) in [
        (
            "alter table t add column b int default 2, add column if not exists a int",
            "ALTER TABLE `t` ADD COLUMN `b` INT DEFAULT 2, ADD COLUMN IF NOT EXISTS `a` INT",
        ),
        (
            "alter table t drop column if exists c, drop column a",
            "ALTER TABLE `t` DROP COLUMN IF EXISTS `c`, DROP COLUMN `a`",
        ),
        (
            "alter table t drop column a, drop column if exists d, drop column c",
            "ALTER TABLE `t` DROP COLUMN `a`, DROP COLUMN IF EXISTS `d`, DROP COLUMN `c`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn multi_spec_long_mixed_statement_preserves_each_action() {
    let statement = parse(
        "alter table t add column d int default 4, add index i3(c), drop column a, \
         drop column if exists z, add column if not exists e int default 5, drop index i2, \
         add column f int default 6, drop column b, drop index i1, add column if not exists c int",
    )
    .expect("Go accepts the mixed multi-spec source row");
    assert_eq!(
        statement.restore(),
        "ALTER TABLE `t` ADD COLUMN `d` INT DEFAULT 4, ADD INDEX `i3`(`c`), DROP COLUMN `a`, DROP COLUMN IF EXISTS `z`, ADD COLUMN IF NOT EXISTS `e` INT DEFAULT 5, DROP INDEX `i2`, ADD COLUMN `f` INT DEFAULT 6, DROP COLUMN `b`, DROP INDEX `i1`, ADD COLUMN IF NOT EXISTS `c` INT"
    );

    let Stmt::Ddl(ddl) = statement else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(table) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    let actions = table.actions.as_slice();
    assert_eq!(actions.len(), 10);
    assert!(matches!(
        actions,
        [
            tidb_ast::AlterTableAction::AddColumn {
                if_not_exists: false,
                column,
                ..
            },
            tidb_ast::AlterTableAction::AddIndexConstraint(_),
            tidb_ast::AlterTableAction::DropColumn {
                if_exists: false,
                name
            },
            tidb_ast::AlterTableAction::DropColumn {
                if_exists: true,
                name: z
            },
            tidb_ast::AlterTableAction::AddColumn {
                if_not_exists: true,
                column: e,
                ..
            },
            tidb_ast::AlterTableAction::DropIndex { name: i2, .. },
            tidb_ast::AlterTableAction::AddColumn {
                if_not_exists: false,
                column: f,
                ..
            },
            tidb_ast::AlterTableAction::DropColumn {
                if_exists: false,
                name: b
            },
            tidb_ast::AlterTableAction::DropIndex { name: i1, .. },
            tidb_ast::AlterTableAction::AddColumn {
                if_not_exists: true,
                column: c,
                ..
            }
        ] if column.name == "d"
            && name == "a"
            && z == "z"
            && e.name == "e"
            && i2 == "i2"
            && f.name == "f"
            && b == "b"
            && i1 == "i1"
            && c.name == "c"
    ));
}

#[test]
fn single_column_if_exists_metadata_is_not_widened() {
    assert_eq!(
        r("alter table t add column if not exists c int"),
        "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `c` INT"
    );
    assert_eq!(
        r("alter table t drop if exists c"),
        "ALTER TABLE `t` DROP COLUMN IF EXISTS `c`"
    );
}
