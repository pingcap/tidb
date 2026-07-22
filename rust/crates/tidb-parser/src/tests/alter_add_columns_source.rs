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

//! Source-owned tests for grouped `ALTER TABLE ADD COLUMN` definitions.
//!
//! Go's `HandParser.parseAlterAdd` accepts the table-element list form after
//! both `ADD` and `ADD COLUMN`. The corresponding `AlterTableSpec.Restore`
//! branch keeps the parentheses even for a one-column list. The defaults in
//! these rows are intentionally invalid at execution time; parser parity
//! still requires retaining their string-literal AST and canonical restore.

use super::*;

#[test]
fn grouped_add_column_literal_defaults_match_go_source_rows() {
    for (sql, expected, type_name, value) in [
        (
            "alter table t1 add column(c tinyint default '11111111')",
            "ALTER TABLE `t1` ADD COLUMN (`c` TINYINT DEFAULT _UTF8MB4'11111111')",
            "TINYINT",
            "11111111",
        ),
        (
            "alter table t1 add column(c tinyint default '11abc')",
            "ALTER TABLE `t1` ADD COLUMN (`c` TINYINT DEFAULT _UTF8MB4'11abc')",
            "TINYINT",
            "11abc",
        ),
        (
            "alter table t1 add column(c datetime default '11abc')",
            "ALTER TABLE `t1` ADD COLUMN (`c` DATETIME DEFAULT _UTF8MB4'11abc')",
            "DATETIME",
            "11abc",
        ),
    ] {
        let statement = parse(sql).expect("Go accepts grouped ADD COLUMN source row");
        assert_eq!(statement.restore(), expected, "source SQL: {sql}");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected ALTER TABLE statement");
        };
        let tidb_ast::DdlStmt::AlterTable(table) = ddl.into_inner() else {
            panic!("expected ALTER TABLE statement");
        };
        let [tidb_ast::AlterTableAction::AddColumns {
            if_not_exists,
            columns,
            constraints,
        }] = table.actions.as_slice()
        else {
            panic!("expected one grouped ADD COLUMN action");
        };
        assert!(!if_not_exists);
        assert!(constraints.is_empty());
        let [column] = columns.as_slice() else {
            panic!("expected one grouped column");
        };
        assert_eq!(column.ty.name, type_name);
        assert!(matches!(
            column.options.as_slice(),
            [tidb_ast::ColumnOption::Default(tidb_ast::Expr::String(actual))] if actual == value
        ));
    }
}

#[test]
fn grouped_add_columns_preserve_multiple_column_order() {
    let statement =
        parse("alter table t add (a tinyint default '1', b datetime default '2024-10-24 12:20')")
            .expect("Go accepts ADD table-element list without COLUMN");
    assert_eq!(
        statement.restore(),
        "ALTER TABLE `t` ADD COLUMN (`a` TINYINT DEFAULT _UTF8MB4'1', `b` DATETIME DEFAULT _UTF8MB4'2024-10-24 12:20')"
    );
}

#[test]
fn grouped_add_column_constraints_match_go_source_rows() {
    for (sql, expected, expected_columns, expected_constraints) in [
        (
            "alter table t add column (index i(a), index i1(a))",
            "ALTER TABLE `t` ADD COLUMN (INDEX `i`(`a`), INDEX `i1`(`a`))",
            0,
            2,
        ),
        (
            "alter table t add column (b int default 2, index i(a), primary key (a))",
            "ALTER TABLE `t` ADD COLUMN (`b` INT DEFAULT 2, INDEX `i`(`a`), PRIMARY KEY(`a`))",
            1,
            2,
        ),
        (
            "alter table t add column if not exists (b int default 2, c int default 3)",
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`b` INT DEFAULT 2, `c` INT DEFAULT 3)",
            2,
            0,
        ),
    ] {
        let statement = parse(sql).expect("Go accepts grouped ADD COLUMN constraint row");
        assert_eq!(statement.restore(), expected, "source SQL: {sql}");
        let Stmt::Ddl(ddl) = statement else {
            panic!("expected ALTER TABLE statement");
        };
        let tidb_ast::DdlStmt::AlterTable(table) = ddl.into_inner() else {
            panic!("expected ALTER TABLE statement");
        };
        let [tidb_ast::AlterTableAction::AddColumns {
            columns,
            constraints,
            ..
        }] = table.actions.as_slice()
        else {
            panic!("expected one grouped ADD COLUMN action");
        };
        assert_eq!(columns.len(), expected_columns);
        assert_eq!(constraints.len(), expected_constraints);
    }
}
