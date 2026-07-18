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

//! Direct `parseAlterTableOptions` shard-row-ID rows from Go `TestDDL` and AST tests.

use super::*;

#[test]
fn alter_shard_row_id_bits_testddl_and_ast_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t SHARD_ROW_ID_BITS 1",
            "ALTER TABLE `t` SHARD_ROW_ID_BITS = 1",
        ),
        (
            "ALTER TABLE t SHARD_ROW_ID_BITS = 1",
            "ALTER TABLE `t` SHARD_ROW_ID_BITS = 1",
        ),
        (
            "ALTER TABLE `db`.`t` SHARD_ROW_ID_BITS = 4",
            "ALTER TABLE `db`.`t` SHARD_ROW_ID_BITS = 4",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    let statement = parse("ALTER TABLE t SHARD_ROW_ID_BITS = 4").unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::AlterTable(table) = *ddl else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        table.actions,
        vec![AlterTableAction::SetTableOptions {
            options: vec![TableOption::ShardRowIdBits("4".to_owned())],
        }]
    );
}

#[test]
fn alter_shard_row_id_bits_keeps_go_integer_boundary() {
    for sql in [
        "ALTER TABLE t SHARD_ROW_ID_BITS",
        "ALTER TABLE t SHARD_ROW_ID_BITS =",
        "ALTER TABLE t SHARD_ROW_ID_BITS = -1",
        "ALTER TABLE t SHARD_ROW_ID_BITS = 1.5",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
