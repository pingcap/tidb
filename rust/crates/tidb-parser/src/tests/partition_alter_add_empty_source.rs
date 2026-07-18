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

//! Direct Go-source coverage for an empty `ADD PARTITION` payload.

use super::*;

/// Go's `parseAlterAdd` accepts `ADD PARTITION` without a count or definition.
/// The zero-valued `Num`/`PartDefinitions` pair restores to the bare action.
#[test]
fn alter_add_partition_empty_payload_matches_go_testddl() {
    for (sql, expected) in [
        (
            "ALTER TABLE employees ADD PARTITION",
            "ALTER TABLE `employees` ADD PARTITION",
        ),
        (
            "ALTER TABLE employees ADD PARTITION NO_WRITE_TO_BINLOG",
            "ALTER TABLE `employees` ADD PARTITION NO_WRITE_TO_BINLOG",
        ),
        (
            "ALTER TABLE employees ADD PARTITION IF NOT EXISTS",
            "ALTER TABLE `employees` ADD PARTITION IF NOT EXISTS",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_add_partition_empty_payload_is_typed_as_zero_count() {
    let Stmt::Ddl(ddl) =
        parse("alter table table_MustBeDefined add partition").expect("parse integration row")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = *ddl else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::Add {
                if_not_exists: false,
                no_write_to_binlog: false,
                spec: tidb_ast::AddPartitionSpec::Count(0),
            }
        )]
    );
}
