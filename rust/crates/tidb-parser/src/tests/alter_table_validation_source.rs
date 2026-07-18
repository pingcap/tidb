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

//! Direct Go-source coverage for standalone ALTER TABLE validation actions.

use super::*;

#[test]
fn alter_table_validation_actions_match_go_testddl() {
    for (sql, expected) in [
        (
            "ALTER TABLE t WITH VALIDATION",
            "ALTER TABLE `t` WITH VALIDATION",
        ),
        (
            "ALTER TABLE t WITHOUT VALIDATION",
            "ALTER TABLE `t` WITHOUT VALIDATION",
        ),
        (
            "ALTER TABLE t WITHOUT VALIDATION, WITH VALIDATION, ADD COLUMN b INT",
            "ALTER TABLE `t` WITHOUT VALIDATION, WITH VALIDATION, ADD COLUMN `b` INT",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_table_validation_actions_are_typed() {
    let Stmt::Ddl(ddl) = parse("ALTER TABLE t WITHOUT VALIDATION, WITH VALIDATION").unwrap() else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = *ddl else {
        panic!("expected ALTER TABLE payload");
    };
    assert_eq!(
        statement.actions,
        vec![
            AlterTableAction::WithoutValidation,
            AlterTableAction::WithValidation,
        ]
    );
}

#[test]
fn alter_table_validation_rejects_missing_keyword() {
    assert!(parse("ALTER TABLE t WITH").is_err());
    assert!(parse("ALTER TABLE t WITHOUT").is_err());
}
