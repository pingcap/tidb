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

//! Direct Go-source coverage for table-level ALTER placement policy.

use super::*;

/// Exact `TestDDL` table-level rows at `pkg/parser/parser_test.go:2765-2766`.
/// Partition placement and CREATE TABLE policy syntax have distinct Go AST
/// owners and do not enter this leaf.
#[test]
fn alter_table_placement_policy_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "alter table t placement policy='ww'",
            "ALTER TABLE `t` PLACEMENT POLICY = `ww`",
        ),
        (
            "alter table t /*T![placement] placement policy=\"ww\" */",
            "ALTER TABLE `t` PLACEMENT POLICY = `ww`",
        ),
        (
            "alter table t placement policy set default",
            "ALTER TABLE `t` PLACEMENT POLICY = `DEFAULT`",
        ),
        (
            "alter table t placement policy default",
            "ALTER TABLE `t` PLACEMENT POLICY = `DEFAULT`",
        ),
        (
            "alter table t placement fourreplicas",
            "ALTER TABLE `t` PLACEMENT POLICY = `fourreplicas`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// Go's `StringName` accepts string literals and identifier-like policy names
/// only; numeric payloads and missing/default-without-SET tails are rejected.
#[test]
fn alter_table_placement_policy_keeps_the_go_payload_boundary() {
    for sql in [
        "alter table t placement policy",
        "alter table t placement policy = 1",
        "alter table t placement policy set",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }

    let Stmt::Ddl(ddl) = parse("alter table t placement policy = 'policy_1'").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::PlacementPolicy(
                "policy_1".to_owned(),
            )],
        }]
    );
}
