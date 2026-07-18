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

//! Exact `ALTER TABLE ... PARTITION ... ATTRIBUTES` rows from Go
//! `pkg/parser/parser_test.go:TestDDL` lines 3463-3471.

use super::*;

#[test]
fn alter_partition_attributes_match_go_parser_restore_and_typed_payload() {
    for (sql, expected) in [
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES='str'",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str'",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES='str1,str2'",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES=\"str1,str2\"",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES 'str1,str2'",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES \"str1,str2\"",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES=DEFAULT",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES=default",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT",
        ),
        (
            "ALTER TABLE t PARTITION p ATTRIBUTES=DeFaUlT",
            "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    let Stmt::Ddl(ddl) = parse("ALTER TABLE t PARTITION p ATTRIBUTES='str'")
        .expect("parse direct Go partition attribute row")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = *ddl else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::SetAttributes {
                partition: "p".to_owned(),
                attributes: Some("str".to_owned()),
            }
        )]
    );

    assert!(parse("ALTER TABLE t PARTITION p ATTRIBUTES").is_err());
}
