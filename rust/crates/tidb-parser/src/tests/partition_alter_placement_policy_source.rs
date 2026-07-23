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

//! Direct Go-source coverage for partition-owned ALTER placement policy.

use super::*;

/// Exact `TestDDL` rows at `pkg/parser/parser_test.go:2837-2852`. The
/// direct action and ADD PARTITION definition share the partition envelope,
/// but never use the root ALTER TABLE table-option action.
#[test]
fn partition_alter_placement_policy_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "alter table m partition t placement policy='ww'",
            "ALTER TABLE `m` PARTITION `t` PLACEMENT POLICY = `ww`",
        ),
        (
            "alter table m partition t /*T![placement] placement policy=\"ww\" */",
            "ALTER TABLE `m` PARTITION `t` PLACEMENT POLICY = `ww`",
        ),
        (
            "alter table m add partition (partition p1 values less than (200) placement policy='ww')",
            "ALTER TABLE `m` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (200) PLACEMENT POLICY = `ww`)",
        ),
        (
            "alter table m add partition (partition p1 values less than (200) /*T![placement] placement policy=\"ww\" */)",
            "ALTER TABLE `m` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (200) PLACEMENT POLICY = `ww`)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn partition_alter_placement_policy_retains_the_partition_target() {
    let Stmt::Ddl(ddl) = parse("alter table t partition p1 placement policy set default")
        .expect("parse direct partition placement row")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::SetOptions {
                partition: "p1".to_owned(),
                options: vec![tidb_ast::TableOption::PlacementPolicy("DEFAULT".to_owned())],
            }
        )]
    );
}

#[test]
fn partition_alter_options_share_the_complete_go_table_option_parser() {
    for (sql, expected) in [
        (
            "alter table t partition 'p' comment='x'",
            "ALTER TABLE `t` PARTITION `p` COMMENT = 'x'",
        ),
        (
            "alter table t partition @p engine=innodb",
            "ALTER TABLE `t` PARTITION `p` ENGINE = innodb",
        ),
        (
            "alter table t partition p placement policy='x' comment='y'",
            "ALTER TABLE `t` PARTITION `p` PLACEMENT POLICY = `x` COMMENT = 'y'",
        ),
        (
            "alter table t partition p affinity='table'",
            "ALTER TABLE `t` PARTITION `p` AFFINITY = 'table'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}
