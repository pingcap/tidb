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

//! Direct Go-source coverage for interval-partition ALTER bounds.

use super::*;

#[test]
fn alter_interval_partition_bounds_restore_like_go() {
    for (sql, expected) in [
        (
            "ALTER TABLE ipt LAST PARTITION LESS THAN (100)",
            "ALTER TABLE `ipt` LAST PARTITION LESS THAN (100)",
        ),
        (
            "ALTER TABLE ipt FIRST PARTITION LESS THAN (30)",
            "ALTER TABLE `ipt` FIRST PARTITION LESS THAN (30)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_interval_partition_bounds_keep_typed_go_payload() {
    let Stmt::Ddl(ddl) = parse("ALTER TABLE ipt LAST PARTITION LESS THAN (100)")
        .expect("parse LAST PARTITION LESS THAN")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::LastPartitionLessThan {
                expr: tidb_ast::NodeBox::new(tidb_ast::Expr::Int("100".to_owned())),
                no_write_to_binlog: false,
            }
        )]
    );

    let Stmt::Ddl(ddl) = parse("ALTER TABLE ipt FIRST PARTITION LESS THAN (30)")
        .expect("parse FIRST PARTITION LESS THAN")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::FirstPartitionLessThan {
                expr: tidb_ast::NodeBox::new(tidb_ast::Expr::Int("30".to_owned())),
                if_exists: false,
            }
        )]
    );
}

#[test]
fn alter_interval_partition_bounds_preserve_source_options() {
    assert_eq!(
        r("ALTER TABLE ipt FIRST PARTITION LESS THAN (30) IF EXISTS"),
        "ALTER TABLE `ipt` FIRST PARTITION LESS THAN (30)"
    );
    assert_eq!(
        r("ALTER TABLE ipt LAST PARTITION LESS THAN (100) NO_WRITE_TO_BINLOG"),
        "ALTER TABLE `ipt` LAST PARTITION LESS THAN (100) NO_WRITE_TO_BINLOG"
    );
}

#[test]
fn alter_interval_partition_bounds_preserve_go_rewrite_source() {
    for (sql, expected) in [
        (
            "ALTER TABLE ipt FIRST PARTITION LESS THAN (30)",
            "FIRST PARTITION LESS THAN (30)",
        ),
        (
            "ALTER TABLE ipt LAST PARTITION LESS THAN (100)",
            "LAST PARTITION LESS THAN (100)",
        ),
        (
            "ALTER TABLE ipt MERGE FIRST PARTITION LESS THAN (60)",
            "MERGE FIRST PARTITION LESS THAN (60)",
        ),
        (
            "ALTER TABLE ipt SPLIT MAXVALUE PARTITION LESS THAN (140)",
            "PARTITION LESS THAN (140)",
        ),
    ] {
        let Stmt::Ddl(ddl) = parse(sql).expect("parse interval ALTER") else {
            panic!("expected DDL");
        };
        let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
            panic!("expected ALTER TABLE");
        };
        let [tidb_ast::AlterTableAction::Partition(action)] = alter.actions.as_slice() else {
            panic!("expected one partition action");
        };
        let bound = match action {
            tidb_ast::AlterPartitionAction::FirstPartitionLessThan { expr, .. }
            | tidb_ast::AlterPartitionAction::LastPartitionLessThan { expr, .. }
            | tidb_ast::AlterPartitionAction::MergeFirstPartitionLessThan { expr }
            | tidb_ast::AlterPartitionAction::SplitMaxValuePartition { expr } => expr,
            _ => panic!("expected interval bound"),
        };
        assert_eq!(bound.original_text(), expected.as_bytes());
        assert_eq!(bound.origin_text_position(), sql.find(expected).unwrap());
    }
}
