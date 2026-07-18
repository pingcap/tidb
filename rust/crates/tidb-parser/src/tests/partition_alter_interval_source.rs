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
    let tidb_ast::DdlStmt::AlterTable(alter) = *ddl else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::LastPartitionLessThan {
                expr: tidb_ast::Expr::Int("100".to_owned()),
                no_write_to_binlog: false,
            }
        )]
    );

    let Stmt::Ddl(ddl) = parse("ALTER TABLE ipt FIRST PARTITION LESS THAN (30)")
        .expect("parse FIRST PARTITION LESS THAN")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = *ddl else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::FirstPartitionLessThan {
                expr: tidb_ast::Expr::Int("30".to_owned()),
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
