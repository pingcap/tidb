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

//! Direct Go-source coverage for interval `SPLIT MAXVALUE` partition bounds.

use super::*;

/// Go's `TestDDL` row at `pkg/parser/parser_test.go:8098` uses the same
/// `AlterTableReorganizeLastPartition` restore as the integration rows.
#[test]
fn alter_split_maxvalue_partition_restore_like_go() {
    for (sql, expected) in [
        (
            "ALTER TABLE t SPLIT MAXVALUE PARTITION LESS THAN (1000)",
            "ALTER TABLE `t` SPLIT MAXVALUE PARTITION LESS THAN (1000)",
        ),
        (
            "alter table ipt split maxvalue partition less than (140)",
            "ALTER TABLE `ipt` SPLIT MAXVALUE PARTITION LESS THAN (140)",
        ),
        (
            "alter table t2 split maxvalue partition less than (140)",
            "ALTER TABLE `t2` SPLIT MAXVALUE PARTITION LESS THAN (140)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_split_maxvalue_partition_retains_bound_expression() {
    let Stmt::Ddl(ddl) = parse("alter table ipt split maxvalue partition less than (140)")
        .expect("parse SPLIT MAXVALUE PARTITION")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::SplitMaxValuePartition {
                expr: tidb_ast::Expr::Int("140".to_owned()),
            }
        )]
    );
}
