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

//! Direct Go-source coverage for `MERGE FIRST PARTITION LESS THAN`.

use super::*;

#[test]
fn alter_merge_first_partition_restore_like_go() {
    for (sql, expected) in [
        (
            "ALTER TABLE ipt MERGE FIRST PARTITION LESS THAN (60)",
            "ALTER TABLE `ipt` MERGE FIRST PARTITION LESS THAN (60)",
        ),
        (
            "ALTER TABLE t2 MERGE FIRST PARTITION LESS THAN (60)",
            "ALTER TABLE `t2` MERGE FIRST PARTITION LESS THAN (60)",
        ),
        (
            "ALTER TABLE t DROP FIRST PARTITION LESS THAN (10)",
            "ALTER TABLE `t` FIRST PARTITION LESS THAN (10)",
        ),
        (
            "ALTER TABLE t DROP FIRST PARTITION LESS THAN (10) IF EXISTS FIRST PARTITION LESS THAN (20)",
            "ALTER TABLE `t` MERGE FIRST PARTITION LESS THAN (20)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn alter_merge_first_partition_keeps_typed_go_payload() {
    let Stmt::Ddl(ddl) = parse("ALTER TABLE ipt MERGE FIRST PARTITION LESS THAN (60)")
        .expect("parse MERGE FIRST PARTITION LESS THAN")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::MergeFirstPartitionLessThan {
                expr: tidb_ast::NodeBox::new(tidb_ast::Expr::Int("60".to_owned())),
            }
        )]
    );
}
