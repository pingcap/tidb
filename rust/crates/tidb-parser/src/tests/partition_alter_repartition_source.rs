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

//! Direct `PARTITION BY` re-partitioning rows from Go
//! `pkg/parser/parser_test.go:TestDDL` lines 3409-3429. The two accepted
//! mixed COMMENT/ENABLE KEYS row is covered in `tests::ddl`; these rows isolate
//! the repartition production's own payload contract.

use super::*;

#[test]
fn alter_table_repartition_source_rows_restore_and_are_terminal() {
    for (sql, expected) in [
        (
            "alter table t partition by hash(a)",
            "ALTER TABLE `t` PARTITION BY HASH (`a`) PARTITIONS 1",
        ),
        (
            "alter table t add column a int partition by hash(a)",
            "ALTER TABLE `t` ADD COLUMN `a` INT PARTITION BY HASH (`a`) PARTITIONS 1",
        ),
        (
            "alter table t add column a int partition by hash(a) update indexes (idx_a global)",
            "ALTER TABLE `t` ADD COLUMN `a` INT PARTITION BY HASH (`a`) PARTITIONS 1 UPDATE INDEXES (`idx_a` GLOBAL)",
        ),
        (
            "alter table t add column a int partition by hash(a) update indexes (idx_a global, idx_b local)",
            "ALTER TABLE `t` ADD COLUMN `a` INT PARTITION BY HASH (`a`) PARTITIONS 1 UPDATE INDEXES (`idx_a` GLOBAL,`idx_b` LOCAL)",
        ),
        (
            "alter table t partition by range(a) (partition x values less than (75))",
            "ALTER TABLE `t` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))",
        ),
        (
            "alter table t partition by range FIELDS(a) (partition x values less than maxvalue)",
            "ALTER TABLE `t` PARTITION BY RANGE COLUMNS (`a`) (PARTITION `x` VALUES LESS THAN (MAXVALUE))",
        ),
        (
            "alter table t partition by list FIELDS(a) (PARTITION p0 VALUES IN (5, 10, 15))",
            "ALTER TABLE `t` PARTITION BY LIST COLUMNS (`a`) (PARTITION `p0` VALUES IN (5, 10, 15))",
        ),
        (
            "alter table t partition by range FIELDS(a,b,c) (partition p1 values less than (1,1,1));",
            "ALTER TABLE `t` PARTITION BY RANGE COLUMNS (`a`,`b`,`c`) (PARTITION `p1` VALUES LESS THAN (1, 1, 1))",
        ),
        (
            "alter table t partition by list FIELDS(a,b,c) (PARTITION p0 VALUES IN ((5, 10, 15)))",
            "ALTER TABLE `t` PARTITION BY LIST COLUMNS (`a`,`b`,`c`) (PARTITION `p0` VALUES IN ((5, 10, 15)))",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    let Stmt::Ddl(ddl) =
        parse("alter table t partition by hash(a)").expect("parse direct Go re-partition row")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(
        alter.actions.as_slice(),
        [tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::Repartition(partitioning)
        )] if partitioning.method.kind == tidb_ast::PartitionType::Hash
            && matches!(partitioning.method.expr, Some(tidb_ast::Expr::Column(ref name)) if name.as_slice() == ["a"])
    ));

    for sql in [
        "alter table t add column a int partition by hash(a) update indexes (idx_a normal)",
        "alter table t add column a int partition by hash(a) update indexes (global)",
        "alter table t partition by range(a)",
        "alter table t partition by range(a) update indexes (a local)",
        "alter table t add column a int, partition by range(a) (partition x values less than (75))",
        "alter table t enable keys, comment = 'cmt', partition by hash(a)",
        "alter table t partition by hash(a) enable keys",
        "alter table t partition by hash(a), enable keys",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
