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

//! Direct Go-source coverage for `DISCARD PARTITION ... TABLESPACE`.

use super::*;

/// These rows are the `TestDDL` cases at `pkg/parser/parser_test.go:6683-6688`.
#[test]
fn alter_discard_partition_tablespace_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t1 DISCARD PARTITION p0 TABLESPACE",
            "ALTER TABLE `t1` DISCARD PARTITION `p0` TABLESPACE",
        ),
        (
            "ALTER TABLE t1 DISCARD PARTITION p0, p1 TABLESPACE",
            "ALTER TABLE `t1` DISCARD PARTITION `p0`,`p1` TABLESPACE",
        ),
        (
            "ALTER TABLE t1 DISCARD PARTITION ALL TABLESPACE",
            "ALTER TABLE `t1` DISCARD PARTITION ALL TABLESPACE",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    for sql in [
        "ALTER TABLE t1 DISCARD PARTITION ALL, p0 TABLESPACE",
        "ALTER TABLE t1 DISCARD PARTITION p0, ALL TABLESPACE",
    ] {
        assert!(parse(sql).is_err(), "Go rejects mixed ALL/name list: {sql}");
    }
}

#[test]
fn alter_discard_partition_tablespace_retains_partition_names() {
    let Stmt::Ddl(ddl) = parse("alter table test_1465 discard partition p1 tablespace")
        .expect("parse integration row")
    else {
        panic!("expected DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(
        alter.actions,
        vec![tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::DiscardTablespace {
                all: false,
                names: vec!["p1".to_owned()],
            }
        )]
    );
}
