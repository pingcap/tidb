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

//! Standalone physical/MERGE table options owned by Go's
//! `parseAlterTableOptions` -> `parseTableOption` transition.

use super::*;

#[test]
fn alter_table_generic_options_match_go_restore() {
    for (sql, expected) in [
        ("ALTER TABLE x UNION = (y)", "ALTER TABLE `x` UNION = (`y`)"),
        (
            "ALTER TABLE x INSERT_METHOD=LAST",
            "ALTER TABLE `x` INSERT_METHOD = LAST",
        ),
        (
            "ALTER TABLE t PRE_SPLIT_REGIONS=6",
            "ALTER TABLE `t` PRE_SPLIT_REGIONS = 6",
        ),
        (
            "ALTER TABLE t AUTO_INCREMENT=1 COMMENT='x'",
            "ALTER TABLE `t` AUTO_INCREMENT = 1 COMMENT = 'x'",
        ),
        (
            "ALTER TABLE t AUTO_ID_CACHE=1 ROW_FORMAT=dynamic",
            "ALTER TABLE `t` AUTO_ID_CACHE = 1 ROW_FORMAT = DYNAMIC",
        ),
        (
            "ALTER TABLE t SHARD_ROW_ID_BITS=4 COMMENT='x'",
            "ALTER TABLE `t` SHARD_ROW_ID_BITS = 4 COMMENT = 'x'",
        ),
        (
            "ALTER TABLE t TTL=c + INTERVAL 1 DAY TTL_ENABLE='on' TTL_JOB_INTERVAL='1h'",
            "ALTER TABLE `t` TTL = `c` + INTERVAL 1 DAY TTL_ENABLE = 'ON' TTL_JOB_INTERVAL = '1h'",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }

    let statement = parse("ALTER TABLE x INSERT_METHOD=LAST").unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::AlterTable(table) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        table.actions,
        vec![AlterTableAction::SetTableOptions {
            options: vec![TableOption::InsertMethod("LAST".to_owned())],
        }]
    );
}

#[test]
fn alter_table_generic_options_keep_source_boundaries() {
    for sql in [
        "ALTER TABLE x INSERT_METHOD",
        "ALTER TABLE x INSERT_METHOD =",
        "ALTER TABLE x PRE_SPLIT_REGIONS = -1",
        "ALTER TABLE x PRE_SPLIT_REGIONS = 1.5",
        "ALTER TABLE x UNION = y",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
