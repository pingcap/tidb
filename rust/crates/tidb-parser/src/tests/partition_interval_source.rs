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

//! Direct Go-source coverage for `TestIntervalPartition`.

use super::*;

#[test]
fn interval_partition_source_rows_restore_like_go() {
    for (sql, expected) in [
        (
            "CREATE TABLE t (c1 integer,c2 integer) PARTITION BY RANGE (c1) INTERVAL (1000)",
            "CREATE TABLE `t` (`c1` INT,`c2` INT) PARTITION BY RANGE (`c1`) INTERVAL (1000)",
        ),
        (
            "CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE (c2) INTERVAL (1 Month)",
            "CREATE TABLE `t` (`c1` INT,`c2` DATE) PARTITION BY RANGE (`c2`) INTERVAL (1 MONTH)",
        ),
        (
            "CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE (c1) (partition p1 values less than (22))",
            "CREATE TABLE `t` (`c1` INT,`c2` DATE) PARTITION BY RANGE (`c1`) (PARTITION `p1` VALUES LESS THAN (22))",
        ),
        (
            "CREATE TABLE t (c1 int, c2 datetime) PARTITION BY RANGE COLUMNS (c2) INTERVAL (1 day) first partition less than (\"2022-01-02\") last partition less than (\"2022-06-01\") NULL PARTITION MAXVALUE PARTITION",
            "CREATE TABLE `t` (`c1` INT,`c2` DATETIME) PARTITION BY RANGE COLUMNS (`c2`) INTERVAL (1 DAY) FIRST PARTITION LESS THAN (_UTF8MB4'2022-01-02') LAST PARTITION LESS THAN (_UTF8MB4'2022-06-01') NULL PARTITION MAXVALUE PARTITION",
        ),
        (
            "ALTER TABLE t LAST PARTITION LESS THAN (1000)",
            "ALTER TABLE `t` LAST PARTITION LESS THAN (1000)",
        ),
        (
            "ALTER TABLE t split MAXVALUE PARTITION LESS THAN (1000)",
            "ALTER TABLE `t` SPLIT MAXVALUE PARTITION LESS THAN (1000)",
        ),
        (
            "ALTER TABLE t merge first PARTITION LESS THAN (1000)",
            "ALTER TABLE `t` MERGE FIRST PARTITION LESS THAN (1000)",
        ),
        (
            "ALTER TABLE t first PARTITION LESS THAN (1000)",
            "ALTER TABLE `t` FIRST PARTITION LESS THAN (1000)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn interval_partition_source_rows_reject_like_go() {
    for sql in [
        "CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE COLUMNS (c2) INTERVAL (1 year) first partition less than (\"2022-02-01\")",
        "ALTER TABLE t REORGANIZE MAX PARTITION INTO NEW LAST PARTITION LESS THAN (1000)",
        "ALTER TABLE t REORGANIZE MAX PARTITION INTO LAST PARTITION LESS THAN (1000)",
        "ALTER TABLE t REORGANIZE MAXVALUE PARTITION INTO NEW LAST PARTITION LESS THAN (1000)",
        "ALTER TABLE t REORGANIZE MAXVALUE PARTITION INTO LAST PARTITION LESS THAN (1000)",
    ] {
        assert!(parse(sql).is_err(), "source SQL should reject: {sql}");
    }
}

#[test]
fn interval_partition_preserves_syntactic_sugar_source_metadata() {
    let sql = "CREATE TABLE t (c1 int, c2 datetime) PARTITION BY RANGE COLUMNS (c2) INTERVAL (1 day) first partition less than (\"2022-01-02\") last partition less than (\"2022-06-01\") NULL PARTITION MAXVALUE PARTITION";
    let Stmt::Ddl(ddl) = parse(sql).expect("interval partition parses") else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.as_ref() else {
        panic!("expected CREATE TABLE");
    };
    let interval = table
        .partitioning
        .as_ref()
        .and_then(|partitioning| partitioning.method.interval.as_ref())
        .expect("interval metadata");
    let start = sql.find("INTERVAL").expect("INTERVAL offset");
    assert_eq!(interval.origin_text_position(), start);
    assert_eq!(interval.original_text(), &sql.as_bytes()[start..]);
}
