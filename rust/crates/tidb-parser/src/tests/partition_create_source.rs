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

//! Directly transcribed creation-side rows from Go's `TestTablePartition`.
//! The negative rows intentionally prove parser-side structural validation;
//! physical DDL semantics remain an explicit executor capability boundary.

use super::*;

#[test]
fn create_table_partition_source_rows() {
    for (sql, expected) in [
        (
            "create table t1 (a int not null,b int not null,c int not null,primary key(a,b)) partition by range (a) partitions 3 (partition x1 values less than (5), partition x2 values less than (10), partition x3 values less than maxvalue)",
            "CREATE TABLE `t1` (`a` INT NOT NULL,`b` INT NOT NULL,`c` INT NOT NULL,PRIMARY KEY(`a`, `b`)) PARTITION BY RANGE (`a`) (PARTITION `x1` VALUES LESS THAN (5),PARTITION `x2` VALUES LESS THAN (10),PARTITION `x3` VALUES LESS THAN (MAXVALUE))",
        ),
        (
            "create table t (id int) partition by range (id) subpartition by key (id) subpartitions 2 (partition p0 values less than (42))",
            "CREATE TABLE `t` (`id` INT) PARTITION BY RANGE (`id`) SUBPARTITION BY KEY (`id`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (42))",
        ),
        (
            "create table t1 (a int) engine=innodb partition by linear hash (a) partitions 1",
            "CREATE TABLE `t1` (`a` INT) ENGINE = innodb PARTITION BY LINEAR HASH (`a`) PARTITIONS 1",
        ),
        (
            "create table t1 (a int) partition by key algorithm = 2 (a) partitions 2",
            "CREATE TABLE `t1` (`a` INT) PARTITION BY KEY ALGORITHM = 2 (`a`) PARTITIONS 2",
        ),
        (
            "create table t (a varchar(100), b int) partition by list columns (a) (partition p1 values in ('a','b','DEFAULT'), partition pdef values in (default))",
            "CREATE TABLE `t` (`a` VARCHAR(100),`b` INT) PARTITION BY LIST COLUMNS (`a`) (PARTITION `p1` VALUES IN (_UTF8MB4'a', _UTF8MB4'b', _UTF8MB4'DEFAULT'),PARTITION `pdef` DEFAULT)",
        ),
        (
            "create table t1 (a int) partition by system_time interval 7 day (partition x history, partition y current)",
            "CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME INTERVAL 7 DAY (PARTITION `x` HISTORY,PARTITION `y` CURRENT)",
        ),
        (
            "create table t1 (a int) partition by system_time limit 50000 (partition x history, partition y current)",
            "CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME LIMIT 50000 (PARTITION `x` HISTORY,PARTITION `y` CURRENT)",
        ),
        (
            "create table t (a int) partition by system_time limit 0 (partition h history, partition c current)",
            "CREATE TABLE `t` (`a` INT) PARTITION BY SYSTEM_TIME (PARTITION `h` HISTORY,PARTITION `c` CURRENT)",
        ),
        (
            "create table t1 (a int, b int) partition by range(a) subpartition by hash(b) (partition x values less than maxvalue (subpartition y,subpartition z))",
            "CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE (`a`) SUBPARTITION BY HASH (`b`) SUBPARTITIONS 2 (PARTITION `x` VALUES LESS THAN (MAXVALUE) (SUBPARTITION `y`,SUBPARTITION `z`))",
        ),
        (
            "create table t1 (a int) partition by hash(a) (partition x) update indexes (idx global, idx2 local)",
            "CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x`) UPDATE INDEXES (`idx` GLOBAL,`idx2` LOCAL)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn create_table_partition_source_rejections() {
    for sql in [
        "create table t (a int) partition by range (a)",
        "create table t (a int) partition by list (a) (partition p values less than (1))",
        "create table t (a int) partition by hash (a) (partition p values less than (1))",
        "create table t (a int) partition by system_time (partition p history)",
        "create table t (a int) partition by system_time interval 7 day limit 1 (partition p history, partition q current)",
        "create table t (a int) partition by range columns () (partition p values less than (1))",
        "create table t (a int) partition by hash (a) partitions 0",
        "create table t (a int) partition by range (a) subpartition by range (a) (partition p values less than (1))",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
