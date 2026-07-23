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

//! Direct source rows from Go's `pkg/parser/parser_test.go:TestSplitPartition`.

use super::*;

#[test]
fn create_table_split_source_selector_from_go_test_split_partition() {
    for (sql, expected) in [
        (
            "create table t (id BIGINT, user_id BIGINT, action_type VARCHAR(20), PRIMARY KEY (id), INDEX idx_user_id (user_id)) SPLIT PRIMARY KEY BETWEEN (0) AND (1000000) REGIONS 4 SPLIT INDEX idx_user_id BETWEEN (1000) AND (100000) REGIONS 3",
            "CREATE TABLE `t` (`id` BIGINT,`user_id` BIGINT,`action_type` VARCHAR(20),PRIMARY KEY(`id`),INDEX `idx_user_id`(`user_id`)) SPLIT PRIMARY KEY BETWEEN (0) AND (1000000) REGIONS 4 SPLIT INDEX `idx_user_id` BETWEEN (1000) AND (100000) REGIONS 3",
        ),
        (
            "create table t (id BIGINT) SPLIT BETWEEN (0) AND (1000000) REGIONS 4",
            "CREATE TABLE `t` (`id` BIGINT) SPLIT BETWEEN (0) AND (1000000) REGIONS 4",
        ),
        (
            "create global temporary table t (id int) on commit delete rows split region table by (1)",
            "CREATE GLOBAL TEMPORARY TABLE `t` (`id` INT) SPLIT BY (1) ON COMMIT DELETE ROWS",
        ),
        (
            "create table t (id int) split index @idx by (1)",
            "CREATE TABLE `t` (`id` INT) SPLIT INDEX `idx` BY (1)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn create_table_split_partition_source_rejections() {
    for sql in [
        "create table t (id int) split region primary (1)",
        "create table t (id int) split index 'idx' by (1)",
        "create table t (id int) split by ()",
        "create table t (id int) split between () and () regions 0",
        "create table t (id int) split by 1",
        "create table t (id int) split between 1 and 2 regions 3",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
