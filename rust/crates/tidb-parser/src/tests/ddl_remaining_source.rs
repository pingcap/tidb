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

//! Remaining direct restore tables from `pkg/parser/ast/ddl_test.go`.

use super::*;
use tidb_ast::RestoreFlags;

#[test]
fn test_admin_repair_table_restore() {
    for (sql, expected) in [
        (
            "ADMIN REPAIR TABLE t CREATE TABLE t (a int)",
            "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` INT)",
        ),
        (
            "ADMIN REPAIR TABLE t CREATE TABLE t (a char(1), b int)",
            "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` CHAR(1),`b` INT)",
        ),
        (
            "ADMIN REPAIR TABLE t CREATE TABLE t (a TINYINT UNSIGNED)",
            "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` TINYINT UNSIGNED)",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_admin_optimize_table_restore() {
    for (sql, expected) in [
        ("OPTIMIZE TABLE t", "OPTIMIZE TABLE `t`"),
        (
            "OPTIMIZE LOCAL TABLE t",
            "OPTIMIZE NO_WRITE_TO_BINLOG TABLE `t`",
        ),
        (
            "OPTIMIZE NO_WRITE_TO_BINLOG TABLE t",
            "OPTIMIZE NO_WRITE_TO_BINLOG TABLE `t`",
        ),
        ("OPTIMIZE TABLE t1, t2", "OPTIMIZE TABLE `t1`, `t2`"),
        ("optimize table t1,t2", "OPTIMIZE TABLE `t1`, `t2`"),
        ("optimize tables t1, t2", "OPTIMIZE TABLE `t1`, `t2`"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_flash_back_database_restore() {
    for (sql, expected) in [
        ("flashback database M", "FLASHBACK DATABASE `M`"),
        ("flashback schema M", "FLASHBACK DATABASE `M`"),
        ("flashback database M to n", "FLASHBACK DATABASE `M` TO `n`"),
        ("flashback schema M to N", "FLASHBACK DATABASE `M` TO `N`"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_table_option_ttl_restore_with_ttl_enable_off_flag() {
    let default = RestoreFlags::DEFAULT | RestoreFlags::WITH_TTL_ENABLE_OFF;
    let special = default | RestoreFlags::TIDB_SPECIAL_COMMENT;
    for (sql, flags, expected) in [
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            default,
            "CREATE TABLE `t` (`created_at` DATETIME) TTL = `created_at` + INTERVAL 1 YEAR TTL_ENABLE = 'OFF'",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR",
            special,
            "CREATE TABLE `t` (`created_at` DATETIME) /*T![ttl] TTL = `created_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE = 'OFF' */",
        ),
        ("alter table t ttl_enable = 'ON'", default, "ALTER TABLE `t`"),
        ("alter table t ttl_enable = 'ON'", special, "ALTER TABLE `t`"),
        (
            "alter table t remove ttl",
            default,
            "ALTER TABLE `t` REMOVE TTL",
        ),
        (
            "alter table t remove ttl",
            special,
            "ALTER TABLE `t` /*T![ttl] REMOVE TTL */",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'",
            default,
            "CREATE TABLE `t` (`created_at` DATETIME) TTL = `created_at` + INTERVAL 1 YEAR TTL_ENABLE = 'OFF'",
        ),
        (
            "create table t (created_at datetime) ttl = created_at + INTERVAL 1 YEAR ttl_enable = 'ON'",
            special,
            "CREATE TABLE `t` (`created_at` DATETIME) /*T![ttl] TTL = `created_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE = 'OFF' */",
        ),
        (
            "alter table t ttl_enable = 'ON' placement policy p1",
            special,
            "ALTER TABLE `t` /*T![placement] PLACEMENT POLICY = `p1` */",
        ),
    ] {
        let restored = parse(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .restore_with_flags(flags);
        assert_eq!(restored, expected, "{sql}");
    }
}
