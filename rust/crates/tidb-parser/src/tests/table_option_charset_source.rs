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

//! Go `pkg/parser/ddl_table_option_parser.go:parseTableOption` charset
//! validation and canonicalization.

use super::*;

#[test]
fn table_charset_names_use_the_source_registry() {
    for sql in [
        "create table t(a int) character set uft8",
        "create table t(a int) character set gkb",
        "create table t(a int) character set laitn1",
        "create table t(a int) default charset=abcdefg",
        "create table t(a int) charset=utf7",
        "create table t(a int) charset=utf7mb4 collate=utf8_general_ci",
    ] {
        assert!(
            parse(sql).is_err(),
            "Go rejects invalid table charset: {sql}"
        );
    }
}

#[test]
fn table_charset_aliases_restore_canonically() {
    for (sql, expected) in [
        (
            "create table t(a int) character set utf8mb3",
            "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = UTF8",
        ),
        (
            "create table t(a int) charset binary",
            "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = BINARY",
        ),
        (
            "create table t(a int) default character set ascii",
            "CREATE TABLE `t` (`a` INT) DEFAULT CHARACTER SET = ASCII",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}
