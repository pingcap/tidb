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

//! Go-source coverage for the `ColumnOptionDefaultValue` parser/restore
//! contract.  `pkg/parser/ddl_table_parser.go:parseColumnOptions` owns the
//! alias normalization, while `pkg/parser/ast/ddl.go:ColumnOption.Restore`
//! owns the outer-parentheses rule.

use super::*;

/// Rows from `pkg/parser/parser_test.go:3010,3044-3049,3065-3070`, extended
/// with the same source AST restore rule for `NEXTVAL` and reused ALTER
/// column definitions.  Each expected output was verified against Go's
/// parser restore oracle.
#[test]
fn column_default_time_aliases_and_function_parentheses_match_go() {
    for (sql, expected) in [
        (
            "create table test (create_date timestamp not null comment 'created' default now())",
            "CREATE TABLE `test` (`create_date` TIMESTAMP NOT NULL COMMENT 'created' DEFAULT CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (a timestamp default (((now()))))",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (a timestamp default now())",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (a timestamp default now() on update now())",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())",
        ),
        (
            "CREATE TABLE IF NOT EXISTS `general_log` (`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` mediumtext NOT NULL,`thread_id` bigint(20) unsigned NOT NULL,`server_id` int(10) unsigned NOT NULL,`command_type` varchar(64) NOT NULL,`argument` mediumblob NOT NULL) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'",
            "CREATE TABLE IF NOT EXISTS `general_log` (`event_time` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` MEDIUMTEXT NOT NULL,`thread_id` BIGINT(20) UNSIGNED NOT NULL,`server_id` INT(10) UNSIGNED NOT NULL,`command_type` VARCHAR(64) NOT NULL,`argument` MEDIUMBLOB NOT NULL) ENGINE = CSV DEFAULT CHARACTER SET = UTF8 COMMENT = 'General log'",
        ),
        (
            "create table t (a timestamp default localtime)",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (a timestamp default localtime())",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (a timestamp default localtimestamp(3))",
            "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(3))",
        ),
        (
            "create table t (d date default current_date)",
            "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))",
        ),
        (
            "create table t (d date default current_date())",
            "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))",
        ),
        (
            "create table t (d date default (current_date()))",
            "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))",
        ),
        (
            "create table t (d date default (curdate()))",
            "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))",
        ),
        (
            "create table t (d date default curdate())",
            "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))",
        ),
        (
            "create table t (n bigint default nextval(seq))",
            "CREATE TABLE `t` (`n` BIGINT DEFAULT (NEXTVAL(`seq`)))",
        ),
        (
            "alter table t add column d date default curdate()",
            "ALTER TABLE `t` ADD COLUMN `d` DATE DEFAULT (CURRENT_DATE())",
        ),
        (
            "alter table t modify d timestamp default localtime(3)",
            "ALTER TABLE `t` MODIFY COLUMN `d` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(3)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    // These are direct Go parser rejection cases.  `NOW` is a
    // function default only with its required parentheses; an ON UPDATE
    // expression follows the same source rule.
    assert!(parse("create table t (a timestamp default now)").is_err());
    assert!(parse("create table t (a timestamp default now() on update now)").is_err());
    assert!(parse("create table t (a timestamp default now() on update (now()))").is_err());
}
