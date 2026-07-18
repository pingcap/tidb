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

//! Shared generated-column paths from `pkg/parser/ddl_table_parser.go`.
//!
//! Go's `HandParser.parseColumnDef` is the single envelope used by CREATE and
//! every column-bearing ALTER action. These rows make that ownership visible:
//! no ALTER action may acquire a separate generated-column or validation path.

use super::*;

#[test]
fn generated_column_body_and_validation_are_shared_by_create_and_alter() {
    // `pkg/parser/parser_test.go:TestGeneratedColumn` owns the CREATE rows;
    // its ALTER counterparts are in the WITH/WITHOUT VALIDATION rows around
    // the same file's generated-column section. All four routes end at the
    // same `parse_column_def` call before entering `column::generated`.
    for (sql, expected) in [
        (
            "CREATE TABLE t (a INT, b INT AS (a + 1))",
            "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`+1) VIRTUAL)",
        ),
        (
            "ALTER TABLE t ADD COLUMN b INT AS (a + 1)",
            "ALTER TABLE `t` ADD COLUMN `b` INT GENERATED ALWAYS AS(`a`+1) VIRTUAL",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN b INT AS (a + 1) STORED",
            "ALTER TABLE `t` MODIFY COLUMN `b` INT GENERATED ALWAYS AS(`a`+1) STORED",
        ),
        (
            "ALTER TABLE t CHANGE COLUMN b c INT AS (a + 1)",
            "ALTER TABLE `t` CHANGE COLUMN `b` `c` INT GENERATED ALWAYS AS(`a`+1) VIRTUAL",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    // Go validates only after the whole shared option list is parsed. A
    // DEFAULT tail must therefore fail identically from each column-def
    // caller, rather than being accepted or rejected by an ALTER-only fork.
    for sql in [
        "CREATE TABLE t (a INT, b INT AS (a + 1) DEFAULT 0)",
        "ALTER TABLE t ADD COLUMN b INT AS (a + 1) DEFAULT 0",
        "ALTER TABLE t MODIFY COLUMN b INT AS (a + 1) DEFAULT 0",
        "ALTER TABLE t CHANGE COLUMN b c INT AS (a + 1) DEFAULT 0",
    ] {
        let error = parse(sql).expect_err("Go rejects DEFAULT on generated columns");
        assert_eq!(
            error.message, "Incorrect usage of DEFAULT and generated column",
            "source SQL: {sql}"
        );
    }
}

#[test]
fn test_ddl_generated_column_rows_execute_without_a_create_only_exception() {
    // Exact `pkg/parser/parser_test.go:TestDDL` rows 3604-3612. This is
    // deliberately separate from the broader cross-route contract above so
    // the source ledger can point at literal upstream inputs.
    for sql in [
        "create table t (a timestamp, b timestamp as (a) not null on update current_timestamp);",
        "create table t (a bigint, b bigint as (a) primary key auto_increment);",
        "create table t (a bigint, b bigint as (a) not null default 10);",
        "alter table t add column (f timestamp as (a+1) default '2019-01-01 11:11:11');",
        "alter table t modify column f int as (a+1) default 55;",
    ] {
        assert!(parse(sql).is_err(), "source SQL: {sql}");
    }
    for (sql, expected) in [
        (
            "create table t (a bigint, b bigint as (a+1) not null);",
            "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL)",
        ),
        (
            "create table t (a bigint, b bigint as (a+1) not null comment 'ttt');",
            "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL COMMENT 'ttt')",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}
