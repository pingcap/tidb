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

//! Source-owned restore-context rows.
//!
//! Go's AST accepts one DDL tree and restores it either as ordinary SQL or
//! with TiDB-only fragments protected by `/*T![feature] ... */`.  Keep those
//! rows in their own leaf so adding a restore flag does not make a parser
//! coverage test silently exercise a different contract.

use super::*;
use tidb_ast::RestoreFlags;

fn restore_special(sql: &str) -> String {
    parse(sql)
        .unwrap_or_else(|error| panic!("Go accepts but Rust rejected {sql}: {error:?}"))
        .restore_with_flags(RestoreFlags::DEFAULT | RestoreFlags::TIDB_SPECIAL_COMMENT)
}

/// Exact restore-flag rows from Go
/// `pkg/parser/parser_test.go:TestWithoutCharsetFlags`.
#[test]
fn string_charset_omission_flags_match_go() {
    let base = RestoreFlags::STRING_SINGLE_QUOTES
        | RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | RestoreFlags::BRACKET_AROUND_BINARY_OPERATION
        | RestoreFlags::NAME_BACK_QUOTES;
    for (sql, flags, expected) in [
        (
            "select 'a'",
            base | RestoreFlags::STRING_WITHOUT_CHARSET,
            "SELECT 'a'",
        ),
        (
            "select _utf8'a'",
            base | RestoreFlags::STRING_WITHOUT_CHARSET,
            "SELECT 'a'",
        ),
        (
            "select _utf8mb4'a'",
            base | RestoreFlags::STRING_WITHOUT_CHARSET,
            "SELECT 'a'",
        ),
        (
            "select _utf8 X'D0B1'",
            base | RestoreFlags::STRING_WITHOUT_CHARSET,
            "SELECT x'd0b1'",
        ),
        (
            "select _utf8mb4'a'",
            base | RestoreFlags::STRING_WITHOUT_DEFAULT_CHARSET,
            "SELECT 'a'",
        ),
        (
            "select _utf8'a'",
            base | RestoreFlags::STRING_WITHOUT_DEFAULT_CHARSET,
            "SELECT _utf8'a'",
        ),
        (
            "select _utf8 X'D0B1'",
            base | RestoreFlags::STRING_WITHOUT_DEFAULT_CHARSET,
            "SELECT _utf8 x'd0b1'",
        ),
    ] {
        assert_eq!(
            parse(sql).unwrap().restore_with_flags(flags),
            expected,
            "source SQL: {sql}"
        );
    }
}

/// Exact rows from Go `pkg/parser/parser_test.go:TestRestoreBinOpWithBrackets`.
#[test]
fn binary_operation_bracket_restore_matches_go() {
    let flags = RestoreFlags::STRING_SINGLE_QUOTES
        | RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | RestoreFlags::BRACKET_AROUND_BINARY_OPERATION
        | RestoreFlags::STRING_WITHOUT_CHARSET
        | RestoreFlags::NAME_BACK_QUOTES;
    for (sql, expected) in [
        ("select mod(a+b, 4)+1", "SELECT (((`a` + `b`) % 4) + 1)"),
        (
            "SELECT MOD(10, 2 BETWEEN 0 and 5)",
            "SELECT (10 % (2 BETWEEN 0 AND 5))",
        ),
        (
            "select mod( year(a) - abs(weekday(a) + dayofweek(a)), 4) + 1",
            "SELECT (((year(`a`) - abs((weekday(`a`) + dayofweek(`a`)))) % 4) + 1)",
        ),
    ] {
        assert_eq!(
            parse(sql).unwrap().restore_with_flags(flags),
            expected,
            "source SQL: {sql}"
        );
    }
}

/// The AFFINITY branch in Go's `TableOption.Restore` uses
/// `WriteWithSpecialComments(tidb.FeatureIDAffinity, ...)` for both CREATE
/// and ALTER ownership paths.
#[test]
fn affinity_uses_the_go_special_comment_feature_id_in_create_and_alter() {
    for (sql, expected) in [
        (
            "create table t (a int) affinity 'table'",
            "CREATE TABLE `t` (`a` INT) /*T![affinity] AFFINITY = 'table' */",
        ),
        (
            "alter table t affinity 'partition'",
            "ALTER TABLE `t` /*T![affinity] AFFINITY = 'partition' */",
        ),
    ] {
        assert_eq!(restore_special(sql), expected, "source SQL: {sql}");
    }
}

/// All four special-comment rows from Go
/// `pkg/parser/ast/ddl_test.go:204 TestDDLConstraintRestore`.
#[test]
fn go_ast_test_ddl_constraint_restore_special_comments() {
    let cases = [
        (
            "CREATE TABLE child (id INT, parent_id INT, PRIMARY KEY (id) CLUSTERED)",
            "CREATE TABLE `child` (`id` INT,`parent_id` INT,PRIMARY KEY(`id`) /*T![clustered_index] CLUSTERED */)",
        ),
        (
            "CREATE TABLE child (id INT, parent_id INT, primary key (id) NONCLUSTERED)",
            "CREATE TABLE `child` (`id` INT,`parent_id` INT,PRIMARY KEY(`id`) /*T![clustered_index] NONCLUSTERED */)",
        ),
        (
            "CREATE TABLE child (id INT, parent_id INT, PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */)",
            "CREATE TABLE `child` (`id` INT,`parent_id` INT,PRIMARY KEY(`id`) /*T![clustered_index] CLUSTERED */)",
        ),
        (
            "CREATE TABLE child (id INT, parent_id INT, primary key (id) /*T![clustered_index] NONCLUSTERED */)",
            "CREATE TABLE `child` (`id` INT,`parent_id` INT,PRIMARY KEY(`id`) /*T![clustered_index] NONCLUSTERED */)",
        ),
    ];
    assert_eq!(cases.len(), 4);
    for (sql, expected) in cases {
        assert_eq!(restore_special(sql), expected, "source SQL: {sql}");
    }
}

/// All seven rows from Go
/// `pkg/parser/ast/ddl_test.go:997 TestPresplitIndexSpecialComments`.
#[test]
fn go_ast_test_presplit_index_special_comments() {
    let cases = [
        (
            "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = 4",
            "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
        ),
        (
            "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS 4",
            "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
        ),
        (
            "ALTER TABLE t ADD PRIMARY KEY (a) CLUSTERED PRE_SPLIT_REGIONS = 4",
            "ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */ /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
        ),
        (
            "ALTER TABLE t ADD PRIMARY KEY (a) PRE_SPLIT_REGIONS = 4 NONCLUSTERED",
            "ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] NONCLUSTERED */ /*T![pre_split] PRE_SPLIT_REGIONS = 4 */",
        ),
        (
            "ALTER TABLE t ADD INDEX (a) PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4)",
            "ALTER TABLE `t` ADD INDEX(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4) */",
        ),
        (
            "ALTER TABLE t ADD INDEX idx(a) pre_split_regions = 100, ADD INDEX idx2(b) pre_split_regions = (by(1),(2),(3))",
            "ALTER TABLE `t` ADD INDEX `idx`(`a`) /*T![pre_split] PRE_SPLIT_REGIONS = 100 */, ADD INDEX `idx2`(`b`) /*T![pre_split] PRE_SPLIT_REGIONS = (BY (1),(2),(3)) */",
        ),
        (
            "ALTER TABLE t ADD INDEX (a) comment 'a' PRE_SPLIT_REGIONS = (between (1, 'a') and (2, 'b') regions 4)",
            "ALTER TABLE `t` ADD INDEX(`a`) COMMENT 'a' /*T![pre_split] PRE_SPLIT_REGIONS = (BETWEEN (1,_UTF8MB4'a') AND (2,_UTF8MB4'b') REGIONS 4) */",
        ),
    ];
    assert_eq!(cases.len(), 7);
    for (sql, expected) in cases {
        assert_eq!(restore_special(sql), expected, "source SQL: {sql}");
    }
}

/// Index-owned special-comment rows from Go
/// `pkg/parser/ast/ddl_test.go:655 TestIfExistsRestore`.
#[test]
fn go_ast_test_if_exists_index_special_comments() {
    let cases = [
        (
            "drop index if exists idx on t",
            "DROP INDEX /*T! IF EXISTS  */`idx` ON `t`",
        ),
        (
            "create unique index if not exists idx on t(c)",
            "CREATE UNIQUE INDEX /*T! IF NOT EXISTS  */`idx` ON `t` (`c`)",
        ),
        (
            "alter table t add key if not exists idx2(c2), add vector index if not exists idx3(c3), add columnar index if not exists idx4(c4)",
            "ALTER TABLE `t` ADD INDEX/*T!  IF NOT EXISTS */ `idx2`(`c2`), ADD VECTOR INDEX/*T!  IF NOT EXISTS */ `idx3`(`c3`), ADD COLUMNAR INDEX/*T!  IF NOT EXISTS */ `idx4`(`c4`)",
        ),
        (
            "alter table t add foreign key if not exists fk(c) references t2(c)",
            "ALTER TABLE `t` ADD CONSTRAINT `fk` FOREIGN KEY /*T! IF NOT EXISTS  */(`c`) REFERENCES `t2`(`c`)",
        ),
        (
            "alter table t drop index if exists idx",
            "ALTER TABLE `t` DROP INDEX /*T! IF EXISTS  */`idx`",
        ),
    ];
    assert_eq!(cases.len(), 5);
    for (sql, expected) in cases {
        assert_eq!(restore_special(sql), expected, "source SQL: {sql}");
    }
}
