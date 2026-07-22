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

//! Direct inline-column key rows from Go `HandParser.parseColumnOptions`.

use super::*;
use tidb_ast::{InlineKeyKind, RestoreFlags};

fn first_inline_option(sql: &str) -> ColumnOption {
    let stmt = parse(sql).unwrap_or_else(|error| panic!("Go accepts {sql}: {error:?}"));
    let tidb_ast::Stmt::Ddl(ddl) = stmt else {
        panic!("expected DDL for {sql}");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
        panic!("expected CREATE TABLE for {sql}");
    };
    // Keep the AST assertion local to source rows while the shared test
    // helper remains restore-oriented.
    table.columns[0].options[0].clone()
}

/// All inline rows in Go `pkg/parser/parser_test.go:TestDDL`, plus bare
/// `KEY` from the same source branch.  `LOCAL` must restore exactly like an
/// omitted suffix; only `GLOBAL` is AST-visible.
#[test]
fn go_parser_test_ddl_inline_key_global_local_rows() {
    for (sql, expected, option) in [
        (
            "create table t (a int key global)",
            "CREATE TABLE `t` (`a` INT PRIMARY KEY GLOBAL)",
            InlineKeyOption::primary(None, true),
        ),
        (
            "create table t (a int key local)",
            "CREATE TABLE `t` (`a` INT PRIMARY KEY)",
            InlineKeyOption::primary(None, false),
        ),
        (
            "create table t (a int primary key local)",
            "CREATE TABLE `t` (`a` INT PRIMARY KEY)",
            InlineKeyOption::primary(None, false),
        ),
        (
            "create table t (a int primary key global)",
            "CREATE TABLE `t` (`a` INT PRIMARY KEY GLOBAL)",
            InlineKeyOption::primary(None, true),
        ),
        (
            "create table t (a int unique local)",
            "CREATE TABLE `t` (`a` INT UNIQUE KEY)",
            InlineKeyOption::unique(false),
        ),
        (
            "create table t (a int unique global)",
            "CREATE TABLE `t` (`a` INT UNIQUE KEY GLOBAL)",
            InlineKeyOption::unique(true),
        ),
        (
            "create table t (a int unique key local)",
            "CREATE TABLE `t` (`a` INT UNIQUE KEY)",
            InlineKeyOption::unique(false),
        ),
        (
            "create table t (a int unique key global)",
            "CREATE TABLE `t` (`a` INT UNIQUE KEY GLOBAL)",
            InlineKeyOption::unique(true),
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        assert_eq!(
            first_inline_option(sql),
            ColumnOption::InlineKey(option),
            "source SQL: {sql}"
        );
    }
}

/// Go parses storage before the optional global/local suffix. Preserve both
/// facts in one payload and use the existing Go clustered-index special
/// comment feature for storage only; inline GLOBAL itself remains ordinary
/// SQL in Go `ast.ColumnOption.Restore`.
#[test]
fn inline_primary_storage_and_global_restore_with_go_special_comment_rules() {
    let sql = "create table t (a int primary key nonclustered global)";
    let stmt = parse(sql).unwrap();
    assert_eq!(
        stmt.restore(),
        "CREATE TABLE `t` (`a` INT PRIMARY KEY NONCLUSTERED GLOBAL)"
    );
    assert_eq!(
        stmt.restore_with_flags(RestoreFlags::DEFAULT | RestoreFlags::TIDB_SPECIAL_COMMENT),
        "CREATE TABLE `t` (`a` INT PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ GLOBAL)"
    );
    assert!(matches!(
        &first_inline_option(sql),
        ColumnOption::InlineKey(InlineKeyOption {
            kind: InlineKeyKind::Primary {
                storage: Some(PrimaryKeyStorage::NonClustered),
            },
            global: true,
        })
    ));
}
