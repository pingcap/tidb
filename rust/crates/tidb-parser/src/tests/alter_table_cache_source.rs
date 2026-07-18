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

//! Direct Go-source coverage for table-level ALTER TABLE CACHE/NOCACHE.

use super::*;

/// Exact TestDDL rows at pkg/parser/parser_test.go:2620-2621.
#[test]
fn alter_table_cache_testddl_rows_match_go_restore() {
    for (sql, expected) in [
        ("ALTER TABLE tmp CACHE", "ALTER TABLE \x60tmp\x60 CACHE"),
        ("ALTER TABLE tmp NOCACHE", "ALTER TABLE \x60tmp\x60 NOCACHE"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// These are dedicated Go alter specifications, not SELECT SQL_CACHE or
/// sequence CACHE options, so their typed action retains the narrow owner.
#[test]
fn alter_table_cache_uses_the_dedicated_typed_action() {
    let Stmt::Ddl(ddl) = parse("alter table t nocache").expect("parse") else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = ddl.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::Cache(
            tidb_ast::AlterTableCacheMode::NoCache
        )]
    );
}
