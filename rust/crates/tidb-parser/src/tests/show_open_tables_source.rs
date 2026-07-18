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

//! Direct Go `TestParseShowOpenTables` rows from `pkg/parser/parser_test.go`.

use super::*;

#[test]
fn show_open_tables_test_parser_rows_restore_like_go() {
    for (sql, expected) in [
        ("show open tables", "SHOW OPEN TABLES"),
        ("show open tables in test", "SHOW OPEN TABLES IN `test`"),
        ("show open tables from test", "SHOW OPEN TABLES IN `test`"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn show_open_tables_keeps_the_optional_schema_outside_show_tables() {
    let tidb_ast::Stmt::Admin(admin) =
        parse("show open tables from executor__show").expect("parse Go source form")
    else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::ShowOpenTables(show) = admin.as_ref() else {
        panic!("expected typed SHOW OPEN TABLES statement");
    };
    assert_eq!(show.database.as_deref(), Some("executor__show"));
}

#[test]
fn show_open_tables_requires_the_tables_noun() {
    assert!(parse("show open").is_err());
}
