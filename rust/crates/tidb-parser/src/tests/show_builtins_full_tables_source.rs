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

//! Direct Go parser rows for the ordinary `SHOW BUILTINS` and
//! `SHOW FULL TABLES` leaves.

use super::*;

#[test]
fn show_builtins_restores_the_source_row() {
    assert_eq!(r("show builtins"), "SHOW BUILTINS");

    let statement = parse("show builtins").expect("parse SHOW BUILTINS");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    assert!(matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowBuiltins));
}

#[test]
fn show_builtins_has_no_filter_or_trailing_payload() {
    for sql in ["show builtins like 'x'", "show builtins where 1 = 1"] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_full_tables_restores_full_scope_and_filter() {
    for (sql, expected) in [
        (
            "show full tables like '%lmn'",
            "SHOW FULL TABLES LIKE _UTF8MB4'%lmn'",
        ),
        (
            "show full tables from demo like 't%'",
            "SHOW FULL TABLES IN `demo` LIKE _UTF8MB4't%'",
        ),
        (
            "show tables where Table_type = 'BASE TABLE'",
            "SHOW TABLES WHERE `Table_type`=_UTF8MB4'BASE TABLE'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn show_tables_keeps_full_and_filter_typed() {
    let statement = parse("show full tables from demo where Table_type = 'BASE TABLE'")
        .expect("parse full SHOW TABLES");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::ShowTables(show) = admin.as_ref() else {
        panic!("expected typed SHOW TABLES");
    };
    assert!(show.full);
    assert_eq!(show.database.as_deref(), Some("demo"));
    assert!(matches!(
        show.filter,
        Some(tidb_ast::ShowTablesFilter::Where(_))
    ));
}

#[test]
fn show_tables_rejects_incomplete_filters() {
    for sql in [
        "show full",
        "show full tables like",
        "show full tables where",
        "show tables from",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
