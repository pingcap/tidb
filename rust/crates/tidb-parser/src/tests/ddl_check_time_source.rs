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

//! Direct source coverage for bare `LOCALTIME`/`LOCALTIMESTAMP` in CHECK
//! expressions.  Go routes both spellings through `parseCurrentFunc` and
//! restores them as nullary functions, even without source parentheses.

use super::*;

#[test]
fn create_table_check_time_keywords_restore_like_go_integration_rows() {
    for (sql, expected) in [
        (
            "CREATE TABLE t1 (f1 DATETIME CHECK (f1 + LOCALTIME > '23:11:21'))",
            "CREATE TABLE `t1` (`f1` DATETIME CHECK(`f1`+LOCALTIME()>_UTF8MB4'23:11:21') ENFORCED)",
        ),
        (
            "CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + LOCALTIMESTAMP > '2011-11-21 01:02:03'))",
            "CREATE TABLE `t1` (`f1` TIMESTAMP CHECK(`f1`+LOCALTIMESTAMP()>_UTF8MB4'2011-11-21 01:02:03') ENFORCED)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn bare_check_time_keywords_are_typed_nullary_functions() {
    let Stmt::Ddl(ddl) = parse("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + LOCALTIME > '23:11:21'))")
        .expect("bare LOCALTIME in CHECK parses")
    else {
        panic!("expected CREATE TABLE");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
        panic!("expected CREATE TABLE payload");
    };
    let ColumnOption::Check(check) = &table.columns[0].options[0] else {
        panic!("expected column CHECK option");
    };
    let Expr::Binary(_, left, _) = &check.expression else {
        panic!("expected binary CHECK expression");
    };
    let Expr::Binary(_, _, function) = left.as_ref() else {
        panic!("expected additive CHECK expression");
    };
    assert!(matches!(
        function.as_ref(),
        Expr::Func { name, args } if name == "LOCALTIME" && args.is_empty()
    ));
}

#[test]
fn bare_check_time_keywords_also_restore_in_alter_add_check() {
    assert_eq!(
        r("ALTER TABLE t1 ADD CHECK (f1 + LOCALTIMESTAMP > '2011-11-21 01:02:03')"),
        "ALTER TABLE `t1` ADD CHECK(`f1`+LOCALTIMESTAMP()>_UTF8MB4'2011-11-21 01:02:03') ENFORCED"
    );
}
