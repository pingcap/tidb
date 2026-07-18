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

//! Direct Go-source coverage for column-level `CHECK` parsing.  The source
//! owner is `pkg/parser/ddl_table_parser.go:parseColumnOptions`; parser rows
//! are from `pkg/parser/parser_test.go:2857-2861, 3802-3807`, with the
//! integration rows transcribed by the differential selector shard.

use super::*;

fn first_column_options(sql: &str) -> Vec<ColumnOption> {
    let Stmt::Ddl(ddl) = parse(sql).expect("parse Go column CHECK source case") else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = *ddl else {
        panic!("expected CREATE TABLE");
    };
    table.columns[0].options.clone()
}

#[test]
fn column_check_source_rows_preserve_typed_payload_and_canonical_restore() {
    // Exact accepted CHECK rows from Go `TestDDL`.  Each one is a column
    // option rather than a table constraint; retaining that location matters
    // for option ordering and for later DDL ownership.
    for (sql, expected, name, enforced) in [
        (
            "CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30))",
            "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED,`First_Name` VARCHAR(30))",
            None,
            true,
        ),
        (
            "CREATE TABLE Customer (SD integer CHECK (SD > 0) not enforced, SS varchar(30) check(ss='test') enforced)",
            "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) NOT ENFORCED,`SS` VARCHAR(30) CHECK(`ss`=_UTF8MB4'test') ENFORCED)",
            None,
            false,
        ),
        (
            "create table t (a int constraint positive_a check(a > 0) enforced)",
            "CREATE TABLE `t` (`a` INT CONSTRAINT `positive_a` CHECK(`a`>0) ENFORCED)",
            Some("positive_a"),
            true,
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        let options = first_column_options(sql);
        assert!(matches!(
            options.as_slice(),
            [ColumnOption::Check(tidb_ast::CheckConstraintDefinition {
                name: actual_name,
                enforced: actual_enforced,
                ..
            }), ..]
                if actual_name.as_deref() == name && *actual_enforced == enforced
        ));
    }
}

#[test]
fn column_check_not_null_injects_a_separate_following_not_null_option() {
    // Go does not attach NOT NULL to `ColumnOptionCheck`.  Its parser
    // appends the completed check and a distinct ColumnOptionNotNull, so this
    // exact shape is observable through CREATE and ALTER column definitions.
    for (sql, expected) in [
        (
            "CREATE TABLE Customer (SD integer CHECK (SD > 0) not null, First_Name varchar(30) comment 'string' not null)",
            "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED NOT NULL,`First_Name` VARCHAR(30) COMMENT 'string' NOT NULL)",
        ),
        (
            "CREATE TABLE Customer (SD integer comment 'string' CHECK (SD > 0) not null)",
            "CREATE TABLE `Customer` (`SD` INT COMMENT 'string' CHECK(`SD`>0) ENFORCED NOT NULL)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        let options = first_column_options(sql);
        assert!(matches!(
            options.as_slice(),
            [.., ColumnOption::Check(_), ColumnOption::NotNull]
        ));
    }
}

#[test]
fn column_constraint_check_does_not_consume_enclosing_table_constraints() {
    // `CONSTRAINT` is shared syntax.  Go peeks through it in
    // `parseColumnOptions` and stops unless it is specifically a column
    // CHECK; otherwise the enclosing table grammar owns the token.
    let Stmt::Ddl(ddl) = parse(
        "create table t (a int constraint a_positive check(a > 0), constraint table_positive check(a < 10))",
    )
    .expect("parse mixed column/table checks")
    else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = *ddl else {
        panic!("expected CREATE TABLE");
    };
    assert!(matches!(
        table.columns[0].options.as_slice(),
        [ColumnOption::Check(tidb_ast::CheckConstraintDefinition {
            name: Some(name),
            ..
        })] if name == "a_positive"
    ));
    assert!(matches!(
        table.table_constraints.as_slice(),
        [TableConstraint::Check(tidb_ast::CheckConstraintDefinition {
            name: Some(name),
            ..
        })] if name == "table_positive"
    ));
}

#[test]
fn column_check_restore_uses_statement_qualifier_context() {
    // Column CHECK expressions use the same restore context as table checks
    // and generated/default expressions.  This is a context contract, not a
    // parser-specific spelling rule.
    let statement = parse("create table t (a int constraint c check(db.t.a > 0))")
        .expect("parse qualified column check");
    assert_eq!(
        statement.restore(),
        "CREATE TABLE `t` (`a` INT CONSTRAINT `c` CHECK(`db`.`t`.`a`>0) ENFORCED)"
    );
    assert_eq!(
        statement.restore_with_flags(
            tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
                | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME,
        ),
        "CREATE TABLE `t` (`a` INT CONSTRAINT `c` CHECK(`a`>0) ENFORCED)"
    );
}
