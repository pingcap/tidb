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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct transcription of the ordinary generated-column portion of
//! `pkg/parser/ddl_table_parser.go`, `pkg/parser/parser_test.go`'s
//! `TestGeneratedColumn`, and `pkg/parser/ast/ddl_test.go`'s generated
//! restore cases. MariaDB period markers, AUTO_RANDOM, SERIAL, and table
//! options deliberately belong to later source-owned slices.

use super::*;

fn generated_option(sql: &str) -> tidb_ast::ColumnOption {
    let Stmt::Ddl(ddl) = parse(sql).expect("parse Go generated-column source case") else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
        panic!("expected CREATE TABLE");
    };
    table.columns[1]
        .options
        .iter()
        .find(|option| matches!(option, tidb_ast::ColumnOption::Generated { .. }))
        .cloned()
        .expect("generated option")
}

#[test]
fn ordinary_generated_column_source_cases_keep_one_typed_option() {
    // These three inputs are `pkg/parser/parser_test.go:TestGeneratedColumn`.
    // The expected SQL includes Go AST canonicalization, not input spelling.
    for (sql, expected, stored, expression_text) in [
        (
            "create table t (c int, d int generated always as (c + 1) virtual)",
            "CREATE TABLE `t` (`c` INT,`d` INT GENERATED ALWAYS AS(`c`+1) VIRTUAL)",
            false,
            b"c + 1".as_slice(),
        ),
        (
            "create table t (c int, d int as (   c + 1   ) virtual)",
            "CREATE TABLE `t` (`c` INT,`d` INT GENERATED ALWAYS AS(`c`+1) VIRTUAL)",
            false,
            b"c + 1".as_slice(),
        ),
        (
            "create table t (c int, d int as (1 + 1) stored)",
            "CREATE TABLE `t` (`c` INT,`d` INT GENERATED ALWAYS AS(1+1) STORED)",
            true,
            b"1 + 1".as_slice(),
        ),
    ] {
        let statement = parse(sql).expect("parse Go generated-column source case");
        assert_eq!(statement.restore(), expected, "source SQL: {sql}");
        let option = generated_option(sql);
        let tidb_ast::ColumnOption::Generated {
            stored: actual,
            expression_text: actual_text,
            ..
        } = option
        else {
            panic!("expected generated option");
        };
        assert_eq!(actual, stored, "source SQL: {sql}");
        assert_eq!(actual_text, expression_text, "source SQL: {sql}");
    }
}

#[test]
fn generated_restore_defaults_virtual_and_preserves_following_options() {
    // Direct `pkg/parser/ast/ddl_test.go:TestGeneratedRestore` / `TestDDL`
    // cases. The normal form always emits VIRTUAL when source omitted it.
    for (sql, expected) in [
        (
            "create table child (id int generated always as(id + 1))",
            "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(`id`+1) VIRTUAL)",
        ),
        (
            "create table child (id int generated always as(id + 1) virtual)",
            "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(`id`+1) VIRTUAL)",
        ),
        (
            "create table child (id int generated always as(id + 1) stored)",
            "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(`id`+1) STORED)",
        ),
        (
            "create table child (id int generated always as(lower(id)) stored)",
            "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(LOWER(`id`)) STORED)",
        ),
        (
            "create table t (a bigint, b bigint as (a+1) not null comment 'ttt')",
            "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL COMMENT 'ttt')",
        ),
        // `pkg/parser/ast/ddl_test.go:TestColumnOptionRestore`: Go's DDL
        // parser normalizes NOW-family aliases before AST restore.
        (
            "create table t (updated_at timestamp on update now())",
            "CREATE TABLE `t` (`updated_at` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP())",
        ),
        (
            "create table t (updated_at timestamp on update localtimestamp(3))",
            "CREATE TABLE `t` (`updated_at` TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(3))",
        ),
        (
            "create table t (updated_on date on update curdate())",
            "CREATE TABLE `t` (`updated_on` DATE ON UPDATE CURRENT_DATE())",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn float_precision_normalization_is_shared_by_ordinary_and_generated_columns() {
    // Direct rows from `pkg/parser/ast/ddl_test.go:TestDDLColumnDefRestore`.
    // Go normalizes these while parsing FieldType, before every DDL consumer
    // (including generated columns) restores the typed AST.
    for (sql, expected) in [
        (
            "create table t (id float(0))",
            "CREATE TABLE `t` (`id` FLOAT)",
        ),
        (
            "create table t (id float(24))",
            "CREATE TABLE `t` (`id` FLOAT)",
        ),
        (
            "create table t (id float(25))",
            "CREATE TABLE `t` (`id` DOUBLE)",
        ),
        (
            "create table t (id float(53))",
            "CREATE TABLE `t` (`id` DOUBLE)",
        ),
        (
            "create table t (id float(7,0))",
            "CREATE TABLE `t` (`id` FLOAT(7,0))",
        ),
        (
            "create table t (id float(25,0))",
            "CREATE TABLE `t` (`id` FLOAT(25,0))",
        ),
        // Exact source rows from
        // `tests/integrationtest/t/explain_generate_column_substitute.test`.
        // These prove generated columns consume the same normalized AST
        // type, without giving that grammar a separate restoration rule.
        (
            "create table t0(c0 float(24), c1 double as (c0) unique)",
            "CREATE TABLE `t0` (`c0` FLOAT,`c1` DOUBLE GENERATED ALWAYS AS(`c0`) VIRTUAL UNIQUE KEY)",
        ),
        (
            "create table t0(c0 float(25), c1 double as (c0) unique)",
            "CREATE TABLE `t0` (`c0` DOUBLE,`c1` DOUBLE GENERATED ALWAYS AS(`c0`) VIRTUAL UNIQUE KEY)",
        ),
        (
            "create table t0(c0 float(24), c1 float as (c0) unique)",
            "CREATE TABLE `t0` (`c0` FLOAT,`c1` FLOAT GENERATED ALWAYS AS(`c0`) VIRTUAL UNIQUE KEY)",
        ),
        (
            "create table t0(c0 float(25), c1 float as (c0) unique)",
            "CREATE TABLE `t0` (`c0` DOUBLE,`c1` FLOAT GENERATED ALWAYS AS(`c0`) VIRTUAL UNIQUE KEY)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    // This is an AST normalization, not a restore-time text substitution.
    // Inspect both sides of the IEEE threshold directly so future generated
    // column work cannot accidentally recreate a context-specific exception.
    for (precision, expected_name) in [(24, "FLOAT"), (25, "DOUBLE")] {
        let Stmt::Ddl(ddl) = parse(&format!("create table t (c float({precision}))"))
            .expect("parse Go FLOAT precision source case")
        else {
            panic!("expected DDL statement");
        };
        let tidb_ast::DdlStmt::CreateTable(table) = ddl.into_inner() else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(table.columns[0].ty.name, expected_name);
        assert!(table.columns[0].ty.args.is_empty());
    }
}

/// `tests/integrationtest/t/ddl/db_integration.test:1067-1073`'s
/// `TestStrictDoubleTypeCheck` runs with TiDB's default strict-double parser
/// setting.  The one-argument DOUBLE form is rejected in that mode, while
/// `(M,D)` remains a valid field type and FLOAT's precision normalization is
/// unrelated.
#[test]
fn strict_double_type_check_rejects_single_argument_double() {
    for sql in [
        "create table double_type_check(id int, c double(10))",
        "alter table double_type_check add column c double(10)",
    ] {
        assert!(parse(sql).is_err(), "Go strict parser rejects: {sql}");
    }
    assert_eq!(
        r("create table double_type_check(id int, c double(10, 2))"),
        "CREATE TABLE `double_type_check` (`id` INT,`c` DOUBLE(10,2))"
    );
}

/// Exact rows from `tests/integrationtest/t/types/const.test:151-177`'s
/// `TestIgnoreSpaceMode`.  The scanner promotes a builtin function name to a
/// keyword only when `(` is immediately adjacent; Go's `isIdentLike` then
/// rejects that token in the CREATE TABLE name slot.  A separating space,
/// backticks, or qualification keeps the same spelling usable as a name.
#[test]
fn builtin_function_names_follow_create_table_ignore_space_boundary() {
    for (sql, expected) in [
        (
            "create table COUNT (a bigint)",
            "CREATE TABLE `COUNT` (`a` BIGINT)",
        ),
        (
            "create table `COUNT`(a bigint)",
            "CREATE TABLE `COUNT` (`a` BIGINT)",
        ),
        (
            "create table types__const.COUNT(a bigint)",
            "CREATE TABLE `types__const`.`COUNT` (`a` BIGINT)",
        ),
        (
            "create table BIT_AND (a bigint)",
            "CREATE TABLE `BIT_AND` (`a` BIGINT)",
        ),
        (
            "create table NOW (a bigint)",
            "CREATE TABLE `NOW` (`a` BIGINT)",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    for sql in [
        "create table COUNT(a bigint)",
        "create table BIT_AND(a bigint)",
        "create table NOW(a bigint)",
    ] {
        assert!(parse(sql).is_err(), "Go rejects builtin table name: {sql}");
    }
}

#[test]
fn generated_restore_uses_general_column_qualifier_context_flags() {
    // The final `pkg/parser/ast/ddl_test.go:TestGeneratedRestore` case is
    // intentionally run with Go's RestoreWithoutSchemaName and
    // RestoreWithoutTableName flags. It proves a restore-context contract,
    // not a generated-column parser rewrite: default restore keeps the
    // qualifier and the flagged path elides it recursively inside LOWER.
    let statement =
        parse("create table child (id int generated always as(lower(child.id)) stored)")
            .expect("parse generated column with qualified expression");
    assert_eq!(
        statement.restore(),
        "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(LOWER(`child`.`id`)) STORED)"
    );
    assert_eq!(
        statement.restore_with_flags(
            tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME
                | tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME,
        ),
        "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(LOWER(`id`)) STORED)"
    );

    // Exercise the independent flag combinations on all three components;
    // this keeps the context mechanism reusable by other AST leaves rather
    // than baking a one-off `child.id` transformation into generated columns.
    let statement =
        parse("create table child (id int generated always as(lower(db.child.id)) stored)")
            .expect("parse schema-qualified generated column expression");
    assert_eq!(
        statement.restore_with_flags(tidb_ast::RestoreFlags::WITHOUT_SCHEMA_NAME),
        "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(LOWER(`child`.`id`)) STORED)"
    );
    assert_eq!(
        statement.restore_with_flags(tidb_ast::RestoreFlags::WITHOUT_TABLE_NAME),
        "CREATE TABLE `child` (`id` INT GENERATED ALWAYS AS(LOWER(`db`.`id`)) STORED)"
    );
}

#[test]
fn generated_columns_reject_the_same_incompatible_options_as_go() {
    // Go `ColumnDef.Validate` rejects these after parsing the full option
    // list. Keep the source diagnostics observable even before parser errors
    // carry TiDB's full numeric error identity.
    for (sql, illegal) in [
        (
            "create table t1 (a int, b int as (a + 1) default 10)",
            "DEFAULT",
        ),
        (
            "create table t1 (a int, b int as (a + 1) auto_increment)",
            "AUTO_INCREMENT",
        ),
        (
            "create table t1 (a int, b int as (a + 1) on update now())",
            "ON UPDATE",
        ),
    ] {
        let error = parse(sql).expect_err("Go rejects incompatible generated option");
        assert_eq!(
            error.message,
            format!("Incorrect usage of {illegal} and generated column"),
            "source SQL: {sql}"
        );
    }
}

#[test]
fn create_table_unmodeled_tails_are_rejected_instead_of_erased() {
    // `parseCreateTableStmt` stops its Go table-option loop at the first
    // non-option, then dispatches the remaining CREATE TABLE grammar. Rust
    // does not model these later tails yet, so they must reach the top-level
    // completion check rather than being consumed token-by-token and lost
    // from restore. Creation-side PARTITION BY now has a typed owner; these
    // The CTAS and duplicate-policy tails now have one source-shaped owner
    // (`ctas_source`); this remaining row guards the still-unmodelled
    // STORAGE tail.
    let sql = "create table t (a int) storage disk engine=innodb";
    let error = parse(sql).expect_err("unmodeled CREATE TABLE tail must not be erased");
    assert_eq!(
        error.message, "unexpected trailing tokens",
        "source SQL: {sql}"
    );
}
