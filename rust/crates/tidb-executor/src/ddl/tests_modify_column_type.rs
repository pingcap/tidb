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

#[allow(unused_imports)]
use tidb_datatype::{FieldType, FieldTypeCode};
#[allow(unused_imports)]
use tidb_ddl_build::modify_column_type::*;

/// The declared type of `CREATE TABLE probe (a <declaration>)`, resolved as
/// a column would be.
fn column_type(declaration: &str) -> FieldType {
    let sql = format!("CREATE TABLE probe (a {declaration})");
    let stmt = tidb_parser::parse(&sql).expect("the probe parses");
    let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
        panic!("expected a DDL statement");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
        panic!("expected CREATE TABLE");
    };
    let column = &create.columns[0];
    let collate = column
        .options
        .iter()
        .find_map(|option| match option {
            tidb_ast::ColumnOption::Collate(name) => Some(name.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "utf8mb4_bin".to_owned());
    let charset = collate
        .split('_')
        .next()
        .expect("a collation names its charset");
    let mut field_type =
        crate::ddl::column_field_type::build_field_type("a", &column.ty, charset, &collate)
            .expect("the probe type resolves");
    if column
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull))
    {
        field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    }
    field_type
}

/// Go `modify_column_test.go::TestGetModifyColumnType`, the strict half:
/// the kind its failpoint records for each type pair, with and without an
/// index on the column.
#[test]
fn the_strict_mode_kinds_are_gos() {
    use ModifyColumnType::*;
    let cases = [
        ("int", "bigint", false, NoReorg),
        ("bigint", "int", false, NoReorgWithCheck),
        ("bigint", "int", true, NoReorgWithCheck),
        ("bigint", "bigint unsigned", false, Reorg),
        ("bigint", "bigint unsigned", true, Reorg),
        ("int unsigned", "bigint", false, Reorg),
        ("int unsigned", "bigint", true, Reorg),
        ("char(10)", "char(20)", false, NoReorg),
        ("char(20)", "char(10)", false, NoReorgWithCheck),
        (
            "char(20) collate utf8mb4_bin",
            "char(10) collate utf8mb4_bin",
            true,
            NoReorgWithCheck,
        ),
        (
            "char(20) collate utf8mb4_general_ci",
            "char(10) collate utf8mb4_general_ci",
            true,
            NoReorgWithCheck,
        ),
        ("char(10)", "varchar(20)", false, NoReorg),
        ("char(20)", "varchar(10)", false, NoReorgWithCheck),
        (
            "char(20) collate utf8mb4_bin",
            "varchar(10) collate utf8mb4_bin",
            true,
            IndexReorg,
        ),
        (
            "char(20) collate utf8mb4_general_ci",
            "varchar(10) collate utf8mb4_general_ci",
            true,
            NoReorgWithCheck,
        ),
        ("varchar(10)", "varchar(20)", false, NoReorg),
        ("varchar(20)", "varchar(10)", false, NoReorgWithCheck),
        (
            "varchar(20) collate utf8mb4_bin",
            "varchar(10) collate utf8mb4_bin",
            true,
            NoReorgWithCheck,
        ),
        (
            "varchar(20) collate utf8mb4_general_ci",
            "varchar(10) collate utf8mb4_general_ci",
            true,
            NoReorgWithCheck,
        ),
        // VARCHAR -> CHAR: Go's failpoint records the kind the precheck
        // recomputes once the rows pass, which is the unadjusted one.
        ("varchar(10)", "char(20)", false, NoReorgWithCheck),
        ("varchar(20)", "char(10)", false, NoReorgWithCheck),
        (
            "varchar(20) collate utf8mb4_bin",
            "char(10) collate utf8mb4_bin",
            true,
            IndexReorg,
        ),
        (
            "varchar(20) collate utf8mb4_general_ci",
            "char(10) collate utf8mb4_general_ci",
            true,
            NoReorgWithCheck,
        ),
        (
            "char(20) collate utf8mb4_bin",
            "varchar(10) collate utf8_unicode_ci",
            true,
            Reorg,
        ),
        (
            "char(20) collate utf8_unicode_ci",
            "varchar(10) collate utf8mb4_bin",
            true,
            Reorg,
        ),
        (
            "varchar(20) collate utf8mb4_bin",
            "char(10) collate utf8_unicode_ci",
            true,
            Reorg,
        ),
        (
            "varchar(20) collate utf8_unicode_ci",
            "char(10) collate utf8mb4_bin",
            true,
            Reorg,
        ),
    ];
    for (before, after, index, expected) in cases {
        let facts = ModifyColumnFacts {
            column_indexed: index,
            strict_sql_mode: true,
            ..ModifyColumnFacts::default()
        };
        assert_eq!(
            get_modify_column_type(&column_type(before), &column_type(after), facts),
            expected,
            "{before} -> {after}, index {index}"
        );
    }
}

/// The non-strict half: every checked kind becomes a rewrite.
#[test]
fn outside_strict_mode_a_checked_change_is_reorganized() {
    for (before, after) in [
        ("bigint", "int"),
        ("char(20)", "char(10)"),
        ("varchar(20)", "varchar(10)"),
        ("char(20)", "varchar(10)"),
        ("varchar(20)", "char(10)"),
    ] {
        assert_eq!(
            get_modify_column_type(
                &column_type(before),
                &column_type(after),
                ModifyColumnFacts::default()
            ),
            ModifyColumnType::Reorg,
            "{before} -> {after}"
        );
    }
}

/// `onModifyColumn` sends VARCHAR -> CHAR through the precheck, and a
/// partitioned table always reorganizes.
#[test]
fn varchar_to_char_prechecks_and_partitions_reorganize() {
    let strict = ModifyColumnFacts {
        strict_sql_mode: true,
        ..ModifyColumnFacts::default()
    };
    assert_eq!(
        modify_column_type(&column_type("varchar(10)"), &column_type("char(5)"), strict),
        ModifyColumnType::Precheck
    );
    assert_eq!(
        modify_column_type(
            &column_type("bigint"),
            &column_type("int"),
            ModifyColumnFacts {
                partitioned: true,
                ..strict
            }
        ),
        ModifyColumnType::Reorg
    );
    assert_eq!(
        modify_column_type(&column_type("int"), &column_type("bigint"), strict),
        ModifyColumnType::NoReorg
    );
    assert_eq!(
        modify_column_type(&column_type("int"), &column_type("int not null"), strict),
        ModifyColumnType::NoReorgWithCheck
    );
}
