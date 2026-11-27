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

//! Go `pkg/ddl/ddl_test.go::TestModifyColumn` and
//! `modify_column_test.go`'s `TestGetModifyColumnType` /
//! `TestConvertBetweenCharAndVarchar` (Go master 307fb3d6ead) over
//! `tidb_ddl_build::modify_column_change`.

use tidb_ddl_build::modify_column_change::{
    analyze_modify_column, convert_between_char_and_varchar, resolve_modify_column_type,
    resolve_modify_column_type_after_precheck,
};
use tidb_model::job_enums::modify_type;
use tidb_model::{ColumnInfo, GoShared, GoSharedPointerSlice, IndexColumn, IndexInfo, TableInfo};
use tidb_mysql::consts::SqlMode;

const STRICT_ALL_TABLES: SqlMode = tidb_mysql::consts::ModeStrictAllTables;
const MODE_NONE: SqlMode = tidb_mysql::consts::ModeNone;
/// Go `mysql.TypeNull`, the legacy stored kind.
const MYSQL_TYPE_NULL: u8 = 6;

/// Go `colDefStrToColInfo`: `alter table t modify column a <def>`'s column,
/// built against the default charset and collation.
fn col_def_str_to_col_info(def: &str) -> ColumnInfo {
    let sql = format!("alter table t modify column a {def}");
    let stmt = tidb_parser::parse(&sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
        panic!("expected a DDL statement");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = &**ddl else {
        panic!("expected ALTER TABLE");
    };
    let Some(tidb_ast::AlterTableAction::ModifyColumn { column, .. }) = alter.actions.first() else {
        panic!("expected MODIFY COLUMN");
    };
    let context = tidb_metabuild::Context::default();
    let mut info = tidb_ddl_build::table_info_build::new_modified_column(column, "utf8mb4", "utf8mb4_bin", &context)
        .unwrap_or_else(|error| panic!("{def}: {error:?}"));
    tidb_ddl_build::table_info_build::process_modify_column_options(&mut info, column, &context)
        .unwrap_or_else(|error| panic!("{def}: {error:?}"));
    info
}

fn table_of(columns: Vec<ColumnInfo>, indices: Vec<IndexInfo>) -> TableInfo {
    TableInfo {
        columns: GoSharedPointerSlice::from_handles(columns.into_iter().map(GoShared::new).map(Some).collect()),
        indices: GoSharedPointerSlice::from_handles(indices.into_iter().map(GoShared::new).map(Some).collect()),
        ..TableInfo::default()
    }
}

fn index_on(id: i64, name: &str, columns: &[(&ColumnInfo, i64)]) -> IndexInfo {
    IndexInfo {
        id,
        name: tidb_ast::CiString::new(name),
        columns: columns
            .iter()
            .map(|(column, offset)| IndexColumn {
                name: column.name.clone(),
                offset: *offset,
                length: -1,
                ..IndexColumn::default()
            })
            .collect::<Vec<_>>()
            .into(),
        state: tidb_model::SchemaState::PUBLIC,
        ..IndexInfo::default()
    }
}

/// Go `newIndexedTable`: the column, with id 1 at offset 0, under one index.
fn new_indexed_table(column: &mut ColumnInfo) -> TableInfo {
    column.id = 1;
    column.offset = 0;
    let index = index_on(1, "idx_a", &[(column, 0)]);
    table_of(vec![column.clone()], vec![index])
}

/// Go `err.Error()`.
fn error_text(error: &tidb_terror::MysqlError) -> String {
    error.error_string()
}

/// Go `TestModifyColumn`: which type changes `analyzeModifyColumn` refuses.
#[test]
fn test_modify_column() {
    let unsupported_charset = |reason: &str| format!("[ddl:8200]Unsupported modify {reason}");
    let cases: Vec<(&str, &str, Option<String>)> = vec![
        ("int", "bigint", None),
        ("int", "int unsigned", None),
        ("varchar(10)", "text", None),
        ("varbinary(10)", "blob", None),
        ("text", "blob", None),
        ("varchar(10)", "varchar(8)", None),
        ("varchar(10)", "varchar(11)", None),
        ("varchar(10) character set utf8 collate utf8_bin", "varchar(10) character set utf8", None),
        ("decimal(2,1)", "decimal(3,2)", None),
        ("decimal(2,1)", "decimal(2,2)", None),
        ("decimal(2,1)", "decimal(2,1)", None),
        ("decimal(2,1)", "int", None),
        ("decimal", "int", None),
        ("decimal(2,1)", "bigint", None),
        ("int", "varchar(10) character set gbk", Some(unsupported_charset("charset from binary to gbk"))),
        ("varchar(10) character set gbk", "int", Some(unsupported_charset("charset from gbk to binary"))),
        ("varchar(10) character set gbk", "varchar(10) character set utf8", Some(unsupported_charset("charset from gbk to utf8"))),
        ("varchar(10) character set gbk", "char(10) character set utf8", Some(unsupported_charset("charset from gbk to utf8"))),
        ("varchar(10) character set utf8", "char(10) character set gbk", Some(unsupported_charset("charset from utf8 to gbk"))),
        ("varchar(10) character set utf8", "varchar(10) character set gbk", Some(unsupported_charset("charset from utf8 to gbk"))),
        (
            "varchar(10) character set gbk collate gbk_bin",
            "varchar(10) character set gbk collate gbk_chinese_ci",
            Some(unsupported_charset("change collate from gbk_bin to gbk_chinese_ci")),
        ),
        ("varchar(10) character set gbk", "varchar(255) character set gbk", None),
        ("binary(4)", "char(10)", None),
        ("varchar(10)", "varchar(10) character set ascii", None),
        ("varchar(10)", "varchar(8) character set ascii", None),
        ("char(4) character set utf8mb4", "varchar(10) character set ascii", None),
        (
            "date",
            "bit(8)",
            Some("[ddl:8200]Unsupported modify column: change from original type date to bit(8) is currently unsupported yet".to_owned()),
        ),
    ];
    for (origin, to, expected) in cases {
        let col_a = col_def_str_to_col_info(origin);
        let col_b = col_def_str_to_col_info(to);
        let tbl = table_of(vec![col_a.clone()], Vec::new());
        let result = analyze_modify_column(&tbl, &col_a, &col_b);
        assert_eq!(result.as_ref().err().map(error_text), expected, "origin:{origin}, to:{to}");
    }
}

/// Go `TestModifyColumn/primaryKeyAndIndexedCollation`.
#[test]
fn test_modify_column_primary_key_and_indexed_collation() {
    let primary_key = |def: &str| {
        let mut column = col_def_str_to_col_info(def);
        column.add_flag(u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
        column
    };
    let pk_flag = "[ddl:8200]Unsupported modify column: this column has primary key flag".to_owned();
    for (old, new) in [
        ("int", "tinyint"),
        ("varchar(10) character set utf8mb4", "varchar(10) character set ascii"),
        ("varchar(10) collate utf8_bin", "varchar(10) collate utf8_general_ci"),
    ] {
        let mut old_col = primary_key(old);
        let new_col = col_def_str_to_col_info(new);
        let tbl = new_indexed_table(&mut old_col);
        assert_eq!(analyze_modify_column(&tbl, &old_col, &new_col).err().as_ref().map(error_text), Some(pk_flag.clone()), "{old} -> {new}");
    }

    let mut idx_old = col_def_str_to_col_info("varchar(10) collate utf8_bin");
    let idx_new = col_def_str_to_col_info("varchar(10) collate utf8_general_ci");
    let idx_tbl = new_indexed_table(&mut idx_old);
    assert_eq!(analyze_modify_column(&idx_tbl, &idx_old, &idx_new).unwrap(), modify_type::INDEX_REORG);
    assert_eq!(
        resolve_modify_column_type(&idx_tbl, &idx_old, &idx_new, modify_type::NONE, STRICT_ALL_TABLES).unwrap(),
        modify_type::REORG
    );

    let mut text_old = col_def_str_to_col_info("text collate utf8_bin");
    let text_new = col_def_str_to_col_info("text collate utf8_general_ci");
    let text_tbl = new_indexed_table(&mut text_old);
    assert_eq!(analyze_modify_column(&text_tbl, &text_old, &text_new).unwrap(), modify_type::REORG);

    let mut reorg_old = col_def_str_to_col_info("char(20) collate utf8mb4_bin");
    let reorg_new = col_def_str_to_col_info("varchar(10) collate utf8_unicode_ci");
    let reorg_tbl = new_indexed_table(&mut reorg_old);
    assert!(analyze_modify_column(&reorg_tbl, &reorg_old, &reorg_new).is_ok());

    // The new collation framework is always on in this port; Go's
    // `SetNewCollationEnabledForTest(false)` half has no counterpart.
    let mut bin_old = primary_key("char(4) character set utf8 collate utf8_bin");
    let bin_new = col_def_str_to_col_info("varchar(8) character set utf8 collate utf8_bin");
    let bin_tbl = new_indexed_table(&mut bin_old);
    assert_eq!(analyze_modify_column(&bin_tbl, &bin_old, &bin_new).err().as_ref().map(error_text), Some(pk_flag));

    let mut ci_old = primary_key("char(4) character set utf8 collate utf8_general_ci");
    let ci_new = col_def_str_to_col_info("varchar(8) character set utf8 collate utf8_general_ci");
    let ci_tbl = new_indexed_table(&mut ci_old);
    assert_eq!(analyze_modify_column(&ci_tbl, &ci_old, &ci_new).unwrap(), modify_type::NO_REORG);
}

/// Go `TestModifyColumn/resolveModifyColumnType`.
#[test]
fn test_modify_column_resolve_modify_column_type() {
    struct Case {
        name: &'static str,
        origin: &'static str,
        to: &'static str,
        has_index: bool,
        partitioned: bool,
        has_tiflash: bool,
        stored_type: u8,
        sql_mode: SqlMode,
        want_initial: u8,
        want_post_pre: u8,
    }
    let case = |name, origin, to, want_initial| Case {
        name,
        origin,
        to,
        has_index: false,
        partitioned: false,
        has_tiflash: false,
        stored_type: modify_type::NONE,
        sql_mode: STRICT_ALL_TABLES,
        want_initial,
        want_post_pre: modify_type::NONE,
    };
    let cases = [
        case("intWidening", "int", "bigint", modify_type::NO_REORG),
        case("intWideningNullToNotNull", "int", "bigint not null", modify_type::NO_REORG_WITH_CHECK),
        case("intNarrowStrict", "bigint", "int", modify_type::NO_REORG_WITH_CHECK),
        Case { sql_mode: MODE_NONE, ..case("intNarrowNonStrict", "bigint", "int", modify_type::REORG) },
        Case { stored_type: MYSQL_TYPE_NULL, ..case("intNarrowLegacyTypeNull", "bigint", "int", modify_type::REORG) },
        Case { partitioned: true, ..case("intNarrowPartitioned", "bigint", "int", modify_type::REORG) },
        Case { has_tiflash: true, ..case("intNarrowTiFlash", "bigint", "int", modify_type::REORG) },
        case("intSignToggleStatsFallback", "int", "int unsigned", modify_type::REORG),
        Case {
            want_post_pre: modify_type::NO_REORG_WITH_CHECK,
            ..case(
                "varcharToCharPrecheckNoIndex",
                "varchar(20) collate utf8mb4_bin",
                "char(10) collate utf8mb4_bin",
                modify_type::PRECHECK,
            )
        },
        Case {
            has_index: true,
            want_post_pre: modify_type::INDEX_REORG,
            ..case(
                "varcharToCharPrecheckIndexedBin",
                "varchar(20) collate utf8mb4_bin",
                "char(10) collate utf8mb4_bin",
                modify_type::PRECHECK,
            )
        },
        case("charToVarcharNoIndex", "char(10) collate utf8mb4_bin", "varchar(20) collate utf8mb4_bin", modify_type::NO_REORG),
        Case {
            has_index: true,
            ..case("charToVarcharIndexedBin", "char(10) collate utf8mb4_bin", "varchar(20) collate utf8mb4_bin", modify_type::INDEX_REORG)
        },
        case("binaryWideningReorg", "binary(4)", "binary(8)", modify_type::REORG),
    ];
    for tc in cases {
        let mut old_col = col_def_str_to_col_info(tc.origin);
        let new_col = col_def_str_to_col_info(tc.to);
        old_col.id = 1;
        old_col.offset = 0;
        let indices = if tc.has_index { vec![index_on(1, "idx_a", &[(&old_col, 0)])] } else { Vec::new() };
        let mut tbl = table_of(vec![old_col.clone()], indices);
        if tc.partitioned {
            tbl.partition = Some(GoShared::new(tidb_model::PartitionInfo::default()));
        }
        if tc.has_tiflash {
            tbl.tiflash_replica = Some(GoShared::new(tidb_model::TiFlashReplicaInfo { count: 1, ..Default::default() }));
        }
        let got = resolve_modify_column_type(&tbl, &old_col, &new_col, tc.stored_type, tc.sql_mode).unwrap();
        assert_eq!(got, tc.want_initial, "{}", tc.name);
        if tc.want_post_pre != modify_type::NONE {
            let post = resolve_modify_column_type_after_precheck(&tbl, &old_col, &new_col, got, tc.sql_mode);
            assert_eq!(post, tc.want_post_pre, "{}", tc.name);
        }
    }
}

/// Go `TestConvertBetweenCharAndVarchar`, with the new collation framework
/// on (this port has no switch).
#[test]
fn test_convert_between_char_and_varchar() {
    use tidb_datatype::FieldTypeCode;
    for varchar_type in [FieldTypeCode::Varchar, FieldTypeCode::VarString] {
        assert!(convert_between_char_and_varchar(varchar_type, FieldTypeCode::String));
        assert!(convert_between_char_and_varchar(FieldTypeCode::String, varchar_type));
    }
    assert!(!convert_between_char_and_varchar(FieldTypeCode::Long, FieldTypeCode::String));
}

/// Go `TestGetModifyColumnType`: the kind its `getModifyColumnType`
/// failpoint records for each change of `t (p1 int, p2 int, a <before>)`,
/// with or without `index idx_a(a), primary key(p1, p2)`. A VARCHAR -> CHAR
/// change records the kind its passing precheck resolves.
#[test]
fn test_get_modify_column_type() {
    let strict_cases = [
        ("int", "bigint", false, modify_type::NO_REORG),
        ("bigint", "int", false, modify_type::NO_REORG_WITH_CHECK),
        ("bigint", "int", true, modify_type::NO_REORG_WITH_CHECK),
        ("bigint", "bigint unsigned", false, modify_type::REORG),
        ("bigint", "bigint unsigned", true, modify_type::REORG),
        ("int unsigned", "bigint", false, modify_type::REORG),
        ("int unsigned", "bigint", true, modify_type::REORG),
        ("char(10)", "char(20)", false, modify_type::NO_REORG),
        ("char(20)", "char(10)", false, modify_type::NO_REORG_WITH_CHECK),
        ("char(20) collate utf8mb4_bin", "char(10) collate utf8mb4_bin", true, modify_type::NO_REORG_WITH_CHECK),
        ("char(20) collate utf8mb4_general_ci", "char(10) collate utf8mb4_general_ci", true, modify_type::NO_REORG_WITH_CHECK),
        ("char(10)", "varchar(20)", false, modify_type::NO_REORG),
        ("char(20)", "varchar(10)", false, modify_type::NO_REORG_WITH_CHECK),
        ("char(20) collate utf8mb4_bin", "varchar(10) collate utf8mb4_bin", true, modify_type::INDEX_REORG),
        ("char(20) collate utf8mb4_general_ci", "varchar(10) collate utf8mb4_general_ci", true, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(10)", "varchar(20)", false, modify_type::NO_REORG),
        ("varchar(20)", "varchar(10)", false, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(20) collate utf8mb4_bin", "varchar(10) collate utf8mb4_bin", true, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(20) collate utf8mb4_general_ci", "varchar(10) collate utf8mb4_general_ci", true, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(10)", "char(20)", false, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(20)", "char(10)", false, modify_type::NO_REORG_WITH_CHECK),
        ("varchar(20) collate utf8mb4_bin", "char(10) collate utf8mb4_bin", true, modify_type::INDEX_REORG),
        ("varchar(20) collate utf8mb4_general_ci", "char(10) collate utf8mb4_general_ci", true, modify_type::NO_REORG_WITH_CHECK),
        // different collation
        ("char(20) collate utf8mb4_bin", "varchar(10) collate utf8_unicode_ci", true, modify_type::REORG),
        ("char(20) collate utf8_unicode_ci", "varchar(10) collate utf8mb4_bin", true, modify_type::REORG),
        ("varchar(20) collate utf8mb4_bin", "char(10) collate utf8_unicode_ci", true, modify_type::REORG),
        ("varchar(20) collate utf8_unicode_ci", "char(10) collate utf8mb4_bin", true, modify_type::REORG),
        // Go master `TestModifyColumnPreserveOriginDefault/indexReorgPreservesOriginDefault`.
        ("char(10) collate utf8mb4_bin", "varchar(20) collate utf8mb4_bin", true, modify_type::INDEX_REORG),
    ];
    let non_strict_cases = [
        ("bigint", "int", false, modify_type::REORG),
        ("char(20)", "char(10)", false, modify_type::REORG),
        ("varchar(20)", "varchar(10)", false, modify_type::REORG),
        ("char(20)", "varchar(10)", false, modify_type::REORG),
        ("varchar(20)", "char(10)", false, modify_type::REORG),
    ];
    let run = |cases: &[(&str, &str, bool, u8)], sql_mode: SqlMode| {
        for &(before, after, index, expected) in cases {
            let mut p1 = col_def_str_to_col_info("int");
            p1.name = tidb_ast::CiString::new("p1");
            (p1.id, p1.offset) = (1, 0);
            let mut p2 = col_def_str_to_col_info("int");
            p2.name = tidb_ast::CiString::new("p2");
            (p2.id, p2.offset) = (2, 1);
            let mut old_col = col_def_str_to_col_info(before);
            (old_col.id, old_col.offset) = (3, 2);
            let new_col = col_def_str_to_col_info(after);
            let indices = if index {
                vec![index_on(1, "idx_a", &[(&old_col, 2)]), index_on(2, "PRIMARY", &[(&p1, 0), (&p2, 1)])]
            } else {
                Vec::new()
            };
            let tbl = table_of(vec![p1, p2, old_col.clone()], indices);
            let mut got = resolve_modify_column_type(&tbl, &old_col, &new_col, modify_type::NONE, sql_mode)
                .unwrap_or_else(|error| panic!("{before} -> {after}: {error:?}"));
            if got == modify_type::PRECHECK {
                got = resolve_modify_column_type_after_precheck(&tbl, &old_col, &new_col, got, sql_mode);
            }
            assert_eq!(got, expected, "before type: {before}, after type: {after}, index {index}");
        }
    };
    run(&strict_cases, STRICT_ALL_TABLES);
    run(&non_strict_cases, MODE_NONE);
}
