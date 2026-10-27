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

//! Go `pkg/ddl/foreign_key.go`'s builder: a statement's FOREIGN KEY
//! definitions as `FKInfo`s.

#[allow(unused_imports)]
use crate::table_info_build::DdlAdmissionError;
#[allow(unused_imports)]
use tidb_ast::{CiString, CreateTableStmt, IndexPart, ReferentialAction, TableConstraint};
#[allow(unused_imports)]
use tidb_datatype::FieldTypeFlags;
#[allow(unused_imports)]
use tidb_error::mysql::MysqlError;

#[allow(unused_imports)]
use tidb_hack::GoToLower;
#[allow(unused_imports)]
use tidb_model::column::find_column_info;
#[allow(unused_imports)]
use tidb_model::index::{
    find_index_by_columns_for_foreign_key, is_index_prefix_covered_for_foreign_key, IndexColumn,
    IndexInfo,
};
#[allow(unused_imports)]
use tidb_model::schema_state::SchemaState;
#[allow(unused_imports)]
use tidb_model::table::{FKInfo, ReferredFKInfo, FK_VERSION1};
#[allow(unused_imports)]
use tidb_model::table_info::TableInfo;
#[allow(unused_imports)]
use tidb_model::TempTableType;

pub type Refusal<T> = Result<T, DdlAdmissionError>;

pub fn refusal(error: MysqlError) -> DdlAdmissionError {
    DdlAdmissionError::from_mysql_error(error)
}

pub fn coded(
    proto: &'static tidb_error::terror::TerrorError,
    message: impl Into<String>,
) -> DdlAdmissionError {
    DdlAdmissionError::from_terror(&proto.generate_with_stack(message.into()))
}

/// Go `ast.ReferOptionType` ordinal.
pub fn refer_opt(action: Option<ReferentialAction>) -> i64 {
    match action.unwrap_or_default() {
        ReferentialAction::NoOption | ReferentialAction::Unknown(_) => 0,
        ReferentialAction::Restrict => 1,
        ReferentialAction::Cascade => 2,
        ReferentialAction::SetNull => 3,
        ReferentialAction::NoAction => 4,
        ReferentialAction::SetDefault => 5,
    }
}

pub fn refer_opt_name(opt: i64) -> &'static str {
    match opt {
        1 => "RESTRICT",
        2 => "CASCADE",
        3 => "SET NULL",
        4 => "NO ACTION",
        5 => "SET DEFAULT",
        _ => "",
    }
}

pub fn part_column(part: &IndexPart) -> Refusal<String> {
    match part {
        IndexPart::Column { name, .. } => Ok(name.clone()),
        IndexPart::Expr { .. } => Err(coded(
            &tidb_error::catalog::infoschema::ERR_CANNOT_ADD_FOREIGN,
            "Cannot add foreign key constraint",
        )),
    }
}

pub fn cannot_add() -> DdlAdmissionError {
    coded(
        &tidb_error::catalog::infoschema::ERR_CANNOT_ADD_FOREIGN,
        "Cannot add foreign key constraint",
    )
}

/// Go `buildFKInfo` (`executor.go:5897`) over the table's finished columns.
pub fn build_fk_info(
    fk_name: &str,
    keys: &[IndexPart],
    refer: &tidb_ast::ForeignKeyReference,
    ref_schema: &str,
    table: &TableInfo,
) -> Refusal<FKInfo> {
    let ref_parts = refer.parts.as_deref().unwrap_or(&[]);
    if keys.len() != ref_parts.len() {
        return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH
                .generate_with_stack_by_args(&[
                    fk_name.to_owned().into(),
                    ("Key reference and table reference don't match".to_owned())
                        .to_string()
                        .into(),
                ]),
        )));
    }
    let ref_table = refer
        .table
        .as_ref()
        .and_then(|path| path.last())
        .cloned()
        .unwrap_or_default();
    // Go `checkTooLongForeignKey`, `checkTooLongSchema`, `checkTooLongTable`.
    crate::check_too_long_identifier(fk_name).map_err(refusal)?;
    crate::check_too_long_identifier(ref_schema).map_err(refusal)?;
    crate::check_too_long_identifier(&ref_table).map_err(refusal)?;
    let on_delete = refer_opt(refer.on_delete);
    let on_update = refer_opt(refer.on_update);
    // Go: columns a STORED generated column depends on.
    let mut base_cols = std::collections::HashSet::new();
    for column in table.columns.iter_deref() {
        let column = column.read();
        if column.is_generated() && column.generated_stored {
            for name in column.dependences.snapshot() {
                base_cols.insert(String::from_utf8_lossy(name.as_bytes()).go_to_lower());
            }
        }
    }
    let mut cols = Vec::with_capacity(keys.len());
    for (position, key) in keys.iter().enumerate() {
        // Go names an expression part's hidden column `_V$_<key>_<position>`
        // and fails to find it among the table's columns (1072).
        if matches!(key, IndexPart::Expr { .. }) {
            return Err(coded(
                &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS,
                format!("Key column '_V$_{fk_name}_{position}' doesn't exist in table"),
            ));
        }
        let name = part_column(key)?;
        let Some(column) = find_column_info(&table.columns, &name) else {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
                    .generate_with_stack_by_args(&[name.to_string().into()]),
            )));
        };
        let column = column.read();
        if column.is_generated() {
            if !column.generated_stored {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN
                        .generate_with_stack_by_args(&[
                            fk_name.to_owned().into(),
                            (column.name.original().to_owned()).to_string().into(),
                        ]),
                )));
            }
            if matches!(on_update, 2 | 3 | 5) {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN
                        .generate_with_stack_by_args(&[(format!(
                            "ON UPDATE {}",
                            refer_opt_name(on_update)
                        ))
                        .to_string()
                        .into()]),
                )));
            }
            if matches!(on_delete, 3 | 5) {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN
                        .generate_with_stack_by_args(&[(format!(
                            "ON DELETE {}",
                            refer_opt_name(on_delete)
                        ))
                        .to_string()
                        .into()]),
                )));
            }
        } else if base_cols.contains(column.name.lowercase())
            && (matches!(on_update, 2 | 3 | 5) || matches!(on_delete, 2 | 3 | 5))
        {
            return Err(cannot_add());
        }
        if column.field_type.has_flag(FieldTypeFlags::NOT_NULL) && (on_delete == 3 || on_update == 3)
        {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL
                    .generate_with_stack_by_args(&[
                        (column.name.original().to_owned()).to_string().into(),
                        fk_name.to_owned().into(),
                    ]),
            )));
        }
        cols.push(CiString::new(name));
    }
    let mut ref_cols = Vec::with_capacity(ref_parts.len());
    for part in ref_parts {
        let name = part_column(part)?;
        crate::check_too_long_identifier(&name).map_err(refusal)?;
        ref_cols.push(CiString::new(name));
    }
    Ok(FKInfo {
        name: CiString::new(fk_name),
        ref_schema: CiString::new(ref_schema),
        ref_table: CiString::new(ref_table),
        ref_cols: ref_cols.into(),
        cols: cols.into(),
        on_delete,
        on_update,
        state: SchemaState::PUBLIC,
        version: if tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) { FK_VERSION1 } else { 0 },
        ..FKInfo::default()
    })
}

/// Go `buildTableInfo`'s `ConstraintForeignKey` arm (`create_table.go:1452`)
/// and `addIndexForForeignKey` (`create_table.go:1740`). `schema` is the
/// child's schema, which Go's preprocessor fills into an unqualified
/// `REFERENCES` (`preprocess.go:2097`).
pub fn build_fk_infos(create: &CreateTableStmt, schema: &str, table: &mut TableInfo) -> Refusal<()> {
    let mut foreign_key_id = table.max_foreign_key_id;
    for constraint in &create.table_constraints {
        let TableConstraint::ForeignKey(definition) = constraint else {
            continue;
        };
        if definition.reference.match_type == tidb_ast::ForeignKeyMatch::Partial {
            return Err(refusal(tidb_error::mysql::MysqlError::unknown(
                "MATCH PARTIAL is not supported yet",
            )));
        }
        foreign_key_id += 1;
        let fk_name = match definition.name.as_deref() {
            Some("") => {
                return Err(coded(
                    &tidb_util::dbterror::ERR_WRONG_NAME_FOR_INDEX,
                    "Incorrect index name ''",
                ))
            }
            Some(name) => name.to_owned(),
            None => format!("fk_{foreign_key_id}"),
        };
        let lower = fk_name.go_to_lower();
        if table
            .foreign_keys
            .iter_deref()
            .any(|fk| fk.read().name.lowercase() == lower)
        {
            return Err(cannot_add());
        }
        let ref_schema = match definition.reference.table.as_deref() {
            Some([schema, _]) => schema.clone(),
            _ => schema.to_owned(),
        };
        let fk = build_fk_info(&fk_name, &definition.parts, &definition.reference, &ref_schema, table)?;
        table.foreign_keys.push_go(fk);
    }
    add_index_for_foreign_key(table)
}

/// Go `addIndexForForeignKey` (`create_table.go:1740`).
pub fn add_index_for_foreign_key(table: &mut TableInfo) -> Refusal<()> {
    let handle = if table.pk_is_handle {
        table.get_pk_col_info().map(|column| column.read().name.lowercase().to_owned())
    } else {
        None
    };
    let fks: Vec<FKInfo> = table.foreign_keys.iter_deref().map(|fk| fk.read().clone()).collect();
    for fk in fks {
        if fk.version < FK_VERSION1 {
            continue;
        }
        let cols = fk.cols.snapshot();
        if cols.len() == 1 && handle.as_deref() == Some(cols[0].lowercase()) {
            continue;
        }
        if find_index_by_columns_for_foreign_key(table, &table.indices, &cols).is_some() {
            continue;
        }
        if table.find_index_by_name(fk.name.lowercase()).is_some() {
            return Err(coded(
                &tidb_util::dbterror::ERR_DUP_KEY_NAME,
                format!("duplicate key name {}", fk.name.original()),
            ));
        }
        let mut columns = Vec::with_capacity(cols.len());
        for name in &cols {
            let column = find_column_info(&table.columns, name.original()).ok_or_else(|| {
                refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS
                        .generate_with_stack_by_args(&[(name.original().to_owned())
                            .to_string()
                            .into()]),
                ))
            })?;
            let column = column.read();
            columns.push(IndexColumn {
                name: column.name.clone(),
                offset: column.offset,
                length: tidb_datatype::UNSPECIFIED_LENGTH,
                ..IndexColumn::default()
            });
        }
        table.max_index_id += 1;
        table.indices.push_go(IndexInfo {
            id: table.max_index_id,
            name: fk.name.clone(),
            columns: columns.into(),
            state: SchemaState::PUBLIC,
            tp: tidb_ast::IndexType::BTREE,
            ..IndexInfo::default()
        });
    }
    Ok(())
}

/// Go `checkTableForeignKey` (`foreign_key.go:260`).
pub fn check_table_foreign_key(refer: &TableInfo, table: &TableInfo, fk: &FKInfo) -> Refusal<()> {
    if refer.temp_table_type != TempTableType::NONE || table.temp_table_type != TempTableType::NONE {
        return Err(cannot_add());
    }
    if refer.ttl_info.is_some() {
        return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK
                .generate_with_stack_by_args(&[]),
        )));
    }
    if refer.partition.is_some() || table.partition.is_some() {
        return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED
                .generate_with_stack_by_args(&[]),
        )));
    }
    let ref_cols = fk.ref_cols.snapshot();
    let cols = fk.cols.snapshot();
    for (i, ref_name) in ref_cols.iter().enumerate() {
        let Some(ref_col) = find_column_info(&refer.columns, ref_name.original()) else {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT
                    .generate_with_stack_by_args(&[
                        (ref_name.original().to_owned()).to_string().into(),
                        (fk.name.original().to_owned()).to_string().into(),
                        (fk.ref_table.original().to_owned()).to_string().into(),
                    ]),
            )));
        };
        let ref_col = ref_col.read();
        if ref_col.is_virtual_generated() {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN
                    .generate_with_stack_by_args(&[
                        (fk.name.original().to_owned()).to_string().into(),
                        (ref_name.original().to_owned()).to_string().into(),
                    ]),
            )));
        }
        let Some(col) = cols.get(i).and_then(|name| find_column_info(&table.columns, name.original()))
        else {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS.generate_with_stack_by_args(&[
                    (cols
                        .get(i)
                        .map(|n| n.original().to_owned())
                        .unwrap_or_default())
                    .to_string()
                    .into(),
                ]),
            )));
        };
        let col = col.read();
        let unsigned = u64::from(FieldTypeFlags::UNSIGNED);
        if col.get_type() != ref_col.get_type()
            || (col.get_flag() & unsigned) != (ref_col.get_flag() & unsigned)
            || col.get_charset() != ref_col.get_charset()
            || col.get_collate() != ref_col.get_collate()
        {
            return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS.generate_with_stack_by_args(&[
                    (col.name.original().to_owned()).to_string().into(),
                    (ref_col.name.original().to_owned()).to_string().into(),
                    (fk.name.original().to_owned()).to_string().into(),
                ]),
            )));
        }
        if ref_cols.len() == 1
            && ref_col.get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0
            && refer.pk_is_handle
        {
            return Ok(());
        }
    }
    if find_index_by_columns_for_foreign_key(refer, &refer.indices, &ref_cols).is_none() {
        return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT
                .generate_with_stack_by_args(&[
                    (fk.name.original().to_owned()).to_string().into(),
                    (fk.ref_table.original().to_owned()).to_string().into(),
                ]),
        )));
    }
    Ok(())
}
