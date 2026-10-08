//! Foreign-key metadata construction and catalog-aware validation for the
//! cluster DDL path, over the persisted `TableInfo` shape.
//!
//! Go splits this across three stages, and so does this module:
//! * [`build_fk_infos`] -- `buildTableInfo`'s `ConstraintForeignKey` arm
//!   (`create_table.go:1452`, `buildFKInfo` at `executor.go:5897`) plus
//!   `addIndexForForeignKey` (`create_table.go:1740`). Catalog-free.
//! * [`check_table_foreign_keys_valid`] -- `checkTableForeignKeysValid`
//!   (`foreign_key.go:151`), the submitter-side check against InfoSchema.
//! * [`check_table_foreign_keys_valid_in_owner`] -- the owner-side
//!   `checkTableForeignKeyValidInOwner` (`foreign_key.go:214`) followed by
//!   `allocateFKIndexID` (`create_table.go:95`).

use crate::cluster_catalog::ClusterCatalog;
use crate::table_info_build::DdlAdmissionError;
use tidb_ast::{CiString, CreateTableStmt, IndexPart, ReferentialAction, TableConstraint};
use tidb_datatype::FieldTypeFlags;
use tidb_executor::DriverError;
use tidb_model::column::find_column_info;
use tidb_model::index::{find_index_by_columns_for_foreign_key, IndexColumn, IndexInfo};
use tidb_model::schema_state::SchemaState;
use tidb_model::table::{FKInfo, FK_VERSION1};
use tidb_model::table_info::TableInfo;
use tidb_model::TempTableType;

type Refusal<T> = Result<T, DdlAdmissionError>;

fn refusal(error: DriverError) -> DdlAdmissionError {
    let error = error.to_mysql_error();
    DdlAdmissionError::with_code(error.code, error.message)
}

fn coded(code: u16, message: impl Into<String>) -> DdlAdmissionError {
    DdlAdmissionError::with_code(code, message.into())
}

/// Go `ast.ReferOptionType` ordinal.
fn refer_opt(action: Option<ReferentialAction>) -> i64 {
    match action.unwrap_or_default() {
        ReferentialAction::NoOption | ReferentialAction::Unknown(_) => 0,
        ReferentialAction::Restrict => 1,
        ReferentialAction::Cascade => 2,
        ReferentialAction::SetNull => 3,
        ReferentialAction::NoAction => 4,
        ReferentialAction::SetDefault => 5,
    }
}

fn refer_opt_name(opt: i64) -> &'static str {
    match opt {
        1 => "RESTRICT",
        2 => "CASCADE",
        3 => "SET NULL",
        4 => "NO ACTION",
        5 => "SET DEFAULT",
        _ => "",
    }
}

fn part_column(part: &IndexPart) -> Refusal<String> {
    match part {
        IndexPart::Column { name, .. } => Ok(name.clone()),
        IndexPart::Expr { .. } => Err(coded(
            tidb_error::tidb::errcode::ErrCannotAddForeign,
            "Cannot add foreign key constraint",
        )),
    }
}

fn check_too_long(identifier: &str) -> Refusal<()> {
    if identifier.chars().count() > 64 {
        return Err(refusal(DriverError::TooLongIdent(identifier.to_owned())));
    }
    Ok(())
}
fn cannot_add() -> DdlAdmissionError {
    coded(
        tidb_error::tidb::errcode::ErrCannotAddForeign,
        "Cannot add foreign key constraint",
    )
}

/// Go `buildFKInfo` (`executor.go:5897`) over the table's finished columns.
fn build_fk_info(
    fk_name: &str,
    keys: &[IndexPart],
    refer: &tidb_ast::ForeignKeyReference,
    ref_schema: &str,
    table: &TableInfo,
) -> Refusal<FKInfo> {
    let ref_parts = refer.parts.as_deref().unwrap_or(&[]);
    if keys.len() != ref_parts.len() {
        return Err(refusal(DriverError::WrongFkDef {
            name: fk_name.to_owned(),
            reason: "Key reference and table reference don't match".to_owned(),
        }));
    }
    let ref_table = refer
        .table
        .as_ref()
        .and_then(|path| path.last())
        .cloned()
        .unwrap_or_default();
    check_too_long(fk_name)?;
    check_too_long(ref_schema)?;
    check_too_long(&ref_table)?;
    let on_delete = refer_opt(refer.on_delete);
    let on_update = refer_opt(refer.on_update);
    // Go: columns a STORED generated column depends on.
    let mut base_cols = std::collections::HashSet::new();
    for column in table.columns.iter_deref() {
        let column = column.read();
        if column.is_generated() && column.generated_stored {
            for name in column.dependences.snapshot() {
                base_cols.insert(String::from_utf8_lossy(name.as_bytes()).to_lowercase());
            }
        }
    }
    let mut cols = Vec::with_capacity(keys.len());
    for key in keys {
        let name = part_column(key)?;
        let Some(column) = find_column_info(&table.columns, &name) else {
            return Err(refusal(DriverError::ForeignKeyChildColumnMissing(name)));
        };
        let column = column.read();
        if column.is_generated() {
            if !column.generated_stored {
                return Err(refusal(DriverError::ForeignKeyUsesVirtualColumn {
                    foreign_key: fk_name.to_owned(),
                    column: column.name.original().to_owned(),
                }));
            }
            if matches!(on_update, 2 | 3 | 5) {
                return Err(refusal(DriverError::WrongFkOptionForGeneratedColumn {
                    clause: format!("ON UPDATE {}", refer_opt_name(on_update)),
                }));
            }
            if matches!(on_delete, 3 | 5) {
                return Err(refusal(DriverError::WrongFkOptionForGeneratedColumn {
                    clause: format!("ON DELETE {}", refer_opt_name(on_delete)),
                }));
            }
        } else if base_cols.contains(column.name.lowercase())
            && (matches!(on_update, 2 | 3 | 5) || matches!(on_delete, 2 | 3 | 5))
        {
            return Err(cannot_add());
        }
        if column.field_type.has_flag(FieldTypeFlags::NOT_NULL) && (on_delete == 3 || on_update == 3)
        {
            return Err(refusal(DriverError::ForeignKeyColumnNotNull {
                column: column.name.original().to_owned(),
                constraint: fk_name.to_owned(),
            }));
        }
        cols.push(CiString::new(name));
    }
    let mut ref_cols = Vec::with_capacity(ref_parts.len());
    for part in ref_parts {
        let name = part_column(part)?;
        check_too_long(&name)?;
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
            return Err(refusal(DriverError::unsupported("MATCH PARTIAL is not supported yet")));
        }
        foreign_key_id += 1;
        let fk_name = match definition.name.as_deref() {
            Some("") => {
                return Err(coded(
                    tidb_error::tidb::errcode::ErrWrongNameForIndex,
                    "Incorrect index name ''",
                ))
            }
            Some(name) => name.to_owned(),
            None => format!("fk_{foreign_key_id}"),
        };
        let lower = fk_name.to_lowercase();
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
fn add_index_for_foreign_key(table: &mut TableInfo) -> Refusal<()> {
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
                tidb_error::tidb::errcode::ErrDupKeyName,
                format!("duplicate key name {}", fk.name.original()),
            ));
        }
        let mut columns = Vec::with_capacity(cols.len());
        for name in &cols {
            let column = find_column_info(&table.columns, name.original())
                .ok_or_else(|| refusal(DriverError::ForeignKeyChildColumnMissing(name.original().to_owned())))?;
            let column = column.read();
            columns.push(IndexColumn {
                name: column.name.clone(),
                offset: column.offset,
                length: tidb_executor::ddl::index_prefix::UNSPECIFIED_LENGTH,
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
        return Err(refusal(DriverError::TtlReferencedByForeignKey));
    }
    if refer.partition.is_some() || table.partition.is_some() {
        return Err(refusal(DriverError::ForeignKeyOnPartitioned));
    }
    let ref_cols = fk.ref_cols.snapshot();
    let cols = fk.cols.snapshot();
    for (i, ref_name) in ref_cols.iter().enumerate() {
        let Some(ref_col) = find_column_info(&refer.columns, ref_name.original()) else {
            return Err(refusal(DriverError::ForeignKeyReferencedColumnMissing {
                column: ref_name.original().to_owned(),
                constraint: fk.name.original().to_owned(),
                table: fk.ref_table.original().to_owned(),
            }));
        };
        let ref_col = ref_col.read();
        if ref_col.is_virtual_generated() {
            return Err(refusal(DriverError::ForeignKeyUsesVirtualColumn {
                foreign_key: fk.name.original().to_owned(),
                column: ref_name.original().to_owned(),
            }));
        }
        let Some(col) = cols.get(i).and_then(|name| find_column_info(&table.columns, name.original()))
        else {
            return Err(refusal(DriverError::ForeignKeyChildColumnMissing(
                cols.get(i).map(|n| n.original().to_owned()).unwrap_or_default(),
            )));
        };
        let col = col.read();
        let unsigned = u64::from(FieldTypeFlags::UNSIGNED);
        if col.get_type() != ref_col.get_type()
            || (col.get_flag() & unsigned) != (ref_col.get_flag() & unsigned)
            || col.get_charset() != ref_col.get_charset()
            || col.get_collate() != ref_col.get_collate()
        {
            return Err(refusal(DriverError::FkIncompatibleColumns {
                referencing: col.name.original().to_owned(),
                referenced: ref_col.name.original().to_owned(),
                constraint: fk.name.original().to_owned(),
            }));
        }
        if ref_cols.len() == 1
            && ref_col.get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0
            && refer.pk_is_handle
        {
            return Ok(());
        }
    }
    if find_index_by_columns_for_foreign_key(refer, &refer.indices, &ref_cols).is_none() {
        return Err(refusal(DriverError::ForeignKeyNoIndexInParent {
            constraint: fk.name.original().to_owned(),
            table: fk.ref_table.original().to_owned(),
        }));
    }
    Ok(())
}

fn lookup<'c>(catalog: &'c ClusterCatalog, schema: &str, table: &str) -> Option<&'c TableInfo> {
    catalog
        .find_table(schema, table)
        .map(|(_, table)| table)
        .filter(|table| table.state == SchemaState::PUBLIC)
}

/// Go `checkTableForeignKeysValid` (`foreign_key.go:151`) and, with
/// `in_owner`, `checkTableForeignKeyValidInOwner` (`foreign_key.go:214`).
/// They differ only in the self-reference same-columns refusal (submitter
/// only) and the error an unopenable parent reports.
fn check_valid(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &TableInfo,
    fk_check: bool,
    in_owner: bool,
) -> Refusal<()> {
    if !tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    let schema_l = schema.to_lowercase();
    for fk in table.foreign_keys.iter_deref() {
        let fk = fk.read();
        if fk.version < FK_VERSION1 {
            continue;
        }
        let refer = if fk.ref_schema.lowercase() == schema_l
            && fk.ref_table.lowercase() == table.name.lowercase()
        {
            if !in_owner {
                let cols = fk.cols.snapshot();
                let ref_cols = fk.ref_cols.snapshot();
                if cols.iter().zip(&ref_cols).all(|(a, b)| a.lowercase() == b.lowercase()) {
                    return Err(cannot_add());
                }
            }
            table
        } else {
            match lookup(catalog, fk.ref_schema.original(), fk.ref_table.original()) {
                Some(refer) => refer,
                None if !fk_check => continue,
                None if in_owner => {
                    return Err(coded(
                        tidb_error::tidb::errcode::ErrNoSuchTable,
                        format!(
                            "Table '{}.{}' doesn't exist",
                            fk.ref_schema.original(),
                            fk.ref_table.original()
                        ),
                    ))
                }
                None => {
                    return Err(refusal(DriverError::ForeignKeyReferencedTableMissing(
                        fk.ref_table.original().to_owned(),
                    )))
                }
            }
        };
        check_table_foreign_key(refer, table, &fk)?;
    }
    // Go `is.GetTableReferredForeignKeys`: children created earlier with
    // `foreign_key_checks=0` that reference this (new) table.
    for database in &catalog.databases {
        for child in &database.tables {
            for fk in child.foreign_keys.iter_deref() {
                let fk = fk.read();
                if fk.version >= FK_VERSION1
                    && fk.ref_schema.lowercase() == schema_l
                    && fk.ref_table.lowercase() == table.name.lowercase()
                {
                    check_table_foreign_key(table, child, &fk)?;
                }
            }
        }
    }
    Ok(())
}

/// Go `checkTableForeignKeysValid`: the submitter-side check.
pub fn check_table_foreign_keys_valid(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &TableInfo,
    fk_check: bool,
) -> Refusal<()> {
    check_valid(catalog, schema, table, fk_check, false)
}

/// Go `checkTableForeignKeyValidInOwner` then `allocateFKIndexID` for every
/// key (`create_table.go:87-98`).
pub fn check_table_foreign_keys_valid_in_owner(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &mut TableInfo,
    fk_check: bool,
) -> Refusal<()> {
    check_valid(catalog, schema, table, fk_check, true)?;
    for fk in table.foreign_keys.iter_deref() {
        table.max_foreign_key_id += 1;
        let mut fk = fk.write();
        fk.id = table.max_foreign_key_id;
        fk.state = SchemaState::PUBLIC;
    }
    Ok(())
}
