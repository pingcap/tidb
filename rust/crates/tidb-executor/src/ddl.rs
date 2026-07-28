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

//! `CREATE TABLE` execution: builds a `tidb_model::TableInfo` from the parsed
//! statement and registers a TiKV-byte-backed table for it -- tying the
//! transcreated metadata structs into the runnable path.
//!
//! This is the metadata slice of Go's `pkg/ddl` `buildTableInfo` /
//! `buildColumnAndConstraint`: column types map through `str_to_type` (Go
//! `types.StrToType`) with flen/decimal from the type arguments and the
//! unsigned flag. DEFERRED (documented): constraints/indexes (PK, UNIQUE,
//! FOREIGN KEY), column options (DEFAULT, NOT NULL, AUTO_INCREMENT, comments),
//! charset/collation resolution beyond the type's own defaults, TEMPORARY,
//! `CREATE TABLE ... LIKE`, partitioning, and the schema-version/DDL-job
//! machinery (the driver applies metadata directly; the DDL job queue is a
//! separate tier).

use crate::driver::{Catalog, DriverError};
use crate::kv_table::{KvColumn, KvIndex, KvTable};
use tidb_ast::CiString;
use tidb_ast::{ColumnDef, ColumnTypeArg, DdlStmt, Stmt};
use tidb_datatype::{
    str_to_type, Datum, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags,
};
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;

/// Builds the column's `FieldType` from its parsed SQL type: code via
/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;
/// Runs a `RENAME TABLE`, moving each pair in written order.
///
/// Captured from TiDB: renaming onto a name that already exists is 1050, and
/// renaming a table that does not exist is 1146. A rename may move the table
/// to another schema, since both sides carry a full path.
pub fn run_rename_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only RENAME TABLE is supported here",
        ));
    };
    let pairs: Vec<(Vec<String>, Vec<String>)> = match &**ddl {
        DdlStmt::RenameTable(rename) => rename.pairs.clone(),
        // `ALTER TABLE x RENAME TO y` is the same operation.
        DdlStmt::AlterTable(alter) => {
            let mut pairs = Vec::new();
            for action in &alter.actions {
                if let tidb_ast::AlterTableAction::RenameTable { new_name } = action {
                    pairs.push((alter.name.clone(), new_name.clone()));
                }
            }
            pairs
        }
        _ => {
            return Err(DriverError::Unsupported(
                "only RENAME TABLE is supported here",
            ))
        }
    };

    for (from, to) in &pairs {
        let (from_db, from_name) = crate::driver::split_table_path_pub(from, current_db)?;
        let (from_db, from_name) = (from_db.to_owned(), from_name.to_owned());
        let (to_db, to_name) = crate::driver::split_table_path_pub(to, current_db)?;
        let (to_db, to_name) = (to_db.to_owned(), to_name.to_owned());

        if catalog.table_in(&from_db, &from_name).is_none() {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{from_db}.{from_name}"),
            )));
        }
        if catalog.table_in(&to_db, &to_name).is_some() {
            return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
                format!("{to_db}.{to_name}"),
            )));
        }
        catalog.rename_table(&from_db, &from_name, &to_db, &to_name);
    }
    Ok(())
}

/// Runs a `TRUNCATE TABLE`, emptying it while keeping its definition.
///
/// Captured from TiDB: the rows and index entries go, the schema and indexes
/// stay, the auto-increment counter restarts, and truncating a table that
/// does not exist is 1146.
pub fn run_truncate_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only TRUNCATE TABLE is supported here",
        ));
    };
    let DdlStmt::TruncateTable(truncate) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only TRUNCATE TABLE is supported here",
        ));
    };
    let (database, name) = crate::driver::split_table_path_pub(truncate, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(&database, &name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    };
    table.truncate();
    Ok(())
}

/// Runs a `CREATE INDEX`, backfilling the existing rows.
///
/// Captured from TiDB: a duplicate index name is 1061, and a unique index
/// whose existing rows already collide is 1062 naming `table.index`, leaving
/// the table without the index.
pub fn run_create_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let DdlStmt::CreateIndex(create) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only CREATE INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&create.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        _ => {
            return Err(DriverError::Unsupported(
                "SPATIAL, FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
            ))
        }
    };
    let columns: Vec<String> = index_part_names(&create.parts)?;
    add_index_to_table(
        catalog,
        &database,
        &table_name,
        &create.name,
        unique,
        &columns,
    )
}

/// The column names an index's key parts name, rejecting the forms this tier
/// does not maintain rather than creating an index that ignores them.
fn index_part_names(parts: &[tidb_ast::IndexPart]) -> Result<Vec<String>, DriverError> {
    let mut names = Vec::with_capacity(parts.len());
    for part in parts {
        let tidb_ast::IndexPart::Column {
            name, prefix_len, ..
        } = part
        else {
            return Err(DriverError::Unsupported(
                "an expression index is not supported yet",
            ));
        };
        if prefix_len.is_some() {
            return Err(DriverError::Unsupported(
                "a prefix-length index is not supported yet",
            ));
        }
        names.push(name.clone());
    }
    Ok(names)
}

/// Adds one index to a table, shared by `CREATE INDEX` and
/// `ALTER TABLE ... ADD INDEX`.
fn add_index_to_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    unique: bool,
    columns: &[String],
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let mut offsets = Vec::with_capacity(columns.len());
    for column in columns {
        offsets.push(
            table
                .columns
                .iter()
                .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| DriverError::UnknownColumnInAlter(column.clone()))?,
        );
    }
    let id = table.next_index_id();
    table
        .create_index(KvIndex {
            id,
            name: index_name.to_owned(),
            unique,
            column_offsets: offsets,
        })
        .map_err(|e| match e {
            crate::kv_table::KvTableError::DuplicateKeyName(name) => {
                DriverError::DuplicateKeyName(name)
            }
            crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                DriverError::DuplicateEntry { value, key }
            }
            other => DriverError::Parse(format!("index creation failed: {other:?}")),
        })
}

/// Runs a `DROP INDEX`.
///
/// Captured from TiDB: dropping one that does not exist is 1091 with its own
/// message, "index nosuch doesn't exist".
pub fn run_drop_index_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::Unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let DdlStmt::DropIndex(drop) = &**ddl else {
        return Err(DriverError::Unsupported(
            "only DROP INDEX is supported here",
        ));
    };
    let (database, table_name) = crate::driver::split_table_path_pub(&drop.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    drop_index_from_table(catalog, &database, &table_name, &drop.name, drop.if_exists)
}

/// Removes one index, shared by `DROP INDEX` and `ALTER TABLE ... DROP INDEX`.
fn drop_index_from_table(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    if_exists: bool,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let dropped = table
        .drop_index(index_name)
        .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    if !dropped && !if_exists {
        return Err(DriverError::UnknownIndex(index_name.to_owned()));
    }
    Ok(())
}

/// Runs an `ALTER TABLE`, applying its actions in source order.
///
/// The rules are captured from TiDB: `ADD COLUMN ... DEFAULT d` gives rows
/// written earlier the value `d` rather than NULL, without rewriting them;
/// `FIRST`/`AFTER` place the column; a duplicate name is 1060, dropping an
/// unknown column is 1091, dropping the last column is 1090, and dropping an
/// integer primary key is TiDB's own 8200.
///
/// DEFERRED (documented): every other ALTER action -- MODIFY, CHANGE, RENAME,
/// index and constraint changes -- is rejected rather than silently accepted,
/// and dropping a column an index uses is rejected rather than leaving the
/// index addressing a column that is gone.
pub fn run_alter_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let alter = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::AlterTable(alter) => alter,
            _ => {
                return Err(DriverError::Unsupported(
                    "only ALTER TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only ALTER TABLE is supported here",
            ))
        }
    };
    let (database, name) = crate::driver::split_table_path_pub(&alter.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    if catalog.table_in(&database, &name).is_none() {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    }

    for action in &alter.actions {
        match action {
            tidb_ast::AlterTableAction::AddColumn {
                column, position, ..
            } => add_column_action(catalog, &database, &name, column, position)?,
            tidb_ast::AlterTableAction::AddColumns {
                columns,
                constraints,
                ..
            } => {
                if !constraints.is_empty() {
                    return Err(DriverError::Unsupported(
                        "adding constraints with ALTER TABLE is not supported yet",
                    ));
                }
                for column in columns {
                    add_column_action(
                        catalog,
                        &database,
                        &name,
                        column,
                        &tidb_ast::ColumnPosition::Default,
                    )?;
                }
            }
            tidb_ast::AlterTableAction::ModifyColumn {
                if_exists,
                column,
                position,
            } => modify_column_action(
                catalog,
                &database,
                &name,
                &column.name,
                column,
                position,
                *if_exists,
            )?,
            tidb_ast::AlterTableAction::ChangeColumn {
                if_exists,
                old_name,
                column,
                position,
            } => {
                let old = old_name
                    .last()
                    .ok_or(DriverError::Unsupported("empty CHANGE COLUMN name"))?;
                modify_column_action(catalog, &database, &name, old, column, position, *if_exists)?;
            }
            tidb_ast::AlterTableAction::DropColumn {
                if_exists,
                name: column_name,
            } => drop_column_action(catalog, &database, &name, column_name, *if_exists)?,
            tidb_ast::AlterTableAction::AddIndexConstraint(index) => {
                let unique = matches!(
                    index.kind,
                    tidb_ast::IndexConstraintKind::Unique
                        | tidb_ast::IndexConstraintKind::UniqueKey
                        | tidb_ast::IndexConstraintKind::UniqueIndex
                );
                match index.kind {
                    tidb_ast::IndexConstraintKind::Key
                    | tidb_ast::IndexConstraintKind::Index
                    | tidb_ast::IndexConstraintKind::Unique
                    | tidb_ast::IndexConstraintKind::UniqueKey
                    | tidb_ast::IndexConstraintKind::UniqueIndex => {}
                    _ => {
                        return Err(DriverError::Unsupported(
                            "this index kind is not supported yet",
                        ))
                    }
                }
                let columns = index_part_names(&index.parts)?;
                let index_name = index
                    .name
                    .clone()
                    .unwrap_or_else(|| columns.first().cloned().unwrap_or_default());
                add_index_to_table(catalog, &database, &name, &index_name, unique, &columns)?;
            }
            // `ALTER TABLE x RENAME TO y` is the same operation as
            // `RENAME TABLE x TO y`.
            tidb_ast::AlterTableAction::RenameTable { new_name } => {
                let (to_db, to_name) = crate::driver::split_table_path_pub(new_name, current_db)?;
                let (to_db, to_name) = (to_db.to_owned(), to_name.to_owned());
                if catalog.table_in(&to_db, &to_name).is_some() {
                    return Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(
                        format!("{to_db}.{to_name}"),
                    )));
                }
                catalog.rename_table(&database, &name, &to_db, &to_name);
            }
            tidb_ast::AlterTableAction::DropIndex {
                if_exists,
                name: index_name,
            } => {
                drop_index_from_table(catalog, &database, &name, index_name, *if_exists)?;
            }
            _ => {
                return Err(DriverError::Unsupported(
                    "this ALTER TABLE action is not supported yet",
                ))
            }
        }
    }
    Ok(())
}

/// One `ADD COLUMN`.
/// Go `getDefaultValue` + `checkDefaultValue`: normalizes a column's written
/// DEFAULT and rejects one the column cannot hold.
///
/// Go converts the value to a "standard format" only for the integer and
/// float/double types -- which is why `INT DEFAULT 7.6` reports `DEFAULT '8'`
/// while `DECIMAL(10,3) DEFAULT 1.23456` keeps `'1.23456'` and only rounds
/// when a row reads it. It then evaluates the stored default with truncation
/// at error level, so a value the column cannot hold is
/// `ErrInvalidDefault` (1067) at DDL time rather than a surprise per row.
fn normalize_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
) -> Result<Datum, DriverError> {
    // Go `checkColumnDefaultValue`: a JSON column's only legal default is
    // NULL. The default is stored as metadata TEXT, and a JSON document has
    // no text form that survives that round trip, so TiDB refuses it up
    // front rather than storing a document it cannot rebuild.
    if !value.is_null() && field_type.code() == tidb_datatype::FieldTypeCode::Json {
        return Err(DriverError::BlobCantHaveDefault(column.to_owned()));
    }
    let invalid = || DriverError::InvalidDefault(column.to_owned());
    let normalized = match field_type.code() {
        tidb_datatype::FieldTypeCode::Tiny
        | tidb_datatype::FieldTypeCode::Short
        | tidb_datatype::FieldTypeCode::Int24
        | tidb_datatype::FieldTypeCode::Long
        | tidb_datatype::FieldTypeCode::LongLong
        | tidb_datatype::FieldTypeCode::Float
        | tidb_datatype::FieldTypeCode::Double => {
            // Go adopts the converted value only when the conversion itself
            // succeeded (`if temp, err := v.ConvertTo(...); err == nil`), and
            // otherwise keeps the original for the check below to report.
            match value.convert_to(field_type, tidb_datatype::STRICT_FLAGS) {
                Ok(converted) if converted.event.is_none() => converted.value,
                _ => value.clone(),
            }
        }
        _ => value.clone(),
    };
    // The check phase: strict conversion of what will be stored.
    let checked = normalized
        .convert_to(field_type, tidb_datatype::STRICT_FLAGS)
        .map_err(|_| invalid())?;
    if checked.event.as_ref().is_some_and(|event| {
        !crate::driver::conversion_event_is_silent(&normalized, field_type, event)
    }) {
        return Err(invalid());
    }
    Ok(normalized)
}

/// `ALTER TABLE ... MODIFY COLUMN` and `... CHANGE COLUMN`, which differ only
/// in whether the column is also renamed.
///
/// Go runs these as one `ActionModifyColumn` job: it finds the old column by
/// name, checks the new type against the stored data, then swaps the column
/// definition in place, keeping the column id so indexes and handles survive.
///
/// NOT MODELLED (documented, and rejected rather than ignored): a type change
/// on a clustered handle column to anything but another integer type (Go 8200
/// "this column has primary key flag"), a BLOB/TEXT column that an index
/// covers (Go 1170), generated columns, and the column options beyond
/// NULL/NOT NULL/DEFAULT that CREATE TABLE also rejects here.
fn modify_column_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    old_name: &str,
    def: &ColumnDef,
    position: &tidb_ast::ColumnPosition,
    if_exists: bool,
) -> Result<(), DriverError> {
    let field_type = field_type_of(def)?;
    let mut default_value = None;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                    expr,
                    &tidb_expr::rewriter::NoResolver,
                )
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                    return Err(DriverError::Unsupported(
                        "an expression DEFAULT is not supported yet",
                    ));
                };
                default_value = Some(
                    constant
                        .eval()
                        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
                );
            }
            tidb_ast::ColumnOption::NotNull | tidb_ast::ColumnOption::Null => {}
            _ => {
                return Err(DriverError::Unsupported(
                    "this column option is not supported in ALTER TABLE MODIFY COLUMN",
                ))
            }
        }
    }
    let not_null = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull));

    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(old_name))
    else {
        // Go's IF EXISTS turns the missing column into a no-op (with a note).
        if if_exists {
            return Ok(());
        }
        return Err(DriverError::UnknownColumnInTable {
            column: old_name.to_owned(),
            table: table_name.to_owned(),
        });
    };
    // A rename onto another column's name is a duplicate, but renaming a
    // column to the name it already has is allowed.
    if !def.name.eq_ignore_ascii_case(old_name)
        && table
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(&def.name))
    {
        return Err(DriverError::DuplicateColumnName(def.name.clone()));
    }

    let mut field_type = field_type;
    if not_null {
        field_type.add_flags(NOT_NULL_FLAG);
    }
    let integer_type = |code| {
        matches!(
            code,
            tidb_datatype::FieldTypeCode::Tiny
                | tidb_datatype::FieldTypeCode::Short
                | tidb_datatype::FieldTypeCode::Int24
                | tidb_datatype::FieldTypeCode::Long
                | tidb_datatype::FieldTypeCode::LongLong
        )
    };
    // Go refuses to move a clustered handle off the integer domain, because
    // the handle IS the row key.
    let is_handle = table.pk_handle_offset() == Some(offset);
    if is_handle && !integer_type(field_type.code()) {
        return Err(DriverError::UnsupportedModifyColumn(
            "this column has primary key flag",
        ));
    }
    // Go `ErrBlobKeyWithoutLength`: an index cannot cover a full BLOB/TEXT.
    let blob_type = matches!(
        field_type.code(),
        tidb_datatype::FieldTypeCode::Blob
            | tidb_datatype::FieldTypeCode::TinyBlob
            | tidb_datatype::FieldTypeCode::MediumBlob
            | tidb_datatype::FieldTypeCode::LongBlob
    );
    if blob_type
        && table
            .indexes()
            .iter()
            .any(|index| index.column_offsets.contains(&offset))
    {
        return Err(DriverError::BlobKeyWithoutLength(def.name.clone()));
    }

    let new_position = match position {
        tidb_ast::ColumnPosition::Default => None,
        tidb_ast::ColumnPosition::First => Some(0),
        tidb_ast::ColumnPosition::After(after) => {
            let target = table
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(after))
                .ok_or_else(|| DriverError::UnknownColumnInTable {
                    column: after.clone(),
                    table: table_name.to_owned(),
                })?;
            // Moving forward, the column lands right after the target once the
            // target has closed the gap the move opened.
            Some(if target > offset { target } else { target + 1 })
        }
    };
    let default_value = match default_value {
        Some(value) => Some(normalize_column_default(value, &field_type, &def.name)?),
        None => None,
    };
    let column = KvColumn {
        name: def.name.clone(),
        id: table.columns[offset].id,
        field_type,
        default_value: default_value.clone(),
        origin_default: default_value,
    };
    table
        .modify_column(offset, column, new_position)
        .map_err(|e| match e {
            crate::kv_table::KvTableError::TruncatedIncorrectValue { kind, value } => {
                DriverError::TruncatedIncorrectValue { kind, value }
            }
            crate::kv_table::KvTableError::DataTruncatedValue { column, value } => {
                DriverError::DataTruncatedValue { column, value }
            }
            crate::kv_table::KvTableError::DataTruncatedAtRow { column, row } => {
                DriverError::DataTruncatedAtRow { column, row }
            }
            crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                DriverError::DuplicateEntry { value, key }
            }
            other => DriverError::Parse(format!("column modification failed: {other:?}")),
        })
}

fn add_column_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    def: &ColumnDef,
    position: &tidb_ast::ColumnPosition,
) -> Result<(), DriverError> {
    let field_type = field_type_of(def)?;
    let mut default_value = None;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                    expr,
                    &tidb_expr::rewriter::NoResolver,
                )
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                    return Err(DriverError::Unsupported(
                        "an expression DEFAULT is not supported yet",
                    ));
                };
                default_value = Some(
                    constant
                        .eval()
                        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
                );
            }
            tidb_ast::ColumnOption::NotNull => {}
            tidb_ast::ColumnOption::Null => {}
            _ => {
                return Err(DriverError::Unsupported(
                    "this column option is not supported in ALTER TABLE ADD COLUMN",
                ))
            }
        }
    }
    let not_null = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull));

    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    if table
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(&def.name))
    {
        return Err(DriverError::DuplicateColumnName(def.name.clone()));
    }
    let index = match position {
        tidb_ast::ColumnPosition::Default => table.columns.len(),
        tidb_ast::ColumnPosition::First => 0,
        tidb_ast::ColumnPosition::After(after) => table
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(after))
            .map(|offset| offset + 1)
            .ok_or_else(|| DriverError::UnknownColumnInAlter(after.clone()))?,
    };
    let mut field_type = field_type;
    if not_null {
        field_type.add_flags(NOT_NULL_FLAG);
    }
    let default_value = match default_value {
        Some(value) => Some(normalize_column_default(value, &field_type, &def.name)?),
        None => None,
    };
    let id = table.next_column_id();
    table.add_column(
        index,
        KvColumn {
            name: def.name.clone(),
            id,
            field_type,
            default_value: default_value.clone(),
            // Rows written before this column existed read back the default.
            origin_default: default_value,
        },
    );
    Ok(())
}

/// One `DROP COLUMN`.
fn drop_column_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    column_name: &str,
    if_exists: bool,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
    else {
        if if_exists {
            return Ok(());
        }
        return Err(DriverError::UnknownColumnInAlter(column_name.to_owned()));
    };
    // Captured: dropping the only column is 1090.
    if table.columns.len() == 1 {
        return Err(DriverError::CannotDropOnlyColumn {
            column: column_name.to_owned(),
            table: table_name.to_owned(),
        });
    }
    // Captured: dropping an integer primary key is TiDB's 8200.
    if table.pk_handle_offset() == Some(offset) {
        return Err(DriverError::UnsupportedDropIntegerPrimaryKey);
    }
    if table.common_handle_offsets().contains(&offset) {
        return Err(DriverError::Unsupported(
            "dropping a clustered primary key column is not supported yet",
        ));
    }
    // Captured from TiDB: a COMPOSITE index over the column refuses the drop
    // with 8200, while a single-column index is dropped along with it.
    if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.len() > 1 && index.column_offsets.contains(&offset))
    {
        return Err(DriverError::CannotDropColumnWithCompositeIndex(
            column_name.to_owned(),
        ));
    }
    let covering: Vec<String> = table
        .indexes()
        .iter()
        .filter(|index| index.column_offsets == [offset])
        .map(|index| index.name.clone())
        .collect();
    for index_name in covering {
        table
            .drop_index(&index_name)
            .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    }
    table.drop_column(offset);
    Ok(())
}

/// Runs a `DROP TABLE`, removing every named table that exists.
///
/// Go drops the tables it finds and reports `ErrBadTable` for the first name
/// it does not, rather than validating the whole list first: captured from
/// TiDB, `drop table d1, nosuch` removes `d1` AND errors. `IF EXISTS`
/// suppresses the error, leaving the drops it did perform.
///
/// DEFERRED (documented): `TEMPORARY` tables, which this executor never
/// models, are rejected rather than silently dropping a permanent table of
/// the same name.
pub fn run_drop_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let drop = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::DropTable(drop) => drop,
            _ => {
                return Err(DriverError::Unsupported(
                    "only DROP TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only DROP TABLE is supported here",
            ))
        }
    };
    if drop.temporary != tidb_ast::DropTemporary::None {
        return Err(DriverError::Unsupported(
            "temporary tables are not supported yet",
        ));
    }

    let mut missing: Option<String> = None;
    for path in &drop.names {
        let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        // A view is not a table: `DROP TABLE v` reports the name as unknown
        // rather than dropping the view (Go's own captured behaviour).
        let dropped =
            !catalog.is_view_in(&database, &name) && catalog.drop_table_in(&database, &name);
        if !dropped && missing.is_none() {
            // Go reports the first missing name, after the drops it performed.
            missing = Some(format!("{database}.{name}"));
        }
    }
    match missing {
        Some(name) if !drop.if_exists => {
            Err(DriverError::Schema(crate::SchemaErrorKind::BadTable(name)))
        }
        _ => Ok(()),
    }
}

/// Go `mysql.AutoIncrementFlag`.
const AUTO_INCREMENT_FLAG: u32 = 1 << 9;

/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: u32 = 1 << 1;

/// Every index a `CREATE TABLE` declares, other than a primary key that
/// became the row handle.
///
/// Go's `buildTableInfo` turns each key constraint into an `IndexInfo` with
/// its own id. The order is table-level constraints first, in written order,
/// then the inline column constraints in column order -- captured from real
/// TiDB's `SHOW CREATE TABLE`, which lists them in index order:
/// `create table x (a bigint unique, b bigint unique, key kb (b))` reports
/// `KEY kb` before `UNIQUE KEY a` and `UNIQUE KEY b`.
///
/// DEFERRED (documented): FULLTEXT, VECTOR and COLUMNAR indexes, prefix
/// lengths, expression keys and index options, all rejected rather than
/// silently created as a plain index.
fn table_indexes(
    create: &tidb_ast::CreateTableStmt,
    columns: &[ColumnInfo],
    pk_is_handle: bool,
) -> Result<Vec<KvIndex>, DriverError> {
    // Go `checkIndexColumn`: a JSON column can never be an index column, in
    // any position of any index kind -- checked here, where every index part
    // resolves its column, so the rule has exactly one home.
    let offset_of = |name: &str| -> Result<usize, DriverError> {
        let offset = columns
            .iter()
            .position(|col| col.name.original().eq_ignore_ascii_case(name))
            .ok_or(DriverError::Unsupported(
                "an index names a column the table does not define",
            ))?;
        if columns[offset].field_type.code() == FieldTypeCode::Json {
            return Err(DriverError::JsonUsedInKey(
                columns[offset].name.original().to_owned(),
            ));
        }
        Ok(offset)
    };
    fn push(indexes: &mut Vec<KvIndex>, name: String, unique: bool, offsets: Vec<usize>) {
        indexes.push(KvIndex {
            id: (indexes.len() + 1) as i64,
            name,
            unique,
            column_offsets: offsets,
        });
    }
    let mut indexes: Vec<KvIndex> = Vec::new();

    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            continue;
        };
        match index.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey if pk_is_handle => continue,
            tidb_ast::IndexConstraintKind::PrimaryKey
            | tidb_ast::IndexConstraintKind::Unique
            | tidb_ast::IndexConstraintKind::UniqueKey
            | tidb_ast::IndexConstraintKind::UniqueIndex
            | tidb_ast::IndexConstraintKind::Key
            | tidb_ast::IndexConstraintKind::Index => {}
            _ => {
                return Err(DriverError::Unsupported(
                    "FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
                ))
            }
        }
        let unique = matches!(
            index.kind,
            tidb_ast::IndexConstraintKind::Unique
                | tidb_ast::IndexConstraintKind::UniqueKey
                | tidb_ast::IndexConstraintKind::UniqueIndex
                | tidb_ast::IndexConstraintKind::PrimaryKey
        );
        let mut offsets = Vec::with_capacity(index.parts.len());
        for part in &index.parts {
            let tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } = part
            else {
                return Err(DriverError::Unsupported(
                    "an expression index is not supported yet",
                ));
            };
            if prefix_len.is_some() {
                return Err(DriverError::Unsupported(
                    "a prefix-length index is not supported yet",
                ));
            }
            offsets.push(offset_of(name)?);
        }
        let name = match index.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey => "PRIMARY".to_owned(),
            _ => index
                .name
                .clone()
                .unwrap_or_else(|| format!("idx_{}", indexes.len() + 1)),
        };
        push(&mut indexes, name, unique, offsets);
    }
    for def in &create.columns {
        for option in &def.options {
            if let tidb_ast::ColumnOption::InlineKey(key) = option {
                match key.kind {
                    tidb_ast::InlineKeyKind::Unique => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, def.name.clone(), true, vec![offset]);
                    }
                    // A primary key that is not the row handle still needs an
                    // index to enforce its uniqueness.
                    tidb_ast::InlineKeyKind::Primary { .. } if !pk_is_handle => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, "PRIMARY".to_owned(), true, vec![offset]);
                    }
                    tidb_ast::InlineKeyKind::Primary { .. } => {}
                }
            }
        }
    }

    Ok(indexes)
}

/// Go `isIntCol`: whether the column's type can carry a handle.
fn is_int_column(column: &ColumnInfo) -> bool {
    matches!(
        column.field_type.code(),
        FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
    )
}

/// The single column a `PRIMARY KEY` names, whether written inline on the
/// column or as a table constraint.
///
/// DEFERRED (documented): a multi-column primary key, which Go turns into a
/// clustered common handle (`IsCommonHandle`); an expression or
/// prefix-length key; and `UNIQUE`/`KEY`/`FOREIGN KEY` constraints, which
/// need the index tier. All are rejected rather than silently dropped, so a
/// table never claims a constraint it does not enforce.
fn primary_key_column(
    create: &tidb_ast::CreateTableStmt,
) -> Result<Option<Vec<String>>, DriverError> {
    let mut found: Option<Vec<String>> = None;
    for def in &create.columns {
        for option in &def.options {
            if let tidb_ast::ColumnOption::InlineKey(key) = option {
                match key.kind {
                    tidb_ast::InlineKeyKind::Primary { .. } => {
                        if found.is_some() {
                            return Err(DriverError::Unsupported(
                                "a table may define only one primary key",
                            ));
                        }
                        found = Some(vec![def.name.clone()]);
                    }
                    // A unique key is collected by `table_indexes`.
                    tidb_ast::InlineKeyKind::Unique => {}
                }
            }
        }
    }
    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            return Err(DriverError::Unsupported(
                "only key table constraints are supported yet",
            ));
        };
        if index.kind != tidb_ast::IndexConstraintKind::PrimaryKey {
            // Unique and secondary keys are collected by `table_indexes`.
            continue;
        }
        if found.is_some() {
            return Err(DriverError::Unsupported(
                "a table may define only one primary key",
            ));
        }
        let mut names = Vec::with_capacity(index.parts.len());
        for part in &index.parts {
            let tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } = part
            else {
                return Err(DriverError::Unsupported(
                    "an expression primary key is not supported yet",
                ));
            };
            if prefix_len.is_some() {
                return Err(DriverError::Unsupported(
                    "a prefix-length primary key is not supported yet",
                ));
            }
            names.push(name.clone());
        }
        found = Some(names);
    }
    Ok(found)
}

/// `str_to_type`, flen/decimal from numeric type arguments, unsigned flag.
fn field_type_of(def: &ColumnDef) -> Result<FieldType, DriverError> {
    let code = str_to_type(&def.ty.name.to_lowercase());
    if code == FieldTypeCode::Unspecified {
        return Err(DriverError::Unsupported("unsupported column type"));
    }
    let mut builder = FieldTypeBuilder::new().with_code(code);
    // Go `setCharsetCollationFlenDecimal`'s `TypeJSON` arm: a JSON column is
    // a BINARY-charset column carrying `BinaryFlag`, which is what makes the
    // wire report `charset=binary, flag=128` for it rather than the table's
    // utf8mb4 default. The document's own text is still UTF-8.
    if code == FieldTypeCode::Json {
        builder = builder
            .charset_set("binary")
            .collation_set("binary")
            .add_flags(FieldTypeFlags::BINARY);
    }
    let mut numeric_args = def.ty.args.iter().filter_map(|arg| match arg {
        ColumnTypeArg::Text(text) => text.parse::<i64>().ok(),
        ColumnTypeArg::Bytes(_) => None,
    });
    if let Some(flen) = numeric_args.next() {
        builder = builder.flen_set(flen);
    }
    if let Some(decimal) = numeric_args.next() {
        builder = builder.decimal_set(decimal);
    }
    if def.ty.unsigned {
        builder = builder.add_flags(FieldTypeFlags::UNSIGNED);
    }
    Ok(builder.build())
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    run_create_table_in(sql, catalog, tidb_executor_default_database())
}

/// The default schema an unqualified `CREATE TABLE` lands in.
fn tidb_executor_default_database() -> &'static str {
    crate::driver::DEFAULT_DATABASE
}

/// [`run_create_table_on`] creating the table in `current_db`.
pub fn run_create_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let create = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(create) => create,
            _ => {
                return Err(DriverError::Unsupported(
                    "only CREATE TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only CREATE TABLE is supported here",
            ))
        }
    };

    if create.like_table.is_some() {
        return Err(DriverError::Unsupported("CREATE TABLE LIKE is deferred"));
    }
    if create.columns.is_empty() {
        return Err(DriverError::Unsupported("a table needs columns"));
    }

    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name);
    if catalog.contains_in(&database, name) {
        if create.if_not_exists {
            return Ok(false);
        }
        return Err(DriverError::Unsupported("table already exists"));
    }

    // Build the ColumnInfos (ids 1..n, offsets in definition order).
    let mut columns = Vec::with_capacity(create.columns.len());
    for (i, def) in create.columns.iter().enumerate() {
        let field_type = field_type_of(def)?;
        let mut col = ColumnInfo::new((i + 1) as i64, &def.name, field_type);
        col.offset = i as i32;
        columns.push(col);
    }

    // The primary key, written either inline on a column or as a table
    // constraint.
    for (i, def) in create.columns.iter().enumerate() {
        if def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull))
        {
            columns[i].add_flag(NOT_NULL_FLAG);
        }
    }

    // Go rejects a second auto column with ErrWrongAutoKey (1075) and a
    // non-integer one with "Incorrect column specifier"; captured from real
    // TiDB, which -- unlike MySQL -- does NOT require the column to be a key.
    let mut auto_increment_offset = None;
    for (i, def) in create.columns.iter().enumerate() {
        if !def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement))
        {
            continue;
        }
        if auto_increment_offset.is_some() {
            return Err(DriverError::WrongAutoKey);
        }
        if !is_int_column(&columns[i]) {
            return Err(DriverError::WrongColumnSpecifier(def.name.clone()));
        }
        // An auto-increment column is implicitly NOT NULL and carries Go's
        // AutoIncrementFlag.
        columns[i].add_flag(NOT_NULL_FLAG | AUTO_INCREMENT_FLAG);
        auto_increment_offset = Some(i);
    }

    let primary_key = primary_key_column(create)?;
    let pk_offsets: Vec<usize> = match &primary_key {
        Some(names) => {
            let mut offsets = Vec::with_capacity(names.len());
            for name in names {
                offsets.push(
                    columns
                        .iter()
                        .position(|col| col.name.original().eq_ignore_ascii_case(name))
                        .ok_or(DriverError::Unsupported(
                            "the primary key names a column the table does not define",
                        ))?,
                );
            }
            offsets
        }
        None => Vec::new(),
    };
    let pk_offset = if pk_offsets.len() == 1 {
        Some(pk_offsets[0])
    } else {
        None
    };
    // Go `isSingleIntPK` + `ShouldBuildClusteredIndex`: a single-column
    // integer primary key becomes the row handle rather than a separate
    // index, which is what `TableInfo.PKIsHandle` records.
    let pk_is_handle = pk_offset.is_some_and(|offset| is_int_column(&columns[offset]));
    // Go `ShouldBuildClusteredIndex` under the default clustered-index mode:
    // a primary key that is not a single integer column becomes a clustered
    // COMMON handle, whose encoding is the row key.
    let common_handle_offsets: Vec<usize> = if pk_is_handle || pk_offsets.is_empty() {
        Vec::new()
    } else {
        pk_offsets.clone()
    };
    for offset in &pk_offsets {
        // Go `checkIndexColumn` reaches a primary key too, and a clustered
        // primary key never becomes an entry in `table_indexes` (its
        // encoding IS the row key), so the JSON refusal has to be repeated
        // on this path rather than being left to the index builder.
        if columns[*offset].field_type.code() == FieldTypeCode::Json {
            return Err(DriverError::JsonUsedInKey(
                columns[*offset].name.original().to_owned(),
            ));
        }
        // A primary key column is implicitly NOT NULL, as in MySQL, and Go
        // marks it PRI (mysql.NotNullFlag, mysql.PriKeyFlag).
        columns[*offset].add_flag(NOT_NULL_FLAG | PRI_KEY_FLAG);
    }

    // Go evaluates a constant DEFAULT at DDL time and stores the value on the
    // ColumnInfo; a NOT NULL column with no DEFAULT keeps NoDefaultValueFlag,
    // which is the `None` case here.
    let mut defaults: Vec<Option<Datum>> = Vec::with_capacity(create.columns.len());
    for def in &create.columns {
        let mut default_value = None;
        for option in &def.options {
            match option {
                tidb_ast::ColumnOption::Default(expr) => {
                    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                        expr,
                        &tidb_expr::rewriter::NoResolver,
                    )
                    .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                    let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                        return Err(DriverError::Unsupported(
                            "an expression DEFAULT is not supported yet",
                        ));
                    };
                    let value = constant
                        .eval()
                        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                    // Go normalizes and checks the written default against
                    // the column's own type at DDL time.
                    default_value = Some(normalize_column_default(
                        value,
                        &columns[defaults.len()].field_type,
                        &def.name,
                    )?);
                }
                // AUTO_INCREMENT is its own value source, handled below.
                tidb_ast::ColumnOption::AutoIncrement => {}
                tidb_ast::ColumnOption::Generated { .. } => {
                    return Err(DriverError::Unsupported(
                        "generated columns are not supported yet",
                    ))
                }
                _ => {}
            }
        }
        defaults.push(default_value);
    }

    let info = TableInfo {
        id: catalog.allocate_table_id(),
        name: CiString::new(name),
        columns,
        pk_is_handle,
        ..TableInfo::default()
    };

    let kv_columns: Vec<KvColumn> = info
        .columns
        .iter()
        .map(|c| KvColumn {
            name: c.name.original().to_owned(),
            id: c.id,
            field_type: c.field_type.clone(),
            default_value: defaults[c.offset as usize].clone(),
            // A column present at CREATE TABLE has no pre-existing rows.
            origin_default: None,
        })
        .collect();
    let table = KvTable::new(info.id, kv_columns);
    let mut table = table;
    table.set_name(name);
    if pk_is_handle {
        if let Some(offset) = pk_offset {
            table.set_pk_handle_offset(offset);
        }
    } else if !common_handle_offsets.is_empty() {
        table.set_common_handle_offsets(common_handle_offsets.clone());
    }
    let clustered = pk_is_handle || !common_handle_offsets.is_empty();
    if let Some(offset) = auto_increment_offset {
        table.set_auto_increment_offset(offset);
    }
    for index in table_indexes(create, &info.columns, clustered)? {
        table.add_index(index);
    }
    catalog.register_kv_in(&database, name, table);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = Catalog::default();
        assert!(run_create_table_on(
            "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))",
            &mut catalog
        )
        .unwrap());

        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a, s) VALUES (7, 'x')",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        let rows = run_select_on(
            "SELECT a, s FROM t",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Datum::Int(7));
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"x"),
            Datum::String(s) => assert_eq!(s.bytes(), b"x"),
            other => panic!("unexpected string datum {other:?}"),
        }
    }

    #[test]
    fn if_not_exists_and_duplicates() {
        let mut catalog = Catalog::default();
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap());
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).is_err());
        assert!(
            !run_create_table_on("CREATE TABLE IF NOT EXISTS t (a INT)", &mut catalog).unwrap()
        );
    }

    /// A single-column integer primary key sets Go's `PKIsHandle`; a
    /// non-integer one does not (`isIntCol`).
    #[test]
    fn primary_key_sets_the_handle_flag_only_for_an_integer_column() {
        let mut catalog = Catalog::default();
        run_create_table_on("CREATE TABLE t (a INT, PRIMARY KEY (a))", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE s (a VARCHAR(4) PRIMARY KEY)", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE h (a INT)", &mut catalog).unwrap();

        let handle_offset = |name: &str| match catalog.get_table_for_test(name) {
            Some(crate::TableEntry::Kv(kv)) => kv.pk_handle_offset(),
            _ => panic!("expected a kv table"),
        };
        assert_eq!(handle_offset("t"), Some(0));
        assert_eq!(handle_offset("s"), None, "a string PK is not a handle");
        assert_eq!(handle_offset("h"), None, "no PK, no handle column");
    }
}
