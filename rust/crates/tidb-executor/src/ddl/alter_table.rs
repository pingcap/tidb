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

//! `ALTER TABLE`: the dispatcher and the per-action work that changes an
//! existing table's columns and options in place.
//!
//! Inside: [`run_alter_table_in`], which applies the statement's actions in
//! source order so a failing action leaves the earlier ones applied;
//! [`add_column_action`], [`modify_column_action`] and
//! [`drop_column_action`], the three column changes, including the read-time
//! `OriginDefaultValue` fill that gives already-written rows a new column's
//! DEFAULT without rewriting their bytes; [`add_foreign_key_action`] and
//! [`drop_foreign_key_action`], which let a constraint be declared and
//! withdrawn after the table exists; [`set_table_options_action`] for
//! the table-level options an ALTER may set; and the two helpers
//! [`normalize_column_default`] and [`existing_table_charset`] that the
//! column actions share. Each doc comment records the captured TiDB error
//! code (1060, 1090, 1091, 8200).
//!
//! Mirrors Go `pkg/ddl/column.go` (`AddColumn`, `ModifyColumn`, `DropColumn`)
//! reached through `pkg/ddl/ddl_api.go`'s `AlterTable` action loop. The index
//! actions an ALTER can also carry are in the sibling `indexes` module, and
//! the type/charset resolution both share lives in the parent.

use super::column_types::{field_type_of, NOT_NULL_FLAG};
use super::indexes::{add_index_to_table, drop_index_from_table, is_visible};
use super::{
    auto_increment_option, Catalog, ColumnDef, DdlStmt, DriverError, KvColumn, Stmt, TableCharset,
};
use tidb_datatype::{Datum, FieldType};

/// Runs an `ALTER TABLE`, applying its actions in source order.
///
/// The rules are captured from TiDB: `ADD COLUMN ... DEFAULT d` gives rows
/// written earlier the value `d` rather than NULL, without rewriting them;
/// `FIRST`/`AFTER` place the column; a duplicate name is 1060, dropping an
/// unknown column is 1091, dropping the last column is 1090, and dropping an
/// integer primary key is TiDB's own 8200.
///
/// DEFERRED (documented): every ALTER action this match does not name --
/// partition changes above all -- is rejected rather than silently accepted,
/// and dropping a column an index uses is rejected rather than leaving the
/// index addressing a column that is gone.
pub fn run_alter_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let stmt = ctx.parse(sql)?;
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

    // A constraint addresses its own side by column offset and the other side
    // by table name, so a layout change or a rename would silently repoint
    // it. Refused rather than corrupted; see `foreign_key::participates`.
    let participates = crate::foreign_key::participates(catalog, &database, &name);
    for action in &alter.actions {
        if participates
            && matches!(
                action,
                tidb_ast::AlterTableAction::AddColumn { .. }
                    | tidb_ast::AlterTableAction::AddColumns { .. }
                    | tidb_ast::AlterTableAction::ModifyColumn { .. }
                    | tidb_ast::AlterTableAction::ChangeColumn { .. }
                    | tidb_ast::AlterTableAction::DropColumn { .. }
                    | tidb_ast::AlterTableAction::RenameTable { .. }
                    // A foreign key names its REFERENCED columns by name, so
                    // renaming one would silently repoint the constraint.
                    | tidb_ast::AlterTableAction::RenameColumn(_)
            )
        {
            return Err(DriverError::Unsupported(
                "changing the columns or name of a table involved in a FOREIGN KEY is not supported yet",
            ));
        }
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
                crate::ddl::indexes::reject_partial_index(&index.options)?;
                // Go `GetName4AnonymousIndex`: an unnamed index takes its
                // first key part's column name, or `expression_index` when
                // that part is an expression and so has no column name.
                let index_name = index
                    .name
                    .clone()
                    .unwrap_or_else(|| match index.parts.first() {
                        Some(tidb_ast::IndexPart::Column { name, .. }) => name.clone(),
                        Some(tidb_ast::IndexPart::Expr { .. }) => "expression_index".to_owned(),
                        None => String::new(),
                    });
                add_index_to_table(
                    catalog,
                    &database,
                    &name,
                    super::indexes::IndexSpec {
                        name: &index_name,
                        unique,
                        parts: &index.parts,
                        visible: is_visible(&index.options),
                    },
                    ctx,
                )?;
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
            tidb_ast::AlterTableAction::AddForeignKey(definition) => {
                add_foreign_key_action(catalog, &database, &name, definition, ctx)?;
            }
            tidb_ast::AlterTableAction::DropForeignKey(drop) => {
                drop_foreign_key_action(catalog, &database, &name, &drop.name)?;
            }
            tidb_ast::AlterTableAction::SetTableOptions { options } => {
                set_table_options_action(catalog, &database, &name, options)?;
            }
            // The four metadata-only actions: a name or a flag changes while
            // every column id, column offset and index entry stays put. See
            // the `alter_metadata` module doc for why they belong together.
            tidb_ast::AlterTableAction::RenameColumn(rename) => {
                super::alter_metadata::rename_column_action(
                    catalog,
                    &database,
                    &name,
                    &rename.from,
                    &rename.to,
                )?;
            }
            tidb_ast::AlterTableAction::RenameIndex(rename) => {
                super::alter_metadata::rename_index_action(
                    catalog,
                    &database,
                    &name,
                    &rename.from,
                    &rename.to,
                )?;
            }
            tidb_ast::AlterTableAction::AlterIndexVisibility(alter) => {
                super::alter_metadata::alter_index_visibility_action(
                    catalog,
                    &database,
                    &name,
                    &alter.name,
                    alter.visibility != tidb_ast::IndexVisibility::Invisible,
                )?;
            }
            tidb_ast::AlterTableAction::AlterColumnDefault(alter) => {
                let column = alter
                    .name
                    .last()
                    .ok_or(DriverError::Unsupported("empty ALTER COLUMN name"))?;
                super::alter_metadata::alter_column_default_action(
                    catalog,
                    &database,
                    &name,
                    column,
                    alter.default_value.as_ref(),
                )?;
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

/// One `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY ...`.
///
/// Go `executor.CreateForeignKey` (`pkg/ddl/executor.go`) plus the job it
/// submits (`onCreateForeignKey`, `pkg/ddl/foreign_key.go`), in the order Go
/// performs them:
///
/// 1. An unnamed constraint is `fk_{MaxForeignKeyID+1}`, and a name the table
///    already declares is 1826 (`checkFKDupName`). That check runs before any
///    reference is resolved, so it fires with `foreign_key_checks` at 0 too.
/// 2. The constraint is built by the SAME [`build_foreign_key`] a
///    `CREATE TABLE` uses, so the DDL-time rules -- 3733 for a virtual
///    generated column on either side, 3104 for an action that would write a
///    stored one, the parent lookup behind `foreign_key_checks` -- hold
///    identically whichever statement declared it.
/// 3. A constraint whose referencing columns no index covers gets one, named
///    after the constraint, exactly as `CREATE TABLE` does. Go creates it
///    here as a real `createIndex` before the job is submitted, which is why
///    the missing-index error inside `checkAddForeignKeyValidInOwner` is not
///    reachable from this path.
/// 4. The rows the table ALREADY holds are checked against the new
///    constraint, and an orphan is 1452 -- see
///    [`crate::foreign_key::require_existing_rows`]. `foreign_key_checks = 0`
///    skips this and only this step, which is how a constraint can be
///    declared over data that does not satisfy it.
fn add_foreign_key_action(
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... ADD FOREIGN KEY needs a storage-backed table",
        ));
    };
    let fk_name = definition
        .name
        .clone()
        .unwrap_or_else(|| table.next_foreign_key_name());
    if table
        .foreign_keys()
        .iter()
        .any(|key| key.name.eq_ignore_ascii_case(&fk_name))
    {
        return Err(DriverError::FkDupName(fk_name));
    }
    let columns: Vec<super::table_constraints::FkColumn> = table
        .columns
        .iter()
        .map(|column| super::table_constraints::FkColumn {
            name: column.name.clone(),
            generated_stored: column.generated.as_ref().map(|generated| generated.stored),
        })
        .collect();
    let clustered: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    };
    let foreign_key = super::table_constraints::build_foreign_key(
        definition,
        fk_name,
        &columns,
        catalog,
        database,
        ctx.foreign_key_checks(),
    )?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... ADD FOREIGN KEY needs a storage-backed table",
        ));
    };
    // Consumed before the rows are read, and NOT given back when they reject
    // the constraint -- see [`crate::kv_table::KvTable::allocate_foreign_key_id`].
    table.allocate_foreign_key_id();
    if ctx.foreign_key_checks() {
        // Go runs this check inside the job, after the constraint and its
        // index are staged, and ROLLS BOTH BACK when it fails: captured, a
        // rejected `ADD FOREIGN KEY` leaves neither the constraint nor the
        // index it would have created. Checking first reaches that state
        // without staging anything to undo.
        crate::foreign_key::require_existing_rows(catalog, database, name, &foreign_key)?;
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... ADD FOREIGN KEY needs a storage-backed table",
        ));
    };
    // Go `CreateForeignKey`'s `createIndex` arm: an existing key whose columns
    // START with the referencing ones already serves the constraint, the
    // clustered handle included; otherwise TiDB adds one named after the
    // constraint. The index a DROPPED constraint left behind counts, which is
    // why re-adding a constraint over the same columns adds no second key.
    let covered = |offsets: &[usize]| offsets.starts_with(&foreign_key.cols);
    if !covered(&clustered)
        && !table
            .indexes()
            .iter()
            .any(|index| covered(&index.column_offsets))
    {
        let id = table.next_index_id();
        table.add_index(super::KvIndex {
            id,
            name: foreign_key.name.clone(),
            unique: false,
            column_offsets: foreign_key.cols.clone(),
            visible: true,
        });
    }
    table.add_foreign_key(foreign_key);
    Ok(())
}

/// One `ALTER TABLE ... DROP FOREIGN KEY name`.
///
/// Go `executor.DropForeignKey` looks the name up and raises
/// `infoschema.ErrForeignKeyNotExists` -- which is `ErrCantDropFieldOrKey`,
/// 1091 with the "check that column/key exists" message -- when the table
/// declares no such constraint. The index the constraint relied on is NOT
/// dropped with it (Go `dropForeignKey` touches only `TableInfo.ForeignKeys`),
/// so `SHOW CREATE TABLE` keeps printing the auto-created key afterwards and
/// a later `ADD FOREIGN KEY` over the same columns reuses it.
fn drop_foreign_key_action(
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    fk_name: &str,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... DROP FOREIGN KEY needs a storage-backed table",
        ));
    };
    if !table.drop_foreign_key(fk_name) {
        return Err(DriverError::UnknownColumnInAlter(fk_name.to_owned()));
    }
    Ok(())
}

/// One `ALTER TABLE ... <table options>`.
///
/// Only `AUTO_INCREMENT=` is carried: Go rebases the allocator, which moves
/// the counter up to the named value and leaves it alone when it has already
/// run past. Any other option is refused rather than silently accepted.
fn set_table_options_action(
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    options: &[tidb_ast::TableOption],
) -> Result<(), DriverError> {
    let seed = auto_increment_option(options)?;
    if options.len() != usize::from(seed.is_some()) {
        return Err(DriverError::Unsupported(
            "this ALTER TABLE table option is not supported yet",
        ));
    }
    let Some(seed) = seed else { return Ok(()) };
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... AUTO_INCREMENT needs a storage-backed table",
        ));
    };
    if table.auto_increment_offset().is_none() {
        // Go `ErrInvalidAutoRandom`-adjacent path: without an auto column
        // there is no allocator to rebase.
        return Err(DriverError::Unsupported(
            "ALTER TABLE ... AUTO_INCREMENT needs an AUTO_INCREMENT column",
        ));
    }
    table
        .rebase_auto_increment(seed)
        .map_err(|error| DriverError::AutoIdUnavailable(error.0))?;
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
pub(crate) fn normalize_column_default(
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
    // Go `checkDefaultValue`'s two arms for a NULL default, in Go's order: a
    // column that cannot HOLD NULL cannot DEFAULT to it, and a primary key
    // says so with its own error rather than 1067.
    if value.is_null() {
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
            return Err(DriverError::PrimaryCantHaveNull);
        }
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
            return Err(DriverError::InvalidDefault(column.to_owned()));
        }
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
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set => {
            enum_set_column_default(&value, field_type).ok_or_else(invalid)?
        }
        tidb_datatype::FieldTypeCode::Bit => bit_column_default(&value, field_type, column)?,
        _ => value.clone(),
    };
    // The check phase: strict conversion of what will be stored.
    let checked = normalized
        .convert_to(field_type, tidb_datatype::STRICT_FLAGS)
        .map_err(|_| invalid())?;
    if checked
        .event
        .as_ref()
        .is_some_and(|event| !crate::driver::conversion_event_is_silent(event))
    {
        return Err(invalid());
    }
    Ok(normalized)
}

/// An `ENUM`/`SET` column's written `DEFAULT`, resolved to a MEMBER of the
/// column's own element list, which is the only thing the column can hold.
///
/// Go `pkg/ddl/add_column.go` reaches the same value by three different
/// routes, and the route is chosen by what the default was WRITTEN as:
///
///  * a hex/bit literal (`DEFAULT 0x61`) is decoded to text in the column's
///    charset and taken verbatim -- `getDefaultValue`'s `KindBinaryLiteral`
///    branch returns before the type switch, so the bytes name the member
///    directly and are never read as a number;
///  * an integer (`DEFAULT 2`) is an INDEX: one-based into the element list
///    for `ENUM` (`getEnumDefaultValue` -> `ParseEnumValue`), a bit mask for
///    `SET` (`ParseSetValue`);
///  * anything else is text matched against the element list under the
///    column's collation, with trailing spaces stripped first for `ENUM`
///    because "trailing spaces are automatically deleted from ENUM member
///    values" (`getEnumDefaultValue` -> `TrimRight` -> `ParseEnumName`), and
///    with the empty string admitted for `SET` as the no-members-set value.
///
/// `None` means no member matches, which the caller reports as 1067. Storing
/// the written literal instead -- the shape this replaced -- leaves a column
/// whose `DEFAULT` is not a value of its own type, so an omitted column on
/// `INSERT` reads something the element list does not contain.
/// A `BIT(n)` column's written `DEFAULT`, settled to the BITS it names.
///
/// Go `pkg/ddl/add_column.go` `getDefaultValue` reaches the same bits from two
/// spellings, and keeps neither of them verbatim:
///
///  * a bit or hex literal (`DEFAULT b'1100110111001'`, `DEFAULT 0x19b9`) is
///    read with `GetBinaryStringDecoded` in the column's charset, which for a
///    `BIT` column is `binary` and so hands back the literal's own bytes;
///  * an INTEGER (`DEFAULT 250`) becomes `NewBinaryLiteralFromUint(v, -1)`,
///    the number's minimal big-endian bytes.
///
/// Go then stores those bytes as the column's `DefaultValue` string, and every
/// surface that prints a default -- `SHOW CREATE TABLE`, `SHOW COLUMNS`,
/// `information_schema.columns` -- renders them with
/// `BinaryLiteral.ToBitLiteralString(true)`, so both spellings print back as
/// `b'11111010'`. Keeping the WRITTEN datum instead prints `DEFAULT '250'`,
/// which re-reads as the three characters `250` and not as the bits.
fn bit_column_default(
    value: &Datum,
    field_type: &FieldType,
    column: &str,
) -> Result<Datum, DriverError> {
    let invalid = || DriverError::InvalidDefault(column.to_owned());
    let bits = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(tidb_datatype::STRICT_FLAGS, field_type.charset_name())
                .into_parts();
            if error.is_some() {
                return Err(invalid());
            }
            tidb_datatype::BinaryLiteral::from(bytes)
        }
        Datum::Int(_) | Datum::UInt(_) => {
            let number = value
                .as_uint()
                .or_else(|| value.as_int().map(|value| value as u64))
                .ok_or_else(invalid)?;
            tidb_datatype::BinaryLiteral::from_uint(number, None)
        }
        // Go falls through to `v.ToString()` for every other kind and lets
        // the check phase decide, so the written value is kept here too.
        _ => return Ok(value.clone()),
    };
    Ok(Datum::BinaryLiteral(bits))
}

fn enum_set_column_default(value: &Datum, field_type: &FieldType) -> Option<Datum> {
    let elements = field_type.elems();
    let collation = field_type.collation();
    let is_set = field_type.code() == tidb_datatype::FieldTypeCode::Set;
    let member = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(
                    tidb_datatype::ConversionFlags::default(),
                    field_type.charset_name(),
                )
                .into_parts();
            if error.is_some() {
                return None;
            }
            String::from_utf8(bytes).ok()?
        }
        Datum::Int(_) | Datum::UInt(_) => {
            let index = value
                .as_uint()
                .or_else(|| value.as_int().map(|v| v as u64))?;
            if is_set {
                tidb_datatype::parse_set_value(elements, index)
                    .ok()?
                    .name()
                    .to_owned()
            } else {
                tidb_datatype::parse_enum_value(elements, index)
                    .ok()?
                    .name()
                    .to_owned()
            }
        }
        _ => {
            let text = value.sql_string().ok()?;
            if is_set {
                tidb_datatype::parse_set_name(elements, &text, collation)
                    .ok()?
                    .name()
                    .to_owned()
            } else {
                tidb_datatype::parse_enum_name(elements, text.trim_end_matches(' '), collation)
                    .ok()?
                    .name()
                    .to_owned()
            }
        }
    };
    Some(Datum::new_collation_string(member, collation))
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
/// The existing table's default charset/collation, which a column added or
/// modified by ALTER TABLE inherits just as a CREATE TABLE column does.
fn existing_table_charset(catalog: &Catalog, database: &str, table_name: &str) -> TableCharset {
    match catalog.table_in(database, table_name) {
        Some(crate::TableEntry::Kv(table)) => table.charset(),
        _ => TableCharset::default(),
    }
}

fn modify_column_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    old_name: &str,
    def: &ColumnDef,
    position: &tidb_ast::ColumnPosition,
    if_exists: bool,
) -> Result<(), DriverError> {
    let field_type = field_type_of(def, existing_table_charset(catalog, database, table_name))?;
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
    // An ALTER-written default is always a literal here: the expression forms
    // are refused above, so the settled value and the ORIGIN_DEFAULT existing
    // rows read back are the same value.
    let default_value = match default_value {
        Some(value) => Some(normalize_column_default(value, &field_type, &def.name)?),
        None => None,
    };
    let stored_default = default_value
        .clone()
        .map(crate::column_default::ColumnDefault::Value);
    let column = KvColumn {
        name: def.name.clone(),
        id: table.columns[offset].id,
        field_type,
        // A generated column option is refused above, so a MODIFY never
        // produces one.
        generated: None,
        default_value: stored_default,
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
    let field_type = field_type_of(def, existing_table_charset(catalog, database, table_name))?;
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
    // An ALTER-written default is always a literal here: the expression forms
    // are refused above, so the settled value and the ORIGIN_DEFAULT existing
    // rows read back are the same value.
    let default_value = match default_value {
        Some(value) => Some(normalize_column_default(value, &field_type, &def.name)?),
        None => None,
    };
    let stored_default = default_value
        .clone()
        .map(crate::column_default::ColumnDefault::Value);
    let id = table.next_column_id();
    let field_type_for_origin = field_type.clone();
    table.add_column(
        index,
        KvColumn {
            name: def.name.clone(),
            id,
            field_type,
            // As in MODIFY: `ALTER TABLE ... ADD COLUMN ... AS (...)` is
            // refused above rather than silently added as a plain column.
            generated: None,
            default_value: stored_default,
            // Rows written before this column existed read back the default.
            // A NOT NULL column with NO default reads back the TYPE's zero
            // instead of NULL: Go fills the backfill value through
            // `GetColOriginDefaultValueWithoutStrictSQLMode`, whose
            // `getColDefaultValueFromNil` takes the non-strict arm by
            // construction and returns `GetZeroValue`. Captured: after
            // `ALTER TABLE q1 ADD COLUMN cc SET('a','b','c','d') NOT NULL`
            // the pre-existing rows read `''`, and `dd INT NOT NULL` reads
            // `0` -- not NULL, which is why an ordinary UPDATE of such a row
            // does not trip the NOT NULL check.
            origin_default: default_value
                .or_else(|| not_null.then(|| crate::bad_null::zero_value(&field_type_for_origin))),
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
