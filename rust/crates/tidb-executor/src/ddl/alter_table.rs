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
use super::table_constraints::{AUTO_INCREMENT_FLAG, PRI_KEY_FLAG};
use super::{
    auto_increment_option, Catalog, ColumnDef, DdlStmt, DriverError, KvColumn, Stmt, TableCharset,
};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

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
                return Err(DriverError::unsupported(
                    "only ALTER TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::unsupported(
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

    // A constraint names its columns and its referenced table, and a DROP
    // COLUMN, a column RENAME or a table RENAME rewrites neither, so each
    // would leave the constraint naming something that is gone. Refused
    // rather than corrupted; see `foreign_key::participates`.
    //
    // ADD COLUMN is NOT in this set: Go's `AddColumn` asks nothing about
    // foreign keys, and a constraint that resolves its names at every use
    // (`KvTable::foreign_key_offsets`) survives the offsets moving.
    // MODIFY/CHANGE is not in it either -- it asks Go's own question in
    // `modify_column_action` below, which is narrower AND stricter than this
    // blanket refusal was: it lets a nullability change through and refuses
    // an incompatible type with Go's 3780/1832/1833 instead of 1105.
    let participates = crate::foreign_key::participates(catalog, &database, &name);
    for action in &alter.actions {
        if participates
            && matches!(
                action,
                tidb_ast::AlterTableAction::DropColumn { .. }
                    | tidb_ast::AlterTableAction::RenameTable { .. }
                    // A foreign key names its REFERENCED columns by name, so
                    // renaming one would silently repoint the constraint.
                    | tidb_ast::AlterTableAction::RenameColumn(_)
            )
        {
            return Err(DriverError::unsupported(
                "changing the columns or name of a table involved in a FOREIGN KEY is not supported yet",
            ));
        }
        match action {
            tidb_ast::AlterTableAction::AddColumn {
                column, position, ..
            } => add_column_action(catalog, &database, &name, column, position, ctx)?,
            tidb_ast::AlterTableAction::AddColumns {
                columns,
                constraints,
                ..
            } => {
                if !constraints.is_empty() {
                    return Err(DriverError::unsupported(
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
                        ctx,
                    )?;
                }
            }
            tidb_ast::AlterTableAction::ModifyColumn {
                if_exists,
                column,
                position,
            } => modify_column_action(
                catalog,
                &ModifyColumnRequest {
                    database: &database,
                    table_name: &name,
                    old_name: &column.name,
                    def: column,
                    position,
                    if_exists: *if_exists,
                    allow_remove_auto_inc: ctx.allow_remove_auto_inc(),
                },
                ctx,
            )?,
            tidb_ast::AlterTableAction::ChangeColumn {
                if_exists,
                old_name,
                column,
                position,
            } => {
                let old = old_name
                    .last()
                    .ok_or(DriverError::unsupported("empty CHANGE COLUMN name"))?;
                modify_column_action(
                    catalog,
                    &ModifyColumnRequest {
                        database: &database,
                        table_name: &name,
                        old_name: old,
                        def: column,
                        position,
                        if_exists: *if_exists,
                        allow_remove_auto_inc: ctx.allow_remove_auto_inc(),
                    },
                    ctx,
                )?;
            }
            tidb_ast::AlterTableAction::DropColumn {
                if_exists,
                name: column_name,
            } => drop_column_action(catalog, &database, &name, column_name, *if_exists, ctx)?,
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
                        return Err(DriverError::unsupported(
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
                        global: index.options.global,
                    },
                    ctx,
                )?;
            }
            // `ALTER TABLE x RENAME TO y` is the same operation as
            // `RENAME TABLE x TO y`.
            tidb_ast::AlterTableAction::RenameTable { new_name } => {
                let (to_db, to_name) = crate::driver::split_table_path_pub(new_name, current_db)?;
                let (to_db, to_name) = (to_db.to_owned(), to_name.to_owned());
                // Go checks the destination SCHEMA before the destination
                // table, and reports a missing one as 1025 with the source
                // left in place.
                if !catalog.has_database(&to_db) {
                    return Err(DriverError::Schema(
                        crate::SchemaErrorKind::RenameTargetDatabaseMissing {
                            from: format!("{database}.{name}"),
                            to: format!("{to_db}.{to_name}"),
                            database: to_db,
                        },
                    ));
                }
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
                drop_index_from_table(catalog, &database, &name, index_name, *if_exists, ctx)?;
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
                    .ok_or(DriverError::unsupported("empty ALTER COLUMN name"))?;
                super::alter_metadata::alter_column_default_action(
                    catalog,
                    &database,
                    &name,
                    column,
                    alter.default_value.as_ref(),
                    ctx,
                )?;
            }
            // `CHECK` constraints, under the `tidb_enable_check_constraint =
            // OFF` model this engine implements (see `crate::ddl`'s doc and
            // `run_create_table_in`). The variable is read by the SESSION,
            // which refuses `ADD CHECK` outright when it is ON and files the
            // per-action `tidb_enable_check_constraint is off` warning when it
            // is OFF; what is left here is what the DDL itself does.
            //
            // Captured from real TiDB with the variable OFF:
            //   alter table t3 add constraint cc check (a > 0)
            //     -> OK, Warning 1105, and SHOW CREATE TABLE is UNCHANGED
            //   insert into t3 values (-1)          -> OK (nothing enforces)
            //   alter table e alter constraint nope not enforced
            //     -> OK, Warning 1105 -- the name is NOT looked up
            //   alter table e drop constraint nope  -> ERROR 3940
            // The asymmetry in the last two is Go's, and is ported as
            // measured: DROP resolves the name and ALTER does not.
            tidb_ast::AlterTableAction::AddCheck(_) | tidb_ast::AlterTableAction::AlterCheck(_) => {
            }
            // No table in this engine can hold a CHECK constraint, so the
            // name never resolves -- which is the same answer Go gives with
            // the variable ON for a name that is not there (captured: 3940).
            tidb_ast::AlterTableAction::DropCheck(drop) => {
                return Err(DriverError::CheckConstraintNotExists(drop.name.clone()));
            }
            _ => {
                return Err(DriverError::unsupported(
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
        return Err(DriverError::unsupported(
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
        return Err(DriverError::unsupported(
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
        crate::foreign_key::require_existing_rows(
            catalog,
            database,
            name,
            &foreign_key,
            &ctx.session_zone(),
        )?;
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE ... ADD FOREIGN KEY needs a storage-backed table",
        ));
    };
    // Go `CreateForeignKey`'s `createIndex` arm: an existing key whose columns
    // START with the referencing ones already serves the constraint, the
    // clustered handle included; otherwise TiDB adds one named after the
    // constraint. The index a DROPPED constraint left behind counts, which is
    // why re-adding a constraint over the same columns adds no second key.
    // Go `IsIndexPrefixCovered`: a key part that stores only a PREFIX of its
    // column cannot answer the constraint's lookup, so it earns no exemption.
    let fk_offsets = table.foreign_key_offsets(&foreign_key).unwrap_or_default();
    let covered = |offsets: &[usize]| offsets.starts_with(&fk_offsets[..]);
    let column_flens: Vec<i64> = table
        .columns
        .iter()
        .map(|column| column.field_type.flen())
        .collect();
    let covered_index = |index: &super::KvIndex| {
        covered(&index.column_offsets)
            && fk_offsets.iter().enumerate().all(|(position, at)| {
                let length = index.prefix_length(position);
                length == crate::ddl::index_prefix::UNSPECIFIED_LENGTH
                    || column_flens.get(*at).is_some_and(|flen| length >= *flen)
            })
    };
    if !covered(&clustered) && !table.indexes().iter().any(covered_index) {
        let id = table.next_index_id();
        table.add_index(super::KvIndex {
            id,
            name: foreign_key.name.clone(),
            unique: false,
            column_offsets: fk_offsets.clone(),
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; fk_offsets.len()],
            visible: true,
            // A foreign key's auto-created index is local to the table it
            // constrains; Go's `FKInfo` carries no `GLOBAL` to record.
            global: false,
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
        return Err(DriverError::unsupported(
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
        return Err(DriverError::unsupported(
            "this ALTER TABLE table option is not supported yet",
        ));
    }
    let Some(seed) = seed else { return Ok(()) };
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE ... AUTO_INCREMENT needs a storage-backed table",
        ));
    };
    if table.auto_increment_offset().is_none() {
        // Go `ErrInvalidAutoRandom`-adjacent path: without an auto column
        // there is no allocator to rebase.
        return Err(DriverError::unsupported(
            "ALTER TABLE ... AUTO_INCREMENT needs an AUTO_INCREMENT column",
        ));
    }
    table
        .rebase_auto_increment(seed)
        .map_err(|error| DriverError::AutoIdUnavailable(error.0))?;
    Ok(())
}

/// Go `checkColumnDefaultValue` (`pkg/ddl/add_column.go:1212`), the BLOB /
/// TEXT / JSON arm, shared by every entry point that Go's `SetDefaultValue`
/// serves: CREATE TABLE, ADD COLUMN, MODIFY/CHANGE COLUMN and
/// ALTER COLUMN ... SET DEFAULT.
///
/// It answers Go's `(hasDefaultValue, value)` pair, and is a SEPARATE step
/// from [`normalize_column_default`] (Go's `getDefaultValue` +
/// `checkDefaultValue`), which runs after it over the value returned here.
///
/// Go, verbatim in shape:
///
/// - non-strict `sql_mode` AND an EMPTY-STRING default: warn 1101 and accept.
///   `BLOB`/`LONGBLOB` (which is where `TEXT`/`LONGTEXT` land) additionally
///   report `hasDefaultValue = false`; `JSON`'s default is rewritten to the
///   text `null`. `TINYBLOB`/`MEDIUMBLOB` and their TEXT spellings keep both
///   the default AND `hasDefaultValue`, which is not an oversight here --
///   Go's `if col.GetType() == mysql.TypeBlob || col.GetType() ==
///   mysql.TypeLongBlob` names only those two.
/// - anything else non-NULL on those types: 1101, in every mode.
///
/// Measured against TiDB (`sql_mode=''`):
///
/// ```text
/// create table n1 (c1 text not null default '')       -> `c1` text NOT NULL
/// create table n4 (c1 tinyblob not null default '')   -> `c1` tinyblob NOT NULL DEFAULT ''
/// create table n3 (c1 json not null default '')       -> `c1` json NOT NULL DEFAULT 'null'
/// create table e1 (c1 text default 'x')               -> ERROR 1101
/// ```
///
/// `hasDefaultValue = false` is not "drop the default": Go still STORES the
/// value, and only a NOT NULL column then takes `NoDefaultValueFlag`, which
/// is what makes `SHOW CREATE TABLE` print no DEFAULT clause. A NULLABLE
/// `text DEFAULT ''` keeps printing `DEFAULT ''`. This tier models that flag
/// as "no default recorded", so the pair maps onto `None` for a NOT NULL
/// column and onto the stored value otherwise.
pub(crate) fn check_column_default_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &crate::StmtContext,
) -> Result<(bool, Datum), DriverError> {
    use tidb_datatype::FieldTypeCode;

    if value.is_null()
        || !matches!(
            field_type.code(),
            FieldTypeCode::Json
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Blob
        )
    {
        return Ok((true, value));
    }
    let empty = matches!(value.as_raw_bytes(), Some(bytes) if bytes.is_empty());
    if ctx.strict() || !empty {
        return Err(DriverError::BlobCantHaveDefault(column.to_owned()));
    }
    let reported = DriverError::BlobCantHaveDefault(column.to_owned()).to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(match field_type.code() {
        FieldTypeCode::Blob | FieldTypeCode::LongBlob => (false, value),
        FieldTypeCode::Json => (true, Datum::new_string("null")),
        _ => (true, value),
    })
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
    // The SESSION's zone. Go's `checkDefaultValue` evaluates the written
    // default through `table.CastValue` with the statement's own type
    // context, and a `TIMESTAMP` column's admissible range is a wall-clock
    // range, so `DEFAULT '2038-01-19 03:14:07'` is accepted at `+00:00` and
    // refused at `-08:00`.
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    // The BLOB/TEXT/JSON rule does NOT live here: it is Go's
    // `checkColumnDefaultValue`, a SEPARATE step that runs before this one at
    // every entry point (see [`check_column_default_value`]). Answering it
    // twice, once as a blanket refusal, is what used to refuse the very
    // `null` that a non-strict JSON default is rewritten to.
    //
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
        // Go's `KindBinaryLiteral || KindMysqlBit` branch runs BEFORE its type
        // switch and covers the string types alongside `BIT`, `ENUM` and
        // `SET`: `DEFAULT 0x61` on a `VARCHAR` is the text `a` decoded in the
        // column's charset, not a hex literal the printers have no rendering
        // for.
        tidb_datatype::FieldTypeCode::String
        | tidb_datatype::FieldTypeCode::Varchar
        | tidb_datatype::FieldTypeCode::VarString => {
            let decoded = match &value {
                Datum::BinaryLiteral(_) | Datum::Bit(_) => {
                    let (bytes, error) = value
                        .binary_string_decoded(
                            tidb_datatype::STRICT_FLAGS,
                            field_type.charset_name(),
                        )
                        .into_parts();
                    if error.is_some() {
                        return Err(invalid());
                    }
                    Datum::new_collation_string(bytes, field_type.collation())
                }
                _ => value.clone(),
            };
            pad_fixed_width_binary_default(decoded, field_type)
        }
        _ => value.clone(),
    };
    // The check phase: strict conversion of what will be stored.
    let checked = normalized
        .convert_to_in(field_type, tidb_datatype::STRICT_FLAGS, zone)
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
/// Go `setDefaultValueWithBinaryPadding`: a FIXED-width binary column pads its
/// stored `DEFAULT` with NUL bytes out to the declared width, exactly as a
/// value written into `BINARY(n)` is padded. `VARBINARY` and every non-binary
/// charset are variable width and keep the default as written.
///
/// Without it, `BINARY(4) DEFAULT 0x61` records a one-byte default for a
/// column that can only ever hold four.
fn pad_fixed_width_binary_default(value: Datum, field_type: &FieldType) -> Datum {
    if field_type.code() != tidb_datatype::FieldTypeCode::String
        || field_type.charset() != tidb_datatype::Charset::Binary
    {
        return value;
    }
    let width = field_type.flen();
    let Some(bytes) = value.as_raw_bytes() else {
        return value;
    };
    if width < 0 || bytes.len() >= width as usize {
        return value;
    }
    let mut padded = bytes.to_vec();
    padded.resize(width as usize, 0);
    Datum::new_collation_string(padded, field_type.collation())
}

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
/// NULL/NOT NULL/DEFAULT/AUTO_INCREMENT that CREATE TABLE also rejects here.
/// A KEY or UNIQUE option lands in that last group, which is Go's rule too:
/// MODIFY may keep a constraint but never ADD one.
///
/// NOT ENFORCED (measured, pinned in `tidb-session`'s `tests_alter_column`):
/// Go's `ErrTooLongKey` (1071) when the new type widens a column an index
/// covers past the key-length limit.
/// The existing table's default charset/collation, which a column added or
/// modified by ALTER TABLE inherits just as a CREATE TABLE column does.
fn existing_table_charset(catalog: &Catalog, database: &str, table_name: &str) -> TableCharset {
    match catalog.table_in(database, table_name) {
        Some(crate::TableEntry::Kv(table)) => table.charset(),
        _ => TableCharset::default(),
    }
}

/// Go `checkTypeChangeSupported` (`pkg/types/field_type.go:1569-1603`):
/// five ORIGIN/TARGET type-pair refusals that are unconditional -- each is a
/// TiDB `// TODO: ... not support yet, should fix here after supported`, not
/// a rule about the DATA in any particular row. Reached only when the two
/// types differ (Go's caller, `CheckModifyTypeCompatible`, only calls this in
/// its "different type" branch; the same-type precision/elems checks live
/// beside the caller, not here).
///
/// The five arms, transcribed in Go's own order:
/// 1. `{date/datetime/timestamp, TIME, YEAR, any string type, JSON} -> BIT`.
/// 2. `{date/datetime/timestamp, TIME, YEAR, DECIMAL, FLOAT, DOUBLE, JSON,
///    BIT} -> {ENUM, SET}`. Note the asymmetry with rule 3: TIME/YEAR are
///    origins here but not there, because rule 3's TIME-as-target case is
///    covered by rule 5 instead for DURATION specifically.
/// 3. `{ENUM, SET, BIT, DECIMAL, FLOAT, DOUBLE} -> date/datetime/timestamp`.
///    DURATION and YEAR are deliberately NOT origins of this rule -- rule 5
///    is what refuses DURATION as a target, and YEAR -> date/datetime/
///    timestamp is accepted by Go.
/// 4. TiDB `VECTOR` (`TypeTiDBVectorFloat32`) as EITHER side.
/// 5. `{ENUM, SET, BIT} -> TIME (DURATION)`.
///
/// Everything else this function is asked about is accepted HERE -- the
/// per-row `convert_to` gate in `KvTable::modify_column` still gets the last
/// word for any row that will not fit the new type.
fn check_type_change_supported(origin: &FieldType, to: &FieldType) -> Result<(), DriverError> {
    let (from_code, to_code) = (origin.code(), to.code());
    if from_code == to_code {
        // Go only reaches `checkTypeChangeSupported` from the "different
        // type" branch of `CheckModifyTypeCompatible`; a same-type MODIFY
        // (e.g. widening a `decimal`'s precision) is judged by other rules
        // this tier applies elsewhere, not by this table.
        return Ok(());
    }

    let is_time_like_origin = |code: FieldTypeCode| {
        code.is_type_time()
            || code == FieldTypeCode::Duration
            || code == FieldTypeCode::Year
            || code.is_string()
            || code == FieldTypeCode::Json
    };
    let refused =
        // Rule 1.
        (is_time_like_origin(from_code) && to_code == FieldTypeCode::Bit)
        // Rule 2.
        || ((from_code.is_type_time()
            || from_code == FieldTypeCode::Duration
            || from_code == FieldTypeCode::Year
            || matches!(
                from_code,
                FieldTypeCode::NewDecimal
                    | FieldTypeCode::Float
                    | FieldTypeCode::Double
                    | FieldTypeCode::Json
                    | FieldTypeCode::Bit
            ))
            && matches!(to_code, FieldTypeCode::Enum | FieldTypeCode::Set))
        // Rule 3.
        || (matches!(
            from_code,
            FieldTypeCode::Enum
                | FieldTypeCode::Set
                | FieldTypeCode::Bit
                | FieldTypeCode::NewDecimal
                | FieldTypeCode::Float
                | FieldTypeCode::Double
        ) && to_code.is_type_time())
        // Rule 4.
        || from_code == FieldTypeCode::VectorFloat32
        || to_code == FieldTypeCode::VectorFloat32
        // Rule 5.
        || (matches!(
            from_code,
            FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
        ) && to_code == FieldTypeCode::Duration);

    if refused {
        return Err(DriverError::UnsupportedModifyColumnType {
            from: origin.compact_str(false),
            to: to.compact_str(false),
        });
    }
    Ok(())
}

/// What one `MODIFY COLUMN` / `CHANGE COLUMN` action states, plus the session
/// facts it is decided against. Grouped because the old column's own
/// definition is only half the input: the rest is what the STATEMENT says and
/// what the SESSION allows.
struct ModifyColumnRequest<'a> {
    database: &'a str,
    table_name: &'a str,
    /// The column being modified, which `CHANGE COLUMN` may rename.
    old_name: &'a str,
    def: &'a ColumnDef,
    position: &'a tidb_ast::ColumnPosition,
    if_exists: bool,
    /// `@@tidb_allow_remove_auto_inc`.
    allow_remove_auto_inc: bool,
}

fn modify_column_action(
    catalog: &mut Catalog,
    request: &ModifyColumnRequest<'_>,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let zone = &ctx.session_zone();
    let &ModifyColumnRequest {
        database,
        table_name,
        old_name,
        def,
        position,
        if_exists,
        allow_remove_auto_inc,
    } = request;
    let field_type = field_type_of(def, existing_table_charset(catalog, database, table_name))?;
    let mut default_value = None;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                    expr,
                    &tidb_expr::rewriter::ZonedNoResolver(zone.clone()),
                )
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                    return Err(DriverError::unsupported(
                        "an expression DEFAULT is not supported yet",
                    ));
                };
                default_value = Some(
                    constant
                        .eval()
                        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?,
                );
            }
            tidb_ast::ColumnOption::NotNull
            | tidb_ast::ColumnOption::Null
            // AUTO_INCREMENT is legal here as long as the column already has
            // it; the set/remove rules are checked below, once the old column
            // is in hand.
            | tidb_ast::ColumnOption::AutoIncrement
            // Whether the generated-ness is allowed to change is a question
            // about the OLD column, so it is asked below once that column is
            // in hand.
            | tidb_ast::ColumnOption::Generated { .. } => {}
            _ => {
                return Err(DriverError::unsupported(
                    "this column option is not supported in ALTER TABLE MODIFY COLUMN",
                ))
            }
        }
    }
    let new_generated_stored = def.options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::Generated { stored, .. } => Some(*stored),
        _ => None,
    });
    let not_null = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull));
    let wants_auto_increment = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement));

    // Phase one reads only, because the foreign-key question below is asked
    // of the WHOLE catalog -- a constraint's other side lives in another table,
    // and often another schema. The mutable borrow is taken once every check
    // has passed, at the point the column is actually replaced.
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(old_name))
    else {
        // Go's `IF EXISTS` demotes the missing column rather than silencing
        // it. Captured: `alter table t modify column if exists no_col bigint`
        // leaves `Note | 1054 | Unknown column 'no_col' in 't'`, and the
        // `CHANGE COLUMN` spelling that shares this action leaves the same.
        let missing = DriverError::UnknownColumnInTable {
            column: old_name.to_owned(),
            table: table_name.to_owned(),
        };
        if !if_exists {
            return Err(missing);
        }
        ctx.append_suppressed(&missing);
        return Ok(());
    };
    // Go `checkModifyGeneratedColumn` (`pkg/ddl/modify_column.go`): a MODIFY
    // may not turn a generated column into an ordinary one, nor an ordinary
    // column into a generated one, nor move a column between VIRTUAL and
    // STORED. All three are ONE error, and Go words it for the STORED case
    // whichever of them happened -- captured, on a VIRTUAL column both
    // directions answer `Error|3106|'Changing the STORED status' is not
    // supported for generated columns.` The wording is Go's; it is ported as
    // measured rather than repaired.
    let old_generated_stored = table.columns[offset]
        .generated
        .as_ref()
        .map(|generated| generated.stored);
    if new_generated_stored != old_generated_stored {
        return Err(DriverError::UnsupportedOnGeneratedColumn(
            "Changing the STORED status".to_owned(),
        ));
    }
    // Keeping the generated-ness is what Go ACCEPTS -- including replacing
    // the expression: captured, `alter table g modify column d int as (a+5)
    // virtual` succeeds and the rows read back recomputed. Rebuilding the
    // expression against the modified table is not modelled yet, so the
    // statement is refused rather than applied with the OLD expression
    // silently kept under a definition that asked for a new one.
    if new_generated_stored.is_some() {
        return Err(DriverError::unsupported(
            "ALTER TABLE MODIFY COLUMN of a generated column is not supported yet",
        ));
    }
    // Go `getModifiableColumnJob` (`pkg/ddl/modify_column.go`) computes
    // `checkModifyColumnWithGeneratedColumnsConstraint` ONCE and then raises
    // it in two different shapes -- the rename arm just below, and the 3106
    // arm further down. The partition question is deliberately NOT asked
    // here: Go's rename-only path asks it, this path does not, and that
    // asymmetry is Go's (`checkPartitionModifiableColumn` polices the type
    // instead).
    let dependent = match table.column_dependent(offset) {
        Some(
            dependent @ (crate::kv_table::ColumnDependent::ExpressionIndex
            | crate::kv_table::ColumnDependent::GeneratedColumn),
        ) => Some(dependent),
        _ => None,
    };
    // A rename onto another column's name is a duplicate, but renaming a
    // column to the name it already has is allowed.
    if !def.name.eq_ignore_ascii_case(old_name) {
        if table
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(&def.name))
        {
            return Err(DriverError::DuplicateColumnName(def.name.clone()));
        }
        // The rename arm raises the dependency error UNWRAPPED: 3108 for a
        // visible generated column, 3837 for the hidden one an expression
        // index was rewritten into.
        if let Some(dependent) = dependent {
            return Err(super::column_dependent_error(dependent, old_name));
        }
    }

    // Go `getModifiableColumnJob` asks this HERE: after the rename checks
    // above and BEFORE the index-flag copy and `checkModifyTypes` below.
    // Keeping Go's position is what decides which error a statement that
    // breaks two rules at once reports.
    let original_type = table.columns[offset].field_type.clone();
    crate::foreign_key::check_modify_column(
        catalog,
        database,
        table_name,
        old_name,
        &original_type,
        &field_type,
    )?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        unreachable!("the table was found above and nothing here removes it");
    };

    let mut field_type = field_type;
    if not_null {
        field_type.add_flags(NOT_NULL_FLAG);
    }
    // Go `pkg/ddl/modify_column.go`: the new column is built from the new
    // definition, then the OLD column's index flags are copied onto it and a
    // primary key re-implies NOT NULL. Without this the definition alone
    // decides, so `ALTER TABLE mc MODIFY COLUMN a bigint` on a primary key
    // silently made the key column nullable and printed `DEFAULT NULL`.
    let old_flags = table.columns[offset].field_type.flags();
    if old_flags & PRI_KEY_FLAG != 0 {
        field_type.add_flags(PRI_KEY_FLAG | NOT_NULL_FLAG);
    }
    // Go `checkModifyTypes` (`pkg/ddl/modify_column.go:2262`), reached right
    // after the index-flag copy above and before the AUTO_INCREMENT checks
    // below -- this is Go's ORDER, not an arbitrary choice: `checkModifyTypes`
    // calls `types.CheckModifyTypeCompatible`, which for a type-changing
    // MODIFY calls `checkTypeChangeSupported` (`pkg/types/field_type.go:1569`)
    // BEFORE any row is read. That location is what makes the refusal fire on
    // an EMPTY table: the per-row `convert_to` gate in `KvTable::modify_column`
    // below never runs when there are zero rows, so without this table-level
    // check every one of Go's five outright refusals would be silently
    // accepted on an empty table.
    check_type_change_supported(&table.columns[offset].field_type, &field_type)?;
    // Go, same file: `can't set auto_increment` (8200) for a column that did
    // not have it, and dropping it needs `@@tidb_allow_remove_auto_inc`.
    // Keeping it is the only combination that changes nothing.
    let was_auto_increment = table.auto_increment_offset() == Some(offset);
    if wants_auto_increment && !was_auto_increment {
        return Err(DriverError::UnsupportedModifyColumn(
            "can't set auto_increment",
        ));
    }
    if wants_auto_increment && default_value.is_some() {
        return Err(DriverError::InvalidDefault(def.name.clone()));
    }
    if was_auto_increment && !wants_auto_increment && !allow_remove_auto_inc {
        return Err(DriverError::UnsupportedModifyColumn(
            "can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled",
        ));
    }
    if was_auto_increment && wants_auto_increment {
        // Nothing in this tier READS this flag -- the observable
        // AUTO_INCREMENT comes from the table-level offset above, which is
        // why no test can kill this line. It is set so that a column reached
        // through MODIFY carries exactly what the CREATE TABLE path
        // (`ddl.rs`) gives the same column, rather than leaving two spellings
        // of one catalog for the first reader of the flag to trip over.
        field_type.add_flags(AUTO_INCREMENT_FLAG | NOT_NULL_FLAG);
    }
    let drop_auto_increment = was_auto_increment && !wants_auto_increment;
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
    // Go `checkIndexInModifiableColumns` (`pkg/ddl/modify_column.go`): every
    // key part over this column is re-validated against the NEW type, under
    // the length that key part will survive with -- which is Go's
    // `UpdateIndexCol` rule, applied by `KvTable::modify_column` itself.
    //
    // This subsumes the `ErrBlobKeyWithoutLength` refusal it replaces: a key
    // part with no surviving prefix over a new BLOB/TEXT column is exactly
    // Go's 1170. A key part that KEEPS a prefix is legal over the same type,
    // which is why the check has to ask about the length rather than about
    // the type alone.
    for index in table.indexes() {
        for (position, at) in index.column_offsets.iter().enumerate() {
            if *at != offset {
                continue;
            }
            let length = index.prefix_length(position);
            let surviving = (field_type.code().is_type_prefixable() && field_type.flen() > length)
                .then_some(length);
            crate::ddl::index_prefix::key_part_length(
                &field_type,
                crate::ddl::index_prefix::IndexedColumn::Named(&def.name),
                surviving,
                true,
            )?;
        }
    }

    // The second shape of the dependency error, and the one this tier used to
    // miss entirely. Go raises it here, after the index checks above and
    // regardless of whether the name or even the TYPE changed:
    //
    //     if errG != nil {
    //         // https://github.com/pingcap/tidb/issues/24321
    //         return nil, dbterror.ErrUnsupportedOnGeneratedColumn.
    //             GenWithStackByArgs(errG.Error())
    //     }
    //
    // The argument is the inner error's FULL `Error()` text, class prefix
    // included, so the wire message nests one error inside another. That is
    // not a slip -- it is in the recording verbatim
    // (`tests/integrationtest/r/ddl/column_change.result:12`):
    //
    //     Error 3106 (HY000): '[ddl:3108]Column 'a' has a generated column
    //     dependency.' is not supported for generated columns.
    //
    // Accepting these left the expression index in place over a column whose
    // TYPE had moved out from under it, so a later read used an index whose
    // expression no longer matched the column. Refusing is what Go does; it is
    // not a stand-in for a rewrite Go performs, because Go does not rewrite
    // for MODIFY any more than it does for RENAME.
    if let Some(dependent) = dependent {
        return Err(DriverError::UnsupportedOnGeneratedColumn(
            super::column_dependent_error_text(dependent, old_name),
        ));
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
    // Go's `SetDefaultValue` order: `checkColumnDefaultValue` (the
    // BLOB/TEXT/JSON rule, which may rewrite the value or report that the
    // column ends up with NO default) and only then the normalization.
    let default_value = match default_value {
        Some(value) => {
            let (has_default, value) =
                check_column_default_value(value, &field_type, &def.name, ctx)?;
            if !has_default && field_type.has_flag(NOT_NULL_FLAG) {
                None
            } else {
                Some(normalize_column_default(
                    value,
                    &field_type,
                    &def.name,
                    zone,
                )?)
            }
        }
        None => None,
    };
    let stored_default = default_value
        .clone()
        .map(crate::column_default::ColumnDefault::Value);
    // Go `updateFKInfoWhenModifyColumn` +
    // `adjustForeignKeyChildTableInfoAfterModifyColumn`: a CHANGE that also
    // renames carries every constraint over the old name onto the new one,
    // on this table AND on every child that refers to it. Done before the
    // column itself is replaced, so `referring` still sees the old name.
    crate::foreign_key::rewrite_column_name(catalog, database, table_name, old_name, &def.name);
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was found above and nothing here removes it");
    };
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
    if drop_auto_increment {
        table.clear_auto_increment_offset();
    }
    table
        .modify_column(offset, column, new_position, zone)
        .map_err(|e| match e {
            crate::kv_table::KvTableError::TruncatedIncorrectValue { kind, value } => {
                DriverError::TruncatedIncorrectValue {
                    kind: kind.to_owned(),
                    value,
                }
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
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let zone = &ctx.session_zone();
    let field_type = field_type_of(def, existing_table_charset(catalog, database, table_name))?;
    let mut default_value = None;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                    expr,
                    &tidb_expr::rewriter::ZonedNoResolver(zone.clone()),
                )
                .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                    return Err(DriverError::unsupported(
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
            // Go `checkAddColumnTooManyColumns`'s neighbour in
            // `pkg/ddl/column.go`: a STORED generated column added by ALTER
            // would have to be backfilled into every existing row, which TiDB
            // refuses outright. Captured: `alter table g add column f int as
            // (a*2) stored` -> `Error|3106|'Adding generated stored column
            // through ALTER TABLE' is not supported for generated columns.`
            // The VIRTUAL form is accepted and computes on read, which is why
            // only this half is refused.
            tidb_ast::ColumnOption::Generated { stored: true, .. } => {
                return Err(DriverError::UnsupportedOnGeneratedColumn(
                    "Adding generated stored column through ALTER TABLE".to_owned(),
                ))
            }
            tidb_ast::ColumnOption::Generated { stored: false, .. } => {}
            _ => {
                return Err(DriverError::unsupported(
                    "this column option is not supported in ALTER TABLE ADD COLUMN",
                ))
            }
        }
    }
    let generated_expression = def.options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::Generated { expression, .. } => Some(expression),
        _ => None,
    });
    let not_null = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull));

    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::unsupported(
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
    // Go's `SetDefaultValue` order: `checkColumnDefaultValue` (the
    // BLOB/TEXT/JSON rule, which may rewrite the value or report that the
    // column ends up with NO default) and only then the normalization.
    let default_value = match default_value {
        Some(value) => {
            let (has_default, value) =
                check_column_default_value(value, &field_type, &def.name, ctx)?;
            if !has_default && field_type.has_flag(NOT_NULL_FLAG) {
                None
            } else {
                Some(normalize_column_default(
                    value,
                    &field_type,
                    &def.name,
                    zone,
                )?)
            }
        }
        None => None,
    };
    let stored_default = default_value
        .clone()
        .map(crate::column_default::ColumnDefault::Value);
    // A generated expression resolves against the columns that will PRECEDE
    // the new one, which is Go's `verifyColumnGeneration` prior-order rule
    // and, for the default append position, every column the table has.
    let generated = match generated_expression {
        Some(expression) => {
            let names: Vec<String> = table.columns[..index]
                .iter()
                .map(|column| column.name.clone())
                .collect();
            let types: Vec<tidb_datatype::FieldType> = table.columns[..index]
                .iter()
                .map(|column| column.field_type.clone())
                .collect();
            Some(
                crate::generated_column::build_added_generated_column(
                    expression, false, &names, &types, zone,
                )
                .map_err(crate::ddl::generated_column_error)?,
            )
        }
        None => None,
    };
    let id = table.next_column_id();
    let field_type_for_origin = field_type.clone();
    table.add_column(
        index,
        KvColumn {
            name: def.name.clone(),
            id,
            field_type,
            generated,
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
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let zone = &ctx.session_zone();
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
    else {
        // Captured: `alter table t drop column if exists no_col` leaves
        // `Note | 1091 | Can't DROP 'no_col'; check that column/key exists` --
        // a DIFFERENT code and text from the MODIFY spelling above, which is
        // why the note carries the suppressed error rather than a shared one.
        let missing = DriverError::UnknownColumnInAlter(column_name.to_owned());
        if !if_exists {
            return Err(missing);
        }
        ctx.append_suppressed(&missing);
        return Ok(());
    };
    // Captured: dropping the only column is 1090.
    if table.columns.len() == 1 {
        return Err(DriverError::CannotDropOnlyColumn {
            column: column_name.to_owned(),
            table: table_name.to_owned(),
        });
    }
    // Go `checkIsDroppableColumn` (`pkg/ddl/executor.go`) runs `isDroppableColumn`
    // and then `checkDropColumnWithPartitionConstraint`, which is the pair
    // `column_dependent` answers: with `index idx((a+b))`, `drop column a` is
    // 3837 `Column 'a' has an expression index dependency and cannot be
    // dropped or renamed` (CAPTURED from TiDB); with `c AS (a+1)` it is 3108;
    // with `partition by hash(a)` it is 3855. Without this the drop succeeded
    // and left the expression naming a column that no longer exists.
    if let Some(dependent) = table.column_dependent(offset) {
        return Err(super::column_dependent_error(dependent, column_name));
    }
    // Captured: dropping an integer primary key is TiDB's 8200.
    if table.pk_handle_offset() == Some(offset) {
        return Err(DriverError::UnsupportedDropIntegerPrimaryKey);
    }
    if table.common_handle_offsets().contains(&offset) {
        return Err(DriverError::unsupported(
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
            .drop_index(&index_name, zone)
            .map_err(|e| DriverError::Parse(format!("index drop failed: {e:?}")))?;
    }
    table.drop_column(offset);
    Ok(())
}

#[cfg(test)]
mod type_change_gate_tests {
    use super::{check_type_change_supported, DriverError};
    use tidb_datatype::{FieldType, FieldTypeCode};

    /// Rule 4 (`field_type.go:1591-1594`, `TypeTiDBVectorFloat32` on either
    /// side) exercised directly rather than through SQL: this tier's
    /// `column_type_code` (`ddl/column_field_type.rs`) does not recognize the
    /// `VECTOR` type name yet, so no `CREATE TABLE`/`ALTER TABLE` statement
    /// can build a `FieldType` carrying `FieldTypeCode::VectorFloat32` today
    /// -- the rule is dead code from the SQL surface's point of view until
    /// VECTOR column declarations land. Calling the gate directly still
    /// proves the logic and keeps it mutation-testable in the meantime.
    #[test]
    fn vector_type_is_refused_on_either_side() {
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let int = FieldType::new(FieldTypeCode::Long);

        assert!(matches!(
            check_type_change_supported(&vector, &int),
            Err(DriverError::UnsupportedModifyColumnType { .. })
        ));
        assert!(matches!(
            check_type_change_supported(&int, &vector),
            Err(DriverError::UnsupportedModifyColumnType { .. })
        ));
    }

    /// MUTATION PROBE for all five rules: with the gate neutered (always
    /// `Ok`), every refusal below would incorrectly succeed. This test
    /// documents the probe; it is not itself the neutering -- see the SQL
    /// pins in `tidb-session`'s `tests_alter_column.rs` for the five
    /// empty-table cases this backs.
    #[test]
    fn control_conversion_is_not_refused() {
        let from = FieldType::new(FieldTypeCode::Long);
        let to = FieldType::new(FieldTypeCode::LongLong);
        assert!(check_type_change_supported(&from, &to).is_ok());
    }
}
