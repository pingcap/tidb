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

//! The `ALTER TABLE` actions Go runs as a pure `TableInfo` edit: no row is
//! read, no index entry is rewritten, and no backfill runs.
//!
//! Inside: [`rename_column_action`] (Go `executor.RenameColumn`),
//! [`rename_index_action`] (Go `executor.RenameIndex` and its
//! `ValidateRenameIndex`), [`alter_index_visibility_action`] (Go
//! `executor.AlterIndexVisibility` and its `validateAlterIndexVisibility`),
//! and [`alter_column_default_action`] (Go `executor.AlterColumn`).
//!
//! They belong together because they share one invariant that makes them
//! cheap and safe: a name or a flag changes, while every column ID, column
//! OFFSET and index entry stays exactly where it was. An index addresses its
//! columns by offset here (`KvIndex::column_offsets`), so renaming a column
//! cannot invalidate a KEY, and renaming an index does not touch the entries
//! filed under its id.
//!
//! That invariant covers indexes and rows, and it does NOT cover the metadata
//! that names columns instead of pointing at them -- a generated column's
//! expression, the hidden generated column behind an expression index, the
//! partition expression, and a foreign key's `cols`. Renaming a column is
//! exactly what invalidates those, which is why
//! [`rename_column_action`] refuses rather than renames whenever
//! [`crate::kv_table::KvTable::column_dependent`] reports one of the first
//! three (Go's 3837 / 3108 / 3855). The fourth is refused a step earlier and
//! more broadly: `ALTER TABLE` on any table participating in a foreign key is
//! unsupported (`crate::ddl::alter_table`), so no rename reaches an `FKInfo`.
//! Go instead REWRITES `fk.Cols` on this path
//! (`pkg/ddl/modify_column.go` `updateFKInfoWhenModifyColumn`), which is the
//! graduation path for that refusal.
//!
//! Mirrors Go `pkg/ddl/executor.go`'s `AlterTable` arms
//! `ast.AlterTableRenameColumn`, `ast.AlterTableRenameIndex`,
//! `ast.AlterTableIndexInvisible` and `ast.AlterTableAlterColumn`, plus the
//! two validators in `pkg/ddl/index.go`. Each error code below is TiDB's own,
//! read off `tests/integrationtest/r/ddl/db_rename.result` and
//! `.../ddl/column_modify.result` -- the suite's recording of real TiDB.

use super::{Catalog, DriverError, KvColumn};

/// Resolves the storage-backed table an action names, or reports 1146.
fn table_of<'a>(
    catalog: &'a mut Catalog,
    database: &str,
    name: &str,
) -> Result<&'a mut crate::kv_table::KvTable, DriverError> {
    match catalog.table_mut_in(database, name) {
        Some(crate::TableEntry::Kv(table)) => Ok(table),
        Some(_) => Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        )),
        None => Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        ))),
    }
}

/// `ALTER TABLE ... RENAME COLUMN old TO new`.
///
/// Go `RenameColumn` in this exact order, which the error a statement gets
/// depends on: the OLD column must exist (1054, even when old and new are the
/// same name), renaming a column to the name it already has is a no-op, the
/// new name may not be `_tidb_rowid` (1166), and the new name may not already
/// be taken (1060).
///
/// The rename is a name assignment and nothing else: the column keeps its id
/// and its offset, so every index over it, every stored row and the handle
/// stay valid without being rewritten -- which is why Go needs no reorg here.
pub(crate) fn rename_column_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    from: &str,
    to: &str,
) -> Result<(), DriverError> {
    let table = table_of(catalog, database, table_name)?;
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(from))
    else {
        return Err(DriverError::UnknownColumnInTable {
            column: from.to_owned(),
            table: table_name.to_owned(),
        });
    };
    // Go returns nil BEFORE the duplicate check when the two names are the
    // same column, so `rename column c1 to c1` succeeds while
    // `rename column c2 to id` is 1060.
    if from.eq_ignore_ascii_case(to) {
        return Ok(());
    }
    if to.eq_ignore_ascii_case("_tidb_rowid") {
        return Err(DriverError::WrongColumnName(to.to_owned()));
    }
    if table
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(to))
    {
        return Err(DriverError::DuplicateColumnName(to.to_owned()));
    }
    // Go `RenameColumn` (`pkg/ddl/executor.go`) asks the name-keyed metadata
    // LAST, after the same-name early return (so `rename column a to a` still
    // succeeds), after `_tidb_rowid` (1166) and after the duplicate name
    // (1060). Both of its questions are asked here:
    // `checkModifyColumnWithGeneratedColumnsConstraint` then
    // `checkDropColumnWithPartitionConstraint`, which is exactly the order
    // `column_dependent` reports in.
    //
    // Nothing below this line rewrites metadata, and that is the point: with
    // the columns' names keying the generated expressions and the partition
    // expression, a rename that got through would leave those reading a name
    // no column has. Go refuses rather than rewriting for the same reason,
    // and `dependency_offsets` may therefore treat a missing name as a bug.
    if let Some(dependent) = table.column_dependent(offset) {
        return Err(super::column_dependent_error(dependent, from));
    }
    table.columns[offset].name = to.to_owned();
    Ok(())
}

/// `ALTER TABLE ... RENAME {INDEX|KEY} old TO new`.
///
/// Go `ValidateRenameIndex` decides all three outcomes, and its ordering is
/// what makes the recorded results in `ddl/db_rename.result` what they are:
/// a missing source is 1176; a source and target that are the SAME SPELLING
/// are ignored outright; and a target that already names a DIFFERENT index
/// (compared case-INsensitively) is 1061 reporting the EXISTING index's
/// spelling, not the one the statement wrote. The gap between those last two
/// is why `rename index k2 to K2` succeeds -- it re-cases one index -- while
/// `rename key k3 to K2` afterwards is `Duplicate key name 'K2'`.
pub(crate) fn rename_index_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    from: &str,
    to: &str,
) -> Result<(), DriverError> {
    let table = table_of(catalog, database, table_name)?;
    if !table
        .indexes()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(from))
    {
        return Err(DriverError::KeyNotExists {
            key: from.to_owned(),
            table: table_name.to_owned(),
        });
    }
    if from == to {
        return Ok(());
    }
    if !from.eq_ignore_ascii_case(to) {
        if let Some(existing) = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(to))
        {
            return Err(DriverError::DuplicateKeyName(existing.name.clone()));
        }
    }
    if let Some(index) = table.index_mut_by_name(from) {
        index.name = to.to_owned();
    }
    Ok(())
}

/// `ALTER TABLE ... ALTER INDEX name {VISIBLE|INVISIBLE}`.
///
/// Go `validateAlterIndexVisibility`: an index that is not there is 1176 (the
/// same error `USE INDEX` on it would give), and setting the visibility it
/// already has is a no-op rather than a job.
///
/// Go then runs `checkInvisibleIndexOnPK` over the RESULT, which is 3522 "A
/// primary key index cannot be invisible". The primary key it means is
/// `TableInfo.GetPrimaryKey`'s, so the rule reaches an index the statement
/// never called PRIMARY: MySQL's IMPLICIT primary key is the first UNIQUE
/// index all of whose columns are NOT NULL, which is why
/// `create table t1(a int NOT NULL, unique(a))` cannot hide `a`
/// (`ddl/db_integration.result`). A table whose handle IS its primary key
/// (Go's `PKIsHandle`, and the clustered common handle with it) has no such
/// index to hide, so the check does not apply.
pub(crate) fn alter_index_visibility_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    index_name: &str,
    visible: bool,
) -> Result<(), DriverError> {
    let table = table_of(catalog, database, table_name)?;
    if !visible && primary_key_index(table).is_some_and(|pk| pk.eq_ignore_ascii_case(index_name)) {
        return Err(DriverError::PrimaryKeyCantBeInvisible);
    }
    let Some(index) = table.index_mut_by_name(index_name) else {
        return Err(DriverError::KeyNotExists {
            key: index_name.to_owned(),
            table: table_name.to_owned(),
        });
    };
    index.visible = visible;
    Ok(())
}

/// Go `TableInfo.GetPrimaryKey`: the index that acts as the table's primary
/// key, which is the one named `PRIMARY` if there is one and otherwise the
/// FIRST unique index whose every column is NOT NULL.
///
/// `None` when the row handle already is the primary key -- Go returns early
/// on `PKIsHandle` because such a table keeps no PRIMARY index at all, and a
/// clustered common handle is the same situation with several columns.
fn primary_key_index(table: &crate::kv_table::KvTable) -> Option<&str> {
    if table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty() {
        return None;
    }
    let mut implicit = None;
    for index in table.indexes() {
        if index.name.eq_ignore_ascii_case("PRIMARY") {
            return Some(&index.name);
        }
        if implicit.is_none()
            && index.unique
            && !index.column_offsets.is_empty()
            && index
                .column_offsets
                .iter()
                .all(|offset| table.columns[*offset].field_type.flags() & super::NOT_NULL_FLAG != 0)
        {
            implicit = Some(index.name.as_str());
        }
    }
    implicit
}

/// `ALTER TABLE ... ALTER [COLUMN] name SET DEFAULT value`.
///
/// Go `AlterColumn` replaces only `ColumnInfo.DefaultValue`; the column's
/// type, id, offset and every stored row are untouched, and rows already
/// written keep whatever they hold. A column the table does not have is 1054.
///
pub(crate) fn alter_column_default_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    column_name: &str,
    default_value: Option<&tidb_ast::Expr>,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Go resolves the table and target column before it examines the new
    // default expression. Preserve that observable error order: a missing
    // target is 1054 even when the DEFAULT itself could not be rewritten.
    let table = table_of(catalog, database, table_name)?;
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
    else {
        return Err(DriverError::UnknownColumnInTable {
            column: column_name.to_owned(),
            table: table_name.to_owned(),
        });
    };
    let Some(expr) = default_value else {
        let column = &mut table.columns[offset];
        column.default_value = None;
        column
            .field_type
            .add_flags(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE);
        return Ok(());
    };
    let field_type = table.columns[offset].field_type.clone();
    let column_info_version = table.columns[offset].column_info_version;
    if table
        .auto_random()
        .is_some_and(|spec| spec.offset == offset)
    {
        return Err(DriverError::InvalidAutoRandom(
            "auto_random is incompatible with default".to_owned(),
        ));
    }
    let default = crate::column_default::build_in_context(expr, &field_type, column_name, ctx)?;
    let crate::column_default::ColumnDefault::Value(value) = default else {
        if table.auto_increment_offset() == Some(offset) {
            return Err(DriverError::InvalidDefault(column_name.to_owned()));
        }
        let column = &mut table.columns[offset];
        column.default_value = Some(default);
        column
            .field_type
            .del_flags(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE);
        return Ok(());
    };
    let zone = &ctx.session_zone();
    // The session first builds and validates the exact ColumnInfo spelling.
    // Keep that spelling in the catalog: omitted writes cast it later, while
    // SHOW CREATE must not replace e.g. DECIMAL 3.14159 with the rounded 3.14.
    let prepared = super::alter_table::prepare_column_default(
        value,
        &field_type,
        column_name,
        column_info_version,
        ctx,
        zone,
    )?;

    // Go runs `checkColumnDefaultValue` a SECOND time in the DDL owner under
    // `newReorgExprCtx` (ModeNone/default statement flags), and turns
    // `!hasDefaultValue` into ErrInvalidDefaultValue. That is why
    // sql_mode='' plus ALTER ... SET DEFAULT '' on TEXT is 1067 rather than
    // ADD COLUMN's lenient acceptance.
    let reorg_ctx = crate::StmtContext::for_dml(false, false, false);
    let (has_default, _) = super::alter_table::check_column_default_value(
        prepared.stored.clone(),
        &field_type,
        column_name,
        &reorg_ctx,
        &tidb_datatype::SessionTimeZone::utc(),
    )?;
    if !has_default {
        return Err(DriverError::InvalidDefault(column_name.to_owned()));
    }
    // `updateColumnDefaultValue` then validates the installed spelling again
    // in that reorg context. The first session validation is not a substitute:
    // the two contexts intentionally have different SQL modes.
    super::alter_table::validate_column_default(
        &prepared.stored,
        &field_type,
        column_name,
        column_info_version,
        // `newReorgExprCtx()` is exactly ModeNone + DefaultStmtFlags here,
        // not the SQL-mode-aware reorg context used by row backfill and not
        // CREATE/ALTER default-admission flags.
        tidb_datatype::DEFAULT_STATEMENT_FLAGS,
        &tidb_datatype::SessionTimeZone::utc(),
    )?;
    let KvColumn {
        default_value: stored,
        field_type,
        ..
    } = &mut table.columns[offset];
    *stored = Some(crate::column_default::ColumnDefault::Value(prepared.stored));
    field_type.del_flags(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE);
    Ok(())
}
