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

//! Prepared column jobs for the existing local ALTER owner. Admission reads
//! the original catalog; execution resolves stable IDs and current offsets.

use super::alter_table::{self, ModifyColumnRequest, PreparedAllocatorChanges};
use super::{Catalog, DriverError, KvColumn};
use tidb_ast::{AlterTableAction, ColumnPosition};

pub(super) enum PreparedColumnChange {
    Add {
        column: KvColumn,
        position: ColumnPosition,
    },
    Drop {
        id: i64,
        name: String,
        invalid_constraint_ids: Vec<i64>,
    },
    Modify {
        old_name: String,
        column: KvColumn,
        position: ColumnPosition,
        new_auto_random: Option<crate::kv_table::AutoRandomSpec>,
        drop_auto_increment: bool,
    },
    Rename {
        id: i64,
        from: String,
        to: String,
    },
    Default {
        column: KvColumn,
    },
}

pub(super) fn table_of<'a>(
    catalog: &'a Catalog,
    database: &str,
    name: &str,
) -> Result<&'a crate::KvTable, DriverError> {
    match catalog.table_in(database, name) {
        Some(crate::TableEntry::Kv(table)) => Ok(table),
        Some(_) => Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        )),
        None => Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        ))),
    }
}

pub(super) fn is_column_change(action: &AlterTableAction) -> bool {
    matches!(
        action,
        AlterTableAction::AddColumn { .. }
            | AlterTableAction::DropColumn { .. }
            | AlterTableAction::ModifyColumn { .. }
            | AlterTableAction::ChangeColumn { .. }
            | AlterTableAction::RenameColumn(_)
            | AlterTableAction::AlterColumnDefault(_)
    )
}

/// Go `ModifyColumn`'s check of a qualified column name (`db.t.c`): its
/// schema and table must be the altered table's.
fn check_column_qualifier(
    qualifier: &[String],
    database: &str,
    table: &str,
) -> Result<(), DriverError> {
    check_column_qualifier_part(qualifier, 2, database, true)?;
    check_column_qualifier_part(qualifier, 1, table, false)
}

/// One part of a column qualifier: the schema (`from_end == 2`, written as
/// `db.t.c`) or the table (`from_end == 1`), compared case-insensitively
/// with the altered table's, `ErrWrongDBName` / `ErrWrongTableName` when it
/// names another.
fn check_column_qualifier_part(
    qualifier: &[String],
    from_end: usize,
    expected: &str,
    schema: bool,
) -> Result<(), DriverError> {
    let written = match (qualifier, from_end) {
        ([schema, _], 2) => schema.as_str(),
        ([table], 1) | ([_, table], 1) => table.as_str(),
        _ => return Ok(()),
    };
    if written.is_empty() || written.to_lowercase() == expected.to_lowercase() {
        return Ok(());
    }
    Err(if schema {
        DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrWrongDBName,
            message: format!("Incorrect database name '{written}'"),
        }
    } else {
        DriverError::Schema(crate::SchemaErrorKind::WrongTableName(written.to_owned()))
    })
}

pub(super) fn prepare(
    action: &AlterTableAction,
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<PreparedColumnChange>, DriverError> {
    match action {
        AlterTableAction::AddColumn {
            if_not_exists,
            column,
            position,
        } => alter_table::prepare_add_column(
            catalog,
            database,
            name,
            column,
            position,
            *if_not_exists,
            ctx,
        ),
        AlterTableAction::DropColumn {
            if_exists,
            name: column,
        } => {
            // Preserve the existing unsupported FK boundary until the full
            // foreign-key drop-column owner is available.
            if crate::foreign_key::participates(catalog, database, name) {
                return Err(DriverError::unsupported("changing the columns of a table involved in a FOREIGN KEY is not supported yet"));
            }
            alter_table::prepare_drop_column(catalog, database, name, column, *if_exists, ctx)
        }
        AlterTableAction::ModifyColumn {
            if_exists,
            column,
            position,
        } => {
            check_column_qualifier(&column.qualifier, database, name)?;
            alter_table::prepare_modify_column(
                catalog,
                &ModifyColumnRequest {
                    database,
                    table_name: name,
                    old_name: &column.name,
                    def: column,
                    position,
                    if_exists: *if_exists,
                    allow_remove_auto_inc: ctx.allow_remove_auto_inc(),
                },
                ctx,
            )
        }
        AlterTableAction::ChangeColumn {
            if_exists,
            old_name,
            column,
            position,
        } => {
            // Go `ChangeColumn` checks the new name's schema, then the old
            // name's, then the new name's table, then the old name's.
            let old_qualifier = &old_name[..old_name.len().saturating_sub(1)];
            check_column_qualifier_part(&column.qualifier, 2, database, true)?;
            check_column_qualifier_part(old_qualifier, 2, database, true)?;
            check_column_qualifier_part(&column.qualifier, 1, name, false)?;
            check_column_qualifier_part(old_qualifier, 1, name, false)?;
            alter_table::prepare_modify_column(
                catalog,
                &ModifyColumnRequest {
                    database,
                    table_name: name,
                    old_name: old_name
                        .last()
                        .ok_or_else(|| DriverError::unsupported("empty CHANGE COLUMN name"))?,
                    def: column,
                    position,
                    if_exists: *if_exists,
                    allow_remove_auto_inc: ctx.allow_remove_auto_inc(),
                },
                ctx,
            )
        }
        AlterTableAction::RenameColumn(rename) => super::alter_metadata::prepare_rename_column(
            catalog,
            database,
            name,
            &rename.from,
            &rename.to,
        ),
        AlterTableAction::AlterColumnDefault(alter) => {
            let column = alter
                .name
                .last()
                .ok_or_else(|| DriverError::unsupported("empty ALTER COLUMN name"))?;
            super::alter_metadata::prepare_column_default_change(
                catalog,
                database,
                name,
                column,
                alter.default_value.as_ref(),
                ctx,
            )
            .map(|column| Some(PreparedColumnChange::Default { column }))
        }
        _ => Ok(None),
    }
}

// Go LocateOffsetToMove resolves AFTER on the current worker schema. Keep
// positions as names across preparation: siblings can move every offset.
pub(super) fn position(
    table: &crate::KvTable,
    position: &ColumnPosition,
    moving: Option<usize>,
    table_name: &str,
) -> Result<Option<usize>, DriverError> {
    match position {
        ColumnPosition::Default => Ok(None),
        ColumnPosition::First => Ok(Some(0)),
        ColumnPosition::After(name) => {
            let offset = table
                .columns()
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| DriverError::UnknownColumnInTable {
                    column: name.clone(),
                    table: table_name.to_owned(),
                })?;
            Ok(Some(if moving.is_some_and(|moving| moving <= offset) {
                offset
            } else {
                offset + 1
            }))
        }
    }
}

impl PreparedColumnChange {
    pub(super) fn execute(
        self,
        catalog: &mut Catalog,
        database: &str,
        table_name: &str,
        ctx: &crate::StmtContext,
        allocators: &mut PreparedAllocatorChanges,
    ) -> Result<(), DriverError> {
        // FK metadata follows a prepared rename before the owning column's
        // name changes, so both parent and child lookups see the old name.
        match &self {
            Self::Modify {
                old_name, column, ..
            } => crate::foreign_key::rewrite_column_name(
                catalog,
                database,
                table_name,
                old_name,
                &column.name,
            ),
            Self::Rename { from, to, .. } => {
                crate::foreign_key::rewrite_column_name(catalog, database, table_name, from, to)
            }
            _ => {}
        }
        let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
            return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                format!("{database}.{table_name}"),
            )));
        };
        let table = std::sync::Arc::make_mut(table);
        // Go `updateTTLInfoWhenModifyColumn`: the TTL config follows a
        // renamed column.
        let renamed = match &self {
            Self::Modify {
                old_name, column, ..
            } => Some((old_name.as_str(), column.name.as_str())),
            Self::Rename { from, to, .. } => Some((from.as_str(), to.as_str())),
            _ => None,
        };
        if let Some((old, new)) = renamed.filter(|(old, new)| !old.eq_ignore_ascii_case(new)) {
            if let Some(mut info) = table.ttl_info().cloned() {
                if info.column_name.original().eq_ignore_ascii_case(old) {
                    info.column_name = tidb_ast::CiString::new(new.to_owned());
                    table.set_ttl_info(Some(info));
                }
            }
        }
        let offset_of = |table: &crate::KvTable, id: i64, name: &str| {
            table
                .columns()
                .iter()
                .position(|column| column.id == id)
                .ok_or_else(|| DriverError::UnknownColumnInTable {
                    column: name.to_owned(),
                    table: table_name.to_owned(),
                })
        };
        match self {
            Self::Add {
                mut column,
                position: requested,
            } => {
                let at = position(table, &requested, None, table_name)?
                    .unwrap_or(table.visible_column_count());
                column.id = table.next_column_id();
                table.add_column(at, column);
            }
            Self::Drop {
                id,
                name,
                invalid_constraint_ids,
            } => {
                let at = offset_of(table, id, &name)?;
                let covering: Vec<_> = table
                    .indexes()
                    .iter()
                    .filter(|index| index.column_offsets == [at])
                    .map(|index| index.name.clone())
                    .collect();
                for index in covering {
                    table.drop_index(&index).map_err(|error| {
                        DriverError::Parse(format!("index drop failed: {error:?}"))
                    })?;
                }
                table.drop_column(at);
                if !invalid_constraint_ids.is_empty() {
                    let infos = table
                        .check_constraint_infos()
                        .iter()
                        .filter(|info| !invalid_constraint_ids.contains(&info.id))
                        .cloned()
                        .collect();
                    table
                        .set_check_constraint_infos(
                            infos,
                            &ctx.session_zone(),
                            ctx.like_default_escape(),
                        )
                        .map_err(super::constraint_changes::check_constraint_table_error)?;
                }
            }
            Self::Modify {
                old_name,
                column,
                position: requested,
                mut new_auto_random,
                drop_auto_increment,
            } => {
                let at = offset_of(table, column.id, &old_name)?;
                let destination = position(table, &requested, Some(at), table_name)?;
                if let Some(spec) = &mut new_auto_random {
                    spec.offset = at;
                }
                if let Some(layout) = table
                    .prepare_alter_auto_random_spec(new_auto_random, at, &column.name)
                    .map_err(super::auto_random::rebase_error)?
                {
                    allocators.layouts.push(layout);
                }
                if drop_auto_increment {
                    table.clear_auto_increment_offset();
                }
                table
                    .modify_column_with_context(at, column, destination, ctx)
                    .map_err(|error| match error {
                        crate::kv_table::KvTableError::TruncatedIncorrectValue { kind, value } => {
                            DriverError::TruncatedIncorrectValue {
                                kind: kind.to_owned(),
                                value,
                            }
                        }
                        crate::kv_table::KvTableError::DataTruncatedValue { column, value } => {
                            DriverError::DataTruncatedValue { column, value }
                        }
                        crate::kv_table::KvTableError::InvalidUseOfNull => {
                            DriverError::InvalidUseOfNull
                        }
                        crate::kv_table::KvTableError::Vector(message) => {
                            DriverError::unsupported(message)
                        }
                        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                            DriverError::DuplicateEntry { value, key }
                        }
                        other => {
                            DriverError::Parse(format!("column modification failed: {other:?}"))
                        }
                    })?;
            }
            Self::Rename { id, from, to } => {
                let at = offset_of(table, id, &from)?;
                table.columns_mut()[at].name = to;
            }
            Self::Default { column } => {
                let at = offset_of(table, column.id, &column.name)?;
                super::alter_metadata::validate_default_execution(&column)?;
                let flag = tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE;
                let missing = column.field_type.has_flag(flag);
                let current = &mut table.columns_mut()[at];
                current.default_value = column.default_value;
                if missing {
                    current.field_type.add_flags(flag);
                } else {
                    current.field_type.del_flags(flag);
                }
            }
        }
        Ok(())
    }
}
