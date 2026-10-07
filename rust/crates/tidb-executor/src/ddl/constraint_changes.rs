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

//! Constraint job admission and staged application for ALTER TABLE.
//! Go builds each job from original metadata, including implicit FK indexes,
//! before its owner scans rows or consumes persistent constraint identifiers.

use super::{Catalog, DriverError};
use crate::kv_table::KvForeignKey;

pub(super) enum PreparedConstraintChange {
    AddForeignKey {
        foreign_key: KvForeignKey,
        index: Option<tidb_ast::IndexConstraintDefinition>,
    },
    DropForeignKey(String),
    Check {
        infos: Vec<tidb_model::table::ConstraintInfo>,
        validate_rows: bool,
    },
}

pub(super) fn is_constraint_change(action: &tidb_ast::AlterTableAction) -> bool {
    use tidb_ast::AlterTableAction::*;
    matches!(
        action,
        AddForeignKey(_) | DropForeignKey(_) | AddCheck(_) | DropCheck(_) | AlterCheck(_)
    )
}

pub(super) fn prepare(
    action: &tidb_ast::AlterTableAction,
    catalog: &Catalog,
    database: &str,
    name: &str,
    ctx: &crate::StmtContext,
    multi_schema: bool,
) -> Result<Option<PreparedConstraintChange>, DriverError> {
    use tidb_ast::AlterTableAction::*;
    let (change, check_job) = match action {
        AddForeignKey(definition) => (
            prepare_add_foreign_key(catalog, database, name, definition, ctx)?,
            None,
        ),
        DropForeignKey(drop) => {
            let table = check_table_clone(catalog, database, name)?;
            if !table
                .foreign_keys()
                .iter()
                .any(|key| key.name.eq_ignore_ascii_case(&drop.name))
            {
                return Err(DriverError::UnknownColumnInAlter(drop.name.clone()));
            }
            (
                PreparedConstraintChange::DropForeignKey(drop.name.clone()),
                None,
            )
        }
        AddCheck(definition) if ctx.enable_check_constraint() => (
            prepare_add_check(
                catalog,
                database,
                name,
                super::check_constraint::CheckConstraintInput {
                    definition: definition.clone(),
                    in_column: None,
                },
                ctx,
            )?,
            Some("add check constraint"),
        ),
        AlterCheck(alter) if ctx.enable_check_constraint() => (
            prepare_alter_check(catalog, database, name, &alter.name, alter.enforced)?,
            Some("alter check constraint"),
        ),
        DropCheck(drop) => (
            prepare_drop_check(catalog, database, name, &drop.name)?,
            Some("drop check constraint"),
        ),
        _ => return Ok(None),
    };
    if multi_schema {
        if let Some(job) = check_job {
            return Err(DriverError::DdlCoded {
                errno: 8200,
                message: format!("Unsupported multi schema change for {job}"),
            });
        }
    }
    Ok(Some(change))
}

impl PreparedConstraintChange {
    pub(super) fn implicit_index(&self) -> Option<&tidb_ast::IndexConstraintDefinition> {
        match self {
            Self::AddForeignKey { index, .. } => index.as_ref(),
            _ => None,
        }
    }

    pub(super) fn execute(
        self,
        catalog: &mut Catalog,
        database: &str,
        name: &str,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        match self {
            Self::AddForeignKey { foreign_key, index } => {
                if let Some(definition) = index {
                    super::index_changes::PreparedIndexChange::Add {
                        name: foreign_key.name.clone(),
                        definition: &definition,
                    }
                    .execute(catalog, database, name, ctx)?;
                }
                // Go rechecks in the owner, then consumes the ID before scanning rows.
                let table = check_table_clone(catalog, database, name)?;
                if table
                    .foreign_keys()
                    .iter()
                    .any(|key| key.name.eq_ignore_ascii_case(&foreign_key.name))
                {
                    return Err(DriverError::FkDupName(foreign_key.name));
                }
                validate_alter_foreign_key_parent(catalog, database, name, &foreign_key)?;
                if let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) {
                    std::sync::Arc::make_mut(table).allocate_foreign_key_id();
                }
                if ctx.foreign_key_checks() {
                    crate::foreign_key::require_existing_rows(
                        catalog,
                        database,
                        name,
                        &foreign_key,
                        &ctx.session_zone(),
                    )?;
                }
                if let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name) {
                    std::sync::Arc::make_mut(table).add_foreign_key(foreign_key);
                }
                catalog.mark_has_foreign_keys();
                Ok(())
            }
            Self::DropForeignKey(name_to_drop) => {
                drop_foreign_key_action(catalog, database, name, &name_to_drop)
            }
            Self::Check {
                infos,
                validate_rows,
            } => {
                let table = check_table_clone(catalog, database, name)?;
                install_check_constraint_infos(
                    catalog,
                    database,
                    name,
                    table,
                    infos,
                    validate_rows,
                    ctx,
                )
            }
        }
    }
}

fn check_constraint_columns(table: &crate::KvTable) -> Vec<tidb_model::ColumnInfo> {
    table
        .visible_columns()
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            let mut info =
                tidb_model::ColumnInfo::new(column.id, &column.name, column.field_type.clone());
            info.offset = offset as i64;
            info.version = column.column_info_version;
            info
        })
        .collect()
}

fn check_constraint_foreign_keys(
    table: &crate::KvTable,
) -> Vec<super::check_constraint::CheckConstraintForeignKey> {
    table
        .foreign_keys()
        .iter()
        .map(
            |foreign_key| super::check_constraint::CheckConstraintForeignKey {
                columns: foreign_key.cols.clone(),
                has_referential_action: foreign_key.on_delete != crate::FkAction::NoOption
                    || foreign_key.on_update != crate::FkAction::NoOption,
            },
        )
        .collect()
}

fn check_table_clone(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
) -> Result<crate::KvTable, DriverError> {
    match catalog.table_in(database, table_name) {
        Some(crate::TableEntry::Kv(table)) => Ok((**table).clone()),
        _ => Err(DriverError::unsupported(
            "CHECK constraints need a storage-backed table",
        )),
    }
}

fn install_check_constraint_infos(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    mut table: crate::KvTable,
    infos: Vec<tidb_model::table::ConstraintInfo>,
    validate_rows: bool,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    table
        .set_check_constraint_infos(infos, &ctx.session_zone(), ctx.like_default_escape())
        .map_err(check_constraint_table_error)?;
    if validate_rows {
        let mut cursor = table
            .row_cursor_with_context(&crate::RowDecodeContext::for_write(ctx))
            .map_err(check_constraint_table_error)?;
        while let Some((_, row)) = cursor.next_row().map_err(check_constraint_table_error)? {
            table
                .validate_check_constraints(&row, ctx)
                .map_err(check_constraint_table_error)?;
        }
    }
    match catalog.table_mut_in(database, table_name) {
        Some(crate::TableEntry::Kv(stored)) => {
            *stored = std::sync::Arc::new(table);
            Ok(())
        }
        _ => Err(DriverError::unsupported(
            "CHECK constraints need a storage-backed table",
        )),
    }
}

pub(super) fn check_constraint_table_error(error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::CheckConstraintViolated(name) => {
            DriverError::CheckConstraintViolated(name)
        }
        error => DriverError::DdlCoded {
            errno: 1105,
            message: format!("{error:?}"),
        },
    }
}

fn prepare_add_check(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    input: super::check_constraint::CheckConstraintInput,
    ctx: &crate::StmtContext,
) -> Result<PreparedConstraintChange, DriverError> {
    let table = check_table_clone(catalog, database, table_name)?;
    let columns = check_constraint_columns(&table);
    let foreign_keys = check_constraint_foreign_keys(&table);
    let mut max_constraint_id = table.max_constraint_id();
    let names = table
        .check_constraint_infos()
        .iter()
        .map(|info| info.name.original().to_owned())
        .collect::<Vec<_>>();
    let built = super::check_constraint::build_constraint_infos(
        &tidb_ast::CiString::new(table_name),
        &columns,
        names,
        &foreign_keys,
        std::slice::from_ref(&input),
        &mut max_constraint_id,
        tidb_model::SchemaState::PUBLIC,
        super::check_constraint::CheckConstraintBuildMode::Alter,
        ctx,
    )
    .map_err(|error| DriverError::DdlCoded {
        errno: error.code,
        message: error.message,
    })?;
    let added = built
        .first()
        .expect("one CHECK input builds one metadata record");
    if super::check_constraint_name_exists_in_schema(
        catalog,
        database,
        Some(table_name),
        added.name.original(),
    ) {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrCheckConstraintDupName,
            message: format!(
                "Duplicate check constraint name '{}'.",
                added.name.original()
            ),
        });
    }
    let validate_rows = added.enforced;
    let mut infos = table.check_constraint_infos().to_vec();
    infos.extend(built);
    Ok(PreparedConstraintChange::Check {
        infos,
        validate_rows,
    })
}

fn prepare_drop_check(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    constraint_name: &str,
) -> Result<PreparedConstraintChange, DriverError> {
    let table = check_table_clone(catalog, database, table_name)?;
    let mut infos = table.check_constraint_infos().to_vec();
    let Some(offset) = infos
        .iter()
        .position(|info| info.name.original().eq_ignore_ascii_case(constraint_name))
    else {
        return Err(DriverError::CheckConstraintNotExists(
            constraint_name.to_owned(),
        ));
    };
    infos.remove(offset);
    Ok(PreparedConstraintChange::Check {
        infos,
        validate_rows: false,
    })
}

fn prepare_alter_check(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    constraint_name: &str,
    enforced: bool,
) -> Result<PreparedConstraintChange, DriverError> {
    let table = check_table_clone(catalog, database, table_name)?;
    let mut infos = table.check_constraint_infos().to_vec();
    let Some(info) = infos
        .iter_mut()
        .find(|info| info.name.original().eq_ignore_ascii_case(constraint_name))
    else {
        return Err(DriverError::CheckConstraintNotExists(
            constraint_name.to_owned(),
        ));
    };
    let validate_rows = enforced && !info.enforced;
    info.enforced = enforced;
    info.state = tidb_model::SchemaState::PUBLIC;
    Ok(PreparedConstraintChange::Check {
        infos,
        validate_rows,
    })
}

fn prepare_add_foreign_key(
    catalog: &Catalog,
    database: &str,
    name: &str,
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    ctx: &crate::StmtContext,
) -> Result<PreparedConstraintChange, DriverError> {
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
            field_type: column.field_type.clone(),
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
        Some(table),
        catalog,
        database,
        ctx.foreign_key_checks(),
        table.partition().is_some(),
    )?;
    validate_alter_foreign_key_parent(catalog, database, name, &foreign_key)?;
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
            && table.partial_index_safe_for_columns(index, &fk_offsets)
            && fk_offsets.iter().enumerate().all(|(position, at)| {
                let length = index.prefix_length(position);
                length == crate::ddl::index_prefix::UNSPECIFIED_LENGTH
                    || column_flens.get(*at).is_some_and(|flen| length >= *flen)
            })
    };
    let index = if !covered(&clustered) && !table.indexes().iter().any(covered_index) {
        let definition = tidb_ast::IndexConstraintDefinition {
            kind: tidb_ast::IndexConstraintKind::Index,
            if_not_exists: false,
            name: Some(foreign_key.name.clone()),
            is_empty_index: false,
            parts: foreign_key
                .cols
                .iter()
                .map(|name| tidb_ast::IndexPart::Column {
                    name: name.clone(),
                    prefix_len: None,
                    desc: false,
                })
                .collect(),
            options: Default::default(),
        };
        super::indexes::prepare_add_index(
            table,
            super::index_changes::add_spec(&foreign_key.name, &definition),
            ctx,
            catalog.max_index_length(),
        )?;
        Some(definition)
    } else {
        None
    };
    Ok(PreparedConstraintChange::AddForeignKey { foreign_key, index })
}

fn validate_alter_foreign_key_parent(
    catalog: &Catalog,
    database: &str,
    child_table: &str,
    foreign_key: &KvForeignKey,
) -> Result<(), DriverError> {
    let self_reference = foreign_key.ref_schema.eq_ignore_ascii_case(database)
        && foreign_key.ref_table.eq_ignore_ascii_case(child_table)
        && foreign_key.cols.len() == foreign_key.ref_cols.len()
        && foreign_key
            .cols
            .iter()
            .zip(&foreign_key.ref_cols)
            .all(|(child, parent)| child.eq_ignore_ascii_case(parent));
    if self_reference {
        return Err(DriverError::DdlCoded {
            errno: 1215,
            message: "Cannot add foreign key constraint".to_owned(),
        });
    }

    // With checks off Go permits an as-yet-missing parent. There is no parent
    // index to validate in that deferred case; when the parent exists, its
    // covering-index rule still applies just as in Go's checkTableForeignKey.
    let Some(crate::TableEntry::Kv(parent)) =
        catalog.get_in(&foreign_key.ref_schema, &foreign_key.ref_table)
    else {
        return Ok(());
    };
    let Some(ref_offsets) = foreign_key
        .ref_cols
        .iter()
        .map(|column| {
            parent
                .columns
                .iter()
                .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(());
    };
    let clustered = parent
        .pk_handle_offset()
        .map(|offset| vec![offset])
        .unwrap_or_else(|| parent.common_handle_offsets().to_vec());
    let clustered_cover = ref_offsets.len() == 1 && ref_offsets == clustered;
    let column_flens: Vec<i64> = parent
        .columns
        .iter()
        .map(|column| column.field_type.flen())
        .collect();
    let index_cover = parent.indexes().iter().any(|index| {
        index.column_offsets.starts_with(&ref_offsets)
            && ref_offsets.iter().enumerate().all(|(position, offset)| {
                let length = index.prefix_length(position);
                length == super::index_prefix::UNSPECIFIED_LENGTH
                    || column_flens
                        .get(*offset)
                        .is_some_and(|flen| length >= *flen)
            })
    });
    if !clustered_cover && !index_cover {
        return Err(DriverError::DdlCoded {
            errno: 1822,
            message: format!(
                "Failed to add the foreign key constraint. Missing index for constraint '{}' in the referenced table '{}'",
                foreign_key.name, foreign_key.ref_table
            ),
        });
    }
    Ok(())
}

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
    if !std::sync::Arc::make_mut(table).drop_foreign_key(fk_name) {
        return Err(DriverError::UnknownColumnInAlter(fk_name.to_owned()));
    }
    Ok(())
}
