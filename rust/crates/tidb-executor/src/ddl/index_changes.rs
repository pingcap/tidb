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

//! Original-schema index admission for local ALTER. Go collects only admitted
//! jobs, including their resolved anonymous names, before checking conflicts.
//! Backfill and metadata application use the staged catalog after admission.

use super::{indexes, Catalog, DriverError};
use tidb_ast::{AlterTableAction, IndexConstraintDefinition};

pub(super) enum PreparedIndexChange<'a> {
    Add {
        name: String,
        definition: &'a IndexConstraintDefinition,
    },
    Drop {
        name: String,
        id: i64,
    },
    Rename {
        from: String,
        to: String,
        id: i64,
        hidden_columns: Vec<(i64, String)>,
    },
    Visibility {
        name: String,
        visible: bool,
    },
}

pub(super) fn is_index_change(action: &AlterTableAction) -> bool {
    matches!(
        action,
        AlterTableAction::AddIndexConstraint(_)
            | AlterTableAction::DropIndex { .. }
            | AlterTableAction::RenameIndex(_)
            | AlterTableAction::AlterIndexVisibility(_)
    )
}

pub(super) fn add_spec<'a>(
    name: &'a str,
    definition: &'a IndexConstraintDefinition,
) -> indexes::IndexSpec<'a> {
    indexes::IndexSpec {
        name,
        comment: definition.options.comment.as_deref().unwrap_or(""),
        unique: matches!(
            definition.kind,
            tidb_ast::IndexConstraintKind::Unique
                | tidb_ast::IndexConstraintKind::UniqueKey
                | tidb_ast::IndexConstraintKind::UniqueIndex
        ),
        parts: &definition.parts,
        visible: indexes::is_visible(&definition.options),
        global: definition.options.global,
        if_not_exists: definition.if_not_exists,
        condition: definition.options.condition.as_ref(),
    }
}

/// None means an admitted no-op, or a non-index action. No rows are read and
/// no catalog object is changed here; notes still belong to the statement.
pub(super) fn prepare<'a>(
    action: &'a AlterTableAction,
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    ctx: &crate::StmtContext,
    multi_schema: bool,
) -> Result<Option<PreparedIndexChange<'a>>, DriverError> {
    if !is_index_change(action) {
        return Ok(None);
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let find = |name: &str| {
        table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(name))
    };
    let missing = |name: &str| DriverError::KeyNotExists {
        key: name.to_owned(),
        table: table_name.to_owned(),
    };
    match action {
        AlterTableAction::AddIndexConstraint(definition) => {
            match definition.kind {
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
            if definition.options.condition.is_some() && table.partition().is_some() {
                return Err(indexes::unsupported_partial_index(
                    "partial index on partitioned table is not supported",
                ));
            }
            let name = definition.name.clone().unwrap_or_else(|| {
                let first = match definition.parts.first() {
                    Some(tidb_ast::IndexPart::Column { name, .. }) => name.as_str(),
                    Some(tidb_ast::IndexPart::Expr { .. }) => "expression_index",
                    None => "",
                };
                indexes::anonymous_index_name(table.indexes(), first)
            });
            Ok(Some(
                match indexes::prepare_add_index(
                    table,
                    add_spec(&name, definition),
                    ctx,
                    catalog.max_index_length(),
                )? {
                    indexes::IndexAdmission::Change(()) => {
                        PreparedIndexChange::Add { name, definition }
                    }
                    indexes::IndexAdmission::Note(note) => {
                        ctx.append_suppressed(&note);
                        return Ok(None);
                    }
                },
            ))
        }
        AlterTableAction::DropIndex { name, if_exists } => Ok(Some(
            match indexes::prepare_drop_index(catalog, database, table_name, name, *if_exists)? {
                indexes::IndexAdmission::Change(id) => PreparedIndexChange::Drop {
                    name: name.clone(),
                    id,
                },
                indexes::IndexAdmission::Note(note) => {
                    ctx.append_suppressed(&note);
                    return Ok(None);
                }
            },
        )),
        AlterTableAction::RenameIndex(rename) => {
            // Go ValidateRenameIndex: missing source, exact-spelling no-op,
            // then a different existing target, in precisely that order.
            let index = find(&rename.from).ok_or_else(|| missing(&rename.from))?;
            if rename.from == rename.to {
                return Ok(None);
            }
            if !rename.from.eq_ignore_ascii_case(&rename.to) {
                if let Some(existing) = find(&rename.to) {
                    return Err(DriverError::DuplicateKeyName(existing.name.clone()));
                }
            }
            // Go renameHiddenColumns/getExpressionIndexOriginName. Offsets
            // address index column references here, so only definitions need
            // new names; IDs and key bytes must stay unchanged.
            let hidden_columns = table
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(offset, column)| {
                    if !table.is_hidden(offset) {
                        return None;
                    }
                    let name = column.name.strip_prefix("_V$_").unwrap_or(&column.name);
                    let origin = name.rsplit_once('_').map_or(name, |(origin, _)| origin);
                    (origin == rename.from)
                        .then(|| (column.id, column.name.replacen(&rename.from, &rename.to, 1)))
                })
                .collect();
            Ok(Some(PreparedIndexChange::Rename {
                from: rename.from.clone(),
                to: rename.to.clone(),
                id: index.id,
                hidden_columns,
            }))
        }
        AlterTableAction::AlterIndexVisibility(alter) => {
            let index = find(&alter.name).ok_or_else(|| missing(&alter.name))?;
            let visible = alter.visibility != tidb_ast::IndexVisibility::Invisible;
            if !multi_schema && index.visible == visible {
                return Ok(None);
            }
            // Go retains visibility no-ops in a multi-schema job, unlike
            // exact-spelling RENAME, so they still participate in conflicts.
            Ok(Some(PreparedIndexChange::Visibility {
                name: alter.name.clone(),
                visible,
            }))
        }
        _ => unreachable!("index action was checked above"),
    }
}

impl PreparedIndexChange<'_> {
    pub(super) fn execute(
        self,
        catalog: &mut Catalog,
        database: &str,
        table_name: &str,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        match self {
            Self::Add { name, definition } => {
                let max_index_length = catalog.max_index_length();
                indexes::add_index_to_table(
                    catalog,
                    database,
                    table_name,
                    add_spec(&name, definition),
                    ctx,
                    max_index_length,
                )
            }
            Self::Drop { id, .. } => {
                indexes::drop_prepared_index(catalog, database, table_name, id, ctx)
            }
            Self::Visibility { name, visible } => {
                super::alter_metadata::alter_index_visibility_action(
                    catalog, database, table_name, &name, visible,
                )
            }
            Self::Rename {
                id,
                to,
                hidden_columns,
                ..
            } => {
                let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name)
                else {
                    return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                        format!("{database}.{table_name}"),
                    )));
                };
                let table = std::sync::Arc::make_mut(table);
                // Go renames while the covering index still exists in the
                // drop-column transition. In the synchronous image that
                // identity may already be retired; it must remain absent.
                if let Some(name) = table
                    .indexes()
                    .iter()
                    .find(|index| index.id == id)
                    .map(|index| index.name.clone())
                {
                    table
                        .index_mut_by_name(&name)
                        .expect("identity resolved above")
                        .name = to;
                }
                for (id, name) in hidden_columns {
                    if let Some(column) = table
                        .columns_mut()
                        .iter_mut()
                        .find(|column| column.id == id)
                    {
                        column.name = name;
                    }
                }
                Ok(())
            }
        }
    }
}
