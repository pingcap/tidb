// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `buildUpdateLists`'s resolution of a single-table UPDATE's `SET`
//! columns: each is found among the table's names (an alias REPLACES the table
//! name as the only qualifier), `_tidb_rowid` is writable only under
//! `tidb_opt_write_row_id`, and a generated column accepts only `DEFAULT`
//! (`IsDefaultExprSameColumn`), which leaves it to its expression.

use tidb_ast::{Expr, UpdateKind, UpdateStmt};
use tidb_datatype::{FieldName, FieldNameMetadata, IdentifierMetadata};
use tidb_exprctx::Columns;

use tidb_infoschema::table_source::TableSource;
use super::update_build::WRITE_ROW_ID_REFUSED;
use super::{find_field_name, PlanBuilder};
use tidb_planner_core_base::PlanError;

/// One resolved `SET` item of a single-table UPDATE.
#[derive(Clone, Debug)]
pub struct SingleUpdateAssignment {
    /// The target column's offset in the table's stored row (its place in
    /// `TableInfo.Columns`), or the stored width for `_tidb_rowid` -- the
    /// convention of the multi-table [`UpdateAssign::column`].
    ///
    /// [`UpdateAssign::column`]: tidb_planner_core_operator_physicalop::UpdateAssign::column
    pub column: usize,
    /// The value to rewrite: `None` for a generated column's own `DEFAULT`
    /// (the generation expression stays the value source); a bare `DEFAULT`
    /// is the assigned column's own default.
    pub value: Option<Expr>,
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Resolves every assignment of a single-table UPDATE, in `SET` order.
    pub fn resolve_single_update_assignments(
        &self,
        update: &UpdateStmt,
        allow_write_row_id: bool,
    ) -> Result<Vec<SingleUpdateAssignment>, PlanError> {
        let UpdateKind::Single(table_ref) = &update.kind else {
            return Err(PlanError::internal(
                "single-table resolution is for a single-table UPDATE",
            ));
        };
        let (database, written) = self.update_table_path(table_ref)?;
        let table = self.source.find_table(&database, &written).ok_or_else(|| {
            PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .fast_generate_by_args(&[(&database).into(), (&written).into()]),
            )
        })?;
        let visible = table_ref.alias.as_deref().unwrap_or(&written);
        let field = |column: &str| {
            FieldName::new(FieldNameMetadata {
                original_table: IdentifierMetadata::new(written.as_str()),
                original_column: IdentifierMetadata::new(column),
                database: IdentifierMetadata::new(table.db_name.as_str()),
                table: IdentifierMetadata::new(visible),
                column: IdentifierMetadata::new(column),
            })
        };
        let columns: Vec<&tidb_infoschema::table_source::SourceColumn> = table
            .columns
            .iter()
            .filter(|column| !column.is_hidden && column.is_public)
            .collect();
        let mut names: Vec<FieldName> = columns.iter().map(|column| field(&column.name)).collect();
        // Go exposes the heap handle to assignment admission, independently of
        // whether anything else names it.
        let extra_handle = !table.pk_is_handle && !table.is_common_handle && !table.is_memory_table;
        if extra_handle {
            names.push(field(tidb_model::column::EXTRA_HANDLE_NAME));
        }
        // A name's position counts the visible columns; the stored row also
        // holds the hidden ones, wherever DDL placed them.
        let stored_offset = |position: usize| {
            columns.get(position).map_or(table.columns.len(), |named| {
                table
                    .columns
                    .iter()
                    .position(|column| column.id == named.id)
                    .expect("a visible column is one of the table's columns")
            })
        };
        let mut resolved = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let column = find_field_name(&names, &assignment.col).ok_or_else(|| {
                PlanError::from(
                    tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                        tidb_hack::go_to_lower(assignment.col.join(".")).into(),
                        "field list".into(),
                    ]),
                )
            })?;
            if extra_handle && column == columns.len() && !allow_write_row_id {
                return Err(PlanError::internal(WRITE_ROW_ID_REFUSED));
            }
            if columns.get(column).is_some_and(|column| column.is_generated) {
                let own_default = match &assignment.value {
                    Expr::Default(None) => true,
                    Expr::Default(Some(path)) => find_field_name(&names, path) == Some(column),
                    _ => false,
                };
                if !own_default {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_BAD_GENERATED_COLUMN
                            .generate_with_stack_by_args(&[
                                (columns[column].name.clone()).into(),
                                written.into(),
                            ]),
                    ));
                }
                resolved.push(SingleUpdateAssignment {
                    column: stored_offset(column),
                    value: None,
                });
                continue;
            }
            let value = match &assignment.value {
                Expr::Default(None) => Expr::Default(Some(assignment.col.clone())),
                value => value.clone(),
            };
            resolved.push(SingleUpdateAssignment {
                column: stored_offset(column),
                value: Some(value),
            });
        }
        Ok(resolved)
    }
}
