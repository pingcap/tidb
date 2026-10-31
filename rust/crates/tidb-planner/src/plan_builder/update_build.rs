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

//! Go `buildUpdate` for a multi-table UPDATE
//! (`logical_plan_builder.go:6067`): the read, every `SET` expression
//! rewritten over it (`buildUpdateLists`), the projection that prunes the
//! columns of read-only sources, and the layout of each updated table in a
//! row of the pruned read (`buildUpdateTblColPosInfos`).

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{Expr, SelectStmt, UpdateKind, UpdateStmt};
use tidb_datatype::FieldName;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_hack::go_to_lower;

use tidb_infoschema::table_source::TableSource;
use super::marker::MarkerKind;
use super::update_targets::{assignment_column, collect_tables, path_column, UpdateTarget};
use super::{
    check_not_multi_column_row, snapshot_schema_and_names, ClauseCode, PlanBuilder,
    EXTRA_COMMIT_TS_ID,
};
use tidb_planner_core_operator_logicalop::{LogicalPlan, LogicalProjection};
use tidb_planner_core_operator_physicalop::{DeleteHandle, UpdateAssign, UpdateTablePos};
use tidb_planner_core_base::PlanError;

/// Go's message for writing `_tidb_rowid` without `tidb_opt_write_row_id`,
/// raised as a plain error and so reaching the client as 1105.
pub const WRITE_ROW_ID_REFUSED: &str =
    "insert, update and replace statements for _tidb_rowid are not supported";

/// One `SET` assignment whose column was found in the read.
struct ResolvedAssignment {
    /// The assignment's position in the `SET` list.
    index: usize,
    /// The column's position in the read.
    position: usize,
    /// The base table the column belongs to.
    target: UpdateTarget,
    /// A generated column assigned its own `DEFAULT`: no expression.
    own_default: bool,
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Builds a multi-table UPDATE's read with its assignments. Returns the
    /// plan, one expression per assignment (`None` for a generated column's
    /// own `DEFAULT`) and the optimization flags; the updated tables'
    /// layouts and the assignments' targets are left in
    /// [`Self::update_tables`] and [`Self::update_assigns`].
    pub fn build_multi_update_dml_source(
        &mut self,
        select: &SelectStmt,
        update: &UpdateStmt,
        allow_write_row_id: bool,
    ) -> Result<(LogicalPlan, Vec<Option<Expression>>, u64), PlanError> {
        let UpdateKind::Multi { from, .. } = &update.kind else {
            return Err(PlanError::internal(
                "a multi-table UPDATE build needs a join source",
            ));
        };
        let mut from_tables = Vec::new();
        collect_tables(&from.left, &mut from_tables);
        if let Some(right) = &from.right {
            collect_tables(right, &mut from_tables);
        }
        self.with_dml_hints(select, |builder| {
            builder.with_dml_ctes(select, |builder| {
                let (mut plan, _) = builder.build_dml_source_in_block(select, false)?;
                builder.cur_clause = ClauseCode::FieldList;
                let (frozen_schema, frozen_names) = snapshot_schema_and_names(&plan);

                // `buildUpdateLists`, first loop: every SET column is found
                // in the read and belongs to an updatable table.
                let mut resolved = Vec::with_capacity(update.assignments.len());
                for (index, assignment) in update.assignments.iter().enumerate() {
                    let column = assignment_column(assignment)?;
                    let position = tidb_expr::find_field_name(&frozen_names, &column)
                        .map_err(|_| {
                            PlanError::from(
                                tidb_plannererrors::ERR_AMBIGUOUS
                                    .generate_with_stack_by_args(&[
                                        (column.display_name()).into(),
                                        "field list".into(),
                                    ]),
                            )
                        })?
                        .ok_or_else(|| {
                            PlanError::from(
                                tidb_plannererrors::ERR_UNKNOWN_COLUMN
                                    .generate_with_stack_by_args(&[
                                        tidb_hack::go_to_lower(column.column.original.clone())
                                            .into(),
                                        "field list".into(),
                                    ]),
                            )
                        })?;
                    let target = builder.updatable_table_of(
                        update,
                        &from_tables,
                        &frozen_names[position].names,
                    )?;
                    resolved.push(ResolvedAssignment {
                        index,
                        position,
                        target,
                        own_default: false,
                    });
                }

                // The second loop: a generated column accepts only DEFAULT,
                // or DEFAULT of the same column.
                for assigned in &mut resolved {
                    let table = builder
                        .source
                        .find_table(&assigned.target.database, &assigned.target.table)
                        .ok_or_else(|| {
                            PlanError::from(
                                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                                    .fast_generate_by_args(&[
                                        (&assigned.target.database).into(),
                                        (&assigned.target.table).into(),
                                    ]),
                            )
                        })?;
                    let id = frozen_schema.columns[assigned.position].id;
                    let Some(offset) = table.columns.iter().position(|column| column.read().id == id)
                    else {
                        continue;
                    };
                    if !table.columns[offset].read().is_generated() {
                        continue;
                    }
                    let same = match &update.assignments[assigned.index].value {
                        Expr::Default(None) => true,
                        Expr::Default(Some(path)) => {
                            let default_column = path_column(path)?;
                            tidb_expr::find_field_name(&frozen_names, &default_column)
                                .ok()
                                .flatten()
                                .is_some_and(|found| found == assigned.position)
                        }
                        _ => false,
                    };
                    if !same {
                        return Err(PlanError::from(
                            tidb_plannererrors::ERR_BAD_GENERATED_COLUMN
                                .generate_with_stack_by_args(&[
                                    (table.columns[offset].read().name.original().to_owned()).into(),
                                    (table.table_name.clone()).into(),
                                ]),
                        ));
                    }
                    assigned.own_default = true;
                }

                // The third loop: rewrite each value over the read.
                let mut expressions = Vec::with_capacity(update.assignments.len());
                for assigned in &resolved {
                    let assignment = &update.assignments[assigned.index];
                    let extra = frozen_schema.columns[assigned.position].id
                        == tidb_model::column::EXTRA_HANDLE_ID;
                    if extra && !allow_write_row_id {
                        return Err(PlanError::internal(WRITE_ROW_ID_REFUSED));
                    }
                    if assigned.own_default {
                        expressions.push(None);
                        continue;
                    }
                    // A bare DEFAULT is the assigned column's own default.
                    let value = match &assignment.value {
                        Expr::Default(None) => Expr::Default(Some(assignment.col.clone())),
                        value => value.clone(),
                    };
                    let mut scratch = Self::clause_scratch(&value);
                    let (next_plan, _) = builder.lower_scalar_subqueries(plan, &mut scratch)?;
                    plan = next_plan;
                    let (schema, names) = snapshot_schema_and_names(&plan);
                    let mut markers = BTreeMap::new();
                    markers.insert(MarkerKind::Column, schema.columns.clone());
                    let rewritten = builder.rewrite_scalar(&scratch, &schema, &names, &markers)?;
                    check_not_multi_column_row(&rewritten)?;
                    expressions.push(Some(rewritten));
                }
                let written: Vec<&ResolvedAssignment> = resolved
                    .iter()
                    .filter(|assigned| !assigned.own_default)
                    .collect();

                // The updated aliases, in the order the read lists them.
                let alias_of = |position: usize| {
                    (
                        frozen_names[position].names.table.lower.clone(),
                        frozen_names[position].names.database.lower.clone(),
                    )
                };
                let mut aliases: Vec<((String, String), &ResolvedAssignment)> = Vec::new();
                for assigned in &written {
                    let alias = alias_of(assigned.position);
                    if !aliases.iter().any(|(existing, _)| *existing == alias) {
                        aliases.push((alias, assigned));
                    }
                }
                aliases.sort_by_key(|(alias, _)| {
                    (0..frozen_names.len())
                        .find(|candidate| alias_of(*candidate) == *alias)
                        .unwrap_or(usize::MAX)
                });

                // Go keeps what an updated alias carries and what the SET
                // expressions read; every other column leaves the read.
                let referenced: BTreeSet<i64> = expressions
                    .iter()
                    .flatten()
                    .flat_map(tidb_expr::simple_expr::extract_columns)
                    .map(|column| column.unique_id)
                    .collect();
                let (schema, names) = snapshot_schema_and_names(&plan);
                let kept: Vec<usize> = (0..schema.columns.len())
                    .filter(|position| {
                        (*position < frozen_names.len()
                            && aliases
                                .iter()
                                .any(|(alias, _)| *alias == alias_of(*position)))
                            || referenced.contains(&schema.columns[*position].unique_id)
                    })
                    .collect();
                if kept.len() < schema.columns.len() {
                    plan = builder.prune_update_read(plan, &kept, &names);
                }
                let new_position = |old: usize| kept.iter().position(|position| *position == old);

                // `buildUpdateTblColPosInfos`: where each updated table's
                // columns and handle sit in a row of the pruned read.
                let mut tables = Vec::new();
                for (alias, first) in &aliases {
                    let target = &first.target;
                    let table = builder
                        .source
                        .find_table(&target.database, &target.table)
                        .ok_or_else(|| PlanError::internal("an updated table vanished"))?;
                    let run: Vec<usize> = (0..frozen_names.len())
                        .filter(|position| alias_of(*position) == *alias)
                        .collect();
                    let mut columns = Vec::new();
                    let mut extra = None;
                    let mut by_offset = vec![None; table.columns.len()];
                    for position in &run {
                        let id = schema.columns[*position].id;
                        if id == tidb_model::column::EXTRA_HANDLE_ID {
                            extra = new_position(*position);
                            continue;
                        }
                        if id == EXTRA_COMMIT_TS_ID {
                            continue;
                        }
                        let Some(offset) = table.columns.iter().position(|column| column.read().id == id)
                        else {
                            return Err(PlanError::internal(
                                "an updated column is not in its table",
                            ));
                        };
                        let Some(row_position) = new_position(*position) else {
                            return Err(PlanError::internal("an updated column was pruned"));
                        };
                        by_offset[offset] = Some(row_position);
                        columns.push((row_position, offset));
                    }
                    let handle = if table.pk_is_handle {
                        DeleteHandle::Int(
                            table
                                .handle_col_offsets
                                .first()
                                .and_then(|offset| by_offset[*offset])
                                .ok_or_else(|| {
                                    PlanError::internal("an updated handle was pruned")
                                })?,
                        )
                    } else if table.is_common_handle {
                        DeleteHandle::Common(
                            table
                                .common_handle_col_offsets
                                .iter()
                                .map(|offset| by_offset[*offset])
                                .collect::<Option<Vec<_>>>()
                                .ok_or_else(|| {
                                    PlanError::internal("an updated common handle was pruned")
                                })?,
                        )
                    } else {
                        DeleteHandle::Extra(extra.ok_or_else(|| {
                            PlanError::internal("an updated heap table has no row id")
                        })?)
                    };
                    let source_ref = from_tables.iter().find(|candidate| {
                        let visible = candidate
                            .alias
                            .as_deref()
                            .or_else(|| candidate.name.last().map(String::as_str))
                            .unwrap_or_default();
                        go_to_lower(visible) == alias.0
                    });
                    tables.push(UpdateTablePos {
                        table_id: table.table_id,
                        database: table.db_name.clone(),
                        table: table.table_name.clone(),
                        visible: frozen_names[run[0]].names.table.original.clone(),
                        stored_width: table.columns.len(),
                        columns,
                        handle,
                        partitions: source_ref
                            .map(|reference| reference.partitions.clone())
                            .unwrap_or_default(),
                    });
                }
                let mut assigns = Vec::with_capacity(written.len());
                for assigned in &written {
                    let alias = alias_of(assigned.position);
                    let table_index = aliases
                        .iter()
                        .position(|(candidate, _)| *candidate == alias)
                        .ok_or_else(|| PlanError::internal("an assigned alias has no layout"))?;
                    let table = builder
                        .source
                        .find_table(&assigned.target.database, &assigned.target.table)
                        .ok_or_else(|| PlanError::internal("an updated table vanished"))?;
                    let id = frozen_schema.columns[assigned.position].id;
                    let column = if id == tidb_model::column::EXTRA_HANDLE_ID {
                        table.columns.len()
                    } else {
                        table
                            .columns
                            .iter()
                            .position(|candidate| candidate.read().id == id)
                            .ok_or_else(|| {
                                PlanError::internal("an assigned column is not in its table")
                            })?
                    };
                    assigns.push(UpdateAssign {
                        table: table_index,
                        column,
                        expr: assigned.index,
                    });
                }
                builder.update_assigns = assigns;
                builder.update_tables = tables;
                Ok((plan, expressions, builder.get_opt_flag()))
            })
        })
    }

    /// Go `buildUpdate`'s read-only-column pruning: a root projection keeps
    /// the columns at `kept`; any other root gets a projection of its own.
    fn prune_update_read(
        &mut self,
        plan: LogicalPlan,
        kept: &[usize],
        names: &[FieldName],
    ) -> LogicalPlan {
        let schema = plan.schema().cloned().unwrap_or_default();
        let output_columns: Vec<_> = kept
            .iter()
            .enumerate()
            .map(|(index, position)| {
                let mut column = schema.columns[*position].clone();
                column.index = index as i64;
                column
            })
            .collect();
        let output_names: Vec<_> = kept.iter().map(|position| names[*position].clone()).collect();
        match plan {
            LogicalPlan::Projection(mut projection) => {
                projection.exprs = kept
                    .iter()
                    .map(|position| projection.exprs[*position].clone())
                    .collect();
                projection
                    .base
                    .base
                    .set_schema(Some(Schema::new(output_columns)));
                projection.base.base.set_output_names(output_names);
                LogicalPlan::Projection(projection)
            }
            other => {
                let exprs = kept
                    .iter()
                    .map(|position| Expression::Column(schema.columns[*position].clone()))
                    .collect();
                let mut projection =
                    LogicalProjection::new(self.base(LogicalProjection::TYPE), exprs);
                projection.base.set_children(vec![other]);
                projection
                    .base
                    .base
                    .set_schema(Some(Schema::new(output_columns)));
                projection.base.base.set_output_names(output_names);
                LogicalPlan::Projection(projection)
            }
        }
    }
}
