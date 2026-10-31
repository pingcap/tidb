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

//! Go `pruneAndBuildColPositionInfoForDelete`
//! (`pkg/planner/core/logical_plan_builder.go:5887`) for a single-table
//! DELETE: the columns the read keeps, and the position information
//! (`physicalop.TblColPosInfo`) the `Delete` executor splits each row by.
//!
//! The columns DELETE reads are the handle's and every index's, because
//! removing a record needs nothing else; the rest is pruned from the read, so
//! `delete from t` scans the narrowest covering index. Go declines for a
//! partitioned table, a partial index, and (statement-wide) a table another
//! table's foreign key references -- the last is the caller's `has_fk`. This
//! port also keeps the whole row where its DELETE executor needs it:
//! generated or hidden (expression-index) columns, and temporary tables.

use std::collections::BTreeSet;

use tidb_ast::SelectStmt;
use tidb_exprctx::Columns;
use tidb_hack::go_to_lower;

use tidb_infoschema::table_source::TableSource;
use super::PlanBuilder;
use tidb_planner_core_operator_physicalop::{DeleteColPos, DeleteHandle, DeleteTablePos};

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// The pruning decision for the single-table read of a DELETE (the
    /// `SELECT` that carries its `FROM`, `WITH` and hints): the position
    /// information, and the `(table or alias, column)` pairs the read keeps
    /// in lower case. `None` where nothing is pruned.
    pub(super) fn single_table_delete_pruning(
        &self,
        select: &SelectStmt,
        has_fk: bool,
    ) -> Option<(DeleteColPos, BTreeSet<(String, String)>)> {
        let from = select.from.as_ref()?;
        if from.right.is_some() {
            return None;
        }
        let tidb_ast::JoinNode::Table(table_ref) = &from.left else {
            return None;
        };
        // A common table expression is not a base table.
        if let [name] = table_ref.name.as_slice() {
            if select.with.as_ref().is_some_and(|with| {
                with.ctes
                    .iter()
                    .any(|cte| go_to_lower(&cte.name) == go_to_lower(name))
            }) {
                return None;
            }
        }
        let (database, name) = match table_ref.name.as_slice() {
            [name] => (self.source.current_database(), name.as_str()),
            [database, name] => (database.as_str(), name.as_str()),
            _ => return None,
        };
        let table = self.source.find_table(database, name)?;
        if table.is_memory_table || has_fk {
            return None;
        }
        if table.is_partitioned
            || table.is_temporary
            || table.columns.iter().any(|column| column.is_hidden)
            || table.columns.iter().any(|column| column.is_generated)
            || table
                .indexes
                .iter()
                .any(|index| !index.read().condition_expr_string.is_empty())
        {
            return None;
        }
        let mut keep = vec![false; table.columns.len()];
        if table.pk_is_handle {
            for offset in &table.handle_col_offsets {
                keep[*offset] = true;
            }
        }
        for offset in &table.common_handle_col_offsets {
            keep[*offset] = true;
        }
        for index in &table.indexes {
            for column in index.read().columns.iter_deref() {
                keep[column.read().offset as usize] = true;
            }
        }
        if keep.iter().all(|kept| *kept) {
            return None;
        }
        let kept: Vec<usize> = (0..keep.len()).filter(|offset| keep[*offset]).collect();
        let extra_handle = !table.pk_is_handle && !table.is_common_handle;
        let visible = go_to_lower(table_ref.alias.as_deref().unwrap_or(name));
        let mut pairs: BTreeSet<(String, String)> = kept
            .iter()
            .map(|offset| (visible.clone(), go_to_lower(&table.columns[*offset].name)))
            .collect();
        if extra_handle {
            pairs.insert((visible, go_to_lower(tidb_model::column::EXTRA_HANDLE_NAME)));
        }
        Some((DeleteColPos { kept, extra_handle }, pairs))
    }
}

/// One table a multi-table DELETE removes rows from, as the statement named
/// it: the schema and name of the base table, and the name (alias, else
/// table name) its columns carry in the read, in lower case.
#[derive(Clone, Debug)]
pub struct DeleteTarget {
    /// The base table's schema.
    pub database: String,
    /// The base table's stored name.
    pub table: String,
    /// The alias, else the table name, lower-cased (Go `TblName.L`).
    pub visible: String,
}

/// What a multi-table DELETE's build is asked to decide.
#[derive(Clone, Debug)]
pub struct DeleteMultiInput {
    /// The tables to delete from, one entry per distinct source.
    pub targets: Vec<DeleteTarget>,
    /// Whether any target is referenced by a foreign key (Go `hasFK`):
    /// no table is pruned then.
    pub has_fk: bool,
}

/// A target's row layout before the final projection fixes positions.
struct PendingTable {
    table_id: i64,
    database: String,
    table: String,
    stored_width: usize,
    /// `(schema position, table column offset)` of each stored column.
    columns: Vec<(usize, usize)>,
    handle: PendingHandle,
}

enum PendingHandle {
    Int(usize),
    Common(Vec<usize>),
    Extra(usize),
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `pruneAndBuildColPositionInfoForDelete` for a multi-table DELETE:
    /// every target keeps the handle's and its indexes' columns while the
    /// other sources are read whole, and each target's [`DeleteTablePos`]
    /// says where its columns and handle sit in the pruned row. Returns the
    /// per-column keep decision over `schema`, and a function from the kept
    /// positions to the finished layouts.
    pub(super) fn multi_delete_layout(
        &self,
        input: &DeleteMultiInput,
        schema: &tidb_expr::schema::Schema,
        names: &[tidb_datatype::FieldName],
    ) -> Option<(Vec<bool>, Vec<DeleteTablePosDraft>)> {
        let mut keep = vec![true; schema.columns.len()];
        let mut drafts = Vec::new();
        for target in &input.targets {
            let table = self.source.find_table(&target.database, &target.table)?;
            if table.is_memory_table {
                return None;
            }
            let run: Vec<usize> = names
                .iter()
                .enumerate()
                .filter(|(position, name)| {
                    *position < schema.columns.len()
                        && name.names.table.lower == target.visible
                        && (name.names.database.lower.is_empty()
                            || name.names.database.lower == go_to_lower(&target.database))
                })
                .map(|(position, _)| position)
                .collect();
            if run.is_empty() {
                return None;
            }
            let offset_of = |position: usize| {
                let id = schema.columns[position].id;
                table.columns.iter().position(|column| column.id == id)
            };
            // Go skips pruning for a partitioned table, a partial index and a
            // foreign key anywhere in the statement; this port also keeps the
            // whole row for generated or hidden columns and temporary tables.
            let skip = input.has_fk
                || table.is_partitioned
                || table.is_temporary
                || table.columns.iter().any(|column| column.is_hidden)
                || table.columns.iter().any(|column| column.is_generated)
                || table
                    .indexes
                    .iter()
                    .any(|index| !index.read().condition_expr_string.is_empty());
            let mut needed = vec![false; table.columns.len()];
            if table.pk_is_handle {
                for offset in &table.handle_col_offsets {
                    needed[*offset] = true;
                }
            }
            for offset in &table.common_handle_col_offsets {
                needed[*offset] = true;
            }
            for index in &table.indexes {
                for column in index.read().columns.iter_deref() {
                    needed[column.read().offset as usize] = true;
                }
            }
            let prune = !skip && needed.iter().any(|kept| !kept);
            let mut columns = Vec::new();
            let mut extra = None;
            let mut by_offset = vec![None; table.columns.len()];
            for position in &run {
                if schema.columns[*position].id == tidb_model::column::EXTRA_HANDLE_ID {
                    extra = Some(*position);
                    continue;
                }
                // Go always prunes `_tidb_commit_ts`.
                if schema.columns[*position].id == super::EXTRA_COMMIT_TS_ID {
                    continue;
                }
                let offset = offset_of(*position)?;
                by_offset[offset] = Some(*position);
                if prune && !needed[offset] {
                    keep[*position] = false;
                    continue;
                }
                columns.push((*position, offset));
            }
            let handle = if table.pk_is_handle {
                PendingHandle::Int(by_offset[*table.handle_col_offsets.first()?]?)
            } else if table.is_common_handle {
                PendingHandle::Common(
                    table
                        .common_handle_col_offsets
                        .iter()
                        .map(|offset| by_offset[*offset])
                        .collect::<Option<Vec<_>>>()?,
                )
            } else {
                PendingHandle::Extra(extra?)
            };
            drafts.push(PendingTable {
                table_id: table.table_id,
                database: table.db_name.clone(),
                table: table.table_name.clone(),
                stored_width: table.columns.len(),
                columns,
                handle,
            });
        }
        Some((
            keep,
            drafts
                .into_iter()
                .map(|draft| DeleteTablePosDraft(draft))
                .collect(),
        ))
    }
}

/// A layout whose positions are still in the unpruned schema.
pub(super) struct DeleteTablePosDraft(PendingTable);

impl DeleteTablePosDraft {
    /// The finished layout once the final projection kept `kept` (ascending
    /// positions of the unpruned schema).
    pub(super) fn finish(self, kept: &[usize]) -> Option<DeleteTablePos> {
        let new_position = |old: usize| kept.iter().position(|position| *position == old);
        let draft = self.0;
        let columns = draft
            .columns
            .iter()
            .map(|(position, offset)| Some((new_position(*position)?, *offset)))
            .collect::<Option<Vec<_>>>()?;
        let handle = match &draft.handle {
            PendingHandle::Int(position) => DeleteHandle::Int(new_position(*position)?),
            PendingHandle::Common(positions) => DeleteHandle::Common(
                positions
                    .iter()
                    .map(|position| new_position(*position))
                    .collect::<Option<Vec<_>>>()?,
            ),
            PendingHandle::Extra(position) => DeleteHandle::Extra(new_position(*position)?),
        };
        Some(DeleteTablePos {
            table_id: draft.table_id,
            database: draft.database,
            table: draft.table,
            stored_width: draft.stored_width,
            columns,
            handle,
        })
    }
}
