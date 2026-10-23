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
use tidb_expr::Columns;
use tidb_hack::go_to_lower;

use super::PlanBuilder;
use super::catalog::TableSource;
use crate::physical::DeleteColPos;

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
                .any(|index| !index.condition_expr_string.is_empty())
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
            for column in &index.columns {
                keep[column.offset] = true;
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
            pairs.insert((
                visible,
                go_to_lower(tidb_model::column::EXTRA_HANDLE_NAME),
            ));
        }
        Some((DeleteColPos { kept, extra_handle }, pairs))
    }
}
