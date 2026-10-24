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

//! The write privileges Go's `buildUpdate` and `buildDelete` append to
//! `b.visitInfo` after they built the read: `UpdatePriv` for each
//! assignment's table (`buildUpdateLists`, with no `authErr`, so a denial is
//! 8121), and `DeletePriv` for each table a DELETE removes rows from (with the
//! `ErrTableaccessDenied` `authErr`).

use tidb_ast::{DeleteKind, DeleteStmt, Expr, UpdateKind, UpdateStmt};
use tidb_expr::Columns;

use super::catalog::TableSource;
use super::dml_source::{dml_source_select, single_table_join};
use super::visit_info::{VisitAuthError, VisitInfo};
use super::PlanBuilder;
use crate::plan_base::PlanError;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Everything Go's `buildUpdate` appends to `b.visitInfo`: what building
    /// the read and the SET expressions visits (`SelectPriv` of each source,
    /// the subqueries' tables), then `UpdatePriv` of each written table.
    ///
    /// A read that cannot be built contributes the visits it made before it
    /// failed: the statement's own planning raises that error, while the write
    /// targets still resolve (or fail) here.
    pub fn update_visits(&mut self, update: &UpdateStmt) -> Result<Vec<VisitInfo>, PlanError> {
        let from = match &update.kind {
            UpdateKind::Single(table) => single_table_join(table),
            UpdateKind::Multi { from, .. } => (**from).clone(),
        };
        let select = dml_source_select(
            update.with.as_ref(),
            &update.hints,
            from,
            update.where_clause.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
        );
        let _built = match &update.kind {
            UpdateKind::Multi { .. } => self
                .build_multi_update_dml_source(&select, update, true)
                .map(|_| ()),
            UpdateKind::Single(_) => {
                self.resolve_single_update_assignments(update, true)
                    .and_then(|assignments| {
                        let values: Vec<Option<Expr>> = assignments
                            .iter()
                            .map(|assignment| assignment.value.clone())
                            .collect();
                        self.build_update_dml_source(&select, &values, false)
                            .map(|_| ())
                    })
            }
        };
        let mut visits = std::mem::take(&mut self.visit_info);
        visits.extend(self.update_write_visits(update)?);
        Ok(visits)
    }

    /// Everything Go's `buildDelete` appends to `b.visitInfo`: the read's
    /// visits (the last, `SelectPriv` of the source, popped for a
    /// non-qualified delete: "it does not require Select Priv"), then
    /// `DeletePriv` of each table removed from.
    pub fn delete_visits(&mut self, delete: &DeleteStmt) -> Result<Vec<VisitInfo>, PlanError> {
        let from = match &delete.kind {
            DeleteKind::Single(table) => single_table_join(table),
            DeleteKind::Multi { from, .. } => (**from).clone(),
        };
        let select = dml_source_select(
            delete.with.as_ref(),
            &delete.hints,
            from,
            delete.where_clause.as_ref(),
            &delete.order_by,
            delete.limit.as_ref(),
        );
        let _built = self.build_dml_source(&select, false);
        let mut visits = std::mem::take(&mut self.visit_info);
        if delete.where_clause.is_none() && delete.order_by.is_empty() {
            visits.pop();
        }
        visits.extend(self.delete_write_visits(delete)?);
        Ok(visits)
    }

    /// Go `buildUpdateLists`'s `appendVisitInfo(UpdatePriv, dbName,
    /// name.OrigTblName.L, ...)` per assignment, each distinct table once
    /// (a repeat of the same lowered pair checks nothing new).
    pub fn update_write_visits(&mut self, update: &UpdateStmt) -> Result<Vec<VisitInfo>, PlanError> {
        let mut visits: Vec<VisitInfo> = Vec::new();
        for target in self.resolve_update_targets(update)? {
            let database = tidb_mysql::to_lowercase(&target.database);
            let table = tidb_mysql::to_lowercase(&target.table);
            if visits
                .iter()
                .any(|visit| visit.database == database && visit.table == table)
            {
                continue;
            }
            visits.push(VisitInfo {
                privilege: tidb_mysql::UpdatePriv,
                database,
                table,
                column: String::new(),
                error: VisitAuthError::None,
            });
        }
        Ok(visits)
    }

    /// Go `buildDelete`'s `appendVisitInfo(DeletePriv, ...)`: one per target
    /// of a multi-table DELETE (each distinct table once), or the only table
    /// of a single-table one -- a common table expression being
    /// `ErrNonUpdatableTable`.
    pub fn delete_write_visits(&self, delete: &DeleteStmt) -> Result<Vec<VisitInfo>, PlanError> {
        let tables: Vec<(String, String)> = match &delete.kind {
            DeleteKind::Multi { .. } => self
                .resolve_delete_targets(delete)?
                .into_iter()
                .map(|target| (target.database, target.table))
                .collect(),
            DeleteKind::Single(table) => {
                if let [name] = table.name.as_slice() {
                    if delete.with.as_ref().is_some_and(|with| {
                        with.ctes.iter().any(|cte| {
                            tidb_hack::go_to_lower(&cte.name) == tidb_hack::go_to_lower(name)
                        })
                    }) {
                        return Err(PlanError::non_updatable_table(table.name.join("."), "DELETE"));
                    }
                }
                vec![self.update_table_path(table)?]
            }
        };
        let mut visits: Vec<VisitInfo> = Vec::new();
        for (database, table) in tables {
            let database = tidb_mysql::to_lowercase(&database);
            let table = tidb_mysql::to_lowercase(&table);
            if visits
                .iter()
                .any(|visit| visit.database == database && visit.table == table)
            {
                continue;
            }
            visits.push(VisitInfo {
                privilege: tidb_mysql::DeletePriv,
                database,
                table: table.clone(),
                column: String::new(),
                error: VisitAuthError::TableAccessDenied {
                    command: "DELETE",
                    table,
                },
            });
        }
        Ok(visits)
    }
}

