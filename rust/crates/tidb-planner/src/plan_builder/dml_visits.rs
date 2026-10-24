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

use tidb_ast::{DeleteKind, DeleteStmt, UpdateStmt};
use tidb_expr::Columns;

use super::catalog::TableSource;
use super::visit_info::{VisitAuthError, VisitInfo};
use super::PlanBuilder;
use crate::plan_base::PlanError;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
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

