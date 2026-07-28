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

//! `EXPLAIN` / `EXPLAIN ANALYZE`: [`Session::explain_stmt`] is the
//! `AdminStmt::Explain` arm of [`crate::Session::dispatch_admin_stmt`],
//! pulled out on its own because the dispatch fans out over every wrapped
//! statement kind (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) twice -- once for
//! the plan-only form and once for `ANALYZE`, which additionally executes.
//!
//! See `tidb_executor::explain`'s module doc for every place this tier's plan
//! text diverges from Go's and why.

use crate::*;

impl Session {
    // Go's preprocessor rejects an unrecognized format name with this exact
    // message before the statement is even planned (captured: `explain
    // format = 'bogus' ...` -> `Unknown EXPLAIN format name: 'bogus'`).
    //
    // Go's EXPLAIN plans without executing (an `EXPLAIN INSERT` inserts no
    // row, captured), and so does this: `tidb_executor::explain_select_stmt`
    // re-runs the driver's own read-path decisions without touching storage.
    // Real `EXPLAIN ANALYZE` EXECUTES the wrapped statement to gather its
    // runtime counters (confirmed by capture), so this tier does too -- see
    // `tidb_executor::explain_analyze_select_stmt`/
    // `explain_analyze_insert_stmt`'s own docs for which operators get a real
    // `actRows` and which print the honest `N/A` placeholder this tier uses
    // for every counter (timing, memory, disk) it does not collect at all.
    pub(crate) fn explain_stmt(
        &mut self,
        explain: &tidb_ast::ExplainStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(format) = tidb_executor::ExplainFormat::parse(&explain.format) else {
            return Err(DriverError::Unsupported("unknown EXPLAIN format name"));
        };
        let Some(target) = explain.statement() else {
            return Err(DriverError::Unsupported(
                "EXPLAIN of a plan digest is not supported yet",
            ));
        };
        let current_db = self.current_db.clone();
        // Both forms plan through the driver's own build path (see
        // `tidb_executor::explain`), which needs the statement context every
        // executor it builds evaluates against -- plain EXPLAIN builds the
        // pipeline without draining it, so no row is produced and no write
        // runs.
        let ctx = self.statement_context(true);
        if explain.analyze {
            let (columns, rows) = match target {
                Stmt::Query(query) => {
                    let tidb_ast::QueryStmt::Select(select) = &**query else {
                        return Err(DriverError::Unsupported(
                            "EXPLAIN ANALYZE of a set operation is not supported yet",
                        ));
                    };
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::explain_analyze_select_stmt(
                            select,
                            catalog,
                            &current_db,
                            &ctx,
                            format,
                        )
                    })?
                }
                Stmt::Dml(dml) => match &**dml {
                    tidb_ast::DmlStmt::Insert(insert) => self.with_catalog_mut(|catalog| {
                        tidb_executor::explain_analyze_insert_stmt(
                            insert,
                            catalog,
                            &current_db,
                            &ctx,
                            format,
                        )
                    })?,
                    tidb_ast::DmlStmt::Update(update) => self.with_catalog_mut(|catalog| {
                        tidb_executor::explain_analyze_update_stmt(
                            update,
                            catalog,
                            &current_db,
                            &ctx,
                            format,
                        )
                    })?,
                    tidb_ast::DmlStmt::Delete(delete) => self.with_catalog_mut(|catalog| {
                        tidb_executor::explain_analyze_delete_stmt(
                            delete,
                            catalog,
                            &current_db,
                            &ctx,
                            format,
                        )
                    })?,
                    _ => {
                        return Err(DriverError::Unsupported(
                            "only EXPLAIN ANALYZE of a SELECT, INSERT, UPDATE, or \
                             DELETE is supported yet",
                        ));
                    }
                },
                _ => {
                    return Err(DriverError::Unsupported(
                        "only EXPLAIN ANALYZE of a SELECT, INSERT, UPDATE, or DELETE \
                         is supported yet",
                    ));
                }
            };
            self.drain_eval_warnings(&ctx);
            return Ok(Some(StmtOutput::Rows { columns, rows }));
        }
        let (columns, rows) = match target {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    return Err(DriverError::Unsupported(
                        "EXPLAIN of a set operation is not supported yet",
                    ));
                };
                self.with_catalog_mut(|catalog| {
                    tidb_executor::explain_select_stmt(select, catalog, &current_db, &ctx, format)
                })?
            }
            // A write's plan is the same plan recorder run over the read path
            // the driver's write executes; nothing is executed -- no row is
            // read or written -- which is also what Go does (`EXPLAIN
            // INSERT` inserts no row, captured).
            Stmt::Dml(dml) => match &**dml {
                tidb_ast::DmlStmt::Insert(insert) => self.with_catalog_mut(|catalog| {
                    tidb_executor::explain_insert_stmt(insert, catalog, &current_db, &ctx, format)
                })?,
                tidb_ast::DmlStmt::Update(update) => self.with_catalog_mut(|catalog| {
                    tidb_executor::explain_update_stmt(update, catalog, &current_db, &ctx, format)
                })?,
                tidb_ast::DmlStmt::Delete(delete) => self.with_catalog_mut(|catalog| {
                    tidb_executor::explain_delete_stmt(delete, catalog, &current_db, &ctx, format)
                })?,
                _ => {
                    return Err(DriverError::Unsupported(
                        "only EXPLAIN of INSERT, UPDATE, or DELETE is supported yet",
                    ));
                }
            },
            _ => {
                return Err(DriverError::Unsupported(
                    "only EXPLAIN of a SELECT, INSERT, UPDATE, or DELETE is supported yet",
                ));
            }
        };
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }
}
