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

//! Go DeleteExec.removeRow / InsertValues.removeRow share record removal and
//! onRemoveRowForFK. Ordinary checks see all statement writes; IGNORE checks
//! one candidate before writing it. Statement staging owns rollback.

use super::{kv_read_error, TableHandle};
use crate::driver::{Catalog, DriverError, TableEntry};
use crate::foreign_key::{self, ParentChange};
use crate::StmtContext;
use tidb_datatype::Datum;
use tidb_planner::physical::FkTriggerNode;

pub(crate) struct DeleteRecords<'a> {
    triggers: &'a [FkTriggerNode],
    tables: Vec<(String, String, Vec<(Vec<Datum>, bool)>)>,
}

impl<'a> DeleteRecords<'a> {
    pub(crate) fn new(triggers: &'a [FkTriggerNode]) -> Self {
        Self {
            triggers,
            tables: Vec::new(),
        }
    }

    pub(crate) fn write(
        &mut self,
        catalog: &mut Catalog,
        database: &str,
        name: &str,
        id: &TableHandle,
        old: &[Datum],
        ignore: bool,
        ctx: &StmtContext,
    ) -> Result<bool, DriverError> {
        let has_triggers = foreign_key::has_triggers(self.triggers, database, name);
        if has_triggers && ignore {
            if let Err(error) = foreign_key::check_parent_changes(
                catalog,
                self.triggers,
                database,
                name,
                &[ParentChange::Delete(old)],
                ctx,
            ) {
                if matches!(error, DriverError::ForeignKeyRowIsReferenced { .. }) {
                    let warning = error.to_mysql_error();
                    ctx.append_warning_parts(warning.code, &warning.message);
                    return Ok(false);
                }
                return Err(error);
            }
        }
        match catalog.get_mut_in(database, name) {
            Some(TableEntry::Kv(kv)) => {
                let handle = id;
                let kv = std::sync::Arc::make_mut(kv);
                kv.delete_row_with_old_context(handle, old, ctx)
                    .map_err(|error| kv_read_error("row delete failed", error))?;
            }
            _ => {
                return Err(DriverError::unsupported(
                    "table storage changed during DELETE",
                ))
            }
        }
        if has_triggers {
            ctx.statement_memory()
                .write_accountant(crate::mem_quota::label::DELETE)
                .account_row(old)
                .map_err(DriverError::from)?;
            let slot = self
                .tables
                .iter()
                .position(|(db, table, _)| db == database && table == name)
                .unwrap_or_else(|| {
                    self.tables
                        .push((database.to_owned(), name.to_owned(), Vec::new()));
                    self.tables.len() - 1
                });
            self.tables[slot].2.push((old.to_vec(), ignore));
        }
        Ok(true)
    }

    pub(crate) fn finish(
        self,
        catalog: &mut Catalog,
        ctx: &StmtContext,
    ) -> Result<(), DriverError> {
        for (database, table, rows) in &self.tables {
            let changes: Vec<_> = rows
                .iter()
                .filter(|(_, ignore)| !ignore)
                .map(|(row, _)| ParentChange::Delete(row))
                .collect();
            foreign_key::check_parent_changes(
                catalog,
                self.triggers,
                database,
                table,
                &changes,
                ctx,
            )?;
        }
        for (database, table, rows) in &self.tables {
            let changes: Vec<_> = rows
                .iter()
                .map(|(row, _)| ParentChange::Delete(row))
                .collect();
            foreign_key::cascade_parent_changes(
                catalog,
                self.triggers,
                database,
                table,
                &changes,
                ctx,
            )?;
        }
        Ok(())
    }
}
