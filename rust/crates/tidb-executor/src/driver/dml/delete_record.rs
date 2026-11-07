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
//! onRemoveRowForFK. Under IGNORE a row's checks run before it is removed
//! (`checkFKIgnoreErr`); otherwise its keys join the statement's checks,
//! which run once every row is written. Statement staging owns rollback.

use crate::WithStatementStore as _;
use super::{kv_read_error, TableHandle};
use crate::driver::{SessionExtendedInfoSchema, DriverError, TableEntry};
use crate::foreign_key::WithForeignKeyTrigger;
use crate::StmtContext;
use std::cell::RefCell;
use tidb_datatype::Datum;

pub(crate) struct DeleteRecords<'a> {
    fk: &'a RefCell<WithForeignKeyTrigger>,
}

impl<'a> DeleteRecords<'a> {
    pub(crate) fn new(fk: &'a RefCell<WithForeignKeyTrigger>) -> Self {
        Self { fk }
    }

    pub(crate) fn write(
        &mut self,
        catalog: &mut SessionExtendedInfoSchema,
        database: &str,
        name: &str,
        id: &TableHandle,
        old: &[Datum],
        ignore: bool,
        ctx: &StmtContext,
    ) -> Result<bool, DriverError> {
        let has_triggers = self.fk.borrow().has_triggers(database, name);
        // Go `DeleteExec`: under IGNORE, a row a check rejects is skipped.
        if has_triggers
            && ignore
            && self
                .fk
                .borrow_mut()
                .check_fk_ignore_err(catalog, database, name, old, ctx)?
        {
            return Ok(false);
        }
        match catalog.get_rows_mut_in(database, name) {
            Some(TableEntry::Kv(kv)) => {
                let handle = id;
                let kv = std::sync::Arc::make_mut(kv);
                kv.with_statement_store(ctx, |table, store| table.delete_row_with_old_context(store, handle, old, ctx))
                    .map_err(|error| kv_read_error("row delete failed", error))?;
            }
            _ => {
                return Err(DriverError::unsupported(
                    "table storage changed during DELETE",
                ))
            }
        }
        // Go `onRemoveRowForFK`: the row's keys and values are all the
        // checks and cascades keep, without a memory tracker.
        if has_triggers {
            self.fk
                .borrow_mut()
                .on_remove_row_for_fk(catalog, database, name, old, ignore, ctx)?;
        }
        Ok(true)
    }
}
