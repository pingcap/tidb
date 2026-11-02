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

//! Shared UPDATE/ODKU record policy (`executor.updateRecord`). The statement
//! adapter owns rollback; this owner never tries to undo rows by writing SQL
//! preimages back into the table. FK checks precede cascades after all records
//! have been written, as in `ExecStmt.handleForeignKeyTrigger`.

use super::*;

pub(crate) struct UpdateRecords<'a> {
    triggers: &'a [tidb_planner_core_operator_physicalop::FkTriggerNode],
    tables: Vec<UpdateTable>,
}

struct UpdateTable {
    database: String,
    name: String,
    updates: Vec<ForeignKeyUpdate>,
}

struct ForeignKeyUpdate {
    old: Vec<Datum>,
    new: Vec<Datum>,
    ignore: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum UpdateOutcome {
    Changed,
    Unchanged,
    Skipped,
    ForeignKeySkipped,
}

impl UpdateOutcome {
    pub(crate) fn changed(self) -> bool {
        matches!(self, Self::Changed)
    }
    pub(crate) fn unchanged(self) -> bool {
        matches!(self, Self::Unchanged)
    }
    /// Go counts a row as touched (`CLIENT_FOUND_ROWS`) only when
    /// `updateRecord` did not report it ignored: a duplicate skipped under
    /// IGNORE is not a found row.
    pub(crate) fn touched(self) -> bool {
        matches!(self, Self::Changed | Self::Unchanged)
    }
    pub(crate) fn ignored(self) -> bool {
        matches!(self, Self::ForeignKeySkipped)
    }
}

impl<'a> UpdateRecords<'a> {
    pub(crate) fn new(triggers: &'a [tidb_planner_core_operator_physicalop::FkTriggerNode]) -> Self {
        Self {
            triggers,
            tables: Vec::new(),
        }
    }

    /// Assignments and on-update timestamps have been evaluated by the caller.
    /// Generated columns, write errors, locks and FK ownership are shared by
    /// all three SQL update entrypoints.
    pub(crate) fn write(
        &mut self,
        catalog: &mut SessionExtendedInfoSchema,
        database: &str,
        name: &str,
        id: &TableHandle,
        old: &[Datum],
        new: &mut Vec<Datum>,
        partitions: Option<&[i64]>,
        ignore: bool,
        generation: GeneratedWrite,
        assigned: &[usize],
        ctx: &crate::StmtContext,
    ) -> Result<UpdateOutcome, DriverError> {
        let _region = tidb_tracing::start_thread_region_ex("executor.updateRecord");
        if self.triggers.iter().any(|trigger| {
            matches!(
                trigger.kind,
                tidb_planner_core_operator_physicalop::FkTriggerKind::Cascade { .. }
            )
        }) {
            ctx.mark_fk_cascades();
        }
        let entry = catalog.get_in(database, name).ok_or_else(|| {
            DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
            )
        })?;
        if old == new {
            if let TableEntry::Kv(kv) = entry {
                let mut key_set = LOCK_ROW_KEY;
                if ctx.lock_unchanged_keys() {
                    key_set |= LOCK_UNIQUE_KEYS;
                }
                add_unchanged_keys_for_lock_by_row(kv, id, old, key_set, ctx)?;
            }
            return Ok(UpdateOutcome::Unchanged);
        }
        if let TableEntry::Kv(kv) = entry {
            // Go `updateRecord` step 4: only the generated assignments the
            // statement's ordered list carries.
            let listed = crate::driver::write_cast::update_generated_offsets(
                kv,
                assigned,
                matches!(generation, GeneratedWrite::OnDuplicate { .. }),
            );
            crate::driver::write_cast::materialize_generated_for_write_selected(
                kv,
                new,
                ctx,
                generation,
                None,
                Some(&listed),
            )?;
        }
        let fk_target = if crate::foreign_key::has_triggers(self.triggers, database, name) {
            let index = self
                .tables
                .iter()
                .position(|table| table.database == database && table.name == name)
                .unwrap_or_else(|| {
                    self.tables.push(UpdateTable {
                        database: database.to_owned(),
                        name: name.to_owned(),
                        updates: Vec::new(),
                    });
                    self.tables.len() - 1
                });
            Some(index)
        } else {
            None
        };
        if fk_target.is_some() && ignore {
            let check = crate::foreign_key::require_child_rows(
                catalog,
                self.triggers,
                database,
                name,
                std::slice::from_ref(new),
                ctx,
            )
            .and_then(|()| {
                // Go `updateRecord` under IGNORE hands every FK check the NEW
                // row (`checkFKIgnoreErr(ctx, sctx, fkChecks, newData)`) and
                // skips `updateRowNeedToCheck`, so a referred-side check asks
                // whether a child references the new key, as a deletion of
                // that key would.
                crate::foreign_key::check_parent_changes(
                    catalog,
                    self.triggers,
                    database,
                    name,
                    &[crate::foreign_key::ParentChange::Delete(new)],
                    ctx,
                )
            });
            if let Err(error) = check {
                if error.equal(&tidb_plannererrors::ERR_NO_REFERENCED_ROW2)
                    || error.equal(&tidb_plannererrors::ERR_ROW_IS_REFERENCED2)
                {
                    let warning = error.to_mysql_error();
                    ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        ));
                    return Ok(UpdateOutcome::ForeignKeySkipped);
                }
                return Err(error);
            }
        }
        let exchange_check =
            super::exchange_partition::exchange_partition_check(catalog, database, name)?;
        // Go updateRecord gives IGNORE's FK checks the evaluated candidate
        // before bad-NULL substitution and partition validation. Rejected FK
        // rows must not produce warnings from these later stages.
        let entry = catalog
            .get_in(database, name)
            .expect("the update target still exists");
        let level = generation.null_level(ctx);
        // Go `updateRecord` checks every column of the written row, the
        // hidden ones where DDL placed them included.
        for (value, column) in new.iter_mut().zip(super::stored_column_metadata(entry)) {
            crate::bad_null::handle_bad_null(value, &column.field_type, &column.name, level, ctx)?;
        }
        // Go `updateRecord`'s `checkRowForExchangePartition`, and
        // `partitionedTableUpdateRecord`'s constraint check for a row moving
        // into the exchanged partition.
        if let Some(check) = &exchange_check {
            check.check_row_for_exchange_partition(new, ctx)?;
            if let TableEntry::Kv(kv) = entry {
                check.check_constraint_for_exchange_partition(kv, new, ctx)?;
            }
        }
        if let (TableEntry::Kv(kv), Some(partitions)) = (entry, partitions) {
            if let Err(error) = kv.validate_update_partitions(old, new, partitions, ctx) {
                return ignored_write_error(kv_write_error(error), ignore, ctx);
            }
        }
        let entry = catalog
            .get_rows_mut_in(database, name)
            .expect("the update target still exists");
        let result = match entry {
            TableEntry::Kv(kv) => {
                let handle = id;
                let kv = std::sync::Arc::make_mut(kv);
                // The table layer currently relies on statement rollback for
                // index-write failures. An ignored duplicate must not dirty
                // that stage or undo earlier successful rows.
                if ignore {
                    match kv.with_statement_store(ctx, |table, store| table.row_conflicts(store, new, ctx)) {
                        Ok(conflicts) => {
                            if let Some(conflict) =
                                conflicts.into_iter().find(|c| c.handle != *handle)
                            {
                                return ignored_write_error(
                                    kv_write_error(conflict.error),
                                    true,
                                    ctx,
                                );
                            }
                        }
                        Err(error) => return ignored_write_error(kv_write_error(error), true, ctx),
                    }
                }
                // Every write caller retains the complete writable preimage.
                kv.with_statement_store(ctx, |table, store| table.update_row_with_old_context(store, handle, Some(old), new, ctx))
                    .map_err(kv_write_error)
            }
            _ => {
                return Err(DriverError::unsupported(
                    "table storage changed during UPDATE",
                ))
            }
        };
        if let Err(error) = result {
            return ignored_write_error(error, ignore, ctx);
        }
        // Go `updateRecord`: a row whose handle stayed locks its unique keys
        // as they were.
        if ctx.lock_unchanged_keys() {
            if let Some(TableEntry::Kv(kv)) = catalog.get_in(database, name) {
                if !kv.handle_columns_changed(old, new) {
                    add_unchanged_keys_for_lock_by_row(kv, id, old, LOCK_UNIQUE_KEYS, ctx)?;
                }
            }
        }
        if let Some(index) = fk_target {
            let accountant = ctx
                .statement_memory()
                .write_accountant(mem_quota::label::UPDATE);
            accountant.account_row(old).map_err(DriverError::from)?;
            accountant.account_row(new).map_err(DriverError::from)?;
            self.tables[index].updates.push(ForeignKeyUpdate {
                old: old.to_vec(),
                new: new.clone(),
                ignore,
            });
        }
        Ok(UpdateOutcome::Changed)
    }

    pub(crate) fn finish(
        self,
        catalog: &mut SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        // All checks see the final statement buffer, including writes to a
        // parent through a later joined target. A failure is returned to the
        // statement adapter so it also rolls back dependent-table writes.
        for table in self.tables.iter().filter(|table| !table.updates.is_empty()) {
            let (database, name, updates) = (&table.database, &table.name, &table.updates);
            let checked: Vec<_> = updates.iter().filter(|update| !update.ignore).collect();
            let old: Vec<_> = checked.iter().map(|update| update.old.clone()).collect();
            let new: Vec<_> = checked.iter().map(|update| update.new.clone()).collect();
            crate::foreign_key::require_updated_child_rows(
                catalog,
                self.triggers,
                database,
                name,
                &old,
                &new,
                ctx,
            )?;
            let changes: Vec<_> = checked
                .iter()
                .map(|update| crate::foreign_key::ParentChange::Update {
                    old: &update.old,
                    new: &update.new,
                })
                .collect();
            crate::foreign_key::check_parent_changes(
                catalog,
                self.triggers,
                database,
                name,
                &changes,
                ctx,
            )?;
        }
        for table in self.tables.iter().filter(|table| !table.updates.is_empty()) {
            let (database, name, updates) = (&table.database, &table.name, &table.updates);
            let changes: Vec<_> = updates
                .iter()
                .map(|update| crate::foreign_key::ParentChange::Update {
                    old: &update.old,
                    new: &update.new,
                })
                .collect();
            crate::foreign_key::cascade_parent_changes(
                catalog,
                self.triggers,
                database,
                name,
                &changes,
                ctx,
            )?;
        }
        Ok(())
    }
}

/// Go `lockRowKey` / `lockUniqueKeys`: which keys
/// [`add_unchanged_keys_for_lock_by_row`] locks.
pub(crate) const LOCK_ROW_KEY: u8 = 1 << 0;
pub(crate) const LOCK_UNIQUE_KEYS: u8 = 1 << 1;

/// Go `addUnchangedKeysForLockByRow`: in a pessimistic transaction, the
/// keys of a row the statement leaves as it is join the keys the statement
/// locks (`TxnCtx.AddUnchangedKeyForLock`).
pub(crate) fn add_unchanged_keys_for_lock_by_row(
    table: &tidb_table::tables::TableCommon,
    handle: &tidb_table::tables::TableHandle,
    row: &[Datum],
    key_set: u8,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(keys) = ctx.selected_lock_keys() else {
        return Ok(());
    };
    if !ctx.pessimistic_transaction() || key_set == 0 {
        return Ok(());
    }
    if key_set & LOCK_ROW_KEY != 0 {
        keys.insert(table.row_lock_key(handle, row, ctx).map_err(kv_write_error)?);
    }
    if key_set & LOCK_UNIQUE_KEYS != 0 {
        for key in table
            .unchanged_unique_lock_keys(handle, row, ctx)
            .map_err(kv_write_error)?
        {
            keys.insert(key);
        }
    }
    Ok(())
}

fn ignored_write_error(
    error: DriverError,
    ignore: bool,
    ctx: &crate::StmtContext,
) -> Result<UpdateOutcome, DriverError> {
    if ignore && error.equal(&tidb_kv::kv_errors::ERR_KEY_EXISTS) {
        let warning = error.to_mysql_error();
        ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        ));
    } else {
        handle_partition_write_error(error, ignore, ctx)?;
    }
    Ok(UpdateOutcome::Skipped)
}
