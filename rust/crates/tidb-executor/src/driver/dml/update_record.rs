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
    triggers: &'a [tidb_planner::physical::FkTriggerNode],
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
    pub(crate) fn touched(self) -> bool {
        !matches!(self, Self::ForeignKeySkipped)
    }
    pub(crate) fn ignored(self) -> bool {
        matches!(self, Self::ForeignKeySkipped)
    }
}

impl<'a> UpdateRecords<'a> {
    pub(crate) fn new(triggers: &'a [tidb_planner::physical::FkTriggerNode]) -> Self {
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
        catalog: &mut Catalog,
        database: &str,
        name: &str,
        id: &TableHandle,
        old: &[Datum],
        new: &mut Vec<Datum>,
        partitions: Option<&[i64]>,
        ignore: bool,
        generation: GeneratedWrite,
        ctx: &crate::StmtContext,
    ) -> Result<UpdateOutcome, DriverError> {
        let entry = catalog.get_in(database, name).ok_or_else(|| {
            DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
                "{database}.{name}"
            )))
        })?;
        if old == new {
            if let (TableEntry::Kv(kv), Some(keys)) = (entry, ctx.selected_lock_keys()) {
                keys.insert(kv.row_lock_key(id, old, ctx).map_err(kv_write_error)?);
            }
            return Ok(UpdateOutcome::Unchanged);
        }
        if let TableEntry::Kv(kv) = entry {
            materialize_generated_for_write(&kv.columns, new, ctx, generation)?;
        }
        let level = generation.null_level(ctx);
        for (value, (column, field_type)) in new.iter_mut().zip(entry.columns()) {
            crate::bad_null::handle_bad_null(value, field_type, column, level, ctx)?;
        }
        if let (TableEntry::Kv(kv), Some(partitions)) = (entry, partitions) {
            if let Err(error) = kv.validate_update_partitions(old, new, partitions, ctx) {
                return ignored_write_error(kv_write_error(error), ignore, ctx);
            }
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
                &ctx.session_zone(),
            )
            .and_then(|()| {
                crate::foreign_key::check_parent_changes(
                    catalog,
                    self.triggers,
                    database,
                    name,
                    &[crate::foreign_key::ParentChange::Update { old, new }],
                    ctx,
                )
            });
            if let Err(error) = check {
                if matches!(
                    error,
                    DriverError::ForeignKeyNoReferencedRow { .. }
                        | DriverError::ForeignKeyRowIsReferenced { .. }
                ) {
                    let warning = error.to_mysql_error();
                    ctx.append_warning_parts(warning.code, &warning.message);
                    return Ok(UpdateOutcome::ForeignKeySkipped);
                }
                return Err(error);
            }
        }
        let entry = catalog
            .get_mut_in(database, name)
            .expect("the update target still exists");
        let result = match entry {
            TableEntry::Kv(kv) => {
                let handle = id;
                let kv = std::sync::Arc::make_mut(kv);
                // The table layer currently relies on statement rollback for
                // index-write failures. An ignored duplicate must not dirty
                // that stage or undo earlier successful rows.
                if ignore {
                    match kv.row_conflicts(new, ctx) {
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
                kv.update_row_with_old_context(handle, Some(old), new, ctx)
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
        catalog: &mut Catalog,
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
                &ctx.session_zone(),
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

fn ignored_write_error(
    error: DriverError,
    ignore: bool,
    ctx: &crate::StmtContext,
) -> Result<UpdateOutcome, DriverError> {
    if ignore && matches!(error, DriverError::DuplicateEntry { .. }) {
        let warning = error.to_mysql_error();
        ctx.append_warning_parts(warning.code, &warning.message);
    } else {
        handle_partition_write_error(error, ignore, ctx)?;
    }
    Ok(UpdateOutcome::Skipped)
}
