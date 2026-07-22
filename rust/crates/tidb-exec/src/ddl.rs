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
// See the License for the specific language governing permissions and
// limitations under the License.

//! DDL coordination and `ALTER TABLE` execution.
//!
//! `Database::run` owns statement-level status; this module owns the complete
//! DDL dispatch and TiDB's static capability boundary before implicit commit.
//! The Go dispatch owner is `pkg/executor/ddl.go`; physical catalog families
//! live in the source-shaped leaves below, preserving any source-ordered
//! catalog checks that occur after commit.

pub(crate) mod affinity;
mod create_table;
mod index;
pub(crate) mod partition_metadata;
mod table;
mod table_capability;

use tidb_ast::{AlterTableAction, ColumnDef, ColumnOption, ColumnPosition, DdlStmt, Expr};
use tidb_datatype::Datum;
use tidb_expr::eval;

use crate::catalog::table_key;
use crate::{Database, ExecError, Outcome, Table};

use self::index::{index_key_group, index_requires_unique_key_group, prepare_index_definition};
use self::table_capability::executable_alter_action;

/// A column definition's declared `DEFAULT` expression, if any — the same
/// extraction `crate::ddl::create_table` does, reused so `ALTER TABLE`
/// keeps `Table::col_defaults` aligned with `cols`/`col_types`.
fn column_default(column: &ColumnDef) -> Option<Expr> {
    column.options.iter().find_map(|o| match o {
        ColumnOption::Default(e) => Some(e.clone()),
        _ => None,
    })
}

impl Database {
    /// Runs one DDL statement. Unsupported statements return before the
    /// implicit-commit boundary; executable DDL then preserves the source
    /// order of commit, catalog resolution, and physical mutation while
    /// delegating each effect to its source-owned leaf.
    pub(crate) fn run_ddl(&mut self, ddl: &DdlStmt) -> Result<Outcome, ExecError> {
        match ddl {
            DdlStmt::CreateTable(statement) => {
                let preflight = create_table::preflight(statement)?;
                self.transaction.commit();
                let name = table_key(&statement.name);
                if self.tables.contains_key(&name) || self.sequences.borrow().contains_key(&name) {
                    if statement.if_not_exists {
                        return Ok(Outcome::Done);
                    }
                    return Err(ExecError::Unsupported("table or sequence already exists"));
                }
                let prepared = create_table::prepare(statement, preflight)?;
                self.publish_table(prepared);
                Ok(Outcome::Done)
            }
            DdlStmt::CreateIndex(_) => Err(ExecError::Unsupported("CREATE INDEX")),
            DdlStmt::DropIndex(_) => Err(ExecError::Unsupported("DROP INDEX")),
            DdlStmt::CreateView(_) => Err(ExecError::Unsupported("CREATE VIEW")),
            DdlStmt::CreateDatabase { .. } => Err(ExecError::Unsupported("CREATE DATABASE")),
            DdlStmt::AlterDatabase { .. } => Err(ExecError::Unsupported("ALTER DATABASE")),
            DdlStmt::AlterInstance(_) => Err(ExecError::Unsupported("ALTER INSTANCE")),
            DdlStmt::AlterRange(_) => Err(ExecError::Unsupported("ALTER RANGE")),
            DdlStmt::CreatePlacementPolicy(_) => {
                Err(ExecError::Unsupported("CREATE PLACEMENT POLICY"))
            }
            DdlStmt::AlterPlacementPolicy(_) => {
                Err(ExecError::Unsupported("ALTER PLACEMENT POLICY"))
            }
            DdlStmt::AlterTable(statement) => {
                let action = executable_alter_action(statement)?;
                if let AlterTableAction::AddIndexConstraint(index) = action {
                    let existing = self
                        .tables
                        .get(&table_key(&statement.name))
                        .ok_or_else(|| ExecError::UnknownTable(table_key(&statement.name)))?;
                    prepare_index_definition(existing, index)?;
                    if index_requires_unique_key_group(index) {
                        index_key_group(existing, index)?;
                    }
                }
                self.transaction.commit();
                self.alter_table(&statement.name, action)?;
                Ok(Outcome::Done)
            }
            DdlStmt::RenameTable(statement) => {
                self.transaction.commit();
                for (old, new) in &statement.pairs {
                    self.rename_table(old, new)?;
                }
                Ok(Outcome::Done)
            }
            DdlStmt::RenameUser { .. } => Err(ExecError::Unsupported("RENAME USER")),
            DdlStmt::LockTables(_) => Err(ExecError::Unsupported("LOCK TABLES")),
            DdlStmt::UnlockTables => Err(ExecError::Unsupported("UNLOCK TABLES")),
            DdlStmt::DropTable(statement) => {
                self.transaction.commit();
                self.drop_table(statement)?;
                Ok(Outcome::Done)
            }
            DdlStmt::DropView { .. } => Err(ExecError::Unsupported("DROP VIEW")),
            DdlStmt::DropDatabase { .. } => Err(ExecError::Unsupported("DROP DATABASE")),
            DdlStmt::DropPlacementPolicy(_) => Err(ExecError::Unsupported("DROP PLACEMENT POLICY")),
            DdlStmt::DropResourceGroup(_) => Err(ExecError::Unsupported("DROP RESOURCE GROUP")),
            DdlStmt::CreateResourceGroup(_) => Err(ExecError::Unsupported("CREATE RESOURCE GROUP")),
            DdlStmt::AlterResourceGroup(_) => Err(ExecError::Unsupported("ALTER RESOURCE GROUP")),
            DdlStmt::CreateMaskingPolicy(_) => Err(ExecError::Unsupported("CREATE MASKING POLICY")),
            DdlStmt::CreateUser { .. } => Err(ExecError::Unsupported("CREATE USER")),
            DdlStmt::CreateRole { .. } => Err(ExecError::Unsupported("CREATE ROLE")),
            DdlStmt::AlterUser(_) => Err(ExecError::Unsupported("ALTER USER")),
            DdlStmt::DropUser { is_role: false, .. } => Err(ExecError::Unsupported("DROP USER")),
            DdlStmt::DropUser { is_role: true, .. } => Err(ExecError::Unsupported("DROP ROLE")),
            DdlStmt::TruncateTable(name) => {
                self.transaction.commit();
                self.truncate_table(name)?;
                Ok(Outcome::Done)
            }
            DdlStmt::CreateSequence(statement) => {
                self.transaction.commit();
                self.create_sequence(statement)?;
                Ok(Outcome::Done)
            }
            DdlStmt::CreateProcedure(_) => Err(ExecError::Unsupported("CREATE PROCEDURE")),
            DdlStmt::AlterSequence(statement) => {
                self.transaction.commit();
                self.alter_sequence(statement)?;
                Ok(Outcome::Done)
            }
            DdlStmt::DropSequence(statement) => {
                self.transaction.commit();
                self.drop_sequence(statement)?;
                Ok(Outcome::Done)
            }
            DdlStmt::DropProcedure(_) => Err(ExecError::Unsupported("DROP PROCEDURE")),
            DdlStmt::FlashbackDatabase(_) => Err(ExecError::Unsupported("FLASHBACK DATABASE")),
            DdlStmt::RecoverTable(_) => Err(ExecError::Unsupported("RECOVER TABLE")),
            DdlStmt::FlashbackToTimestamp(_) => Err(ExecError::Unsupported("FLASHBACK CLUSTER")),
            DdlStmt::FlashbackTable(_) => Err(ExecError::Unsupported("FLASHBACK TABLE")),
            DdlStmt::OptimizeTable(_) => Err(ExecError::Unsupported("OPTIMIZE TABLE")),
            DdlStmt::RepairTable(_) => Err(ExecError::Unsupported("ADMIN REPAIR TABLE")),
        }
    }

    /// `ADD COLUMN` inserts a new column at `position` (the end, if
    /// unwritten): every existing row gets the column's `DEFAULT` value (or
    /// `NULL` if it has none — verified against real TiDB, which backfills
    /// existing rows with the declared default rather than leaving them
    /// `NULL`), and every `key_groups` index at or past the insertion point
    /// shifts by one so existing `PRIMARY KEY`/`UNIQUE` constraints keep
    /// pointing at the right column; a `PRIMARY KEY`/`UNIQUE` option on the
    /// new column itself becomes its own group, same as `CREATE TABLE`.
    /// `DROP COLUMN` removes a column from every row and from `key_groups`
    /// (shifting later indices down, and dropping any group left empty).
    pub(crate) fn alter_table(
        &mut self,
        name: &[String],
        action: &AlterTableAction,
    ) -> Result<(), ExecError> {
        let key = table_key(name);
        // `RENAME` moves the catalog entry itself, so it's handled before
        // the other actions' shared `get_mut` borrow below (which none of
        // them need) — reuses the same mechanics `RENAME TABLE` uses.
        if let AlterTableAction::RenameTable { new_name } = action {
            return self.rename_table(name, new_name);
        }
        let default_value = match action {
            AlterTableAction::AddColumn { column, .. } => {
                match column.options.iter().find_map(|o| match o {
                    ColumnOption::Default(e) => Some(e),
                    _ => None,
                }) {
                    Some(e) => eval(e)?,
                    None => Datum::Null,
                }
            }
            AlterTableAction::DropColumn { .. }
            | AlterTableAction::ModifyColumn { .. }
            | AlterTableAction::ChangeColumn { .. }
            | AlterTableAction::AddIndexConstraint(_)
            | AlterTableAction::AddForeignKey(_) => Datum::Null,
            AlterTableAction::RenameTable { .. } => unreachable!("handled above"),
            _ => unreachable!("ALTER capability preflight rejected unsupported action"),
        };
        let table = self
            .tables
            .get_mut(&key)
            .ok_or_else(|| ExecError::UnknownTable(key.clone()))?;
        match action {
            AlterTableAction::AddColumn {
                column, position, ..
            } => {
                let at = match position {
                    ColumnPosition::Default => table.cols.len(),
                    ColumnPosition::First => 0,
                    ColumnPosition::After(name) => {
                        let idx = table
                            .cols
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(name))
                            .ok_or_else(|| ExecError::UnknownColumn(name.clone()))?;
                        idx + 1
                    }
                };
                for group in &mut table.key_groups {
                    for idx in group.iter_mut() {
                        if *idx >= at {
                            *idx += 1;
                        }
                    }
                }
                if let Some(auto_increment) = &mut table.auto_increment {
                    if auto_increment.column >= at {
                        auto_increment.column += 1;
                    }
                }
                table.cols.insert(at, column.name.clone());
                table.col_types.insert(at, column.ty.clone());
                table.col_defaults.insert(at, column_default(column));
                for row in &mut table.rows {
                    row.insert(at, default_value.clone());
                }
                if column
                    .options
                    .iter()
                    .any(ColumnOption::is_inline_primary_key)
                {
                    table.key_groups.insert(0, vec![at]);
                }
                if column
                    .options
                    .iter()
                    .any(ColumnOption::is_inline_unique_key)
                {
                    table.key_groups.push(vec![at]);
                }
            }
            AlterTableAction::DropColumn { name, .. } => {
                let idx = table
                    .cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::UnknownColumn(name.clone()))?;
                match table.auto_increment {
                    Some(auto_increment) if auto_increment.column == idx => {
                        return Err(ExecError::Unsupported("DROP AUTO_INCREMENT column"));
                    }
                    Some(mut auto_increment) if auto_increment.column > idx => {
                        auto_increment.column -= 1;
                        table.auto_increment = Some(auto_increment);
                    }
                    _ => {}
                }
                table.cols.remove(idx);
                table.col_types.remove(idx);
                table.col_defaults.remove(idx);
                for row in &mut table.rows {
                    row.remove(idx);
                }
                table.key_groups = table
                    .key_groups
                    .iter()
                    .filter_map(|group| {
                        let shifted: Vec<usize> = group
                            .iter()
                            .filter(|&&i| i != idx)
                            .map(|&i| if i > idx { i - 1 } else { i })
                            .collect();
                        (!shifted.is_empty()).then_some(shifted)
                    })
                    .collect();
            }
            AlterTableAction::ModifyColumn { column, position } => {
                let from = table
                    .cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(&column.name))
                    .ok_or_else(|| ExecError::UnknownColumn(column.name.clone()))?;
                if table
                    .auto_increment
                    .is_some_and(|auto_increment| auto_increment.column == from)
                {
                    return Err(ExecError::Unsupported("MODIFY AUTO_INCREMENT column"));
                }
                table.col_types[from] = column.ty.clone();
                table.col_defaults[from] = column_default(column);
                reposition_column(table, from, position)?;
            }
            AlterTableAction::ChangeColumn {
                old_name,
                column,
                position,
            } => {
                let from = table
                    .cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(old_name))
                    .ok_or_else(|| ExecError::UnknownColumn(old_name.clone()))?;
                if table
                    .auto_increment
                    .is_some_and(|auto_increment| auto_increment.column == from)
                {
                    return Err(ExecError::Unsupported("CHANGE AUTO_INCREMENT column"));
                }
                table.cols[from] = column.name.clone();
                table.col_types[from] = column.ty.clone();
                table.col_defaults[from] = column_default(column);
                reposition_column(table, from, position)?;
            }
            AlterTableAction::RenameTable { .. } => unreachable!("handled above"),
            // Physical scans remain full-table scans, but the catalog must
            // retain the logical key: it owns TiDB's local index-name
            // namespace and lets later DDL validate against it.
            AlterTableAction::AddIndexConstraint(index) => {
                let metadata = prepare_index_definition(table, index)?;
                if index_requires_unique_key_group(index) {
                    table.key_groups.push(index_key_group(table, index)?);
                }
                table.indexes.push(metadata);
            }
            AlterTableAction::AddForeignKey(_) => {
                return Err(ExecError::Unsupported("ALTER TABLE ADD FOREIGN KEY"));
            }
            _ => unreachable!("ALTER capability preflight rejected unsupported action"),
        }
        Ok(())
    }
}

/// Moves the column at `from` to `position` (a `MODIFY`/`CHANGE COLUMN`
/// `FIRST`/`AFTER`), reindexing `key_groups` consistently — a no-op for
/// `ColumnPosition::Default`, which leaves the column where it is. The
/// target index is resolved against the ORIGINAL (pre-move) column list —
/// verified against real TiDB: e.g. moving a column to right after itself
/// is a no-op. Implemented by simulating the same `remove` + `insert` on a
/// plain index vector to get an old-index → new-index remap, then applying
/// that identically to `cols`, every row, and `key_groups` — avoids
/// hand-deriving the shift arithmetic for a move (as opposed to a pure
/// insert or pure delete, which `AddColumn`/`DropColumn` handle directly).
fn reposition_column(
    table: &mut Table,
    from: usize,
    position: &ColumnPosition,
) -> Result<(), ExecError> {
    let raw_to = match position {
        ColumnPosition::Default => return Ok(()),
        ColumnPosition::First => 0,
        ColumnPosition::After(name) => {
            let idx = table
                .cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
                .ok_or_else(|| ExecError::UnknownColumn(name.clone()))?;
            idx + 1
        }
    };
    let to = if raw_to > from { raw_to - 1 } else { raw_to };
    if to == from {
        return Ok(());
    }

    let mut order: Vec<usize> = (0..table.cols.len()).collect();
    let moved = order.remove(from);
    order.insert(to, moved);
    let mut old_to_new = vec![0usize; order.len()];
    for (new_idx, &old_idx) in order.iter().enumerate() {
        old_to_new[old_idx] = new_idx;
    }

    let name = table.cols.remove(from);
    table.cols.insert(to, name);
    let ty = table.col_types.remove(from);
    table.col_types.insert(to, ty);
    let def = table.col_defaults.remove(from);
    table.col_defaults.insert(to, def);
    for row in &mut table.rows {
        let v = row.remove(from);
        row.insert(to, v);
    }
    for group in &mut table.key_groups {
        for idx in group.iter_mut() {
            *idx = old_to_new[*idx];
        }
    }
    if let Some(auto_increment) = &mut table.auto_increment {
        auto_increment.column = old_to_new[auto_increment.column];
    }
    Ok(())
}
