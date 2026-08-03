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

//! Referential integrity: Go's `pkg/executor/foreign_key.go`
//! (`FKCheckExec`/`FKCascadeExec`) and the plan builders that install them
//! (`pkg/planner/core/foreign_key.go`).
//!
//! Go attaches an `FKCheck` operator to every write whose table declares a
//! foreign key (the CHILD side) and an `FKCascade` operator to every write
//! whose table is REFERRED to by one (the PARENT side). This module is those
//! two operators, expressed as functions over the catalog rather than as plan
//! nodes, because this tier's writes are catalog mutations rather than a
//! pipeline.
//!
//! # The rules, each re-confirmed via `rust/difftests/gorun`
//!
//! * **MATCH SIMPLE** (MySQL/TiDB's only implemented mode): a child row whose
//!   referencing columns contain ANY `NULL` is not checked at all, composite
//!   keys included. `(1, NULL)` and `(NULL, 2)` both insert into a child of a
//!   `(x, y)` parent that holds only `(1, 1)`; `(2, 2)` is rejected.
//! * **Parent-side triggering**: a `DELETE` of a referenced row triggers, and
//!   so does an `UPDATE` that actually CHANGES a referenced value. Touching an
//!   unreferenced column (`SET name = 'z'`), or assigning a referenced column
//!   its own current value (`SET id = 1 WHERE id = 1`), triggers nothing.
//! * **`CASCADE` is transitive**: deleting a `p` row removes the matching `c`
//!   rows AND the `g` rows that referenced those, through as many hops as
//!   exist. `ON UPDATE CASCADE` repoints them the same way.
//! * **`SET NULL`** nulls the referencing columns; because that CHANGES the
//!   child's own referenced values, it recurses into the child's dependents
//!   exactly as an update does.
//! * **`RESTRICT`, `NO ACTION`, `SET DEFAULT`, and no clause at all** all
//!   reject the parent mutation. `SET DEFAULT` is not an approximation: InnoDB
//!   never implemented it, so real MySQL and TiDB treat it as `RESTRICT`.
//! * **`IGNORE`** (`INSERT IGNORE`, `DELETE IGNORE`) downgrades a violation
//!   from a statement error to a per-row skip, with a warning.
//! * **`REPLACE` on the parent side triggers**, because the row it displaces
//!   is withdrawn exactly as a `DELETE`'s is (Go `InsertValues.removeRow` ->
//!   `onRemoveRowForFK`). A `REPLACE` that displaces NOTHING -- an identical
//!   row, or a row nothing collides with -- withdraws nothing and triggers
//!   nothing.
//!
//! # The DDL-time rules
//!
//! * A constraint may not name a **VIRTUAL generated column** on either side
//!   (3733), and a **STORED generated CHILD column** may not carry an action
//!   that would WRITE it -- `ON UPDATE CASCADE`/`SET NULL`, `ON DELETE SET
//!   NULL` (3104). `ON DELETE CASCADE` removes the row rather than writing
//!   the column, and is accepted. See [`crate::ddl::table_constraints`].
//! * A constraint can be **added and dropped after the fact**
//!   (`ALTER TABLE ... ADD/DROP FOREIGN KEY`, see
//!   [`crate::ddl::alter_table`]), on the same rules a `CREATE TABLE` clause
//!   is admitted by, plus two of its own: a duplicate constraint name is
//!   1826, and the rows the table ALREADY holds are checked, so an ADD over
//!   an orphan is 1452 rather than a silent blessing.
//! * The **index a constraint relies on may not be dropped** (1553), on
//!   either side, unless another index still covers the same columns or the
//!   referenced column is the clustered handle. See [`check_index_needed`].
//!
//! * **`foreign_key_checks = 0`** disables every ROW-level rule above, plus
//!   the DDL-time checks that RESOLVE a reference (`DROP TABLE` of a
//!   referenced parent, the `REFERENCES` clause at `CREATE TABLE`, the
//!   parent-side half of 3733, and the existing-row check an
//!   `ALTER TABLE ... ADD FOREIGN KEY` runs). It is NOT retroactive: rows written while it
//!   was off stay, unchecked, when it is turned back on.
//!
//!   It does NOT reach the two rules that never look at the other table:
//!   captured, the CHILD-side 3733 and the 1553 index check both still fire
//!   with the switch at 0. Go reaches the first from `buildFKInfo` and the
//!   second from a gate on the GLOBAL `vardef.EnableForeignKey`, neither of
//!   which the session switch touches.
//!
//! # NOT MODELLED (documented)
//!
//! * A partial mutation is not rolled back when a later dependent restricts:
//!   the cascade pass runs a whole-level `RESTRICT` check before it mutates
//!   anything at that level, so a single-level statement is all-or-nothing,
//!   but a deeper level that restricts after a shallower one cascaded leaves
//!   the shallower change applied. Real TiDB rolls the statement back.
//! * `RENAME TABLE` does not rewrite the `ref_table` of the constraints that
//!   pointed at the old name.

use tidb_datatype::Datum;

use crate::driver::{Catalog, DriverError, TableEntry};
use crate::kv_table::{FkAction, KvForeignKey};

/// MySQL's `FK_MAX_CASCADE_DEL`: the deepest a cascade may recurse before Go
/// raises `ErrFkExceedMaxDepth` (3008).
const MAX_CASCADE_DEPTH: usize = 15;

/// Which side of a mutation a violation was found on, so the caller can raise
/// the 1452 or the 1451 that Go raises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Child,
    Parent,
}

/// Renders the constraint the way Go's error text quotes it, which is the
/// `CONSTRAINT ... FOREIGN KEY ... REFERENCES ...` clause.
fn constraint_text(foreign_key: &KvForeignKey) -> String {
    format!(
        "CONSTRAINT `{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`)",
        foreign_key.name,
        foreign_key.cols.join("`, `"),
        foreign_key.ref_table,
        foreign_key.ref_cols.join("`, `"),
    )
}

fn violation(side: Side, database: &str, table: &str, foreign_key: &KvForeignKey) -> DriverError {
    let name = format!("`{database}`.`{table}`");
    let constraint = constraint_text(foreign_key);
    match side {
        Side::Child => DriverError::ForeignKeyNoReferencedRow {
            table: name,
            constraint,
        },
        Side::Parent => DriverError::ForeignKeyRowIsReferenced {
            table: name,
            constraint,
        },
    }
}

/// The key a row presents to one foreign key, or `None` when MATCH SIMPLE
/// skips it because some component is `NULL`.
fn key_at(row: &[Datum], offsets: &[usize]) -> Option<Vec<Datum>> {
    let mut key = Vec::with_capacity(offsets.len());
    for offset in offsets {
        match row.get(*offset) {
            None | Some(Datum::Null) => return None,
            Some(value) => key.push(value.clone()),
        }
    }
    Some(key)
}

/// Reads every row of a table, or `None` when the name does not resolve to a
/// byte-backed table (a view, a matrix table, a dropped parent).
fn scan(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<Vec<Datum>>> {
    match catalog.get_mut_for_foreign_key(database, table)? {
        TableEntry::Kv(kv) => kv.scan_rows(zone).ok(),
        _ => None,
    }
}

/// The foreign keys a table declares, with the child's column names.
fn declared(catalog: &Catalog, database: &str, table: &str) -> (Vec<KvForeignKey>, Vec<String>) {
    match catalog.get_in(database, table) {
        Some(TableEntry::Kv(kv)) => (
            kv.foreign_keys().to_vec(),
            kv.columns.iter().map(|c| c.name.clone()).collect(),
        ),
        _ => (Vec::new(), Vec::new()),
    }
}

/// Go `buildFKCheckForReferredFK`'s index, computed on demand: every
/// `(schema, table, foreign key)` whose constraint REFERS to `database.table`.
fn referring(
    catalog: &Catalog,
    database: &str,
    table: &str,
) -> Vec<(String, String, KvForeignKey)> {
    let mut found = Vec::new();
    for (child_db, child_table) in catalog.table_paths() {
        let (keys, _) = declared(catalog, &child_db, &child_table);
        for foreign_key in keys {
            if foreign_key.ref_schema.eq_ignore_ascii_case(database)
                && foreign_key.ref_table.eq_ignore_ascii_case(table)
            {
                found.push((child_db.clone(), child_table.clone(), foreign_key));
            }
        }
    }
    found
}

/// Resolves the REFERENCING columns' offsets in the child's own schema.
///
/// The constraint stores names, so this runs against the column list as it is
/// NOW: an `ALTER TABLE` that moved a column leaves the constraint checking
/// the columns it was declared over, not whatever sits at the old offsets.
/// `None` means a referencing column is gone, which DDL refuses to do.
fn child_offsets(names: &[String], foreign_key: &KvForeignKey) -> Option<Vec<usize>> {
    foreign_key
        .cols
        .iter()
        .map(|name| {
            names
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))
        })
        .collect()
}

/// Resolves the referenced columns' offsets in the parent's schema.
fn parent_offsets(
    catalog: &Catalog,
    foreign_key: &KvForeignKey,
) -> Option<(Vec<usize>, Vec<String>)> {
    let entry = catalog.get_in(&foreign_key.ref_schema, &foreign_key.ref_table)?;
    let names = entry.column_names();
    let mut offsets = Vec::with_capacity(foreign_key.ref_cols.len());
    for name in &foreign_key.ref_cols {
        offsets.push(
            names
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))?,
        );
    }
    Some((offsets, names))
}

/// Go `FKCheckExec` on the CHILD side: for each candidate row, the name of
/// the first foreign key it violates.
///
/// The rows are not yet written, which is why they are passed in rather than
/// read back: Go checks the row it is about to add, not the table.
pub(crate) fn check_child_rows(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    rows: &[Vec<Datum>],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Option<DriverError>>, DriverError> {
    let mut verdicts = vec![None; rows.len()];
    let (keys, columns) = declared(catalog, database, table);
    for foreign_key in &keys {
        let Some(child) = child_offsets(&columns, foreign_key) else {
            continue;
        };
        // Every row's key for this constraint, deduplicated so a wide insert
        // scans the parent once. A `Datum` is not hashable (its numeric
        // variants compare across representations), so the association is an
        // association list rather than a map.
        let mut wanted: Vec<(Vec<Datum>, Vec<usize>)> = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if verdicts[index].is_some() {
                continue;
            }
            let Some(key) = key_at(row, &child) else {
                continue;
            };
            match wanted.iter_mut().find(|(seen, _)| *seen == key) {
                Some((_, indexes)) => indexes.push(index),
                None => wanted.push((key, vec![index])),
            }
        }
        if wanted.is_empty() {
            continue;
        }
        let Some((offsets, _)) = parent_offsets(catalog, foreign_key) else {
            // The parent is gone (only reachable with foreign_key_checks off
            // at CREATE TABLE time); Go has nothing to check against either.
            continue;
        };
        let Some(parent_rows) = scan(
            catalog,
            &foreign_key.ref_schema,
            &foreign_key.ref_table,
            zone,
        ) else {
            continue;
        };
        for parent in &parent_rows {
            if let Some(key) = key_at(parent, &offsets) {
                wanted.retain(|(seen, _)| *seen != key);
            }
        }
        for (_, indexes) in &wanted {
            for index in indexes {
                verdicts[*index] = Some(violation(Side::Child, database, table, foreign_key));
            }
        }
    }
    Ok(verdicts)
}

/// Go `checkForeignKeyConstrain`: the rows a table ALREADY holds, checked
/// against a constraint that is being ADDED to it.
///
/// Go runs this as one statement -- `select 1 from child where <cols> is not
/// null and (<cols>) not in (select <refcols> from parent) limit 1` -- and
/// raises `ErrNoReferencedRow2` (1452) when it returns a row, which is why an
/// `ALTER TABLE ... ADD FOREIGN KEY` over orphaned rows fails instead of
/// blessing them. Only the NEW constraint is checked: rows that already
/// violate an OLDER constraint (written while `foreign_key_checks` was off)
/// are not this statement's business, and Go's query names one `fkInfo`.
///
/// `foreign_key_checks = 0` skips it entirely -- Go returns before running
/// the query at all -- so the caller decides whether to call this.
pub(crate) fn require_existing_rows(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    foreign_key: &KvForeignKey,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(), DriverError> {
    let (_, columns) = declared(catalog, database, table);
    let Some(child) = child_offsets(&columns, foreign_key) else {
        return Ok(());
    };
    let Some((offsets, _)) = parent_offsets(catalog, foreign_key) else {
        return Ok(());
    };
    let Some(parent_rows) = scan(
        catalog,
        &foreign_key.ref_schema,
        &foreign_key.ref_table,
        zone,
    ) else {
        return Ok(());
    };
    let Some(rows) = scan(catalog, database, table, zone) else {
        return Ok(());
    };
    for row in &rows {
        // MATCH SIMPLE, and Go's `%n is not null` conjunction: a row with any
        // NULL in the referencing columns is not checked.
        let Some(key) = key_at(row, &child) else {
            continue;
        };
        if !parent_rows
            .iter()
            .any(|parent| key_at(parent, &offsets).is_some_and(|found| found == key))
        {
            return Err(violation(Side::Child, database, table, foreign_key));
        }
    }
    Ok(())
}

/// Go `FKCheckExec` on the child side, as a statement-level gate: the first
/// violating row fails the whole statement.
pub(crate) fn require_child_rows(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    rows: &[Vec<Datum>],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(), DriverError> {
    match check_child_rows(catalog, database, table, rows, zone)?
        .into_iter()
        .flatten()
        .next()
    {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// What a parent-side statement does to one parent row.
pub(crate) enum ParentChange<'a> {
    /// The row goes away.
    Delete(&'a [Datum]),
    /// The row's values change; only a CHANGED referenced value triggers.
    Update {
        /// The row as it stands.
        old: &'a [Datum],
        /// The row as the statement would leave it.
        new: &'a [Datum],
    },
}

/// Go `FKCascadeExec` on the PARENT side: applies every dependent
/// constraint's own action to `changes`, which the caller has NOT yet
/// applied to the parent itself.
///
/// The caller applies its own change afterwards, so a `RESTRICT` that fires
/// here leaves the parent untouched -- which is what makes a restricted
/// `DELETE` a no-op rather than a half-done statement.
pub(crate) fn cascade_parent_changes(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    changes: &[ParentChange<'_>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    cascade_at_depth(catalog, database, table, changes, 0, ctx)
}

fn cascade_at_depth(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    changes: &[ParentChange<'_>],
    depth: usize,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if depth > MAX_CASCADE_DEPTH {
        return Err(DriverError::ForeignKeyCascadeTooDeep);
    }
    let dependents = referring(catalog, database, table);
    if dependents.is_empty() {
        return Ok(());
    }
    // A cascade in Go is a SUB-STATEMENT: `FKCascadeExec.buildExecutor` builds
    // an `UPDATE`/`DELETE` over the child table and runs it against the SAME
    // `StmtCtx.MemTracker`, so the child rows it reads and stages count
    // against `tidb_mem_quota_query` exactly as the outer statement's do. The
    // cascade operator itself accounts nothing in either tier -- this is the
    // sub-statement's accounting, at the one place this tier reads the child.
    let accountant = ctx
        .statement_memory()
        .write_accountant(crate::mem_quota::label::FK_CASCADE);
    // Every dependent's RESTRICT verdict is taken BEFORE any of them mutates,
    // so a statement whose first dependent cascades and whose second
    // restricts changes nothing at this level.
    let mut plans = Vec::with_capacity(dependents.len());
    for (child_db, child_table, foreign_key) in dependents {
        let Some((offsets, _)) = parent_offsets(catalog, &foreign_key) else {
            continue;
        };
        // The referenced keys this statement withdraws, paired with the
        // replacement an `ON UPDATE CASCADE` would write.
        let mut withdrawn: Vec<(Vec<Datum>, Option<Vec<Datum>>)> = Vec::new();
        for change in changes {
            match change {
                ParentChange::Delete(row) => {
                    if let Some(key) = key_at(row, &offsets) {
                        withdrawn.push((key, None));
                    }
                }
                ParentChange::Update { old, new } => {
                    let (Some(before), after) = (key_at(old, &offsets), key_at(new, &offsets))
                    else {
                        continue;
                    };
                    // Assigning a referenced column its own value, or
                    // touching an unreferenced one, withdraws nothing.
                    if Some(&before) == after.as_ref() {
                        continue;
                    }
                    withdrawn.push((before, after));
                }
            }
        }
        if withdrawn.is_empty() {
            continue;
        }
        let (_, child_columns) = declared(catalog, &child_db, &child_table);
        let Some(child) = child_offsets(&child_columns, &foreign_key) else {
            continue;
        };
        let Some(child_rows) = scan(catalog, &child_db, &child_table, &ctx.session_zone()) else {
            continue;
        };
        let mut affected: Vec<(usize, Option<Vec<Datum>>)> = Vec::new();
        for (index, row) in child_rows.iter().enumerate() {
            accountant.account_row(row).map_err(DriverError::from)?;
            let Some(key) = key_at(row, &child) else {
                continue;
            };
            if let Some((_, replacement)) = withdrawn.iter().find(|(from, _)| *from == key) {
                affected.push((index, replacement.clone()));
            }
        }
        if affected.is_empty() {
            continue;
        }
        // `ON DELETE`/`ON UPDATE` are separate actions on the same
        // constraint; which one applies is decided by the statement.
        let deleting = changes
            .iter()
            .any(|change| matches!(change, ParentChange::Delete(_)));
        let action = if deleting {
            foreign_key.on_delete
        } else {
            foreign_key.on_update
        };
        if action == FkAction::Restrict {
            return Err(violation(
                Side::Parent,
                &child_db,
                &child_table,
                &foreign_key,
            ));
        }
        plans.push((
            child_db,
            child_table,
            foreign_key,
            action,
            deleting,
            affected,
            child,
        ));
    }

    for (child_db, child_table, _foreign_key, action, deleting, affected, child) in plans {
        let Some(child_rows) = scan(catalog, &child_db, &child_table, &ctx.session_zone()) else {
            continue;
        };
        match action {
            FkAction::Restrict => unreachable!("a restrict returned above"),
            FkAction::Cascade if deleting => {
                // ON DELETE CASCADE: the dependents go away, and so do THEIR
                // dependents -- the recursion is what makes it transitive.
                let doomed: Vec<Vec<Datum>> = affected
                    .iter()
                    .map(|(index, _)| child_rows[*index].clone())
                    .collect();
                let nested: Vec<ParentChange<'_>> =
                    doomed.iter().map(|row| ParentChange::Delete(row)).collect();
                cascade_at_depth(catalog, &child_db, &child_table, &nested, depth + 1, ctx)?;
                delete_rows(
                    catalog,
                    &child_db,
                    &child_table,
                    &doomed,
                    &ctx.session_zone(),
                )?;
            }
            FkAction::Cascade | FkAction::SetNull => {
                // ON UPDATE CASCADE repoints the referencing columns; SET
                // NULL nulls them. Both CHANGE the child's own row, so the
                // child's own dependents see an update.
                let mut rewritten = Vec::with_capacity(affected.len());
                for (index, replacement) in &affected {
                    let old = child_rows[*index].clone();
                    let mut new = old.clone();
                    for (position, offset) in child.iter().enumerate() {
                        new[*offset] = match (action, replacement) {
                            (FkAction::Cascade, Some(values)) => values[position].clone(),
                            _ => Datum::Null,
                        };
                    }
                    accountant.account_row(&new).map_err(DriverError::from)?;
                    rewritten.push((old, new));
                }
                let nested: Vec<ParentChange<'_>> = rewritten
                    .iter()
                    .map(|(old, new)| ParentChange::Update { old, new })
                    .collect();
                cascade_at_depth(catalog, &child_db, &child_table, &nested, depth + 1, ctx)?;
                rewrite_rows(catalog, &child_db, &child_table, &rewritten, ctx)?;
            }
        }
    }
    Ok(())
}

/// Deletes the rows equal to `rows`, matched by value because a cascade names
/// its victims by content rather than by handle.
fn delete_rows(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    rows: &[Vec<Datum>],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(kv)) = catalog.get_mut_for_foreign_key(database, table) else {
        return Ok(());
    };
    let stored = kv
        .scan_rows_with_handles(zone)
        .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
    let mut remaining: Vec<&Vec<Datum>> = rows.iter().collect();
    for (handle, row) in stored {
        if let Some(position) = remaining.iter().position(|wanted| ***wanted == row[..]) {
            remaining.swap_remove(position);
            kv.delete_row(&handle, zone)
                .map_err(|e| DriverError::Parse(format!("row delete failed: {e:?}")))?;
        }
    }
    Ok(())
}

/// Applies `(old, new)` rewrites, matched by the old row's value.
fn rewrite_rows(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    rewrites: &[(Vec<Datum>, Vec<Datum>)],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(kv)) = catalog.get_mut_for_foreign_key(database, table) else {
        return Ok(());
    };
    let stored = kv
        .scan_rows_with_handles(&ctx.session_zone())
        .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
    let mut remaining: Vec<&(Vec<Datum>, Vec<Datum>)> = rewrites.iter().collect();
    for (handle, row) in stored {
        if let Some(position) = remaining.iter().position(|(old, _)| old[..] == row[..]) {
            let (_, new) = remaining.swap_remove(position);
            kv.update_row(&handle, new, ctx)
                .map_err(|e| DriverError::Parse(format!("row update failed: {e:?}")))?;
        }
    }
    Ok(())
}

/// Whether a table takes part in any foreign key, as the declaring child or
/// as the referenced parent.
///
/// A constraint stores BOTH sides as names now (Go `FKInfo.Cols` and
/// `FKInfo.RefTable`), so repositioning a column no longer moves the
/// constraint off its columns -- `KvTable::foreign_key_offsets` resolves the
/// names against the current column list at every use. What is still
/// unmodelled is a DDL that makes one of those names WRONG: `RENAME TABLE`
/// and `DROP TABLE` leave `ref_table` dangling, so both stay REFUSED on a
/// participating table rather than silently breaking the reference. Go
/// rewrites the affected `FKInfo`s instead, which is the graduation path.
/// A column RENAME is still in that group: `rename_column_action` assigns the
/// new name and rewrites nothing else, so a constraint over the renamed column
/// would be left naming a column no table has.
///
/// `MODIFY`/`CHANGE` is NOT in that group any more: it asks
/// [`check_modify_column`] the same question Go's
/// `checkModifyColumnWithForeignKeyConstraint` asks, and a `CHANGE` that also
/// renames rewrites the constraint through [`rewrite_column_name`], which is
/// Go's `updateFKInfoWhenModifyColumn` plus
/// `adjustForeignKeyChildTableInfoAfterModifyColumn`.
pub(crate) fn participates(catalog: &Catalog, database: &str, table: &str) -> bool {
    let (declared_keys, _) = declared(catalog, database, table);
    !declared_keys.is_empty() || !referring(catalog, database, table).is_empty()
}

/// Go `ddl.isAcceptableForeignKeyColumnChange` (`pkg/ddl/foreign_key.go`).
///
/// `new` is the type the `MODIFY` asks for, `original` the column's type
/// today, and `related` the type of the column on the OTHER side of the
/// constraint. Reached only once the two TYPES already agree, so this decides
/// the WIDTH question alone.
///
/// The integer arm is Go's, comment included: an integer's `Flen` is a display
/// width and says nothing about the value range, so every integer width move
/// is acceptable. Captured: `modify user_id int(5)` over an `int(11)` column
/// referencing an `int(11)` succeeds.
fn acceptable_column_change(
    new: &tidb_datatype::FieldType,
    original: &tidb_datatype::FieldType,
    related: &tidb_datatype::FieldType,
) -> bool {
    use tidb_datatype::FieldTypeCode::*;
    if matches!(new.code(), Tiny | Short | Int24 | Long | LongLong) {
        return true;
    }
    if new.flen() < related.flen() || new.flen() < original.flen() {
        return false;
    }
    // A decimal's precision and scale are both part of the stored key, so
    // Go refuses ANY move of either -- including a WIDENING one, which is why
    // `decimal(10,2)` -> `decimal(12,2)` is 1832 rather than accepted.
    if new.code() == NewDecimal
        && (new.flen() != original.flen() || new.decimal() != original.decimal())
    {
        return false;
    }
    true
}

/// The `FieldType` of `column` in `database.table`, or `None` when either the
/// table or the column is gone.
fn column_type(
    catalog: &Catalog,
    database: &str,
    table: &str,
    column: &str,
) -> Option<tidb_datatype::FieldType> {
    match catalog.get_in(database, table)? {
        TableEntry::Kv(kv) => kv
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .map(|c| c.field_type.clone()),
        _ => None,
    }
}

/// Go `ddl.checkModifyColumnWithForeignKeyConstraint` (`pkg/ddl/foreign_key.go`).
///
/// Asked once per `MODIFY`/`CHANGE COLUMN`, from BOTH directions:
///
/// * the constraints this table DECLARES over the column, checked against the
///   parent's referenced column -- a type move is 3780, a width move Go does
///   not accept is 1832;
/// * the constraints OTHER tables declare AGAINST this column, checked against
///   each child's referencing column -- the same type move is 3780, and the
///   width move is 1833 naming the child as `schema.table`.
///
/// Go's early return is load-bearing and is kept: when type, `Flen` and
/// `Decimal` are all unchanged the check is skipped entirely, which is what
/// lets `alter table orders modify user_id int null` -- a NULLABILITY change
/// and nothing else -- through on a constrained column. That statement is in
/// the recording (`executor/foreign_key.result`), and refusing it was this
/// tier's last divergence in that topic.
pub(crate) fn check_modify_column(
    catalog: &Catalog,
    database: &str,
    table: &str,
    old_name: &str,
    original: &tidb_datatype::FieldType,
    new: &tidb_datatype::FieldType,
) -> Result<(), DriverError> {
    if new.code() == original.code()
        && new.flen() == original.flen()
        && new.decimal() == original.decimal()
    {
        return Ok(());
    }
    let (declared_keys, _) = declared(catalog, database, table);
    for foreign_key in &declared_keys {
        for (i, col) in foreign_key.cols.iter().enumerate() {
            if !col.eq_ignore_ascii_case(old_name) {
                continue;
            }
            let referenced = &foreign_key.ref_cols[i];
            // Go reads the parent through the infoschema and propagates its
            // error; a parent that is gone cannot answer the question, and
            // this tier has no such error to raise here.
            let Some(refer) = column_type(
                catalog,
                &foreign_key.ref_schema,
                &foreign_key.ref_table,
                referenced,
            ) else {
                continue;
            };
            if new.code() != refer.code() {
                return Err(DriverError::FkIncompatibleColumns {
                    referencing: old_name.to_owned(),
                    referenced: referenced.clone(),
                    constraint: foreign_key.name.clone(),
                });
            }
            if !acceptable_column_change(new, original, &refer) {
                return Err(DriverError::ForeignKeyColumnCannotChange {
                    column: old_name.to_owned(),
                    constraint: foreign_key.name.clone(),
                });
            }
        }
    }
    for (child_db, child_table, foreign_key) in referring(catalog, database, table) {
        for (i, col) in foreign_key.ref_cols.iter().enumerate() {
            if !col.eq_ignore_ascii_case(old_name) {
                continue;
            }
            let child_column = &foreign_key.cols[i];
            let Some(child) = column_type(catalog, &child_db, &child_table, child_column) else {
                continue;
            };
            if new.code() != child.code() {
                // Go names the CHILD's column first here, where the declared
                // side above names this table's own.
                return Err(DriverError::FkIncompatibleColumns {
                    referencing: child_column.clone(),
                    referenced: old_name.to_owned(),
                    constraint: foreign_key.name.clone(),
                });
            }
            if !acceptable_column_change(new, original, &child) {
                return Err(DriverError::ForeignKeyColumnCannotChangeChild {
                    column: old_name.to_owned(),
                    constraint: foreign_key.name.clone(),
                    child_table: format!(
                        "{}.{}",
                        child_db.to_lowercase(),
                        child_table.to_lowercase()
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Go `ddl.updateFKInfoWhenModifyColumn` plus
/// `ddl.adjustForeignKeyChildTableInfoAfterModifyColumn`: a `CHANGE COLUMN`
/// that RENAMES carries every constraint naming the old name onto the new one.
///
/// Both directions, because a constraint stores the two sides in two different
/// tables: this table's own `cols`, and every child's `ref_cols`. Captured:
/// after `alter table orders change user_id uid int`, `SHOW CREATE TABLE`
/// prints `` FOREIGN KEY (`uid`) `` and the parent-side `modify id bigint`
/// still reports the constraint under the NEW child column name.
pub(crate) fn rewrite_column_name(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    old_name: &str,
    new_name: &str,
) {
    if old_name.eq_ignore_ascii_case(new_name) {
        return;
    }
    if let Some(TableEntry::Kv(kv)) = catalog.table_mut_in(database, table) {
        for foreign_key in kv.foreign_keys_mut() {
            for col in &mut foreign_key.cols {
                if col.eq_ignore_ascii_case(old_name) {
                    *col = new_name.to_owned();
                }
            }
        }
    }
    let children: Vec<(String, String)> = referring(catalog, database, table)
        .into_iter()
        .map(|(db, tbl, _)| (db, tbl))
        .collect();
    for (child_db, child_table) in children {
        let Some(TableEntry::Kv(kv)) = catalog.table_mut_in(&child_db, &child_table) else {
            continue;
        };
        for foreign_key in kv.foreign_keys_mut() {
            if !foreign_key.ref_schema.eq_ignore_ascii_case(database)
                || !foreign_key.ref_table.eq_ignore_ascii_case(table)
            {
                continue;
            }
            for col in &mut foreign_key.ref_cols {
                if col.eq_ignore_ascii_case(old_name) {
                    *col = new_name.to_owned();
                }
            }
        }
    }
}

/// Go `ddl.checkIndexNeededInForeignKey`: an index a foreign key relies on
/// may not be dropped while the constraint stands (1553).
///
/// Both sides are covered, and each is its own captured case: the PARENT's
/// index over the referenced columns is what makes the reference resolvable,
/// and the CHILD's index over the referencing columns is what makes the
/// child-side check affordable.
///
/// Two exemptions, both from Go and both captured:
///
/// * A REMAINING index that also covers the columns makes the drop legal --
///   `alter table t1 add index idx2(b)` lets `drop index idx1(b)` through.
/// * A referenced column that IS the clustered primary key needs no index of
///   its own (`tbInfo.PKIsHandle && len(cols) == 1`). This is PARENT-side
///   only: captured, a child's own `index fk(b)` referencing a clustered
///   `t1(id)` is still 1553.
///
/// NOT gated by `foreign_key_checks`. Captured: with the session variable set
/// to 0, `alter table t1 drop index idx1` is STILL 1553, because Go gates
/// this check on the global `vardef.EnableForeignKey` rather than on the
/// session switch that governs row-level checking.
pub(crate) fn check_index_needed(
    catalog: &Catalog,
    database: &str,
    table: &str,
    index_name: &str,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(kv)) = catalog.get_in(database, table) else {
        return Ok(());
    };
    let Some(dropping) = kv
        .indexes()
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
    else {
        return Ok(());
    };
    // Go's `IsIndexPrefixCoveredForForeignKey`: the index serves the
    // constraint when its LEADING key parts are exactly the constrained
    // columns, in order.
    let covers = |offsets: &[usize]| -> bool {
        dropping.column_offsets.len() >= offsets.len()
            && dropping.column_offsets[..offsets.len()] == *offsets
    };
    let remaining_covers = |offsets: &[usize]| -> bool {
        kv.indexes().iter().any(|index| {
            !index.name.eq_ignore_ascii_case(index_name)
                && index.column_offsets.len() >= offsets.len()
                && index.column_offsets[..offsets.len()] == *offsets
        })
    };
    let refused = || DriverError::DropIndexNeededInForeignKey(dropping.name.clone());

    // The constraints this table DECLARES: the referencing columns are its
    // own, resolved from their names into current offsets.
    let own: Vec<String> = kv.columns.iter().map(|c| c.name.clone()).collect();
    for foreign_key in kv.foreign_keys() {
        let Some(child) = child_offsets(&own, foreign_key) else {
            continue;
        };
        if covers(&child) && !remaining_covers(&child) {
            return Err(refused());
        }
    }
    // The constraints that REFER here: the referenced columns, resolved into
    // this table's offsets.
    for (_, _, foreign_key) in referring(catalog, database, table) {
        let Some((offsets, _)) = parent_offsets(catalog, &foreign_key) else {
            continue;
        };
        if !covers(&offsets) {
            continue;
        }
        if offsets.len() == 1 && kv.is_clustered_handle_column(offsets[0]) {
            continue;
        }
        if !remaining_covers(&offsets) {
            return Err(refused());
        }
    }
    Ok(())
}

/// Go `checkDropTableHasForeignKeyReferredInOwner`: a table may not be
/// dropped while a table OUTSIDE this statement still references it.
pub(crate) fn check_drop_tables(
    catalog: &Catalog,
    dropping: &[(String, String)],
) -> Result<(), DriverError> {
    for (database, table) in dropping {
        for (child_db, child_table, foreign_key) in referring(catalog, database, table) {
            if dropping.iter().any(|(db, name)| {
                db.eq_ignore_ascii_case(&child_db) && name.eq_ignore_ascii_case(&child_table)
            }) {
                continue;
            }
            return Err(violation(
                Side::Parent,
                &child_db,
                &child_table,
                &foreign_key,
            ));
        }
    }
    Ok(())
}
