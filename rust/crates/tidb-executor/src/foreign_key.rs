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
//! DML builders attach resolved FKCheck/FKCascade policies to their physical
//! root. Row callbacks consume those policies, and statement completion runs
//! ordinary checks before cascades. Dependent cascades use the same policy
//! builder; DDL validation still resolves the new constraint being installed.
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
//!   constrained column is the clustered handle. See [`check_index_needed`].
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
//! * Cascades rely on the session/cluster statement stage for rollback. The
//!   functions here do not create a second transaction or replay row preimages.
//! * Multi-action ALTER atomicity remains outside this module's metadata
//!   model. Whole-table renames rewrite `ref_schema` and `ref_table` through
//!   [`rewrite_table_references`], and column renames rewrite `cols` and
//!   `ref_cols` through [`rewrite_column_name`].

use tidb_datatype::Datum;

use crate::driver::{Catalog, DriverError, TableEntry};
use crate::kv_table::{FkAction, KvForeignKey, RowDecodeContext, TableHandle};
use tidb_hack::GoToLower;
use tidb_planner::physical::{FkTriggerKind, FkTriggerNode};

/// MySQL's `FK_MAX_CASCADE_DEL`: the deepest a cascade may recurse before Go
/// raises `ErrFkExceedMaxDepth` (3008).
const MAX_CASCADE_DEPTH: usize = 15;

/// Renders the constraint the way Go's error text quotes it, which is the
/// `CONSTRAINT ... FOREIGN KEY ... REFERENCES ...` clause.
pub(crate) fn constraint_text(foreign_key: &KvForeignKey) -> String {
    format!(
        "CONSTRAINT `{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`)",
        foreign_key.name,
        foreign_key.cols.join("`, `"),
        foreign_key.ref_table,
        foreign_key.ref_cols.join("`, `"),
    )
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

/// Go FKCheckExec.updateRowNeedToCheck compares through the source collator.
/// Comparison errors mean the value must be checked, not that it is unchanged.
fn same_key(left: &[Datum], right: &[Datum], ctx: &crate::StmtContext) -> bool {
    let zone = ctx.session_zone();
    let context = tidb_datatype::ConversionContext::new(
        tidb_expr::Columns::type_flags(ctx),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &tidb_datatype::IGNORE_CONVERSION_WARNINGS,
    );
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            let (ordering, error) = left.compare_with_context(
                right,
                left.collation().unwrap_or(tidb_datatype::Collation::Binary),
                &context,
                &zone,
            );
            error.is_none() && ordering == std::cmp::Ordering::Equal
        })
}

fn lookup_handles(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    index: Option<i64>,
    key: &[Datum],
    limit: usize,
    ctx: &crate::StmtContext,
) -> Result<Vec<TableHandle>, DriverError> {
    let Some(TableEntry::Kv(kv)) = catalog.get_mut_for_foreign_key(database, table) else {
        return Err(DriverError::unsupported(
            "foreign key table changed after planning",
        ));
    };
    std::sync::Arc::make_mut(kv)
        .foreign_key_handles(index, key, &ctx.session_zone(), limit)
        .map_err(|error| crate::driver::kv_read_error("foreign key lookup failed", error))
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
pub(crate) fn referring(
    catalog: &Catalog,
    database: &str,
    table: &str,
) -> Vec<(String, String, KvForeignKey)> {
    // Sysbench and the normal TiDB bootstrap have no foreign-key declarations.
    // Avoid rebuilding and sorting the complete catalog path list for every
    // UPDATE/DELETE in that case; catalogs that do declare one retain the
    // deterministic scan below.
    if !catalog.has_foreign_keys() {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (child_db, child_table, entry) in catalog.table_entries() {
        let TableEntry::Kv(child) = entry else {
            continue;
        };
        for foreign_key in child.foreign_keys() {
            if foreign_key.ref_schema.eq_ignore_ascii_case(database)
                && foreign_key.ref_table.eq_ignore_ascii_case(table)
            {
                found.push((child_db, child_table, foreign_key));
            }
        }
    }
    // Preserve the existing schema/table cascade order and declaration order
    // within each child. Only matching constraints need owned metadata: the
    // cascade mutates the catalog after this borrow ends. Unrelated tables
    // contribute no name, column, or constraint copies.
    found.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    found
        .into_iter()
        .map(|(database, table, key)| (database.to_owned(), table.to_owned(), key.clone()))
        .collect()
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

/// Whether the retained root owns a constraint for this target.
pub(crate) fn has_triggers(triggers: &[FkTriggerNode], database: &str, table: &str) -> bool {
    triggers.iter().any(|node| targets(node, database, table))
}

fn targets(node: &FkTriggerNode, database: &str, table: &str) -> bool {
    let (db, name) = if node.kind == FkTriggerKind::ChildCheck {
        (&node.child_database, &node.child_table)
    } else {
        (&node.parent_database, &node.parent_table)
    };
    db.eq_ignore_ascii_case(database) && name.eq_ignore_ascii_case(table)
}

fn planned_violation(node: &FkTriggerNode) -> DriverError {
    let table = format!("`{}`.`{}`", node.child_database, node.child_table);
    let constraint = node.constraint.clone();
    if node.kind == FkTriggerKind::ChildCheck {
        DriverError::ForeignKeyNoReferencedRow { table, constraint }
    } else {
        DriverError::ForeignKeyRowIsReferenced { table, constraint }
    }
}

/// Go FKCheckExec uses the offsets and constraints selected by the DML plan.
pub(crate) fn require_updated_child_rows(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    old: &[Vec<Datum>],
    new: &[Vec<Datum>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    check_child_row_changes(catalog, triggers, database, table, Some(old), new, ctx)
}

fn check_child_row_changes(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    old: Option<&[Vec<Datum>]>,
    rows: &[Vec<Datum>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    for node in triggers
        .iter()
        .filter(|node| node.kind == FkTriggerKind::ChildCheck && targets(node, database, table))
    {
        let child = &node.child_offsets;
        let mut wanted: Vec<Vec<Datum>> = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let Some(key) = key_at(row, child) else {
                continue;
            };
            if old.is_some_and(|old| {
                key_at(&old[index], child).is_some_and(|before| same_key(&before, &key, ctx))
            }) {
                continue;
            }
            if !wanted.contains(&key) {
                wanted.push(key);
            }
        }
        if wanted.is_empty() {
            continue;
        }
        for key in wanted {
            if lookup_handles(
                catalog,
                &node.parent_database,
                &node.parent_table,
                node.lookup_index,
                &key,
                1,
                ctx,
            )?
            .is_empty()
            {
                return Err(planned_violation(node));
            }
        }
    }
    Ok(())
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
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (_, columns) = declared(catalog, database, table);
    let Some(child) = child_offsets(&columns, foreign_key) else {
        return Ok(());
    };
    let Some((offsets, _)) = parent_offsets(catalog, foreign_key) else {
        return Ok(());
    };
    let Some(TableEntry::Kv(parent)) =
        catalog.get_in(&foreign_key.ref_schema, &foreign_key.ref_table)
    else {
        return Ok(());
    };
    let index = parent.foreign_key_lookup_index(&offsets).ok_or_else(|| {
        DriverError::ForeignKeyNoReferencedRow {
            table: format!("`{database}`.`{table}`"),
            constraint: constraint_text(foreign_key),
        }
    })?;
    let Some(TableEntry::Kv(child_table)) = catalog.get_mut_for_foreign_key(database, table) else {
        return Ok(());
    };
    let mut rows = std::sync::Arc::make_mut(child_table)
        .row_cursor_with_context(&RowDecodeContext::for_write(ctx))
        .map_err(|error| crate::driver::kv_read_error("foreign key validation failed", error))?;
    while let Some((_, row)) = rows
        .next_row()
        .map_err(|error| crate::driver::kv_read_error("foreign key validation failed", error))?
    {
        let Some(key) = key_at(&row, &child) else {
            continue;
        };
        if lookup_handles(
            catalog,
            &foreign_key.ref_schema,
            &foreign_key.ref_table,
            index,
            &key,
            1,
            ctx,
        )?
        .is_empty()
        {
            return Err(DriverError::ForeignKeyNoReferencedRow {
                table: format!("`{database}`.`{table}`"),
                constraint: constraint_text(foreign_key),
            });
        }
    }
    Ok(())
}

/// Go `FKCheckExec` on the child side, as a statement-level gate: the first
/// violating row fails the whole statement.
pub(crate) fn require_child_rows(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    rows: &[Vec<Datum>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    check_child_row_changes(catalog, triggers, database, table, None, rows, ctx)
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

/// Go `FKCascadeExec` on the parent side. UPDATE completes these actions
/// after the statement's record writes and checks, including DELETE/REPLACE. Statement staging owns
/// rollback of both parent and dependent rows if any action fails.
pub(crate) fn cascade_parent_changes(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    changes: &[ParentChange<'_>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    cascade_at_depth(catalog, triggers, database, table, changes, 0, false, ctx)
}

/// The restricting FK checks run before cascades and, under IGNORE, before
/// the candidate row is written. Consume only the retained check policies.
pub(crate) fn check_parent_changes(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    changes: &[ParentChange<'_>],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    cascade_at_depth(catalog, triggers, database, table, changes, 0, true, ctx)
}

fn cascade_at_depth(
    catalog: &mut Catalog,
    triggers: &[FkTriggerNode],
    database: &str,
    table: &str,
    changes: &[ParentChange<'_>],
    depth: usize,
    check_only: bool,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if depth > MAX_CASCADE_DEPTH {
        return Err(DriverError::ForeignKeyCascadeTooDeep);
    }
    let dependents: Vec<_> = triggers
        .iter()
        .filter(|node| node.kind != FkTriggerKind::ChildCheck && targets(node, database, table))
        .collect();
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
    for node in dependents {
        let (child_db, child_table) = (&node.child_database, &node.child_table);
        let (action, deleting) = match node.kind {
            FkTriggerKind::ChildCheck => unreachable!(),
            FkTriggerKind::ParentCheck => (
                FkAction::Restrict,
                changes
                    .iter()
                    .any(|change| matches!(change, ParentChange::Delete(_))),
            ),
            FkTriggerKind::Cascade {
                on_delete,
                set_null,
            } => (
                if set_null {
                    FkAction::SetNull
                } else {
                    FkAction::Cascade
                },
                on_delete,
            ),
        };
        if check_only != (node.kind == FkTriggerKind::ParentCheck) {
            continue;
        }
        let offsets = &node.parent_offsets;
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
                    if check_only
                        && after
                            .as_ref()
                            .is_some_and(|after| same_key(&before, after, ctx))
                    {
                        continue;
                    }
                    withdrawn.push((before, after));
                }
            }
        }
        if withdrawn.is_empty() {
            continue;
        }
        let child = &node.child_offsets;
        let mut child_rows = Vec::new();
        let mut child_handles = Vec::new();
        let mut affected: Vec<(usize, Option<Vec<Datum>>)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (key, replacement) in withdrawn {
            let handles = lookup_handles(
                catalog,
                child_db,
                child_table,
                node.lookup_index,
                &key,
                if check_only { 1 } else { usize::MAX },
                ctx,
            )?;
            if check_only && !handles.is_empty() {
                return Err(planned_violation(node));
            }
            for handle in handles {
                if !seen.insert(handle.clone()) {
                    continue;
                }
                let Some(TableEntry::Kv(kv)) =
                    catalog.get_mut_for_foreign_key(child_db, child_table)
                else {
                    return Err(DriverError::unsupported(
                        "foreign key table changed during cascade",
                    ));
                };
                let row = std::sync::Arc::make_mut(kv)
                    .get_row_by_handle_with_context(&handle, &RowDecodeContext::for_write(ctx))
                    .map_err(|error| {
                        crate::driver::kv_read_error("foreign key cascade read failed", error)
                    })?
                    .ok_or_else(|| {
                        DriverError::unsupported("foreign key index has no matching record")
                    })?;
                accountant.account_row(&row).map_err(DriverError::from)?;
                affected.push((child_rows.len(), replacement.clone()));
                child_handles.push(handle);
                child_rows.push(row);
            }
        }
        if affected.is_empty() {
            continue;
        }
        match action {
            FkAction::NoOption | FkAction::Restrict | FkAction::NoAction | FkAction::SetDefault => {
                unreachable!("a restricting action returned above")
            }
            FkAction::Cascade if deleting => {
                // ON DELETE CASCADE: the dependents go away, and so do THEIR
                // dependents -- the recursion is what makes it transitive.
                let doomed: Vec<Vec<Datum>> = affected
                    .iter()
                    .map(|(index, _)| child_rows[*index].clone())
                    .collect();
                let nested: Vec<ParentChange<'_>> =
                    doomed.iter().map(|row| ParentChange::Delete(row)).collect();
                let nested_triggers =
                    cascade_triggers(catalog, child_db, child_table, true, child, ctx)?;
                let Some(TableEntry::Kv(kv)) =
                    catalog.get_mut_for_foreign_key(child_db, child_table)
                else {
                    return Err(DriverError::unsupported(
                        "foreign key table changed during cascade",
                    ));
                };
                let kv = std::sync::Arc::make_mut(kv);
                for (index, _) in &affected {
                    kv.delete_row_with_old_context(
                        &child_handles[*index],
                        &child_rows[*index],
                        ctx,
                    )
                    .map_err(crate::driver::kv_write_error)?;
                }
                cascade_at_depth(
                    catalog,
                    &nested_triggers,
                    child_db,
                    child_table,
                    &nested,
                    depth + 1,
                    true,
                    ctx,
                )?;
                cascade_at_depth(
                    catalog,
                    &nested_triggers,
                    &child_db,
                    &child_table,
                    &nested,
                    depth + 1,
                    false,
                    ctx,
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
                    if let Some(TableEntry::Kv(kv)) = catalog.get_in(&child_db, &child_table) {
                        crate::driver::materialize_generated_for_write(
                            &kv.columns,
                            &mut new,
                            ctx,
                            crate::driver::GeneratedWrite::Update {
                                row_index: rewritten.len(),
                            },
                        )?;
                        let level = crate::bad_null::NullLevel::from_is_error(
                            ctx.strict() && !ctx.ignore_err(),
                        );
                        for (value, column) in new.iter_mut().zip(kv.columns.iter()) {
                            crate::bad_null::handle_bad_null(
                                value,
                                &column.field_type,
                                &column.name,
                                level,
                                ctx,
                            )?;
                        }
                    }
                    accountant.account_row(&new).map_err(DriverError::from)?;
                    rewritten.push((old, new));
                }
                let nested: Vec<ParentChange<'_>> = rewritten
                    .iter()
                    .map(|(old, new)| ParentChange::Update { old, new })
                    .collect();
                let nested_triggers =
                    cascade_triggers(catalog, child_db, child_table, false, child, ctx)?;
                let Some(TableEntry::Kv(kv)) =
                    catalog.get_mut_for_foreign_key(child_db, child_table)
                else {
                    return Err(DriverError::unsupported(
                        "foreign key table changed during cascade",
                    ));
                };
                let kv = std::sync::Arc::make_mut(kv);
                for ((index, _), (old, new)) in affected.iter().zip(&rewritten) {
                    kv.update_row_with_old_context(&child_handles[*index], Some(old), new, ctx)
                        .map_err(crate::driver::kv_write_error)?;
                }
                let old: Vec<_> = rewritten.iter().map(|(old, _)| old.clone()).collect();
                let new: Vec<_> = rewritten.iter().map(|(_, new)| new.clone()).collect();
                require_updated_child_rows(
                    catalog,
                    &nested_triggers,
                    child_db,
                    child_table,
                    &old,
                    &new,
                    ctx,
                )?;
                cascade_at_depth(
                    catalog,
                    &nested_triggers,
                    child_db,
                    child_table,
                    &nested,
                    depth + 1,
                    true,
                    ctx,
                )?;
                cascade_at_depth(
                    catalog,
                    &nested_triggers,
                    &child_db,
                    &child_table,
                    &nested,
                    depth + 1,
                    false,
                    ctx,
                )?;
            }
        }
    }
    Ok(())
}

/// Cascades build a dependent statement in Go; resolve that statement's
/// policy through the same builder rather than rediscovering constraints in
/// the row callbacks.
fn cascade_triggers(
    catalog: &Catalog,
    database: &str,
    table: &str,
    deleting: bool,
    columns: &[usize],
    ctx: &crate::StmtContext,
) -> Result<Vec<FkTriggerNode>, DriverError> {
    let names = catalog
        .get_in(database, table)
        .map(TableEntry::column_names)
        .unwrap_or_default();
    let spec = crate::driver::fk_trigger_plan::FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: columns
            .iter()
            .filter_map(|offset| names.get(*offset).cloned())
            .collect(),
        ..Default::default()
    };
    crate::driver::fk_trigger_plan::build_fk_triggers(
        catalog,
        ctx,
        if deleting { "Delete" } else { "Update" },
        &spec,
        &tidb_planner::plan_base::PlanIdAllocator::new(),
    )
}

/// Whether a table takes part in any foreign key, as the declaring child or
/// as the referenced parent.
///
/// A constraint stores BOTH sides as names now (Go `FKInfo.Cols` and
/// `FKInfo.RefTable`), so repositioning a column no longer moves the
/// constraint off its columns -- `KvTable::foreign_key_offsets` resolves the
/// names against the current column list at every use. `RENAME TABLE` is
/// handled by [`rewrite_table_references`], which follows Go's metadata
/// rewrite over every child and the moved table itself. `DROP TABLE` still
/// refuses a participating parent before removal, while a column RENAME
/// rewrites both sides through [`rewrite_column_name`].
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

/// Rewrites every foreign key that names `from_database.from_table` as its
/// parent, including a self-reference on the table being moved. This is Go's
/// `updateFKInfoWhenRenameTable` metadata maintenance, performed before the
/// catalog key is moved so the source table is included in the same pass.
pub(crate) fn rewrite_table_references(
    catalog: &mut Catalog,
    from_database: &str,
    from_table: &str,
    to_database: &str,
    to_table: &str,
) {
    if !crate::ddl::rename_changes_fk_reference(from_table, to_table) {
        return;
    }
    let tables: Vec<(String, String)> = catalog
        .database_names()
        .into_iter()
        .flat_map(|database| {
            catalog
                .table_names(&database)
                .unwrap_or_default()
                .into_iter()
                .map(move |table| (database.clone(), table))
        })
        .collect();
    for (database, table) in tables {
        let Some(TableEntry::Kv(table)) = catalog.table_mut_in(&database, &table) else {
            continue;
        };
        for foreign_key in std::sync::Arc::make_mut(table).foreign_keys_mut() {
            if foreign_key.ref_schema.eq_ignore_ascii_case(from_database)
                && foreign_key.ref_table.eq_ignore_ascii_case(from_table)
            {
                foreign_key.ref_schema = to_database.to_owned();
                foreign_key.ref_table = to_table.to_owned();
            }
        }
    }
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
            // error before it can compare the two column types.  This matters
            // for unchecked, deferred foreign keys: a CHANGE COLUMN against
            // a parent that has not landed yet is 1146, not an accepted local
            // rename (or a later generic type error).
            let Some(parent) = catalog.get_in(&foreign_key.ref_schema, &foreign_key.ref_table)
            else {
                return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                    format!("{}.{}", foreign_key.ref_schema, foreign_key.ref_table),
                )));
            };
            let Some(refer) = column_type(
                catalog,
                &foreign_key.ref_schema,
                &foreign_key.ref_table,
                referenced,
            ) else {
                // Keep the same infoschema lookup order as Go: once the
                // parent exists, a stale referenced column is reported as
                // 1054 rather than silently skipping this constraint.
                if matches!(parent, TableEntry::Kv(_)) {
                    return Err(DriverError::UnknownColumnInTable {
                        column: referenced.clone(),
                        table: foreign_key.ref_table.clone(),
                    });
                }
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
                        child_db.go_to_lower(),
                        child_table.go_to_lower()
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
        for foreign_key in std::sync::Arc::make_mut(kv).foreign_keys_mut() {
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
        for foreign_key in std::sync::Arc::make_mut(kv).foreign_keys_mut() {
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
/// * A single constrained column that IS the clustered primary key needs no
///   index of its own (`tbInfo.PKIsHandle && len(cols) == 1`). This applies to
///   both the table's declared (CHILD) constraints and the constraints that
///   REFER to it (PARENT), exactly as Go's shared `checkFn` does.
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
                && kv.partial_index_safe_for_columns(index, offsets)
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
        if covers(&child)
            && kv.partial_index_safe_for_columns(dropping, &child)
            && !remaining_covers(&child)
            && !(child.len() == 1 && kv.is_clustered_handle_column(child[0]))
        {
            return Err(refused());
        }
    }
    // The constraints that REFER here: the referenced columns, resolved into
    // this table's offsets.
    for (_, _, foreign_key) in referring(catalog, database, table) {
        let Some((offsets, _)) = parent_offsets(catalog, &foreign_key) else {
            continue;
        };
        if !covers(&offsets) || !kv.partial_index_safe_for_columns(dropping, &offsets) {
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
/// dropped while a table OUTSIDE this statement still references it. Unlike
/// TRUNCATE, DROP uses the dedicated 3730 `ErrForeignKeyCannotDrop` diagnostic.
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
            return Err(DriverError::ForeignKeyTableCannotDrop {
                parent_table: table.clone(),
                constraint: foreign_key.name,
                child_table,
            });
        }
    }
    Ok(())
}

/// Go `checkTruncateTableHasForeignKeyReferredInOwner` raises
/// `ErrTruncateIllegalForeignKey` (1701), rather than the row-level 1451 used
/// by DELETE/UPDATE. `detail` is the child-side text Go places inside the
/// parentheses. DROP TABLE uses the dedicated 3730 variant above.
pub(crate) fn table_referenced(
    child_db: &str,
    child_table: &str,
    foreign_key: &KvForeignKey,
) -> DriverError {
    DriverError::ForeignKeyTableReferenced {
        detail: format!(
            "`{child_db}`.`{child_table}` CONSTRAINT `{}`",
            foreign_key.name
        ),
    }
}

/// Finds the first child outside `ignored` that still references a parent.
/// The caller supplies the ignored set because TRUNCATE treats a self-
/// reference as safe, while DROP TABLE uses the complete statement list.
/// Go `checkTableHasForeignKeyReferred` (`pkg/ddl/ttl.go:100-102`) boolean
/// form: whether ANY table declares a foreign key referencing this one. The
/// TTL config refuses to be added to such a parent.
pub(crate) fn is_table_referred(catalog: &Catalog, database: &str, table: &str) -> bool {
    !referring(catalog, database, table).is_empty()
}

pub(crate) fn find_table_referred(
    catalog: &Catalog,
    database: &str,
    table: &str,
    ignored: &[(String, String)],
) -> Option<DriverError> {
    referring(catalog, database, table)
        .into_iter()
        .find(|(child_db, child_table, _)| {
            !ignored.iter().any(|(db, name)| {
                db.eq_ignore_ascii_case(child_db) && name.eq_ignore_ascii_case(child_table)
            })
        })
        .map(|(child_db, child_table, foreign_key)| {
            table_referenced(&child_db, &child_table, &foreign_key)
        })
}

/// Go `checkDatabaseHasForeignKeyReferred`: before removing a schema, find a
/// parent table in it whose child lives outside the schema. Children in the
/// same DROP DATABASE statement are ignored because they disappear together.
pub fn find_database_referred(catalog: &Catalog, database: &str) -> Option<DriverError> {
    let target_tables: Vec<String> = catalog
        .table_paths()
        .into_iter()
        .filter(|(db, _)| db.eq_ignore_ascii_case(database))
        .map(|(_, table)| table)
        .collect();
    for parent_table in target_tables {
        if let Some((_, child_table, foreign_key)) = referring(catalog, database, &parent_table)
            .into_iter()
            .find(|(child_db, _, _)| !child_db.eq_ignore_ascii_case(database))
        {
            return Some(DriverError::ForeignKeyDatabaseReferenced {
                parent_table,
                constraint: foreign_key.name,
                child_table,
            });
        }
    }
    None
}
