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

use crate::KvTableReader;
use tidb_datatype::Datum;

use crate::driver::{Catalog, DriverError, TableEntry};
use crate::kv_table::{FkAction, KvForeignKey, RowDecodeContext, TableHandle};
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_planner::physical::{FkTriggerKind, FkTriggerNode};

/// MySQL's `FK_MAX_CASCADE_DEL`: the deepest a cascade may recurse before Go
/// raises `ErrFkExceedMaxDepth` (3008).
const MAX_CASCADE_DEPTH: usize = 15;

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
            kv.columns().iter().map(|c| c.name.clone()).collect(),
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
            if foreign_key.version >= tidb_model::table::FK_VERSION1
                && foreign_key.ref_schema.go_lower_eq(database)
                && foreign_key.ref_table.go_lower_eq(table)
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
        .map(|name| names.iter().position(|column| column.go_lower_eq(name)))
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
        offsets.push(names.iter().position(|column| column.go_lower_eq(name))?);
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
    db.go_lower_eq(database) && name.go_lower_eq(table)
}

/// Go `FKCheck.FailedErr`, built when the check was planned.
fn planned_violation(node: &FkTriggerNode) -> DriverError {
    if node.kind == FkTriggerKind::ChildCheck {
        DriverError::from(
            tidb_error::plannererrors::ERR_NO_REFERENCED_ROW2
                .fast_generate_by_args(&[node.fk_string.clone().into()]),
        )
    } else {
        DriverError::from(
            tidb_error::plannererrors::ERR_ROW_IS_REFERENCED2
                .generate_with_stack_by_args(&[node.fk_string.clone().into()]),
        )
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
            // Go `FKCheckExec.addRowNeedToCheck` appends every row's key; a
            // linear de-duplication here made a large INSERT quadratic.
            wanted.push(key);
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

/// Go `checkForeignKeyConstrain` for a foreign key named `name` that
/// `database`.`table` in `catalog` already carries: the production node's
/// DDL transaction builds `catalog` from the child and its parent over the
/// transaction's storage and checks the child's existing rows here.
///
/// # Errors
///
/// 1452 naming the constraint, or a read failure.
pub fn check_foreign_key_constrain(
    catalog: &mut Catalog,
    database: &str,
    table: &str,
    name: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(child)) = catalog.table_in(database, table) else {
        return Ok(());
    };
    let Some(foreign_key) = child
        .foreign_keys()
        .iter()
        .find(|key| key.name.go_lower_eq(name))
        .cloned()
    else {
        return Ok(());
    };
    require_existing_rows(catalog, database, table, &foreign_key, ctx)
}

/// Go `checkForeignKeyConstrain`'s failure:
/// `dbterror.ErrNoReferencedRow2.GenWithStackByArgs(fkInfo.String(schema, table))`,
/// over `job.SchemaName` and `tblInfo.Name.L`.
fn no_referenced_row(database: &str, table: &str, foreign_key: &KvForeignKey) -> DriverError {
    let fk =
        crate::driver::fk_trigger_plan::meta(foreign_key).string(database, &table.go_to_lower());
    DriverError::from(
        tidb_util::dbterror::ERR_NO_REFERENCED_ROW2.generate_with_stack_by_args(&[fk.into()]),
    )
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
    let index = parent
        .foreign_key_lookup_index(&offsets)
        .ok_or_else(|| no_referenced_row(database, table, foreign_key))?;
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
            return Err(no_referenced_row(database, table, foreign_key));
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
        return Err(DriverError::from(
            tidb_util::dbterror::exeerrors::ERR_FOREIGN_KEY_CASCADE_DEPTH_EXCEEDED
                .generate_with_stack_by_args(&[crate::foreign_key::MAX_CASCADE_DEPTH.into()]),
        ));
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
        // Go `FKCascadeExec.buildFKCascadePlan` builds the child `DELETE`
        // or `UPDATE` once the parent withdrew a key, and
        // `OptimizeForForeignKeyCascade` checks its table locks.
        if let Some(locked_tables) = ctx.locked_tables() {
            check_fk_cascade_table_lock(
                locked_tables,
                catalog,
                child_db,
                child_table,
                deleting && action == FkAction::Cascade,
            )?;
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
                            kv,
                            &mut new,
                            ctx,
                            crate::driver::GeneratedWrite::Update {
                                row_index: rewritten.len(),
                            },
                            None,
                        )?;
                        let level = crate::bad_null::NullLevel::from_is_error(
                            ctx.strict() && !ctx.ignore_err(),
                        );
                        for (value, column) in new.iter_mut().zip(kv.columns().iter()) {
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

/// Go `OptimizeForForeignKeyCascade`'s `CheckTableLock` over the cascade
/// sub-statement it builds: the child `DELETE` (`delete`) or `UPDATE` reads
/// the child table and then writes it, in `buildDelete`/`buildUpdate`'s
/// visit order.
fn check_fk_cascade_table_lock(
    locked_tables: &std::collections::BTreeMap<i64, tidb_model::TableLockTpInfo>,
    catalog: &Catalog,
    db: &str,
    table: &str,
    delete: bool,
) -> Result<(), DriverError> {
    if locked_tables.is_empty() && !catalog.has_table_locks() {
        return Ok(());
    }
    let write = if delete {
        tidb_mysql::DeletePriv
    } else {
        tidb_mysql::UpdatePriv
    };
    crate::lock::check_visits(
        &crate::lock::Checker::new(locked_tables, catalog),
        &[(db, table, tidb_mysql::SelectPriv), (db, table, write)],
    )
}
